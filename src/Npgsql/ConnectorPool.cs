using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Npgsql.Logging;
using Npgsql.Util;

namespace Npgsql
{
    /// <summary>
    /// Connection pool for PostgreSQL physical connections. Attempts to allocate connections over MaxPoolSize will
    /// block until someone releases. Implementation is completely lock-free to avoid contention, and ensure FIFO
    /// for open attempts waiting (because the pool is at capacity).
    /// </summary>
    sealed class ConnectorPool : IDisposable
    {
        #region Implementation notes

        // General
        //
        // * When we're at capacity (Busy==Max) further open attempts wait until someone releases.
        //   This must happen in FIFO (first to block on open is the first to release), otherwise some attempts may get
        //   starved and time out. This is why we use a ConcurrentQueue.
        // * We must avoid a race condition whereby an open attempt starts waiting at the same time as another release
        //   puts a connector back into the idle list. This would potentially make the waiter wait forever/time out.
        //
        // Rules
        // * You *only* create a new connector if Total < Max.
        // * You *only* go into waiting if Busy == Max (which also implies Idle == 0)

        #endregion Implementation notes

        #region Fields

        internal NpgsqlConnectionStringBuilder Settings { get; }

        /// <summary>
        /// Contains the connection string returned to the user from <see cref="NpgsqlConnection.ConnectionString"/>
        /// after the connection has been opened. Does not contain the password unless Persist Security Info=true.
        /// </summary>
        internal string UserFacingConnectionString { get; }

        readonly int _max;
        readonly int _min;

        readonly NpgsqlConnector?[] _idle;
        readonly NpgsqlConnector?[] _open; // TODO could be a 'bitmap' whether the index has a connector or not

        readonly ConcurrentQueue<(TaskCompletionSource<NpgsqlConnector?> TaskCompletionSource, bool IsAsync)> _waiting;

        [StructLayout(LayoutKind.Explicit)]
        internal struct PoolState
        {
            [FieldOffset(0)]
            internal int Idle;
            [FieldOffset(4)]
            internal int Busy;
            [FieldOffset(0)]
            internal long All;

            [FieldOffset(8)]
            internal int Open;

            internal PoolState Copy() => new PoolState { All = Volatile.Read(ref All) };

            public override string ToString()
                => $"[{Open} total, {Idle} idle, {Busy} busy]";
        }

        internal PoolState State;

        /// <summary>
        /// Incremented every time this pool is cleared via <see cref="NpgsqlConnection.ClearPool"/> or
        /// <see cref="NpgsqlConnection.ClearAllPools"/>. Allows us to identify connections which were
        /// created before the clear.
        /// </summary>
        int _clearCounter;

        static readonly TimerCallback PruningTimerCallback = PruneIdleConnectors;
        Timer? _pruningTimer;
        readonly TimeSpan _pruningInterval;

        /// <summary>
        /// Maximum number of possible connections in any pool.
        /// </summary>
        internal const int PoolSizeLimit = 1024;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(ConnectorPool));

        #endregion

        internal ConnectorPool(NpgsqlConnectionStringBuilder settings, string connString)
        {
            Debug.Assert(PoolSizeLimit <= short.MaxValue,
                "PoolSizeLimit cannot be larger than short.MaxValue unless PoolState is refactored to hold larger values.");

            if (settings.MaxPoolSize < settings.MinPoolSize)
                throw new ArgumentException($"Connection can't have MaxPoolSize {settings.MaxPoolSize} under MinPoolSize {settings.MinPoolSize}");

            Settings = settings;

            _max = settings.MaxPoolSize;
            _min = settings.MinPoolSize;

            UserFacingConnectionString = settings.PersistSecurityInfo
                ? connString
                : settings.ToStringWithoutPassword();

            _pruningInterval = TimeSpan.FromSeconds(Settings.ConnectionPruningInterval);
            _idle = new NpgsqlConnector[_max];
            _open = new NpgsqlConnector[_max];
            _waiting = new ConcurrentQueue<(TaskCompletionSource<NpgsqlConnector?> TaskCompletionSource, bool IsAsync)>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryAllocateFast(NpgsqlConnection conn, [NotNullWhen(true)] out NpgsqlConnector? connector)
        {
            Counters.SoftConnectsPerSecond.Increment();

            // We start scanning for an idle connector in "random" places in the array, to avoid
            // too much interlocked operations "contention" at the beginning.
            var start = Thread.CurrentThread.ManagedThreadId % _max;

            // Idle may indicate that there are idle connectors, with the subsequent scan failing to find any.
            // This can happen because of race conditions with Release(), which updates Idle before actually putting
            // the connector in the list, or because of other allocation attempts, which remove the connector from
            // the idle list before updating Idle.
            // Loop until either State.Idle is 0 or you manage to remove a connector.
            connector = null;
            while (Volatile.Read(ref State.Idle) > 0)
            {
                for (var i = start; connector == null && i < _max; i++)
                {
                    // First check without an Interlocked operation, it's faster
                    if (_idle[i] == null)
                        continue;

                    // If we saw a connector in this slot, atomically exchange it with a null.
                    // Either we get a connector out which we can use, or we get null because
                    // someone has taken it in the meanwhile. Either way put a null in its place.
                    connector = Interlocked.Exchange(ref _idle[i], null);
                }

                for (var i = 0; connector == null && i < start; i++)
                {
                    // Same as above
                    if (_idle[i] == null)
                        continue;
                    connector = Interlocked.Exchange(ref _idle[i], null);
                }

                if (connector == null)
                    return false;

                Counters.NumberOfFreeConnections.Decrement();

                // An connector could be broken because of a keepalive that occurred while it was
                // idling in the pool
                // TODO: Consider removing the pool from the keepalive code. The following branch is simply irrelevant
                // if keepalive isn't turned on.
                if (connector.IsBroken)
                {
                    CloseConnector(connector, true);
                    continue;
                }

                connector.Connection = conn;

                // We successfully extracted an idle connector, update state
                Counters.NumberOfActiveConnections.Increment();
                Interlocked.Increment(ref State.Busy);
                Interlocked.Decrement(ref State.Idle);
                CheckInvariants(State);
                return true;
            }

            connector = null;
            return false;
        }

        internal async ValueTask<NpgsqlConnector> AllocateLong(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken)
        {
            // No idle connector was found in the pool.
            // We now loop until one of three things happen:
            // 1. The pool isn't at max capacity (Total < Max), so we can create a new physical connection.
            // 2. The pool is at maximum capacity and there are no idle connectors (Busy == Max),
            // so we enqueue an open attempt into the waiting queue, so that the next release will unblock it.
            // 3. An connector makes it into the idle list (race condition with another Release().
            while (true)
            {
                NpgsqlConnector? connector;
                var openCount = Volatile.Read(ref State.Open);

                if (openCount < _max)
                {
                    var newOpenCount = openCount + 1;
                    // We're under the pool's max capacity, try to "allocate" a slot for a new physical connection.
                    if (Interlocked.CompareExchange(ref State.Open, newOpenCount, openCount) != openCount)
                    {
                        // Our attempt to increment the open counter failed, loop again and retry.
                        continue;
                    }

                    try
                    {
                        // We've managed to increase the open counter, open a physical connections.
                        connector = new NpgsqlConnector(conn) { ClearCounter = _clearCounter };
                        await connector.Open(timeout, async, cancellationToken);

                        // We immediately store the connector as well, assigning it an index
                        // that will be used during the lifetime of the connector for both _idle and _all.
                        for (var i = 0; i < _open.Length; i++)
                        {
                            if (Interlocked.CompareExchange(ref _open[i], connector, null) == null)
                            {
                                connector.PoolIndex = i;
                                break;
                            }
                        }

                        // Start the pruning timer if we're above MinPoolSize
                        if (_pruningTimer == null && openCount > _min)
                        {
                            var newPruningTimer = new Timer(PruningTimerCallback, this, -1, -1);
                            if (Interlocked.CompareExchange(ref _pruningTimer, newPruningTimer, null) == null)
                                newPruningTimer.Change(_pruningInterval, _pruningInterval);
                            else
                            {
                                // Someone beat us to it
                                newPruningTimer.Dispose();
                            }
                        }
                    }
                    catch
                    {
                        // Physical open failed, decrement the open counter back down.
                        conn.Connector = null;
                        Interlocked.Decrement(ref State.Open);
                        throw;
                    }

                    Counters.NumberOfPooledConnections.Increment();
                    Counters.NumberOfActiveConnections.Increment();
                    Interlocked.Increment(ref State.Busy);
                    CheckInvariants(State);
                    return connector;
                }

                if (Volatile.Read(ref State.Busy) == _max)
                {
                    // Scenario: pre-empted waiter
                    // here

                    // Pool is exhausted.
                    // Enqueue an allocate attempt into the waiting queue so that the next release will unblock us.
                    var tcs = new TaskCompletionSource<NpgsqlConnector?>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _waiting.Enqueue((tcs, async));

                    // Scenario: pre-empted waiter
                    // By here we are visible to any releases that start, however we may have been pre-empted
                    // right before we could signal to a running release we were going into the wait queue.
                    // We do a correction for that right here after our own enqueue by re-checking Busy.
                    if (Volatile.Read(ref State.Busy) < _max)
                    {
                        // If setting this fails we have been raced to completion by a Release().
                        // Otherwise we have an idle connector to try and race to.
                        if (!tcs.TrySetCanceled())
                        {
                            connector = tcs.Task.Result;

                            // Our task completion may contain a null in order to unblock us, allowing us to try
                            // allocating again.
                            if (connector == null)
                                continue;

                            // Note that we don't update counters or any state since the connector is being
                            // handed off from one open connection to another.
                            connector.Connection = conn;
                            return connector;
                        }

                        continue;
                    }

                    try
                    {
                        if (async)
                        {
                            if (timeout.IsSet)
                            {
                                // Use Task.Delay to implement the timeout, but cancel the timer if we actually
                                // do complete successfully
                                var delayCancellationToken = new CancellationTokenSource();
                                using (cancellationToken.Register(s => ((CancellationTokenSource)s!).Cancel(),
                                    delayCancellationToken))
                                {
                                    var timeLeft = timeout.TimeLeft;
                                    if (timeLeft <= TimeSpan.Zero ||
                                        await Task.WhenAny(tcs.Task,
                                            Task.Delay(timeLeft, delayCancellationToken.Token)) != tcs.Task)
                                    {
                                        // Delay task completed first, either because of a user cancellation or an actual timeout
                                        cancellationToken.ThrowIfCancellationRequested();
                                        throw new NpgsqlException(
                                            $"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {Settings.Timeout} seconds)");
                                    }
                                }

                                delayCancellationToken.Cancel();
                            }
                            else
                            {
                                using (cancellationToken.Register(
                                    s => ((TaskCompletionSource<NpgsqlConnector?>)s).SetCanceled(), tcs))
                                    await tcs.Task;
                            }
                        }
                        else
                        {
                            if (timeout.IsSet)
                            {
                                var timeLeft = timeout.TimeLeft;
                                if (timeLeft <= TimeSpan.Zero || !tcs.Task.Wait(timeLeft))
                                    throw new NpgsqlException(
                                        $"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {Settings.Timeout} seconds)");
                            }
                            else
                                tcs.Task.Wait();
                        }
                    }
                    catch
                    {
                        // We're here if the timeout expired or the cancellation token was triggered.
                        // Transition our Task to cancelled, so that the next time someone releases
                        // a connection they'll skip over it.
                        tcs.TrySetCanceled();

                        // There's still a chance of a race condition, whereby the task was transitioned to
                        // completed in the meantime.
                        if (tcs.Task.Status != TaskStatus.RanToCompletion)
                            throw;
                    }

                    // Note that we don't update counters since the connector is being
                    // handed off from one open connection to another.
                    Debug.Assert(tcs.Task.IsCompleted);
                    connector = tcs.Task.Result;

                    if (connector == null)
                        continue;

                    connector.Connection = conn;

                    return connector;
                }

                // We didn't create a new connector or start waiting, which means there's a new idle connector, try
                // getting it
                Debug.Assert(Volatile.Read(ref State.Idle) > 0);
                if (TryAllocateFast(conn, out connector))
                    return connector;
            }

            // Cannot be here
        }

        internal void Release(NpgsqlConnector connector)
        {
            Counters.SoftDisconnectsPerSecond.Increment();
            Counters.NumberOfActiveConnections.Decrement();

            // If Clear/ClearAll has been been called since this connector was first opened,
            // throw it away. The same if it's broken (in which case CloseConnector is only
            // used to update state/perf counter).
            if (connector.ClearCounter < _clearCounter || connector.IsBroken)
            {
                CloseConnector(connector, false);
                return;
            }

            connector.Reset();

            // If there are any pending waiters we hand the connector off to them directly.
            while (_waiting.TryDequeue(out var waitingOpenAttempt))
            {
                var tcs = waitingOpenAttempt.TaskCompletionSource;

                // We have a pending waiter. "Complete" it, handing off the connector.
                if (tcs.TrySetResult(connector))
                    return;

                // If the open attempt timed out, the Task's state will be set to Canceled and our
                // TrySetResult fails. Try again.
                Debug.Assert(tcs.Task.IsCanceled);
            }

            // Scenario: pre-empted release
            // Right here between our check for waiters and our signalling decrement for storing
            // a connector there could have been an awaiter enqueueing, we compensate at the end.

            // If we're here, we put the connector back in the idle list
            // We decrement Busy, any allocate that is racing us will not match Busy == _max
            // and will not enqueue but try to get our connector.
            Interlocked.Decrement(ref State.Busy);
            connector.ReleaseTimestamp = DateTime.UtcNow;
            _idle[connector.PoolIndex] = connector;
            Interlocked.Increment(ref State.Idle);
            CheckInvariants(State);

            // Scenario: pre-empted release
            // Unblock any potential waiter that raced us by handing it a null result.
            // We try to complete exactly one waiter as long as there are any the queue.
            while(_waiting.TryDequeue(out var racedWaiter))
            {
                if (racedWaiter.TaskCompletionSource.TrySetResult(null))
                    break;
            }

            // Scenario: pre-empted waiter
            // Could have a pre-empted waiter, that didn't enqueue yet waking up right after
            // our correcting dequeue, it will do a check itself after enqueue for Busy < _max.
        }

        void CloseConnector(NpgsqlConnector connector, bool wasIdle)
        {
            try
            {
                connector.Close();

                // REVIEW: Do we want this?
                // Keep these decrements inside the try, if a connector fails to close we want to respect
                // the actual maximum connections to the postgres server, not pretend we actually freed one.
                if (wasIdle)
                    Interlocked.Decrement(ref State.Idle);
                else
                    Interlocked.Decrement(ref State.Busy);

                // We don't need to interlock for clearing open as this slot isn't ever written to concurrently.
                _open[connector.PoolIndex] = null;

                Interlocked.Decrement(ref State.Open);
                CheckInvariants(State);

                Counters.NumberOfPooledConnections.Decrement();
            }
            catch (Exception e)
            {
                Log.Warn("Exception while closing outdated connector", e, connector.Id);
            }

            while (_pruningTimer != null && Volatile.Read(ref State.Open) <= _min)
            {
                var oldTimer = _pruningTimer;
                if (Interlocked.CompareExchange(ref _pruningTimer, null, oldTimer) == oldTimer)
                {
                    oldTimer.Dispose();
                    break;
                }
            }
        }

        static void PruneIdleConnectors(object? state)
        {
            var pool = (ConnectorPool)state!;
            var idle = pool._idle;
            var now = DateTime.UtcNow;
            var idleLifetime = pool.Settings.ConnectionIdleLifetime;

            for (var i = 0; i < idle.Length; i++)
            {
                if (Volatile.Read(ref pool.State.Open) <= pool._min)
                    return;

                var connector = idle[i];
                if (connector == null || (now - connector.ReleaseTimestamp).TotalSeconds < idleLifetime)
                    continue;
                if (Interlocked.CompareExchange(ref idle[i], null, connector) == connector)
                    pool.CloseConnector(connector, true);
            }
        }

        internal void Clear()
        {
            for (var i = 0; i < _idle.Length; i++)
            {
                var connector = Interlocked.Exchange(ref _idle[i], null);
                if (connector != null)
                    CloseConnector(connector, true);
            }

            _clearCounter++;
        }

        #region Pending Enlisted Connections

        internal void AddPendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    list = _pendingEnlistedConnectors[transaction] = new List<NpgsqlConnector>();
                list.Add(connector);
            }
        }

        internal void TryRemovePendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    return;
                list.Remove(connector);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
            }
        }

        internal NpgsqlConnector? TryAllocateEnlistedPending(Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    return null;
                var connector = list[list.Count - 1];
                list.RemoveAt(list.Count - 1);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
                return connector;
            }
        }

        // Note that while the dictionary is thread-safe, we assume that the lists it contains don't need to be
        // (i.e. access to connectors of a specific transaction won't be concurrent)
        readonly Dictionary<Transaction, List<NpgsqlConnector>> _pendingEnlistedConnectors
            = new Dictionary<Transaction, List<NpgsqlConnector>>();

        #endregion

        #region Misc

        [Conditional("DEBUG")]
        void CheckInvariants(PoolState state)
        {
            if (state.Open > _max)
                throw new NpgsqlException($"Pool is over capacity (Total={state.Open}, Max={_max})");
            if (state.Open < 0)
                throw new NpgsqlException("Open is negative");
            if (state.Idle < 0)
                throw new NpgsqlException("Idle is negative");
            if (state.Busy < 0)
                throw new NpgsqlException("Busy is negative");
        }

        public void Dispose() => _pruningTimer?.Dispose();

        public override string ToString() => State.Copy().ToString();

        #endregion Misc
    }
}
