using System.Collections.Concurrent;
using System.Diagnostics;

namespace TxtDb.Storage.Services;

/// <summary>
/// DeadlockAwareLockManager - Implements timeout-based deadlock detection and prevention
/// Provides transaction-aware locking with configurable timeouts to prevent deadlocks
/// </summary>
public class DeadlockAwareLockManager
{
    private readonly ConcurrentDictionary<string, ResourceLock> _resourceLocks = new();
    private readonly ConcurrentDictionary<long, HashSet<string>> _transactionLocks = new();
    private readonly object _lockTableLock = new object();
    private readonly int _deadlockTimeoutMs;

    public DeadlockAwareLockManager(int deadlockTimeoutMs = 30000)
    {
        _deadlockTimeoutMs = deadlockTimeoutMs;
    }

    /// <summary>
    /// Acquires a lock on a resource for the specified transaction
    /// Throws TimeoutException if deadlock is detected (timeout exceeded)
    /// </summary>
    public void AcquireLock(long transactionId, string resourceId)
    {
        if (_deadlockTimeoutMs <= 0)
        {
            // Deadlock detection disabled - use simple locking
            AcquireLockInternal(transactionId, resourceId, Timeout.Infinite);
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            AcquireLockInternal(transactionId, resourceId, _deadlockTimeoutMs);
            
            // Track this lock for the transaction
            lock (_lockTableLock)
            {
                if (!_transactionLocks.ContainsKey(transactionId))
                {
                    _transactionLocks[transactionId] = new HashSet<string>();
                }
                _transactionLocks[transactionId].Add(resourceId);
            }
        }
        catch (TimeoutException)
        {
            stopwatch.Stop();
            
            // CRITICAL: Release any locks this transaction may have acquired before timing out
            ReleaseLocks(transactionId);
            
            throw new TimeoutException(
                $"Deadlock detected: Transaction {transactionId} timed out waiting for lock on '{resourceId}' " +
                $"after {stopwatch.ElapsedMilliseconds}ms (timeout: {_deadlockTimeoutMs}ms). " +
                $"This indicates a potential deadlock situation.");
        }
    }

    /// <summary>
    /// Releases all locks held by the specified transaction
    /// </summary>
    public void ReleaseLocks(long transactionId)
    {
        lock (_lockTableLock)
        {
            if (_transactionLocks.TryRemove(transactionId, out var lockedResources))
            {
                foreach (var resourceId in lockedResources)
                {
                    if (_resourceLocks.TryGetValue(resourceId, out var resourceLock))
                    {
                        resourceLock.Release(transactionId);
                        
                        // Clean up empty resource locks
                        if (!resourceLock.IsHeld())
                        {
                            _resourceLocks.TryRemove(resourceId, out _);
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// Gets diagnostic information about current locks
    /// </summary>
    public LockDiagnostics GetDiagnostics()
    {
        lock (_lockTableLock)
        {
            return new LockDiagnostics
            {
                ActiveResourceLocks = _resourceLocks.Count,
                ActiveTransactions = _transactionLocks.Count,
                DeadlockTimeoutMs = _deadlockTimeoutMs,
                ResourceDetails = _resourceLocks.ToDictionary(
                    kvp => kvp.Key,
                    kvp => new ResourceLockInfo
                    {
                        IsLocked = kvp.Value.IsHeld(),
                        HolderTransactionId = kvp.Value.GetHolder(),
                        WaitingCount = kvp.Value.GetWaitingCount()
                    }
                )
            };
        }
    }

    private void AcquireLockInternal(long transactionId, string resourceId, int timeoutMs)
    {
        var resourceLock = _resourceLocks.GetOrAdd(resourceId, _ => new ResourceLock(resourceId));
        
        if (!resourceLock.TryAcquire(transactionId, timeoutMs))
        {
            throw new TimeoutException(
                $"Failed to acquire lock on '{resourceId}' for transaction {transactionId} within {timeoutMs}ms timeout");
        }
    }

    /// <summary>
    /// Represents a lock on a specific resource
    /// </summary>
    private class ResourceLock
    {
        private readonly string _resourceId;
        private readonly SemaphoreSlim _semaphore = new(1, 1);
        private long _holderTransactionId;
        private volatile bool _isHeld;
        private readonly object _stateLock = new object();

        public ResourceLock(string resourceId)
        {
            _resourceId = resourceId;
        }

        public bool TryAcquire(long transactionId, int timeoutMs)
        {
            // Check if this transaction already holds the lock (reentrant)
            lock (_stateLock)
            {
                if (_isHeld && _holderTransactionId == transactionId)
                {
                    return true; // Reentrant lock
                }
            }

            var acquired = _semaphore.Wait(timeoutMs);
            if (acquired)
            {
                lock (_stateLock)
                {
                    _holderTransactionId = transactionId;
                    _isHeld = true;
                }
            }
            
            return acquired;
        }

        public void Release(long transactionId)
        {
            lock (_stateLock)
            {
                if (_isHeld && _holderTransactionId == transactionId)
                {
                    _isHeld = false;
                    _holderTransactionId = 0;
                    _semaphore.Release();
                }
            }
        }

        public bool IsHeld()
        {
            lock (_stateLock)
            {
                return _isHeld;
            }
        }

        public long GetHolder()
        {
            lock (_stateLock)
            {
                return _isHeld ? _holderTransactionId : 0;
            }
        }

        public int GetWaitingCount()
        {
            return _semaphore.CurrentCount == 0 ? 1 : 0; // Approximation
        }
    }
}

/// <summary>
/// Diagnostic information about the current state of locks
/// </summary>
public class LockDiagnostics
{
    public int ActiveResourceLocks { get; set; }
    public int ActiveTransactions { get; set; }
    public int DeadlockTimeoutMs { get; set; }
    public Dictionary<string, ResourceLockInfo> ResourceDetails { get; set; } = new();
}

/// <summary>
/// Information about a specific resource lock
/// </summary>
public class ResourceLockInfo
{
    public bool IsLocked { get; set; }
    public long HolderTransactionId { get; set; }
    public int WaitingCount { get; set; }
}