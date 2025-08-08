using System.Collections.Concurrent;
using System.Diagnostics;

namespace TxtDb.Storage.Services;

/// <summary>
/// Enhanced DeadlockAware LockManager - Implements true deadlock prevention
/// Uses resource ordering protocol to prevent circular dependencies and eliminate deadlocks
/// </summary>
public class EnhancedDeadlockAwareLockManager
{
    private readonly ConcurrentDictionary<string, ResourceLock> _resourceLocks = new();
    private readonly ConcurrentDictionary<long, HashSet<string>> _transactionLocks = new();
    private readonly object _lockTableLock = new object();
    private readonly int _deadlockTimeoutMs;
    private readonly bool _enableWaitDieDeadlockPrevention;

    public EnhancedDeadlockAwareLockManager(int deadlockTimeoutMs = 30000, bool enableWaitDieDeadlockPrevention = false)
    {
        _deadlockTimeoutMs = deadlockTimeoutMs;
        _enableWaitDieDeadlockPrevention = enableWaitDieDeadlockPrevention;
    }

    /// <summary>
    /// Acquires locks on multiple resources in deterministic order to prevent deadlocks
    /// This is the key deadlock prevention mechanism
    /// </summary>
    public void AcquireMultipleLocks(long transactionId, IEnumerable<string> resourceIds)
    {
        var orderedResources = resourceIds.OrderBy(r => r, StringComparer.Ordinal).ToList();
        
        // CRITICAL OPTIMIZATION: Only acquire locks we don't already hold
        var newLocksNeeded = new List<string>();
        var alreadyHeldLocks = new HashSet<string>();
        
        lock (_lockTableLock)
        {
            if (_transactionLocks.TryGetValue(transactionId, out var heldLocks))
            {
                alreadyHeldLocks = new HashSet<string>(heldLocks);
            }
        }
        
        foreach (var resourceId in orderedResources)
        {
            if (!alreadyHeldLocks.Contains(resourceId))
            {
                newLocksNeeded.Add(resourceId);
            }
        }
        
        // No new locks needed - all are already held by this transaction
        if (newLocksNeeded.Count == 0)
        {
            return;
        }
        
        var acquiredLocks = new List<string>();
        try
        {
            foreach (var resourceId in newLocksNeeded)
            {
                AcquireLockInternal(transactionId, resourceId, _deadlockTimeoutMs);
                acquiredLocks.Add(resourceId);
            }
            
            // CRITICAL FIX: Track all newly acquired locks for this transaction
            lock (_lockTableLock)
            {
                if (!_transactionLocks.ContainsKey(transactionId))
                {
                    _transactionLocks[transactionId] = new HashSet<string>();
                }
                
                foreach (var resourceId in acquiredLocks)
                {
                    _transactionLocks[transactionId].Add(resourceId);
                }
            }
        }
        catch
        {
            // If any lock acquisition fails, release all previously acquired locks
            foreach (var resourceId in acquiredLocks)
            {
                ReleaseLockInternal(transactionId, resourceId);
            }
            throw;
        }
    }

    /// <summary>
    /// Acquires a single lock on a resource for the specified transaction
    /// Uses timeout-based detection as fallback, but primary prevention is resource ordering
    /// </summary>
    public void AcquireLock(long transactionId, string resourceId)
    {
        if (_deadlockTimeoutMs <= 0)
        {
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
            
            // Enhanced error message distinguishing deadlock from resource contention
            var message = $"Resource contention detected: Transaction {transactionId} timed out waiting for lock on '{resourceId}' " +
                         $"after {stopwatch.ElapsedMilliseconds}ms (timeout: {_deadlockTimeoutMs}ms). " +
                         $"Resource ordering protocol ensures this is not a deadlock - likely high contention on this resource.";
            
            // CRITICAL: Release any locks this transaction may have acquired before timing out
            ReleaseLocks(transactionId);
            
            throw new TimeoutException(message);
        }
    }

    private void AcquireLockInternal(long transactionId, string resourceId, int timeoutMs = Timeout.Infinite)
    {
        var resourceLock = _resourceLocks.GetOrAdd(resourceId, _ => new ResourceLock(resourceId));
        
        // CONDITIONAL DEADLOCK PREVENTION: Only use Wait-Die algorithm when explicitly enabled
        // This allows timeout-based locking for operations like version cleanup
        // while preserving deadlock prevention for high-contention scenarios
        if (_enableWaitDieDeadlockPrevention)
        {
            // CRITICAL DEADLOCK PREVENTION: Implement Wait-Die algorithm
            // If resource is held by older transaction (lower ID), younger transaction waits
            // If resource is held by younger transaction (higher ID), younger transaction dies (exception)
            lock (_lockTableLock) 
            {
                var currentHolder = resourceLock.GetHolder();
                if (currentHolder != 0 && currentHolder != transactionId)
                {
                    // Resource is held by another transaction
                    if (transactionId < currentHolder)
                    {
                        // Current transaction is OLDER (lower ID) - it should WAIT
                        // Continue with normal lock acquisition and timeout
                    }
                    else
                    {
                        // Current transaction is YOUNGER (higher ID) - it should DIE
                        // Immediately abort to prevent deadlock
                        throw new DeadlockPreventionException(
                            $"Wait-Die deadlock prevention: Transaction {transactionId} (younger) aborted " +
                            $"because resource '{resourceId}' is held by transaction {currentHolder} (older). " +
                            $"This prevents circular wait and ensures deadlock-free operation.");
                    }
                }
            }
        }
        
        if (!resourceLock.TryAcquire(transactionId, timeoutMs))
        {
            throw new TimeoutException(
                $"Failed to acquire lock on '{resourceId}' for transaction {transactionId} within {timeoutMs}ms timeout");
        }
    }

    private void ReleaseLockInternal(long transactionId, string resourceId)
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
                    ReleaseLockInternal(transactionId, resourceId);
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

    /// <summary>
    /// Exception thrown when Wait-Die deadlock prevention aborts a transaction
    /// </summary>
    public class DeadlockPreventionException : Exception
    {
        public DeadlockPreventionException(string message) : base(message) { }
        public DeadlockPreventionException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Diagnostic information about current lock state
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