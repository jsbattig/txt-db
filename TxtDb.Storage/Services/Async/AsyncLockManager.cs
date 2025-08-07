using System.Collections.Concurrent;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Thread-safe reference counting structure for lock management
/// CRITICAL FIX: Supports atomic operations to prevent race conditions
/// </summary>
internal class LockReferenceInfo
{
    public int Count;

    public LockReferenceInfo(int initialCount)
    {
        Count = initialCount;
    }
}

/// <summary>
/// Async Lock Manager for Performance Optimization
/// Phase 1: Foundation - Replaces object locks with SemaphoreSlim for async-friendly locking
/// CRITICAL: Prevents deadlocks while supporting async/await patterns
/// </summary>
public class AsyncLockManager : IDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new();
    private readonly ConcurrentDictionary<string, LockReferenceInfo> _lockReferenceCounts = new();
    private readonly object _lockCreationLock = new object();
    private readonly ConcurrentBag<AsyncLockHandle> _activeLockHandles = new();
    private readonly TaskCompletionSource<bool> _disposalCompletionSource = new();
    private volatile bool _disposed = false;
    private volatile bool _disposing = false;

    /// <summary>
    /// Acquires an async lock for the specified key
    /// </summary>
    /// <param name="lockKey">Unique identifier for the lock</param>
    /// <param name="timeout">Maximum time to wait for lock acquisition</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Disposable lock handle</returns>
    /// <exception cref="TimeoutException">Thrown when lock cannot be acquired within timeout</exception>
    /// <exception cref="OperationCanceledException">Thrown when operation is cancelled</exception>
    public async Task<AsyncLockHandle> AcquireLockAsync(string lockKey, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        if (_disposed || _disposing)
            throw new ObjectDisposedException(nameof(AsyncLockManager));

        var actualTimeout = timeout ?? Timeout.InfiniteTimeSpan;
        var semaphore = GetOrCreateSemaphore(lockKey);

        bool acquired = false;
        try
        {
            // CRITICAL FIX: Wrap semaphore operations in try-catch to handle disposal race conditions
            if (actualTimeout == Timeout.InfiniteTimeSpan)
            {
                try
                {
                    await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    acquired = true;
                }
                catch (ObjectDisposedException) when (_disposed)
                {
                    // Lock manager was disposed while waiting - this is expected during shutdown
                    throw new ObjectDisposedException(nameof(AsyncLockManager));
                }
            }
            else
            {
                try
                {
                    acquired = await semaphore.WaitAsync(actualTimeout, cancellationToken).ConfigureAwait(false);
                    if (!acquired)
                    {
                        throw new TimeoutException($"Failed to acquire lock '{lockKey}' within {actualTimeout.TotalMilliseconds}ms");
                    }
                }
                catch (ObjectDisposedException) when (_disposed)
                {
                    // Lock manager was disposed while waiting - this is expected during shutdown
                    throw new ObjectDisposedException(nameof(AsyncLockManager));
                }
            }

            // Double-check we weren't disposed while acquiring the lock
            if (_disposed || _disposing)
            {
                // Clean up - release the lock we just acquired
                try { semaphore.Release(); } catch { }
                throw new ObjectDisposedException(nameof(AsyncLockManager));
            }

            var lockHandle = new AsyncLockHandle(this, lockKey, semaphore);
            _activeLockHandles.Add(lockHandle);
            return lockHandle;
        }
        catch
        {
            if (!acquired)
            {
                // If we failed to acquire the lock, decrement the reference count
                DecrementLockReference(lockKey);
            }
            throw;
        }
    }

    /// <summary>
    /// Gets or creates a semaphore for the specified lock key
    /// </summary>
    private SemaphoreSlim GetOrCreateSemaphore(string lockKey)
    {
        if (_disposed || _disposing)
            throw new ObjectDisposedException(nameof(AsyncLockManager));

        // Double-checked locking pattern for thread-safe semaphore creation
        if (_locks.TryGetValue(lockKey, out var existingSemaphore))
        {
            // CRITICAL FIX: Check if the semaphore was disposed during concurrent disposal
            if (_disposed || _disposing)
                throw new ObjectDisposedException(nameof(AsyncLockManager));
                
            IncrementLockReference(lockKey);
            return existingSemaphore;
        }

        lock (_lockCreationLock)
        {
            // Double-check disposal status inside the lock
            if (_disposed || _disposing)
                throw new ObjectDisposedException(nameof(AsyncLockManager));
                
            if (_locks.TryGetValue(lockKey, out existingSemaphore))
            {
                IncrementLockReference(lockKey);
                return existingSemaphore;
            }

            var newSemaphore = new SemaphoreSlim(1, 1); // Binary semaphore (mutex)
            _locks[lockKey] = newSemaphore;
            _lockReferenceCounts[lockKey] = new LockReferenceInfo(1);
            return newSemaphore;
        }
    }

    /// <summary>
    /// Increments the reference count for a lock using atomic operations
    /// </summary>
    private void IncrementLockReference(string lockKey)
    {
        _lockReferenceCounts.AddOrUpdate(
            lockKey, 
            new LockReferenceInfo(1), 
            (key, info) => 
            {
                Interlocked.Increment(ref info.Count);
                return info;
            }
        );
    }

    /// <summary>
    /// Decrements the reference count for a lock and removes it if no longer referenced
    /// Uses atomic operations to prevent race conditions
    /// </summary>
    private void DecrementLockReference(string lockKey)
    {
        if (_lockReferenceCounts.TryGetValue(lockKey, out var info))
        {
            var newCount = Interlocked.Decrement(ref info.Count);
            
            if (newCount <= 0)
            {
                // Only one thread will successfully remove the lock
                if (_lockReferenceCounts.TryRemove(lockKey, out _) && 
                    _locks.TryRemove(lockKey, out var semaphore))
                {
                    // CRITICAL FIX: Dispose semaphore safely, handling concurrent access
                    try
                    {
                        semaphore.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Expected during shutdown - semaphore already disposed
                    }
                }
            }
        }
    }

    /// <summary>
    /// Releases a lock (called by AsyncLockHandle)
    /// </summary>
    internal void ReleaseLock(string lockKey, SemaphoreSlim semaphore)
    {
        if (!_disposed)
        {
            try
            {
                semaphore.Release();
                DecrementLockReference(lockKey);
            }
            catch (ObjectDisposedException)
            {
                // CRITICAL FIX: Handle cases where semaphore is disposed during concurrent operations
                // This can happen during shutdown when multiple threads are releasing locks
                // Just ignore the error - the lock is effectively released
            }
        }
    }

    /// <summary>
    /// Disposes the lock manager and releases all locks with proper synchronization
    /// CRITICAL FIX: Uses proper disposal coordination to prevent race conditions
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            // Set disposal flags atomically
            lock (_lockCreationLock)
            {
                if (_disposed) return;
                _disposing = true;
                _disposed = true;
            }
            
            // Mark all active lock handles as disposed (snapshot to avoid collection modification)
            try
            {
                var handleArray = _activeLockHandles.ToArray();
                foreach (var lockHandle in handleArray)
                {
                    try
                    {
                        lockHandle.MarkAsDisposed();
                    }
                    catch
                    {
                        // Ignore disposal errors for handles
                    }
                }
            }
            catch
            {
                // Ignore enumeration errors during disposal
            }

            // Dispose all semaphores (snapshot to avoid collection modification)
            try
            {
                var lockArray = _locks.ToArray();
                foreach (var kvp in lockArray)
                {
                    try
                    {
                        kvp.Value.Dispose();
                    }
                    catch
                    {
                        // Ignore disposal errors
                    }
                }
            }
            catch
            {
                // Ignore enumeration errors during disposal
            }

            // Clear collections
            try
            {
                _locks.Clear();
                _lockReferenceCounts.Clear();
            }
            catch
            {
                // Ignore clearing errors
            }
            
            // Signal completion of disposal
            try
            {
                _disposalCompletionSource.TrySetResult(true);
            }
            catch
            {
                // Ignore completion source errors
            }
        }
    }
}

/// <summary>
/// Disposable handle for async locks
/// CRITICAL: Must be disposed to release the lock and prevent resource leaks
/// </summary>
public class AsyncLockHandle : IDisposable
{
    private readonly AsyncLockManager _lockManager;
    private readonly string _lockKey;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed = false;

    internal AsyncLockHandle(AsyncLockManager lockManager, string lockKey, SemaphoreSlim semaphore)
    {
        _lockManager = lockManager;
        _lockKey = lockKey;
        _semaphore = semaphore;
    }

    /// <summary>
    /// Indicates whether the lock is still held
    /// </summary>
    public bool IsLocked => !_disposed;

    /// <summary>
    /// Marks the lock handle as disposed (called by lock manager)
    /// </summary>
    internal void MarkAsDisposed()
    {
        _disposed = true;
    }

    /// <summary>
    /// Releases the lock
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _lockManager.ReleaseLock(_lockKey, _semaphore);
        }
    }
}