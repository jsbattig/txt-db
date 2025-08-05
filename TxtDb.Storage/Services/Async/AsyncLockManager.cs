using System.Collections.Concurrent;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async Lock Manager for Performance Optimization
/// Phase 1: Foundation - Replaces object locks with SemaphoreSlim for async-friendly locking
/// CRITICAL: Prevents deadlocks while supporting async/await patterns
/// </summary>
public class AsyncLockManager : IDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new();
    private readonly ConcurrentDictionary<string, int> _lockReferenceCounts = new();
    private readonly object _lockCreationLock = new object();
    private readonly ConcurrentBag<AsyncLockHandle> _activeLockHandles = new();
    private bool _disposed = false;

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
        if (_disposed)
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
            if (_disposed)
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
        if (_disposed)
            throw new ObjectDisposedException(nameof(AsyncLockManager));

        // Double-checked locking pattern for thread-safe semaphore creation
        if (_locks.TryGetValue(lockKey, out var existingSemaphore))
        {
            // CRITICAL FIX: Check if the semaphore was disposed during concurrent disposal
            if (_disposed)
                throw new ObjectDisposedException(nameof(AsyncLockManager));
                
            IncrementLockReference(lockKey);
            return existingSemaphore;
        }

        lock (_lockCreationLock)
        {
            // Double-check disposal status inside the lock
            if (_disposed)
                throw new ObjectDisposedException(nameof(AsyncLockManager));
                
            if (_locks.TryGetValue(lockKey, out existingSemaphore))
            {
                IncrementLockReference(lockKey);
                return existingSemaphore;
            }

            var newSemaphore = new SemaphoreSlim(1, 1); // Binary semaphore (mutex)
            _locks[lockKey] = newSemaphore;
            _lockReferenceCounts[lockKey] = 1;
            return newSemaphore;
        }
    }

    /// <summary>
    /// Increments the reference count for a lock
    /// </summary>
    private void IncrementLockReference(string lockKey)
    {
        _lockReferenceCounts.AddOrUpdate(lockKey, 1, (key, count) => count + 1);
    }

    /// <summary>
    /// Decrements the reference count for a lock and removes it if no longer referenced
    /// </summary>
    private void DecrementLockReference(string lockKey)
    {
        if (_lockReferenceCounts.TryGetValue(lockKey, out var count))
        {
            var newCount = count - 1;
            
            if (newCount <= 0)
            {
                // Remove the lock if no longer referenced
                if (_locks.TryRemove(lockKey, out var semaphore))
                {
                    semaphore.Dispose();
                }
                _lockReferenceCounts.TryRemove(lockKey, out _);
            }
            else
            {
                _lockReferenceCounts[lockKey] = newCount;
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
    /// Disposes the lock manager and releases all locks
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            // Mark all active lock handles as disposed
            foreach (var lockHandle in _activeLockHandles)
            {
                lockHandle.MarkAsDisposed();
            }

            // Dispose all semaphores
            foreach (var semaphore in _locks.Values)
            {
                try
                {
                    semaphore.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }

            _locks.Clear();
            _lockReferenceCounts.Clear();
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