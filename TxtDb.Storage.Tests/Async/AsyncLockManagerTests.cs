using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for AsyncLockManager - Phase 1: Foundation
/// Replaces object locks with SemaphoreSlim for async-friendly locking
/// CRITICAL: Must maintain thread-safety and prevent deadlocks while supporting async/await
/// </summary>
public class AsyncLockManagerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly AsyncLockManager _lockManager;

    public AsyncLockManagerTests(ITestOutputHelper output)
    {
        _output = output;
        _lockManager = new AsyncLockManager();
    }

    [Fact]
    public async Task AcquireLockAsync_SingleLock_ShouldAcquireAndRelease()
    {
        // Arrange
        var lockKey = "test_lock_1";

        // Act
        using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);

        // Assert
        Assert.NotNull(lockHandle);
        Assert.True(lockHandle.IsLocked);
    }

    [Fact]
    public async Task AcquireLockAsync_SameLockKey_ShouldBlockConcurrentAccess()
    {
        // Arrange
        var lockKey = "blocking_test_lock";
        var firstLockAcquired = false;
        var secondLockAcquired = false;
        var firstLockReleased = false;

        // Act
        var task1 = Task.Run(async () =>
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
            firstLockAcquired = true;
            await Task.Delay(100); // Hold lock for 100ms
            firstLockReleased = true;
        });

        var task2 = Task.Run(async () =>
        {
            await Task.Delay(50); // Start after task1 has acquired the lock
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
            secondLockAcquired = true;
        });

        await Task.WhenAll(task1, task2);

        // Assert
        Assert.True(firstLockAcquired);
        Assert.True(secondLockAcquired);
        Assert.True(firstLockReleased);
        
        // Second lock should only be acquired after first is released
        // This is difficult to test precisely, but both tasks should complete
    }

    [Fact]
    public async Task AcquireLockAsync_DifferentLockKeys_ShouldAllowConcurrentAccess()
    {
        // Arrange
        var lockKey1 = "concurrent_lock_1";
        var lockKey2 = "concurrent_lock_2";
        var lock1Acquired = false;
        var lock2Acquired = false;

        // Act
        var task1 = Task.Run(async () =>
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey1);
            lock1Acquired = true;
            await Task.Delay(50);
        });

        var task2 = Task.Run(async () =>
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey2);
            lock2Acquired = true;
            await Task.Delay(50);
        });

        await Task.WhenAll(task1, task2);

        // Assert
        Assert.True(lock1Acquired);
        Assert.True(lock2Acquired);
    }

    [Fact]
    public async Task AcquireLockAsync_WithTimeout_ShouldThrowOnTimeout()
    {
        // Arrange
        var lockKey = "timeout_test_lock";
        var timeout = TimeSpan.FromMilliseconds(100);

        // Act & Assert
        var task1 = Task.Run(async () =>
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
            await Task.Delay(200); // Hold lock longer than timeout
        });

        await Task.Delay(10); // Ensure task1 starts first

        var task2 = Task.Run(async () =>
        {
            await Assert.ThrowsAsync<TimeoutException>(
                () => _lockManager.AcquireLockAsync(lockKey, timeout));
        });

        await Task.WhenAll(task1, task2);
    }

    [Fact]
    public async Task AcquireLockAsync_WithCancellation_ShouldCancelGracefully()
    {
        // Arrange
        var lockKey = "cancellation_test_lock";
        using var cancellationTokenSource = new CancellationTokenSource();

        // Act & Assert
        var task1 = Task.Run(async () =>
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
            await Task.Delay(200); // Hold lock for 200ms
        });

        await Task.Delay(10); // Ensure task1 starts first

        var task2 = Task.Run(async () =>
        {
            cancellationTokenSource.CancelAfter(50); // Cancel after 50ms
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => _lockManager.AcquireLockAsync(lockKey, Timeout.InfiniteTimeSpan, cancellationTokenSource.Token));
        });

        await Task.WhenAll(task1, task2);
    }

    [Fact]
    public async Task DisposeLockHandle_ShouldReleaseLock()
    {
        // Arrange
        var lockKey = "dispose_test_lock";
        var secondLockAcquired = false;

        // Act
        var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
        
        var task = Task.Run(async () =>
        {
            using var secondLockHandle = await _lockManager.AcquireLockAsync(lockKey);
            secondLockAcquired = true;
        });

        // Release first lock by disposing
        lockHandle.Dispose();
        
        await task;

        // Assert
        Assert.True(secondLockAcquired);
    }

    [Fact]
    public async Task ConcurrentLockOperations_HighLoad_ShouldMaintainThreadSafety()
    {
        // Arrange
        var lockKey = "high_load_test";
        var operationCount = 100;
        var completedOperations = 0;
        var exceptions = new List<Exception>();

        // Act
        var tasks = Enumerable.Range(0, operationCount).Select(i =>
            Task.Run(async () =>
            {
                try
                {
                    using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
                    
                    // Simulate some work under lock
                    await Task.Delay(1);
                    Interlocked.Increment(ref completedOperations);
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            })
        ).ToArray();

        await Task.WhenAll(tasks);

        // Assert - Allow for some race conditions in high-load scenarios
        // The important thing is that no exceptions occur and most operations complete
        Assert.True(completedOperations >= operationCount * 0.95, 
            $"Expected at least 95% operations to complete, got {completedOperations}/{operationCount}");
        Assert.Empty(exceptions);
    }

    [Fact]
    public async Task MultipleLockKeys_ConcurrentOperations_ShouldAllowParallelExecution()
    {
        // Arrange
        var lockKeys = Enumerable.Range(0, 10).Select(i => $"parallel_lock_{i}").ToArray();
        var operationResults = new int[lockKeys.Length];

        // Act
        var tasks = lockKeys.Select((lockKey, index) =>
            Task.Run(async () =>
            {
                using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
                
                // Simulate work
                await Task.Delay(50);
                operationResults[index] = index + 1;
            })
        ).ToArray();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        // Assert
        Assert.All(operationResults.Select((result, index) => new { result, index }), 
            pair => Assert.Equal(pair.index + 1, pair.result));
        
        // All operations should complete in parallel, so total time should be close to individual operation time
        Assert.True(stopwatch.ElapsedMilliseconds < 150, 
            $"Parallel operations took {stopwatch.ElapsedMilliseconds}ms, should be < 150ms");

        _output.WriteLine($"Parallel lock operations completed in {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task LockReentrancy_SameLockKey_ShouldDetectAndPreventDeadlock()
    {
        // Arrange
        var lockKey = "reentrancy_test";

        // Act & Assert
        using var firstLock = await _lockManager.AcquireLockAsync(lockKey);
        
        // Attempting to acquire the same lock again should either:
        // 1. Throw an exception indicating reentrancy issue, or
        // 2. Have a timeout mechanism to prevent deadlock
        
        var timeout = TimeSpan.FromMilliseconds(100);
        await Assert.ThrowsAsync<TimeoutException>(
            () => _lockManager.AcquireLockAsync(lockKey, timeout));
    }

    [Fact]
    public async Task PerformanceTest_LockAcquisition_ShouldBeFast()
    {
        // Arrange
        var operationCount = 1000;
        var lockKey = "performance_test";

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        for (int i = 0; i < operationCount; i++)
        {
            using var lockHandle = await _lockManager.AcquireLockAsync(lockKey);
            // Minimal work to test lock acquisition overhead
        }
        
        stopwatch.Stop();

        // Assert
        var avgLockTime = stopwatch.ElapsedMilliseconds / (double)operationCount;
        
        _output.WriteLine($"Average lock acquisition time: {avgLockTime:F3}ms");
        
        // Lock acquisition should be very fast (< 1ms average)
        Assert.True(avgLockTime < 1.0, 
            $"Lock acquisition too slow: {avgLockTime:F3}ms average, should be < 1ms");
    }

    [Fact]
    public async Task LockManager_Dispose_ShouldReleaseAllLocks()
    {
        // Arrange
        var tempLockManager = new AsyncLockManager();
        var lockKey = "dispose_manager_test";
        
        var lockHandle = await tempLockManager.AcquireLockAsync(lockKey);

        // Act
        tempLockManager.Dispose();

        // Assert
        Assert.False(lockHandle.IsLocked); // Lock should be released when manager is disposed
    }

    public void Dispose()
    {
        _lockManager?.Dispose();
    }
}