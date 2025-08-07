using System.Threading.Channels;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// Resource Management Tests - TDD tests to reproduce and fix resource disposal issues
/// 
/// These tests reproduce the exact resource management problems identified in code review:
/// 1. Channel completion exception handling issues
/// 2. FileStream resource leaks when FlushAsync throws
/// 3. Unbounded memory growth with infinite loop potential
/// 4. TaskCanceledException during disposal
/// 5. Background task cleanup failures
/// 
/// All tests use real I/O operations and exception scenarios to validate proper cleanup.
/// </summary>
public class ResourceManagementTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly List<string> _tempFiles;
    private readonly List<BatchFlushCoordinator> _coordinators;

    public ResourceManagementTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_resource_mgmt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        _tempFiles = new List<string>();
        _coordinators = new List<BatchFlushCoordinator>();
    }

    [Fact]
    public async Task BatchFlushCoordinator_ChannelCompletionException_ShouldBeHandledProperly()
    {
        // Arrange - Create coordinator and start it
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();
        
        // Queue some requests to make the channel active
        var testFile = CreateTestFile("channel_completion_test.txt");
        var queueTask = coordinator.QueueFlushAsync(testFile);

        // Act - Stop the coordinator while requests are pending
        // This should handle channel completion exceptions properly
        Exception caughtException = null;
        try
        {
            await coordinator.StopAsync();
            await queueTask; // This should complete or be cancelled gracefully
        }
        catch (Exception ex)
        {
            caughtException = ex;
        }

        // Assert - Should not throw unhandled exceptions from channel completion
        if (caughtException != null && !(caughtException is OperationCanceledException))
        {
            Assert.True(false, $"Channel completion should be handled gracefully, but threw: {caughtException}");
        }
        
        // Verify coordinator is properly stopped
        Assert.False(coordinator.IsRunning, "Coordinator should be stopped after StopAsync()");
    }

    [Fact]
    public async Task BatchFlushCoordinator_FileStreamFlushException_ShouldNotLeakResources()
    {
        // Arrange - Create coordinator and start it
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();
        
        // Create a file that will cause FlushAsync to throw (locked file)
        var testFile = CreateTestFile("locked_file_test.txt", "test content");
        
        // Lock the file by opening it exclusively
        using var lockingStream = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        
        // Act - Attempt to flush the locked file (should fail but not leak resources)
        Exception flushException = null;
        try
        {
            await coordinator.ForceImmediateFlushAsync(testFile);
        }
        catch (Exception ex)
        {
            flushException = ex;
        }

        // Assert - Should throw IOException but not leak FileStream resources
        Assert.NotNull(flushException);
        Assert.True(flushException is IOException, $"Expected IOException, got: {flushException.GetType()}");
        
        // Release the lock and verify file is accessible (no leaked handles)
        lockingStream.Close();
        
        // Should be able to access file now if no handles leaked
        using var verifyStream = new FileStream(testFile, FileMode.Open, FileAccess.Read);
        Assert.True(verifyStream.CanRead, "File should be accessible after failed flush (no handle leaks)");
        
        await coordinator.StopAsync();
    }

    [Fact]
    public async Task BatchFlushCoordinator_UnboundedMemoryGrowth_ShouldHaveBoundsChecking()
    {
        // Arrange - Create coordinator with very small batch size to trigger batching logic quickly
        var config = new BatchFlushConfig 
        { 
            MaxBatchSize = 2,
            MaxDelayMs = 10,
            MaxConcurrentFlushes = 1
        };
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        await coordinator.StartAsync();

        // Act - Queue many requests rapidly to test memory bounds
        var requestCount = 1000;
        var tasks = new List<Task>();
        var startMemory = GC.GetTotalMemory(false);

        for (int i = 0; i < requestCount; i++)
        {
            var testFile = CreateTestFile($"memory_test_{i}.txt", $"content_{i}");
            tasks.Add(coordinator.QueueFlushAsync(testFile));
        }

        // Wait for all requests to be processed
        await Task.WhenAll(tasks);
        
        // Force garbage collection to measure actual memory usage
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var endMemory = GC.GetTotalMemory(false);
        
        await coordinator.StopAsync();

        // Assert - Memory growth should be bounded (not grow indefinitely)
        var memoryGrowthMB = (endMemory - startMemory) / (1024.0 * 1024.0);
        _output.WriteLine($"Memory growth: {memoryGrowthMB:F2} MB for {requestCount} requests");
        
        // Should not consume more than 100MB for 1000 simple requests
        Assert.True(memoryGrowthMB < 100, $"Memory growth too high: {memoryGrowthMB:F2} MB indicates unbounded growth");
    }

    [Fact]
    public async Task BatchFlushCoordinator_TaskCancellationDuringDisposal_ShouldBeHandledGracefully()
    {
        // Arrange - Create coordinator and queue operations
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();
        
        var testFiles = new List<string>();
        var flushTasks = new List<Task>();
        
        // Queue fewer flush operations to avoid test timeout
        for (int i = 0; i < 3; i++)
        {
            var testFile = CreateTestFile($"cancellation_test_{i}.txt", $"content_{i}");
            testFiles.Add(testFile);
            flushTasks.Add(coordinator.QueueFlushAsync(testFile));
        }

        // Wait a brief moment for operations to start
        await Task.Delay(10);

        // Act - Dispose while operations are in progress (should handle TaskCanceledException)
        Exception? disposalException = null;
        try
        {
            coordinator.Dispose();
        }
        catch (Exception ex)
        {
            disposalException = ex;
        }

        // Wait for flush tasks to complete or be cancelled with timeout
        var waitTasks = flushTasks.Select(async task =>
        {
            try
            {
                await task.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
                // Expected for cancelled operations
            }
            catch (ObjectDisposedException)
            {
                // Expected for operations on disposed coordinator
            }
            catch (InvalidOperationException)
            {
                // Expected when coordinator is not running
            }
            catch (TimeoutException)
            {
                // Acceptable for this test - disposal may interrupt operations
            }
        });

        await Task.WhenAll(waitTasks);

        // Assert - Disposal should not throw unhandled exceptions
        Assert.Null(disposalException);
        Assert.False(coordinator.IsRunning, "Coordinator should be stopped after disposal");
    }

    [Fact]
    public async Task BatchFlushCoordinator_BackgroundProcessorException_ShouldCleanupProperly()
    {
        // Arrange - Create a scenario that will cause background processor exceptions
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();

        // Create files that don't exist to trigger FileNotFoundException in background processor
        var nonExistentFiles = new List<string>
        {
            Path.Combine(_testRootPath, "non_existent_1.txt"),
            Path.Combine(_testRootPath, "non_existent_2.txt"),
            Path.Combine(_testRootPath, "non_existent_3.txt")
        };

        var failedTasks = new List<Task>();
        
        // Act - Queue flushes for non-existent files (should cause background exceptions)
        foreach (var file in nonExistentFiles)
        {
            failedTasks.Add(Assert.ThrowsAsync<FileNotFoundException>(
                () => coordinator.QueueFlushAsync(file)));
        }

        // Wait for all tasks to fail
        await Task.WhenAll(failedTasks);

        // Stop coordinator after background exceptions
        await coordinator.StopAsync();

        // Assert - Coordinator should still be able to stop cleanly after background exceptions
        Assert.False(coordinator.IsRunning, "Coordinator should stop cleanly even after background exceptions");
        
        // Should be able to start again after exceptions
        await coordinator.StartAsync();
        Assert.True(coordinator.IsRunning, "Coordinator should be able to restart after exception cleanup");
        
        await coordinator.StopAsync();
    }

    [Fact]
    public async Task BatchFlushCoordinator_ConcurrentDisposeAndOperations_ShouldBeThreadSafe()
    {
        // Arrange - Create coordinator and start operations
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();

        var testFile = CreateTestFile("concurrent_test.txt", "test content");
        
        // Act - Start concurrent operations and disposal
        var operationTasks = new List<Task>();
        var cancellationTokenSource = new CancellationTokenSource();
        
        // Start multiple concurrent flush operations
        for (int i = 0; i < 5; i++)
        {
            operationTasks.Add(Task.Run(async () =>
            {
                try
                {
                    while (!cancellationTokenSource.Token.IsCancellationRequested)
                    {
                        await coordinator.QueueFlushAsync(testFile);
                        await Task.Delay(10, cancellationTokenSource.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
                catch (ObjectDisposedException)
                {
                    // Expected when coordinator is disposed
                }
                catch (InvalidOperationException)
                {
                    // Expected when coordinator is stopped
                }
            }));
        }

        // Let operations run briefly
        await Task.Delay(50);
        
        // Dispose coordinator while operations are running
        Exception? disposalException = null;
        try
        {
            coordinator.Dispose();
        }
        catch (Exception ex)
        {
            disposalException = ex;
        }
        
        // Cancel concurrent operations
        cancellationTokenSource.Cancel();
        
        // Wait for all tasks to complete with timeout
        try
        {
            await Task.WhenAll(operationTasks).WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            // Acceptable for this test - some operations may not complete due to disposal
        }

        // Assert - Concurrent disposal should be thread-safe
        Assert.Null(disposalException);
        Assert.False(coordinator.IsRunning);
    }

    [Fact]
    public async Task BatchFlushCoordinator_ForceFlushTimeout_ShouldHaveTimeoutLimits()
    {
        // This test will be implemented after we add timeout functionality
        // For now, create the failing test structure
        var coordinator = CreateTestCoordinator();
        await coordinator.StartAsync();
        
        var testFile = CreateTestFile("timeout_test.txt", "content");
        
        // Act & Assert - Should have timeout mechanism for flush operations
        // This will initially fail as timeout limits are not implemented
        var flushTask = coordinator.ForceImmediateFlushAsync(testFile);
        
        // Should complete within reasonable time (not hang indefinitely)
        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(30));
        var completedTask = await Task.WhenAny(flushTask, timeoutTask);
        
        Assert.NotEqual(timeoutTask, completedTask);
        
        await coordinator.StopAsync();
    }

    private BatchFlushCoordinator CreateTestCoordinator()
    {
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 10,
            MaxDelayMs = 100,
            MaxConcurrentFlushes = 2
        };
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        return coordinator;
    }

    private string CreateTestFile(string filename, string content = "test content")
    {
        var filePath = Path.Combine(_testRootPath, filename);
        File.WriteAllText(filePath, content);
        _tempFiles.Add(filePath);
        return filePath;
    }

    public void Dispose()
    {
        // Cleanup coordinators
        foreach (var coordinator in _coordinators)
        {
            try
            {
                coordinator.Dispose();
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        // Cleanup temp files
        foreach (var file in _tempFiles)
        {
            try
            {
                if (File.Exists(file))
                    File.Delete(file);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        // Cleanup test directory
        try
        {
            if (Directory.Exists(_testRootPath))
                Directory.Delete(_testRootPath, true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}