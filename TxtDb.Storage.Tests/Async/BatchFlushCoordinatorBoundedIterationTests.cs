using System.Diagnostics;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD Tests for BatchFlushCoordinator bounded iteration fixes - Phase 3 MVCC Stabilization
/// Tests prevention of infinite loops in batch processing
/// All tests use real batch processing (no mocking)
/// </summary>
public class BatchFlushCoordinatorBoundedIterationTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly List<BatchFlushCoordinator> _coordinators = new();

    public BatchFlushCoordinatorBoundedIterationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task BatchProcessor_WithSlowRequestQueue_ShouldNotInfiniteLoop()
    {
        // Arrange - Create coordinator with small batch size and short delay
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 5,
            MaxDelayMs = 100, // Very short delay to test bounded iteration
            MaxConcurrentFlushes = 1
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Create test files to flush
        var testFiles = new List<string>();
        for (int i = 0; i < 3; i++)
        {
            var filePath = Path.Combine(Path.GetTempPath(), $"test_file_{i}_{Guid.NewGuid():N}.txt");
            await File.WriteAllTextAsync(filePath, $"Test content {i}");
            testFiles.Add(filePath);
        }
        
        // Act - Queue some requests and measure processing time
        var stopwatch = Stopwatch.StartNew();
        var queueTasks = new List<Task>();
        
        // Queue 3 requests with small delays between them
        for (int i = 0; i < testFiles.Count; i++)
        {
            var taskId = i;
            var filePath = testFiles[i];
            queueTasks.Add(Task.Run(async () =>
            {
                await Task.Delay(50 * taskId); // Stagger requests
                await coordinator.QueueFlushAsync(filePath, FlushPriority.Normal);
                _output.WriteLine($"Request {taskId} queued at {stopwatch.ElapsedMilliseconds}ms");
            }));
        }
        
        // Wait for all requests to complete
        await Task.WhenAll(queueTasks);
        stopwatch.Stop();
        
        _output.WriteLine($"Total processing time: {stopwatch.ElapsedMilliseconds}ms");
        
        // Assert - Processing should complete within reasonable time (not infinite loop)
        // With MaxDelayMs = 100ms, processing should complete well within 1 second
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, 
            $"Batch processing took too long ({stopwatch.ElapsedMilliseconds}ms), suggesting infinite loop");
        
        // Verify coordinator state
        Assert.True(coordinator.BatchCount >= 1, "At least one batch should have been processed");
        Assert.True(coordinator.ActualFlushCount >= 1, "At least one flush should have occurred");
        
        await coordinator.StopAsync();
        
        // Clean up test files
        foreach (var filePath in testFiles)
        {
            try { File.Delete(filePath); } catch { }
        }
    }

    [Fact]
    public async Task BatchProcessor_WithEmptyQueue_ShouldExitGracefully()
    {
        // Arrange - Create coordinator that will process an empty queue
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 10,
            MaxDelayMs = 50, // Very short delay
            MaxConcurrentFlushes = 1
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Act - Don't queue anything, let the processor run for a bit
        var stopwatch = Stopwatch.StartNew();
        
        // Wait for some time to ensure the processor doesn't get stuck
        await Task.Delay(200);
        
        await coordinator.StopAsync();
        stopwatch.Stop();
        
        _output.WriteLine($"Empty queue processing time: {stopwatch.ElapsedMilliseconds}ms");
        
        // Assert - Should stop cleanly without hanging
        Assert.True(stopwatch.ElapsedMilliseconds < 500, 
            "Coordinator should stop cleanly within reasonable time");
    }

    [Fact]
    public async Task BatchProcessor_WithManySmallBatches_ShouldRespectIterationLimits()
    {
        // Arrange - Create coordinator with limits designed to test bounds checking
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 3,
            MaxDelayMs = 200,
            MaxConcurrentFlushes = 1
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Create test files to flush
        var testFiles = new List<string>();
        for (int i = 0; i < 15; i++)
        {
            var filePath = Path.Combine(Path.GetTempPath(), $"bounded_test_{i}_{Guid.NewGuid():N}.txt");
            await File.WriteAllTextAsync(filePath, $"Test content for bounded test {i}");
            testFiles.Add(filePath);
        }
        
        // Act - Queue many requests quickly to test iteration bounds
        var stopwatch = Stopwatch.StartNew();
        var queueTasks = new List<Task>();
        
        for (int i = 0; i < testFiles.Count; i++)
        {
            var filePath = testFiles[i];
            queueTasks.Add(coordinator.QueueFlushAsync(filePath, FlushPriority.Normal));
        }
        
        await Task.WhenAll(queueTasks);
        stopwatch.Stop();
        
        _output.WriteLine($"Bounded iteration test completed in {stopwatch.ElapsedMilliseconds}ms");
        _output.WriteLine($"Batches processed: {coordinator.BatchCount}");
        _output.WriteLine($"Actual flushes: {coordinator.ActualFlushCount}");
        
        // Assert - Should process requests efficiently without infinite loops
        Assert.True(stopwatch.ElapsedMilliseconds < 2000, 
            $"Processing 15 requests took too long ({stopwatch.ElapsedMilliseconds}ms), suggesting unbounded iteration");
        
        // Should create multiple batches due to batch size limit (15 requests / 3 per batch = 5 batches)
        Assert.True(coordinator.BatchCount >= 3, $"Expected at least 3 batches, got {coordinator.BatchCount}");
        
        await coordinator.StopAsync();
        
        // Clean up test files
        foreach (var filePath in testFiles)
        {
            try { File.Delete(filePath); } catch { }
        }
    }

    [Fact]
    public async Task BatchProcessor_WithCircuitBreakerCondition_ShouldBreakSafely()
    {
        // Arrange - Create coordinator with conditions that might trigger circuit breaker behavior
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 2,
            MaxDelayMs = 100,
            MaxConcurrentFlushes = 1
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Create a test file to flush
        var testFilePath = Path.Combine(Path.GetTempPath(), $"circuit_test_{Guid.NewGuid():N}.txt");
        await File.WriteAllTextAsync(testFilePath, "Circuit breaker test content");
        
        // Act - Create a scenario where reader might return false repeatedly
        var stopwatch = Stopwatch.StartNew();
        
        // Queue one request, then force flush to test boundary conditions
        await coordinator.QueueFlushAsync(testFilePath, FlushPriority.Normal);
        await coordinator.ForceFlushAllPendingAsync();
        
        stopwatch.Stop();
        
        _output.WriteLine($"Circuit breaker test completed in {stopwatch.ElapsedMilliseconds}ms");
        
        // Assert - Should complete without hanging
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, 
            "Circuit breaker test should complete quickly without infinite loops");
        
        await coordinator.StopAsync();
        
        // Clean up test file
        try { File.Delete(testFilePath); } catch { }
    }

    public void Dispose()
    {
        foreach (var coordinator in _coordinators)
        {
            try
            {
                coordinator.StopAsync().Wait(TimeSpan.FromSeconds(1));
                coordinator.Dispose();
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }
}