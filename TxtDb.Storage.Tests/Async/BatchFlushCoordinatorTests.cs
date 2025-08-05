using System.Diagnostics;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD Tests for BatchFlushCoordinator - Epic 002 Phase 3
/// Tests the batch flushing system that reduces FlushToDisk calls by 50%
/// Focus: Channel-based queuing, background processing, priority handling
/// </summary>
public class BatchFlushCoordinatorTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private BatchFlushCoordinator? _coordinator;

    public BatchFlushCoordinatorTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_batch_flush_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
    }

    [Fact]
    public void FlushRequest_ShouldInitializeWithCorrectProperties()
    {
        // Arrange
        var filePath = "/test/path/file.json";
        var priority = FlushPriority.Critical;
        var scheduledTime = DateTime.UtcNow.AddMilliseconds(100);

        // Act - This will fail until we implement FlushRequest
        var request = new FlushRequest(filePath, priority, scheduledTime);

        // Assert
        Assert.Equal(filePath, request.FilePath);
        Assert.Equal(priority, request.Priority);
        Assert.Equal(scheduledTime, request.ScheduledTime);
        Assert.True(request.CreatedTime <= DateTime.UtcNow);
        Assert.False(request.IsCompleted);
    }

    [Fact]
    public void FlushRequest_ShouldSupportTaskCompletionSource()
    {
        // Arrange & Act - This will fail until we implement FlushRequest with TaskCompletionSource
        var request = new FlushRequest("/test/file.json", FlushPriority.Normal);

        // Assert
        Assert.NotNull(request.CompletionSource);
        Assert.False(request.CompletionSource.Task.IsCompleted);
        
        // Test completion
        request.SetCompleted();
        Assert.True(request.IsCompleted);
        Assert.True(request.CompletionSource.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public void FlushRequest_ShouldSupportErrorCompletion()
    {
        // Arrange
        var request = new FlushRequest("/test/file.json", FlushPriority.Normal);
        var error = new IOException("Test flush error");

        // Act
        request.SetError(error);

        // Assert
        Assert.True(request.IsCompleted);
        Assert.True(request.CompletionSource.Task.IsFaulted);
        Assert.Equal(error, request.CompletionSource.Task.Exception?.InnerException);
    }

    [Fact]
    public void BatchFlushCoordinator_ShouldInitializeWithDefaultConfiguration()
    {
        // Act - This will fail until we implement BatchFlushCoordinator
        _coordinator = new BatchFlushCoordinator();

        // Assert
        Assert.Equal(100, _coordinator.MaxBatchSize);
        Assert.Equal(50, _coordinator.MaxDelayMs);
        Assert.False(_coordinator.IsRunning);
    }

    [Fact]
    public void BatchFlushCoordinator_ShouldInitializeWithCustomConfiguration()
    {
        // Arrange
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 200,
            MaxDelayMs = 100,
            MaxConcurrentFlushes = 4
        };

        // Act
        _coordinator = new BatchFlushCoordinator(config);

        // Assert
        Assert.Equal(200, _coordinator.MaxBatchSize);
        Assert.Equal(100, _coordinator.MaxDelayMs);
        Assert.Equal(4, _coordinator.MaxConcurrentFlushes);
    }

    [Fact]
    public async Task BatchFlushCoordinator_StartAsync_ShouldStartBackgroundProcessing()
    {
        // Arrange
        _coordinator = new BatchFlushCoordinator();

        // Act
        await _coordinator.StartAsync();

        // Assert
        Assert.True(_coordinator.IsRunning);
    }

    [Fact]
    public async Task BatchFlushCoordinator_StopAsync_ShouldStopBackgroundProcessing()
    {
        // Arrange
        _coordinator = new BatchFlushCoordinator();
        await _coordinator.StartAsync();

        // Act
        await _coordinator.StopAsync();

        // Assert
        Assert.False(_coordinator.IsRunning);
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldProcessSingleFlushRequest()
    {
        // Arrange
        _coordinator = new BatchFlushCoordinator();
        await _coordinator.StartAsync();

        var testFile = Path.Combine(_testRootPath, "test_single.txt");
        await File.WriteAllTextAsync(testFile, "Test content");

        // Act
        var flushTask = _coordinator.QueueFlushAsync(testFile, FlushPriority.Normal);
        await flushTask;

        // Assert - The file should have been flushed
        Assert.True(flushTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldBatchMultipleFlushRequests()
    {
        // Arrange
        var config = new BatchFlushConfig { MaxBatchSize = 3, MaxDelayMs = 100 };
        _coordinator = new BatchFlushCoordinator(config);
        await _coordinator.StartAsync();

        var testFiles = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var testFile = Path.Combine(_testRootPath, $"test_batch_{i}.txt");
            await File.WriteAllTextAsync(testFile, $"Test content {i}");
            testFiles.Add(testFile);
        }

        // Act - Queue multiple flush requests
        var flushTasks = testFiles.Select(file => 
            _coordinator.QueueFlushAsync(file, FlushPriority.Normal)).ToArray();

        await Task.WhenAll(flushTasks);

        // Assert - All flushes should complete successfully
        Assert.All(flushTasks, task => Assert.True(task.IsCompletedSuccessfully));
        
        // Verify that batching occurred (fewer actual flush operations than requests)
        Assert.True(_coordinator.BatchCount > 0, "Should have created at least one batch");
        Assert.True(_coordinator.BatchCount < testFiles.Count, "Should have batched requests together");
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldPrioritizeCriticalFlushes()
    {
        // Arrange
        var config = new BatchFlushConfig { MaxBatchSize = 10, MaxDelayMs = 200 };
        _coordinator = new BatchFlushCoordinator(config);
        await _coordinator.StartAsync();

        var normalFile = Path.Combine(_testRootPath, "normal.txt");
        var criticalFile = Path.Combine(_testRootPath, "critical.txt");
        
        await File.WriteAllTextAsync(normalFile, "Normal content");
        await File.WriteAllTextAsync(criticalFile, "Critical content");

        // Act - Queue normal flush first, then critical
        var normalTask = _coordinator.QueueFlushAsync(normalFile, FlushPriority.Normal);
        await Task.Delay(10); // Small delay to ensure order
        var criticalTask = _coordinator.QueueFlushAsync(criticalFile, FlushPriority.Critical);

        var completedTask = await Task.WhenAny(normalTask, criticalTask);

        // Assert - Critical should complete first despite being queued later
        Assert.Equal(criticalTask, completedTask);
        Assert.True(criticalTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldHandleFlushErrors()
    {
        // Arrange
        _coordinator = new BatchFlushCoordinator();
        await _coordinator.StartAsync();

        var nonExistentFile = Path.Combine(_testRootPath, "does_not_exist.txt");

        // Act & Assert
        var flushTask = _coordinator.QueueFlushAsync(nonExistentFile, FlushPriority.Normal);
        
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await flushTask);
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldRespectMaxDelayTimeout()
    {
        // Arrange
        var config = new BatchFlushConfig { MaxBatchSize = 100, MaxDelayMs = 50 };
        _coordinator = new BatchFlushCoordinator(config);
        await _coordinator.StartAsync();

        var testFile = Path.Combine(_testRootPath, "delay_test.txt");
        await File.WriteAllTextAsync(testFile, "Delay test content");

        // Act
        var stopwatch = Stopwatch.StartNew();
        var flushTask = _coordinator.QueueFlushAsync(testFile, FlushPriority.Normal);
        await flushTask;
        stopwatch.Stop();

        // Assert - Should complete within max delay time
        Assert.True(stopwatch.ElapsedMilliseconds <= 100, 
            $"Flush should complete within max delay, took {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldMeasureFlushReduction()
    {
        // Arrange
        var config = new BatchFlushConfig { MaxBatchSize = 10, MaxDelayMs = 50 };
        _coordinator = new BatchFlushCoordinator(config);
        await _coordinator.StartAsync();

        var testFiles = new List<string>();
        for (int i = 0; i < 25; i++) // 25 files to test batching
        {
            var testFile = Path.Combine(_testRootPath, $"reduction_test_{i}.txt");
            await File.WriteAllTextAsync(testFile, $"Content {i}");
            testFiles.Add(testFile);
        }

        // Act - Queue all flush requests simultaneously
        var flushTasks = testFiles.Select(file => 
            _coordinator.QueueFlushAsync(file, FlushPriority.Normal)).ToArray();

        await Task.WhenAll(flushTasks);

        // Assert - Should achieve significant reduction in flush operations
        var reductionPercentage = ((double)(testFiles.Count - _coordinator.ActualFlushCount) / testFiles.Count) * 100;
        
        _output.WriteLine($"Queued {testFiles.Count} flush requests");
        _output.WriteLine($"Actual flush operations: {_coordinator.ActualFlushCount}");
        _output.WriteLine($"Reduction: {reductionPercentage:F1}%");
        
        Assert.True(reductionPercentage >= 50, 
            $"Should achieve at least 50% reduction, got {reductionPercentage:F1}%");
    }

    public void Dispose()
    {
        try
        {
            _coordinator?.StopAsync().Wait(TimeSpan.FromSeconds(5));
            _coordinator?.Dispose();
            
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup errors are not critical for tests
        }
    }
}