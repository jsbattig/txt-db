using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// TDD Tests for MonitoredAsyncStorageSubsystem - Phase 4 of Epic 002
/// Tests automatic metric collection wrapper for AsyncStorageSubsystem
/// All tests use real storage operations and measurements, no mocking allowed
/// </summary>
public class MonitoredAsyncStorageSubsystemTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly MonitoredAsyncStorageSubsystem _monitoredStorage;

    public MonitoredAsyncStorageSubsystemTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_monitored_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _monitoredStorage = new MonitoredAsyncStorageSubsystem();
        _monitoredStorage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = false
        });
    }

    [Fact]
    public void MonitoredAsyncStorageSubsystem_Constructor_ShouldInitializeWithMetrics()
    {
        // Act & Assert
        Assert.NotNull(_monitoredStorage.Metrics);
        Assert.Equal(0L, _monitoredStorage.Metrics.TotalOperations);
        Assert.Equal(0L, _monitoredStorage.Metrics.TotalReadOperations);
        Assert.Equal(0L, _monitoredStorage.Metrics.TotalWriteOperations);
        Assert.Equal(0L, _monitoredStorage.Metrics.TotalTransactionOperations);
        Assert.Equal(0L, _monitoredStorage.Metrics.TotalErrors);
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_BeginTransactionAsync_ShouldTrackTransactionMetrics()
    {
        // Arrange
        var initialTransactionCount = _monitoredStorage.Metrics.TotalTransactionOperations;

        // Act
        var stopwatch = Stopwatch.StartNew();
        var transactionId = await _monitoredStorage.BeginTransactionAsync();
        stopwatch.Stop();

        // Assert
        Assert.Equal(initialTransactionCount + 1, _monitoredStorage.Metrics.TotalTransactionOperations);
        Assert.Equal(1L, _monitoredStorage.Metrics.TotalOperations);
        
        var transactionStats = _monitoredStorage.Metrics.GetTransactionLatencyStatistics();
        Assert.Equal(1, transactionStats.SampleCount);
        Assert.True(transactionStats.AverageMs > 0);
        Assert.True(transactionStats.AverageMs < 1000); // Should be fast
        
        _output.WriteLine($"Transaction begin latency: {transactionStats.AverageMs:F2}ms");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_InsertObjectAsync_ShouldTrackWriteMetrics()
    {
        // Arrange
        var txn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(txn, "test.namespace");
        var initialWriteCount = _monitoredStorage.Metrics.TotalWriteOperations;

        // Act
        var testObject = new { Id = 1, Name = "Test Object", Data = "Sample data for write operation" };
        var pageId = await _monitoredStorage.InsertObjectAsync(txn, "test.namespace", testObject);

        // Assert
        Assert.Equal(initialWriteCount + 1, _monitoredStorage.Metrics.TotalWriteOperations);
        Assert.NotNull(pageId);
        Assert.NotEmpty(pageId);
        
        var writeStats = _monitoredStorage.Metrics.GetWriteLatencyStatistics();
        Assert.True(writeStats.SampleCount > 0);
        Assert.True(writeStats.AverageMs > 0);
        Assert.True(writeStats.AverageMs < 100); // Should be reasonably fast
        
        _output.WriteLine($"Write operation latency: {writeStats.AverageMs:F2}ms");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_ReadPageAsync_ShouldTrackReadMetrics()
    {
        // Arrange
        var txn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(txn, "test.read.namespace");
        var testObject = new { Id = 2, Name = "Read Test Object", Data = "Data for read operation testing" };
        var pageId = await _monitoredStorage.InsertObjectAsync(txn, "test.read.namespace", testObject);
        await _monitoredStorage.CommitTransactionAsync(txn);
        
        var readTxn = await _monitoredStorage.BeginTransactionAsync();
        var initialReadCount = _monitoredStorage.Metrics.TotalReadOperations;

        // Act
        var readData = await _monitoredStorage.ReadPageAsync(readTxn, "test.read.namespace", pageId);

        // Assert
        Assert.Equal(initialReadCount + 1, _monitoredStorage.Metrics.TotalReadOperations);
        Assert.NotNull(readData);
        Assert.NotEmpty(readData);
        
        var readStats = _monitoredStorage.Metrics.GetReadLatencyStatistics();
        Assert.True(readStats.SampleCount > 0);
        Assert.True(readStats.AverageMs > 0);
        Assert.True(readStats.AverageMs < 50); // Reads should be very fast
        
        _output.WriteLine($"Read operation latency: {readStats.AverageMs:F2}ms");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_CommitTransactionAsync_ShouldTrackCommitMetrics()
    {
        // Arrange
        var txn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(txn, "test.commit.namespace");
        await _monitoredStorage.InsertObjectAsync(txn, "test.commit.namespace", new { Id = 3, Data = "Commit test" });
        var initialTransactionCount = _monitoredStorage.Metrics.TotalTransactionOperations;

        // Act
        await _monitoredStorage.CommitTransactionAsync(txn);

        // Assert
        Assert.Equal(initialTransactionCount + 1, _monitoredStorage.Metrics.TotalTransactionOperations);
        
        var transactionStats = _monitoredStorage.Metrics.GetTransactionLatencyStatistics();
        Assert.True(transactionStats.SampleCount >= 2); // Begin + Commit
        Assert.True(transactionStats.AverageMs > 0);
        
        _output.WriteLine($"Transaction operations recorded: {transactionStats.SampleCount}");
        _output.WriteLine($"Average transaction latency: {transactionStats.AverageMs:F2}ms");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_ConcurrentOperations_ShouldTrackActiveOperations()
    {
        // Arrange
        var @namespace = "test.concurrent.namespace";
        var setupTxn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(setupTxn, @namespace);
        await _monitoredStorage.CommitTransactionAsync(setupTxn);

        var initialActiveOps = _monitoredStorage.Metrics.ActiveAsyncOperations;
        Assert.Equal(0, initialActiveOps);

        // Act - Start multiple concurrent operations
        var concurrentTasks = new List<Task>();
        var maxConcurrent = 5;
        var operationsPerTask = 10;

        for (int i = 0; i < maxConcurrent; i++)
        {
            var taskIndex = i;
            concurrentTasks.Add(Task.Run(async () =>
            {
                for (int j = 0; j < operationsPerTask; j++)
                {
                    var txn = await _monitoredStorage.BeginTransactionAsync();
                    await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { 
                        TaskId = taskIndex, 
                        OperationId = j, 
                        Data = $"Concurrent test data {taskIndex}_{j}" 
                    });
                    await _monitoredStorage.CommitTransactionAsync(txn);
                    
                    // Small delay to allow observation of concurrent operations
                    await Task.Delay(1);
                }
            }));
        }

        // Monitor active operations during execution
        var maxActiveObserved = 0;
        var monitoringTask = Task.Run(async () =>
        {
            while (!Task.WhenAll(concurrentTasks).IsCompleted)
            {
                var currentActive = _monitoredStorage.Metrics.ActiveAsyncOperations;
                maxActiveObserved = Math.Max(maxActiveObserved, currentActive);
                await Task.Delay(1);
            }
        });

        await Task.WhenAll(concurrentTasks);
        await monitoringTask;

        // Assert
        var finalActiveOps = _monitoredStorage.Metrics.ActiveAsyncOperations;
        var maxConcurrentRecorded = _monitoredStorage.Metrics.MaxConcurrentOperations;
        
        Assert.Equal(0, finalActiveOps); // Should return to zero after completion
        Assert.True(maxConcurrentRecorded > 0, "Should have recorded concurrent operations");
        Assert.True(maxActiveObserved > 0, "Should have observed active operations");
        
        var totalExpectedOps = maxConcurrent * operationsPerTask * 3; // Begin + Insert + Commit per operation
        Assert.True(_monitoredStorage.Metrics.TotalOperations >= totalExpectedOps);
        
        _output.WriteLine($"Max concurrent operations observed: {maxActiveObserved}");
        _output.WriteLine($"Max concurrent operations recorded: {maxConcurrentRecorded}");
        _output.WriteLine($"Total operations completed: {_monitoredStorage.Metrics.TotalOperations}");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_ErrorHandling_ShouldTrackErrorMetrics()
    {
        // Arrange
        var initialErrorCount = _monitoredStorage.Metrics.TotalErrors;

        // Act - Attempt operations that should fail
        try
        {
            // Try to read from non-existent namespace
            var invalidTxn = await _monitoredStorage.BeginTransactionAsync();
            await _monitoredStorage.ReadPageAsync(invalidTxn, "nonexistent.namespace", "999");
        }
        catch
        {
            // Expected to fail
        }

        try
        {
            // Try to insert into non-existent namespace
            var invalidTxn2 = await _monitoredStorage.BeginTransactionAsync();
            await _monitoredStorage.InsertObjectAsync(invalidTxn2, "another.nonexistent.namespace", new { Data = "test" });
        }
        catch
        {
            // Expected to fail
        }

        // Assert
        var finalErrorCount = _monitoredStorage.Metrics.TotalErrors;
        Assert.True(finalErrorCount > initialErrorCount, "Should have recorded errors");
        
        var errorStats = _monitoredStorage.Metrics.GetErrorStatistics();
        Assert.True(errorStats.TotalErrors > 0);
        Assert.NotEmpty(errorStats.ErrorsByType);
        
        _output.WriteLine($"Errors recorded: {errorStats.TotalErrors}");
        foreach (var errorType in errorStats.ErrorsByType)
        {
            _output.WriteLine($"  {errorType.Key}: {errorType.Value} occurrences");
        }
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_PerformanceSnapshot_ShouldCaptureCompleteState()
    {
        // Arrange - Perform various operations to populate metrics
        var @namespace = "test.snapshot.namespace";
        var txn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(txn, @namespace);
        
        // Multiple write operations
        for (int i = 0; i < 5; i++)
        {
            await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { Id = i, Data = $"Snapshot test {i}" });
        }
        
        await _monitoredStorage.CommitTransactionAsync(txn);
        
        // Read operations
        var readTxn = await _monitoredStorage.BeginTransactionAsync();
        var readResults = await _monitoredStorage.GetMatchingObjectsAsync(readTxn, @namespace, "*");
        await _monitoredStorage.CommitTransactionAsync(readTxn);

        // Act
        var snapshot = _monitoredStorage.Metrics.GetPerformanceSnapshot();

        // Assert
        Assert.True(snapshot.TotalOperations > 0);
        Assert.True(snapshot.TotalReadOperations > 0);
        Assert.True(snapshot.TotalWriteOperations > 0);
        Assert.True(snapshot.TotalTransactionOperations > 0);
        Assert.Equal(0, snapshot.ActiveAsyncOperations); // Should be idle
        Assert.True(snapshot.SuccessRate >= 0 && snapshot.SuccessRate <= 100);
        Assert.True(snapshot.Timestamp <= DateTime.UtcNow);
        Assert.True(snapshot.Timestamp > DateTime.UtcNow.AddMinutes(-1));
        
        _output.WriteLine($"Performance Snapshot:");
        _output.WriteLine($"  Total Operations: {snapshot.TotalOperations}");
        _output.WriteLine($"  Read Operations: {snapshot.TotalReadOperations}");
        _output.WriteLine($"  Write Operations: {snapshot.TotalWriteOperations}");
        _output.WriteLine($"  Transaction Operations: {snapshot.TotalTransactionOperations}");
        _output.WriteLine($"  Success Rate: {snapshot.SuccessRate:F2}%");
        _output.WriteLine($"  Current Throughput: {snapshot.CurrentThroughput:F2} ops/sec");
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_ThroughputMeasurement_ShouldCalculateOperationsPerSecond()
    {
        // Arrange
        var @namespace = "test.throughput.namespace";
        var setupTxn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(setupTxn, @namespace);
        await _monitoredStorage.CommitTransactionAsync(setupTxn);

        var startTime = DateTime.UtcNow;
        var operationCount = 20; // Reasonable number for throughput test

        // Act - Perform operations as quickly as possible
        var stopwatch = Stopwatch.StartNew();
        
        for (int i = 0; i < operationCount; i++)
        {
            var txn = await _monitoredStorage.BeginTransactionAsync();
            await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { 
                Id = i, 
                Data = $"Throughput test {i}",
                Timestamp = DateTime.UtcNow
            });
            await _monitoredStorage.CommitTransactionAsync(txn);
        }
        
        stopwatch.Stop();
        var endTime = DateTime.UtcNow;

        // Assert
        var actualThroughput = _monitoredStorage.Metrics.CalculateThroughput(startTime, endTime);
        var measuredThroughputFromStopwatch = (operationCount * 3.0) / stopwatch.Elapsed.TotalSeconds; // 3 ops per iteration (begin+insert+commit)
        
        Assert.True(actualThroughput > 0, "Should measure positive throughput");
        Assert.True(Math.Abs(actualThroughput - measuredThroughputFromStopwatch) < measuredThroughputFromStopwatch * 0.5, 
                   "Measured throughput should be reasonably close to calculated throughput");
        
        _output.WriteLine($"Operations performed: {operationCount} (3 storage ops each = {operationCount * 3} total)");
        _output.WriteLine($"Time elapsed: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        _output.WriteLine($"Measured throughput: {actualThroughput:F2} ops/sec");
        _output.WriteLine($"Calculated throughput: {measuredThroughputFromStopwatch:F2} ops/sec");
        
        // Epic 002 target is 200+ ops/sec - document current performance
        if (actualThroughput >= 200)
        {
            _output.WriteLine($"✅ Epic 002 throughput target ACHIEVED: {actualThroughput:F2} >= 200 ops/sec");
        }
        else
        {
            _output.WriteLine($"📊 Epic 002 throughput target progress: {actualThroughput:F2} / 200 ops/sec ({actualThroughput/200*100:F1}%)");
        }
    }

    public void Dispose()
    {
        try
        {
            _monitoredStorage?.Dispose();
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup failed - not critical for tests
        }
    }
}