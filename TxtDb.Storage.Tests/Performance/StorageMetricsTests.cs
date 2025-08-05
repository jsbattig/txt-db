using System.Diagnostics;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// TDD Tests for StorageMetrics - Phase 4 of Epic 002
/// Tests comprehensive performance monitoring and metrics collection capabilities
/// All tests use real operations and measurements, no mocking allowed
/// </summary>
public class StorageMetricsTests
{
    private readonly ITestOutputHelper _output;

    public StorageMetricsTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void StorageMetrics_Constructor_ShouldInitializeWithZeroCounters()
    {
        // Act
        var metrics = new StorageMetrics();

        // Assert
        Assert.Equal(0L, metrics.TotalOperations);
        Assert.Equal(0L, metrics.TotalReadOperations);
        Assert.Equal(0L, metrics.TotalWriteOperations);
        Assert.Equal(0L, metrics.TotalTransactionOperations);
        Assert.Equal(0L, metrics.TotalErrors);
        Assert.Equal(0L, metrics.ReadErrors);
        Assert.Equal(0L, metrics.WriteErrors);
        Assert.Equal(0L, metrics.TransactionErrors);
    }

    [Fact]
    public void StorageMetrics_RecordReadOperation_ShouldIncrementCountersAndRecordLatency()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var latency = TimeSpan.FromMilliseconds(2.5);

        // Act
        metrics.RecordReadOperation(latency, success: true);

        // Assert
        Assert.Equal(1L, metrics.TotalOperations);
        Assert.Equal(1L, metrics.TotalReadOperations);
        Assert.Equal(0L, metrics.TotalWriteOperations);
        Assert.Equal(0L, metrics.ReadErrors);
        
        // Verify latency recorded
        var readLatencyStats = metrics.GetReadLatencyStatistics();
        Assert.Equal(2.5, readLatencyStats.AverageMs, 1);
        Assert.Equal(2.5, readLatencyStats.P50Ms, 1);
        Assert.Equal(2.5, readLatencyStats.P95Ms, 1);
        Assert.Equal(2.5, readLatencyStats.P99Ms, 1);
        Assert.Equal(1, readLatencyStats.SampleCount);
    }

    [Fact]
    public void StorageMetrics_RecordReadOperation_WithError_ShouldIncrementErrorCounters()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var latency = TimeSpan.FromMilliseconds(15.0);

        // Act
        metrics.RecordReadOperation(latency, success: false);

        // Assert
        Assert.Equal(1L, metrics.TotalOperations);
        Assert.Equal(1L, metrics.TotalReadOperations);
        Assert.Equal(1L, metrics.TotalErrors);
        Assert.Equal(1L, metrics.ReadErrors);
        
        // Error latencies should still be recorded for analysis
        var readLatencyStats = metrics.GetReadLatencyStatistics();
        Assert.Equal(15.0, readLatencyStats.AverageMs, 1);
        Assert.Equal(1, readLatencyStats.SampleCount);
    }

    [Fact]
    public void StorageMetrics_RecordWriteOperation_ShouldIncrementCountersAndRecordLatency()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var latency = TimeSpan.FromMilliseconds(7.2);

        // Act
        metrics.RecordWriteOperation(latency, success: true);

        // Assert
        Assert.Equal(1L, metrics.TotalOperations);
        Assert.Equal(0L, metrics.TotalReadOperations);
        Assert.Equal(1L, metrics.TotalWriteOperations);
        Assert.Equal(0L, metrics.WriteErrors);
        
        // Verify latency recorded
        var writeLatencyStats = metrics.GetWriteLatencyStatistics();
        Assert.Equal(7.2, writeLatencyStats.AverageMs, 1);
        Assert.Equal(7.2, writeLatencyStats.P50Ms, 1);
        Assert.Equal(7.2, writeLatencyStats.P95Ms, 1);
        Assert.Equal(7.2, writeLatencyStats.P99Ms, 1);
        Assert.Equal(1, writeLatencyStats.SampleCount);
    }

    [Fact]
    public void StorageMetrics_RecordTransactionOperation_ShouldIncrementCountersAndRecordLatency()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var latency = TimeSpan.FromMilliseconds(12.8);

        // Act
        metrics.RecordTransactionOperation(latency, success: true);

        // Assert
        Assert.Equal(1L, metrics.TotalOperations);
        Assert.Equal(1L, metrics.TotalTransactionOperations);
        Assert.Equal(0L, metrics.TransactionErrors);
        
        // Verify latency recorded
        var transactionLatencyStats = metrics.GetTransactionLatencyStatistics();
        Assert.Equal(12.8, transactionLatencyStats.AverageMs, 1);
        Assert.Equal(12.8, transactionLatencyStats.P50Ms, 1);
        Assert.Equal(12.8, transactionLatencyStats.P95Ms, 1);
        Assert.Equal(12.8, transactionLatencyStats.P99Ms, 1);
        Assert.Equal(1, transactionLatencyStats.SampleCount);
    }

    [Fact]
    public void StorageMetrics_MultipleOperations_ShouldCalculateCorrectLatencyPercentiles()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var readLatencies = new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 };

        // Act - Record multiple read operations
        foreach (var latency in readLatencies)
        {
            metrics.RecordReadOperation(TimeSpan.FromMilliseconds(latency), success: true);
        }

        // Assert
        Assert.Equal(10L, metrics.TotalReadOperations);
        
        var stats = metrics.GetReadLatencyStatistics();
        Assert.Equal(5.5, stats.AverageMs, 1); // Average of 1-10
        Assert.Equal(5.5, stats.P50Ms, 1); // 50th percentile for 10 items: (5+6)/2 = 5.5
        Assert.Equal(10.0, stats.P95Ms, 1); // 95th percentile (adjusted for our percentile calculation)
        Assert.Equal(10.0, stats.P99Ms, 1); // 99th percentile (max for this sample)
        Assert.Equal(10, stats.SampleCount);
    }

    [Fact]
    public void StorageMetrics_CalculateThroughput_ShouldReturnOperationsPerSecond()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var startTime = DateTime.UtcNow;
        
        // Simulate operations over 2 seconds
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
        metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(2), success: true);
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
        metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(2), success: true);
        
        var endTime = startTime.AddSeconds(2); // Simulate 2 seconds elapsed

        // Act
        var throughput = metrics.CalculateThroughput(startTime, endTime);

        // Assert
        Assert.Equal(2.0, throughput, 1); // 4 operations / 2 seconds = 2 ops/sec
    }

    [Fact]
    public void StorageMetrics_GetCurrentThroughput_ShouldReturnRecentOperationsPerSecond()
    {
        // Arrange
        var metrics = new StorageMetrics();
        
        // Act - Record operations and check throughput
        for (int i = 0; i < 10; i++)
        {
            metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
            Thread.Sleep(50); // 50ms between operations = 20 ops/sec theoretical max
        }
        
        var currentThroughput = metrics.GetCurrentThroughput();

        // Assert
        Assert.True(currentThroughput > 0, "Should calculate non-zero throughput");
        Assert.True(currentThroughput <= 20, "Should not exceed theoretical maximum");
        _output.WriteLine($"Measured current throughput: {currentThroughput:F2} ops/sec");
    }

    [Fact]
    public void StorageMetrics_RecordBatchFlushMetrics_ShouldTrackFlushReduction()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act
        metrics.RecordBatchFlushMetrics(beforeFlushCount: 100, afterFlushCount: 12);

        // Assert
        Assert.Equal(100L, metrics.FlushCallsBeforeBatching);
        Assert.Equal(12L, metrics.FlushCallsAfterBatching);
        Assert.Equal(88.0, metrics.FlushReductionPercentage, 1); // (100-12)/100 * 100 = 88%
    }

    [Fact]
    public void StorageMetrics_GetSuccessRate_ShouldCalculateCorrectPercentage()
    {
        // Arrange
        var metrics = new StorageMetrics();
        
        // Act - Mix of successful and failed operations
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(2), success: true);
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(3), success: false);
        metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(4), success: true);
        metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(5), success: false);

        // Assert
        var successRate = metrics.GetSuccessRate();
        Assert.Equal(60.0, successRate, 1); // 3 successful out of 5 total = 60%
        
        var readSuccessRate = metrics.GetReadSuccessRate();
        Assert.Equal(66.67, readSuccessRate, 1); // 2 successful out of 3 reads = 66.67%
        
        var writeSuccessRate = metrics.GetWriteSuccessRate();
        Assert.Equal(50.0, writeSuccessRate, 1); // 1 successful out of 2 writes = 50%
    }

    [Fact]
    public void StorageMetrics_ThreadSafety_ShouldHandleConcurrentOperations()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var operationCount = 1000;
        var tasks = new List<Task>();

        // Act - Multiple threads recording operations concurrently
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < operationCount / 10; j++)
                {
                    metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
                    metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(2), success: true);
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        // Assert
        Assert.Equal(operationCount * 2L, metrics.TotalOperations); // Each iteration does read + write
        Assert.Equal(operationCount, metrics.TotalReadOperations);
        Assert.Equal(operationCount, metrics.TotalWriteOperations);
        Assert.Equal(0L, metrics.TotalErrors);
    }
}