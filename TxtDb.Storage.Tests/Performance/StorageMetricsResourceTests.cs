using System.Diagnostics;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// TDD Tests for StorageMetrics Resource Utilization and Advanced Metrics - Phase 4 of Epic 002
/// Tests resource tracking, thread pool monitoring, and batch flush optimization metrics
/// All tests use real measurements and tracking, no mocking allowed
/// </summary>
public class StorageMetricsResourceTests
{
    private readonly ITestOutputHelper _output;

    public StorageMetricsResourceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void StorageMetrics_TrackActiveAsyncOperations_ShouldMonitorConcurrentOperations()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act & Assert - Track active operations
        Assert.Equal(0, metrics.ActiveAsyncOperations);
        
        metrics.IncrementActiveOperations();
        Assert.Equal(1, metrics.ActiveAsyncOperations);
        
        metrics.IncrementActiveOperations();
        metrics.IncrementActiveOperations();
        Assert.Equal(3, metrics.ActiveAsyncOperations);
        
        metrics.DecrementActiveOperations();
        Assert.Equal(2, metrics.ActiveAsyncOperations);
        
        metrics.DecrementActiveOperations();
        metrics.DecrementActiveOperations();
        Assert.Equal(0, metrics.ActiveAsyncOperations);
        
        // Should not go below zero
        metrics.DecrementActiveOperations();
        Assert.Equal(0, metrics.ActiveAsyncOperations);
    }

    [Fact]
    public void StorageMetrics_TrackMaxConcurrentOperations_ShouldRecordPeakUsage()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act
        Assert.Equal(0, metrics.MaxConcurrentOperations);
        
        metrics.IncrementActiveOperations(); // 1
        Assert.Equal(1, metrics.MaxConcurrentOperations);
        
        metrics.IncrementActiveOperations(); // 2
        metrics.IncrementActiveOperations(); // 3
        Assert.Equal(3, metrics.MaxConcurrentOperations);
        
        metrics.DecrementActiveOperations(); // 2
        metrics.DecrementActiveOperations(); // 1
        Assert.Equal(3, metrics.MaxConcurrentOperations); // Should stay at peak
        
        metrics.IncrementActiveOperations(); // 2
        metrics.IncrementActiveOperations(); // 3
        metrics.IncrementActiveOperations(); // 4
        Assert.Equal(4, metrics.MaxConcurrentOperations); // New peak
    }

    [Fact]
    public void StorageMetrics_ThreadPoolStatus_ShouldTrackThreadPoolUtilization()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act
        metrics.UpdateThreadPoolStatus();
        var threadPoolStats = metrics.GetThreadPoolStatus();

        // Assert
        Assert.True(threadPoolStats.WorkerThreads >= 0);
        Assert.True(threadPoolStats.CompletionPortThreads >= 0);
        Assert.True(threadPoolStats.MaxWorkerThreads > 0);
        Assert.True(threadPoolStats.MaxCompletionPortThreads > 0);
        Assert.True(threadPoolStats.WorkerThreads <= threadPoolStats.MaxWorkerThreads);
        Assert.True(threadPoolStats.CompletionPortThreads <= threadPoolStats.MaxCompletionPortThreads);
        
        // Calculate utilization percentages
        var workerUtilization = (double)(threadPoolStats.MaxWorkerThreads - threadPoolStats.WorkerThreads) / threadPoolStats.MaxWorkerThreads * 100;
        var ioUtilization = (double)(threadPoolStats.MaxCompletionPortThreads - threadPoolStats.CompletionPortThreads) / threadPoolStats.MaxCompletionPortThreads * 100;
        
        Assert.True(workerUtilization >= 0 && workerUtilization <= 100);
        Assert.True(ioUtilization >= 0 && ioUtilization <= 100);
        
        _output.WriteLine($"Worker threads: {threadPoolStats.WorkerThreads}/{threadPoolStats.MaxWorkerThreads} (Available: {workerUtilization:F1}%)");
        _output.WriteLine($"I/O threads: {threadPoolStats.CompletionPortThreads}/{threadPoolStats.MaxCompletionPortThreads} (Available: {ioUtilization:F1}%)");
    }

    [Fact]
    public void StorageMetrics_MemoryUsageTracking_ShouldMonitorMemoryConsumption()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act
        metrics.UpdateMemoryUsage();
        var memoryStats = metrics.GetMemoryUsage();

        // Assert
        Assert.True(memoryStats.WorkingSetBytes > 0);
        Assert.True(memoryStats.PrivateMemoryBytes > 0);
        Assert.True(memoryStats.GC0Collections >= 0);
        Assert.True(memoryStats.GC1Collections >= 0);
        Assert.True(memoryStats.GC2Collections >= 0);
        
        _output.WriteLine($"Working Set: {memoryStats.WorkingSetBytes / 1024 / 1024:F2} MB");
        _output.WriteLine($"Private Memory: {memoryStats.PrivateMemoryBytes / 1024 / 1024:F2} MB");
        _output.WriteLine($"GC Collections - Gen0: {memoryStats.GC0Collections}, Gen1: {memoryStats.GC1Collections}, Gen2: {memoryStats.GC2Collections}");
    }

    [Fact]
    public void StorageMetrics_BatchFlushOptimization_ShouldTrackDetailedFlushMetrics()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act
        metrics.RecordBatchFlushMetrics(beforeFlushCount: 150, afterFlushCount: 18);
        metrics.RecordBatchFlushMetrics(beforeFlushCount: 200, afterFlushCount: 25);
        metrics.RecordBatchFlushMetrics(beforeFlushCount: 100, afterFlushCount: 15);

        // Assert
        Assert.Equal(450L, metrics.FlushCallsBeforeBatching); // 150 + 200 + 100
        Assert.Equal(58L, metrics.FlushCallsAfterBatching);   // 18 + 25 + 15
        
        var reductionPercentage = metrics.FlushReductionPercentage;
        var expectedReduction = (450.0 - 58.0) / 450.0 * 100.0;
        Assert.Equal(expectedReduction, reductionPercentage, 1);
        
        Assert.True(reductionPercentage > 80, "Should achieve significant flush reduction");
        _output.WriteLine($"Flush reduction: {reductionPercentage:F2}% (Target: >50%)");
    }

    [Fact]
    public void StorageMetrics_FlushLatencyTracking_ShouldMeasureFlushPerformance()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var flushLatencies = new[] { 5.2, 3.8, 7.1, 4.5, 6.3, 2.9, 8.4, 5.7 };

        // Act
        foreach (var latency in flushLatencies)
        {
            metrics.RecordFlushLatency(TimeSpan.FromMilliseconds(latency));
        }

        // Assert
        var flushStats = metrics.GetFlushLatencyStatistics();
        Assert.Equal(flushLatencies.Length, flushStats.SampleCount);
        Assert.Equal(flushLatencies.Average(), flushStats.AverageMs, 1);
        
        // Verify percentiles
        var sortedLatencies = flushLatencies.OrderBy(x => x).ToArray();
        // Calculate expected P50 for even-length array: average of two middle elements
        var expectedP50 = sortedLatencies.Length % 2 == 0 ? 
            (sortedLatencies[sortedLatencies.Length / 2 - 1] + sortedLatencies[sortedLatencies.Length / 2]) / 2.0 : 
            sortedLatencies[sortedLatencies.Length / 2];
        var p95Index = (int)(sortedLatencies.Length * 0.95);
        
        Assert.Equal(expectedP50, flushStats.P50Ms, 1);
        Assert.True(flushStats.P95Ms >= sortedLatencies[p95Index]);
        
        _output.WriteLine($"Flush latency - Avg: {flushStats.AverageMs:F2}ms, P50: {flushStats.P50Ms:F2}ms, P95: {flushStats.P95Ms:F2}ms");
    }

    [Fact]
    public void StorageMetrics_RollingThroughputWindow_ShouldTrackRecentPerformance()
    {
        // Arrange
        var metrics = new StorageMetrics();
        var stopwatch = Stopwatch.StartNew();

        // Act - Record operations with timestamps
        for (int i = 0; i < 20; i++)
        {
            metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1), success: true);
            Thread.Sleep(25); // 25ms intervals = theoretical 40 ops/sec
        }
        
        stopwatch.Stop();

        // Assert
        var rollingWindow = metrics.GetRollingThroughput(TimeSpan.FromSeconds(1)); // Last 1 second
        var overallThroughput = metrics.CalculateThroughput(DateTime.UtcNow.Subtract(stopwatch.Elapsed), DateTime.UtcNow);
        
        Assert.True(rollingWindow > 0, "Should measure rolling throughput");
        Assert.True(overallThroughput > 0, "Should measure overall throughput");
        
        _output.WriteLine($"Rolling throughput (1s window): {rollingWindow:F2} ops/sec");
        _output.WriteLine($"Overall throughput: {overallThroughput:F2} ops/sec");
        _output.WriteLine($"Theoretical max (25ms intervals): 40 ops/sec");
    }

    [Fact]
    public void StorageMetrics_ErrorCategorization_ShouldTrackErrorTypesByCategory()
    {
        // Arrange
        var metrics = new StorageMetrics();

        // Act - Record different types of errors
        metrics.RecordError("TimeoutException", "Read operation timed out");
        metrics.RecordError("IOException", "Failed to write to disk");
        metrics.RecordError("TimeoutException", "Write operation timed out");
        metrics.RecordError("InvalidOperationException", "Transaction conflict");
        metrics.RecordError("IOException", "Disk full");

        // Assert
        var errorStats = metrics.GetErrorStatistics();
        Assert.Equal(5, errorStats.TotalErrors);
        Assert.Equal(3, errorStats.ErrorsByType.Count); // 3 unique error types
        
        Assert.Equal(2, errorStats.ErrorsByType["TimeoutException"]);
        Assert.Equal(2, errorStats.ErrorsByType["IOException"]);
        Assert.Equal(1, errorStats.ErrorsByType["InvalidOperationException"]);
        
        var mostCommonError = errorStats.GetMostCommonErrorType();
        Assert.True(mostCommonError == "TimeoutException" || mostCommonError == "IOException");
        
        _output.WriteLine($"Error breakdown:");
        foreach (var kvp in errorStats.ErrorsByType)
        {
            _output.WriteLine($"  {kvp.Key}: {kvp.Value} occurrences");
        }
    }

    [Fact]
    public void StorageMetrics_PerformanceSnapshot_ShouldCaptureCompleteMetricsState()
    {
        // Arrange
        var metrics = new StorageMetrics();
        
        // Populate with sample data
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(2.3), success: true);
        metrics.RecordWriteOperation(TimeSpan.FromMilliseconds(4.7), success: true);
        metrics.RecordTransactionOperation(TimeSpan.FromMilliseconds(8.1), success: true);
        metrics.RecordReadOperation(TimeSpan.FromMilliseconds(1.8), success: false);
        metrics.RecordBatchFlushMetrics(beforeFlushCount: 50, afterFlushCount: 8);
        metrics.IncrementActiveOperations();
        metrics.IncrementActiveOperations();
        metrics.UpdateThreadPoolStatus();
        metrics.UpdateMemoryUsage();

        // Act
        var snapshot = metrics.GetPerformanceSnapshot();

        // Assert
        Assert.Equal(4L, snapshot.TotalOperations);
        Assert.Equal(2L, snapshot.TotalReadOperations);
        Assert.Equal(1L, snapshot.TotalWriteOperations);
        Assert.Equal(1L, snapshot.TotalTransactionOperations);
        Assert.Equal(1L, snapshot.TotalErrors);
        Assert.Equal(75.0, snapshot.SuccessRate, 1); // 3 successful out of 4
        Assert.Equal(84.0, snapshot.FlushReductionPercentage, 1); // (50-8)/50 * 100
        Assert.Equal(2, snapshot.ActiveAsyncOperations);
        Assert.True(snapshot.Timestamp <= DateTime.UtcNow);
        Assert.True(snapshot.Timestamp > DateTime.UtcNow.AddMinutes(-1));
        
        _output.WriteLine($"Performance snapshot captured at {snapshot.Timestamp}:");
        _output.WriteLine($"  Operations: {snapshot.TotalOperations} (Success rate: {snapshot.SuccessRate:F1}%)");
        _output.WriteLine($"  Flush reduction: {snapshot.FlushReductionPercentage:F1}%");
        _output.WriteLine($"  Active operations: {snapshot.ActiveAsyncOperations}");
    }
}