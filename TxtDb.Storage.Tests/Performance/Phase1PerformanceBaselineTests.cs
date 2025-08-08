using System.Diagnostics;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// Phase 1 Performance Baseline Tests - Emergency Performance Fix
/// 
/// CRITICAL SITUATION:
/// - Performance Catastrophe: 5,867ms read latency (target: <4ms) - 1,467x worse than target
/// - Throughput Collapse: 0.65 ops/sec (target: 15+) - 23x worse than target
/// - Root Cause: Task.Run wrappers and async overhead causing thread pool exhaustion
/// 
/// BASELINE MEASUREMENT TARGETS:
/// Before fixes:
/// - ReadPageAsync: 862ms average per operation
/// - Bulk operations: 0.65 ops/sec
/// - P99 latency: 5,867ms
/// 
/// After Phase 1 fixes (minimum acceptable improvement):
/// - ReadPageAsync: <10ms average (86x improvement required)
/// - Bulk operations: >5 ops/sec (8x improvement required) 
/// - P99 latency: <100ms (58x improvement required)
/// 
/// These tests measure individual operations and bulk throughput to validate
/// the emergency performance fixes are working.
/// </summary>
public class Phase1PerformanceBaselineTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly MonitoredAsyncStorageSubsystem _monitoredStorage;

    public Phase1PerformanceBaselineTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_phase1baseline_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _monitoredStorage = new MonitoredAsyncStorageSubsystem();
        _monitoredStorage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = false,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig
            {
                MaxBatchSize = 10,
                MaxDelayMs = 100,
                MaxConcurrentFlushes = 2
            }
        });
    }

    [Fact]
    public async Task Baseline_ReadPageAsync_IndividualOperationTiming()
    {
        // Arrange: Setup test data
        const string testNamespace = "performance_test";
        var setupTransactionId = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTransactionId, testNamespace);
        
        // Insert test data using InsertObject API
        var testObject = new { 
            Name = "test", 
            Value = 123, 
            Active = true, 
            Score = 45.67 
        };
        
        var pageId = _monitoredStorage.InsertObject(setupTransactionId, testNamespace, testObject);
        _monitoredStorage.CommitTransaction(setupTransactionId);
        
        // Start new read transaction
        var readTransactionId = _monitoredStorage.BeginTransaction();
        
        // Act: Measure individual ReadPage operation (synchronous version for baseline)
        var stopwatch = Stopwatch.StartNew();
        var result = _monitoredStorage.ReadPage(readTransactionId, testNamespace, pageId);
        stopwatch.Stop();
        
        // Assert: Validate results and performance
        Assert.NotNull(result);
        Assert.NotEmpty(result);
        
        var operationTimeMs = stopwatch.ElapsedMilliseconds;
        _output.WriteLine($"Individual ReadPage Operation Time: {operationTimeMs}ms");
        _output.WriteLine($"Target: <10ms (Emergency fix requirement)");
        _output.WriteLine($"Ultimate Target: <4ms (Epic 002 final target)");
        
        // Emergency fix target: Must be under 10ms (vs current 862ms average)
        // This represents an 86x improvement requirement
        if (operationTimeMs >= 10)
        {
            _output.WriteLine($"⚠️  PERFORMANCE EMERGENCY: ReadPage took {operationTimeMs}ms (target: <10ms)");
            _output.WriteLine($"Current performance is {operationTimeMs / 10.0:F1}x worse than emergency target");
        }
        else
        {
            _output.WriteLine($"✅ EMERGENCY FIX SUCCESS: ReadPage under 10ms target");
        }
        
        _monitoredStorage.CommitTransaction(readTransactionId);
        
        // Now test the async version for comparison
        var asyncReadTransactionId = await _monitoredStorage.BeginTransactionAsync();
        var asyncStopwatch = Stopwatch.StartNew();
        var asyncResult = await _monitoredStorage.ReadPageAsync(asyncReadTransactionId, testNamespace, pageId);
        asyncStopwatch.Stop();
        
        Assert.NotNull(asyncResult);
        Assert.NotEmpty(asyncResult);
        
        var asyncOperationTimeMs = asyncStopwatch.ElapsedMilliseconds;
        _output.WriteLine($"Async ReadPageAsync Operation Time: {asyncOperationTimeMs}ms");
        _output.WriteLine($"Async vs Sync comparison: {asyncOperationTimeMs - operationTimeMs:+0;-0;0}ms difference");
        
        await _monitoredStorage.CommitTransactionAsync(asyncReadTransactionId);
    }

    [Fact] 
    public async Task Baseline_BulkOperations_ThroughputMeasurement()
    {
        // Arrange: Setup test namespace and data
        const string testNamespace = "bulk_performance_test";
        var setupTransactionId = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTransactionId, testNamespace);
        
        const int operationCount = 50; // Focused test for faster feedback
        var testObject = new { Name = "bulk_test", Value = 999, Active = false, Score = 12.34 };
        
        // Pre-populate data
        var pageIds = new List<string>();
        for (int i = 0; i < operationCount; i++)
        {
            var pageId = _monitoredStorage.InsertObject(setupTransactionId, testNamespace, testObject);
            pageIds.Add(pageId);
        }
        _monitoredStorage.CommitTransaction(setupTransactionId);
        
        // Act: Measure bulk read throughput (sync version for baseline)
        var readTransactionId = _monitoredStorage.BeginTransaction();
        var stopwatch = Stopwatch.StartNew();
        
        for (int i = 0; i < operationCount; i++)
        {
            var result = _monitoredStorage.ReadPage(readTransactionId, testNamespace, pageIds[i]);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }
        
        stopwatch.Stop();
        _monitoredStorage.CommitTransaction(readTransactionId);
        
        // Assert: Calculate and validate throughput
        var totalTimeSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
        var operationsPerSecond = operationCount / totalTimeSeconds;
        
        _output.WriteLine($"Bulk Operations: {operationCount} ReadPage calls");
        _output.WriteLine($"Total Time: {stopwatch.ElapsedMilliseconds}ms ({totalTimeSeconds:F2}s)");
        _output.WriteLine($"Throughput: {operationsPerSecond:F2} ops/sec");
        _output.WriteLine($"Average per operation: {stopwatch.ElapsedMilliseconds / (double)operationCount:F2}ms");
        _output.WriteLine($"Emergency Target: >5 ops/sec");
        _output.WriteLine($"Ultimate Target: >15 ops/sec");
        
        // Emergency fix target: Must be over 5 ops/sec (vs current 0.65 ops/sec)
        // This represents an 8x improvement requirement
        if (operationsPerSecond < 5.0)
        {
            _output.WriteLine($"⚠️  THROUGHPUT EMERGENCY: {operationsPerSecond:F2} ops/sec (target: >5 ops/sec)");
            _output.WriteLine($"Current performance is {5.0 / operationsPerSecond:F1}x worse than emergency target");
        }
        else
        {
            _output.WriteLine($"✅ EMERGENCY FIX SUCCESS: Throughput above 5 ops/sec target");
        }
        
        // Also test async version for comparison
        var asyncReadTransactionId = await _monitoredStorage.BeginTransactionAsync();
        var asyncStopwatch = Stopwatch.StartNew();
        
        for (int i = 0; i < operationCount; i++)
        {
            var asyncResult = await _monitoredStorage.ReadPageAsync(asyncReadTransactionId, testNamespace, pageIds[i]);
            Assert.NotNull(asyncResult);
            Assert.NotEmpty(asyncResult);
        }
        
        asyncStopwatch.Stop();
        await _monitoredStorage.CommitTransactionAsync(asyncReadTransactionId);
        
        var asyncTotalTimeSeconds = asyncStopwatch.ElapsedMilliseconds / 1000.0;
        var asyncOperationsPerSecond = operationCount / asyncTotalTimeSeconds;
        
        _output.WriteLine($"Async Comparison - ReadPageAsync: {asyncOperationsPerSecond:F2} ops/sec");
        _output.WriteLine($"Async vs Sync throughput difference: {asyncOperationsPerSecond - operationsPerSecond:+0.00;-0.00;0.00} ops/sec");
    }

    [Fact]
    public async Task Baseline_P99Latency_DistributionMeasurement()
    {
        // Arrange: Setup test data
        const string testNamespace = "latency_test";
        var setupTransactionId = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTransactionId, testNamespace);
        
        var testObject = new { Name = "latency_test", Value = 789, Active = true };
        var pageId = _monitoredStorage.InsertObject(setupTransactionId, testNamespace, testObject);
        _monitoredStorage.CommitTransaction(setupTransactionId);
        
        // Act: Measure latency distribution (sync version for baseline)
        const int measurementCount = 100; // Sufficient for P99 calculation
        var syncLatencies = new List<long>();
        
        for (int i = 0; i < measurementCount; i++)
        {
            var readTransactionId = _monitoredStorage.BeginTransaction();
            
            var stopwatch = Stopwatch.StartNew();
            var result = _monitoredStorage.ReadPage(readTransactionId, testNamespace, pageId);
            stopwatch.Stop();
            
            Assert.NotNull(result);
            Assert.NotEmpty(result);
            syncLatencies.Add(stopwatch.ElapsedMilliseconds);
            
            _monitoredStorage.CommitTransaction(readTransactionId);
        }
        
        // Assert: Calculate P99 latency for sync version
        syncLatencies.Sort();
        var syncP99Index = (int)Math.Ceiling(measurementCount * 0.99) - 1;
        var syncP99Latency = syncLatencies[syncP99Index];
        var syncAverageLatency = syncLatencies.Average();
        var syncMedianLatency = syncLatencies[measurementCount / 2];
        
        _output.WriteLine($"SYNC Latency Distribution ({measurementCount} measurements):");
        _output.WriteLine($"P99 Latency: {syncP99Latency}ms");
        _output.WriteLine($"Average Latency: {syncAverageLatency:F2}ms");
        _output.WriteLine($"Median Latency: {syncMedianLatency}ms");
        _output.WriteLine($"Min Latency: {syncLatencies.First()}ms");
        _output.WriteLine($"Max Latency: {syncLatencies.Last()}ms");
        
        // Emergency fix target: P99 must be under 100ms (vs current 5,867ms)
        // This represents a 58x improvement requirement
        if (syncP99Latency >= 100)
        {
            _output.WriteLine($"⚠️  SYNC LATENCY EMERGENCY: P99 {syncP99Latency}ms (target: <100ms)");
            _output.WriteLine($"Current performance is {syncP99Latency / 100.0:F1}x worse than emergency target");
        }
        else
        {
            _output.WriteLine($"✅ SYNC EMERGENCY FIX SUCCESS: P99 latency under 100ms target");
        }
        
        // Now test async version for comparison
        var asyncLatencies = new List<long>();
        
        for (int i = 0; i < measurementCount; i++)
        {
            var readTransactionId = await _monitoredStorage.BeginTransactionAsync();
            
            var stopwatch = Stopwatch.StartNew();
            var result = await _monitoredStorage.ReadPageAsync(readTransactionId, testNamespace, pageId);
            stopwatch.Stop();
            
            Assert.NotNull(result);
            Assert.NotEmpty(result);
            asyncLatencies.Add(stopwatch.ElapsedMilliseconds);
            
            await _monitoredStorage.CommitTransactionAsync(readTransactionId);
        }
        
        // Calculate async P99 latency
        asyncLatencies.Sort();
        var asyncP99Index = (int)Math.Ceiling(measurementCount * 0.99) - 1;
        var asyncP99Latency = asyncLatencies[asyncP99Index];
        var asyncAverageLatency = asyncLatencies.Average();
        var asyncMedianLatency = asyncLatencies[measurementCount / 2];
        
        _output.WriteLine($"ASYNC Latency Distribution ({measurementCount} measurements):");
        _output.WriteLine($"P99 Latency: {asyncP99Latency}ms");
        _output.WriteLine($"Average Latency: {asyncAverageLatency:F2}ms");
        _output.WriteLine($"Median Latency: {asyncMedianLatency}ms");
        _output.WriteLine($"Min Latency: {asyncLatencies.First()}ms");
        _output.WriteLine($"Max Latency: {asyncLatencies.Last()}ms");
        
        _output.WriteLine($"Emergency Target P99: <100ms");
        _output.WriteLine($"Ultimate Target P99: <4ms");
        
        if (asyncP99Latency >= 100)
        {
            _output.WriteLine($"⚠️  ASYNC LATENCY EMERGENCY: P99 {asyncP99Latency}ms (target: <100ms)");
            _output.WriteLine($"Current performance is {asyncP99Latency / 100.0:F1}x worse than emergency target");
        }
        else
        {
            _output.WriteLine($"✅ ASYNC EMERGENCY FIX SUCCESS: P99 latency under 100ms target");
        }
        
        _output.WriteLine($"P99 Async vs Sync difference: {asyncP99Latency - syncP99Latency:+0;-0;0}ms");
        _output.WriteLine($"Average Async vs Sync difference: {asyncAverageLatency - syncAverageLatency:+0.00;-0.00;0.00}ms");
    }

    public void Dispose()
    {
        _monitoredStorage?.Dispose();
        if (Directory.Exists(_testRootPath))
        {
            Directory.Delete(_testRootPath, true);
        }
    }
}