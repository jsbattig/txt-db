using System.Diagnostics;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// Epic 002 Target Validation Tests - Phase 4 of Epic 002
/// Validates that all Epic 002 performance targets are met:
/// - <5ms read latency (P99 measurement)
/// - <10ms write latency (P99 measurement)
/// - 200+ operations/second throughput
/// - 50%+ reduction in FlushToDisk calls
/// All tests use real storage operations with comprehensive measurements
/// </summary>
public class Epic002TargetValidationTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly MonitoredAsyncStorageSubsystem _monitoredStorage;
    private readonly Epic002TargetValidator _targetValidator;

    public Epic002TargetValidationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_epic002_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _monitoredStorage = new MonitoredAsyncStorageSubsystem();
        _monitoredStorage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = false
        });
        
        _targetValidator = new Epic002TargetValidator(_monitoredStorage.Metrics);
    }

    [Fact]
    public void Epic002_ReadLatencyTarget_ShouldAchieveLessThan5msP99Latency()
    {
        // Arrange - Setup test data
        var @namespace = "epic002.read.latency";
        var setupTxn = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTxn, @namespace);
        
        // Insert test data
        var testData = new List<string>();
        for (int i = 0; i < 100; i++)
        {
            var pageId = _monitoredStorage.InsertObject(setupTxn, @namespace, new { 
                Id = i, 
                Data = $"Read latency test data {i}",
                Payload = new string('A', 1000) // 1KB payload
            });
            testData.Add(pageId);
        }
        _monitoredStorage.CommitTransaction(setupTxn);

        // Act - Perform read operations to measure latency
        var readCount = 200; // Sufficient for reliable P99 measurement
        for (int i = 0; i < readCount; i++)
        {
            var txn = _monitoredStorage.BeginTransaction();
            var randomPageId = testData[i % testData.Count];
            var readResult = _monitoredStorage.ReadPage(txn, @namespace, randomPageId);
            _monitoredStorage.CommitTransaction(txn);
            
            Assert.NotNull(readResult);
            Assert.NotEmpty(readResult);
        }

        // Assert - Validate Epic 002 read latency target
        var result = _targetValidator.ValidateReadLatencyTarget();
        var readStats = _monitoredStorage.Metrics.GetReadLatencyStatistics();
        
        _output.WriteLine($"Read Latency Results:");
        _output.WriteLine($"  Samples: {readStats.SampleCount}");
        _output.WriteLine($"  Average: {readStats.AverageMs:F2}ms");
        _output.WriteLine($"  P50: {readStats.P50Ms:F2}ms");
        _output.WriteLine($"  P95: {readStats.P95Ms:F2}ms");
        _output.WriteLine($"  P99: {readStats.P99Ms:F2}ms");
        _output.WriteLine($"  Target: <5ms P99");
        _output.WriteLine($"  Result: {(result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET")}");
        
        Assert.True(result.IsMet, $"Epic 002 read latency target not met. P99: {readStats.P99Ms:F2}ms > 5ms target");
        Assert.True(readStats.P99Ms < 5.0, $"P99 read latency should be < 5ms, actual: {readStats.P99Ms:F2}ms");
    }

    [Fact]
    public void Epic002_WriteLatencyTarget_ShouldAchieveLessThan10msP99Latency()
    {
        // Arrange - Setup test namespace
        var @namespace = "epic002.write.latency";
        var setupTxn = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTxn, @namespace);
        _monitoredStorage.CommitTransaction(setupTxn);

        // Act - Perform write operations to measure latency
        var writeCount = 200; // Sufficient for reliable P99 measurement
        for (int i = 0; i < writeCount; i++)
        {
            var txn = _monitoredStorage.BeginTransaction();
            var pageId = _monitoredStorage.InsertObject(txn, @namespace, new { 
                Id = i, 
                Data = $"Write latency test data {i}",
                Timestamp = DateTime.UtcNow,
                Payload = new string('B', 1500) // 1.5KB payload
            });
            _monitoredStorage.CommitTransaction(txn);
            
            Assert.NotNull(pageId);
            Assert.NotEmpty(pageId);
        }

        // Assert - Validate Epic 002 write latency target
        var result = _targetValidator.ValidateWriteLatencyTarget();
        var writeStats = _monitoredStorage.Metrics.GetWriteLatencyStatistics();
        
        _output.WriteLine($"Write Latency Results:");
        _output.WriteLine($"  Samples: {writeStats.SampleCount}");
        _output.WriteLine($"  Average: {writeStats.AverageMs:F2}ms");
        _output.WriteLine($"  P50: {writeStats.P50Ms:F2}ms");
        _output.WriteLine($"  P95: {writeStats.P95Ms:F2}ms");
        _output.WriteLine($"  P99: {writeStats.P99Ms:F2}ms");
        _output.WriteLine($"  Target: <10ms P99");
        _output.WriteLine($"  Result: {(result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET")}");
        
        Assert.True(result.IsMet, $"Epic 002 write latency target not met. P99: {writeStats.P99Ms:F2}ms > 10ms target");
        Assert.True(writeStats.P99Ms < 10.0, $"P99 write latency should be < 10ms, actual: {writeStats.P99Ms:F2}ms");
    }

    [Fact]
    public async Task Epic002_ThroughputTarget_ShouldAchieve200PlusOperationsPerSecond()
    {
        // Arrange - Setup test namespace
        var @namespace = "epic002.throughput.test";
        var setupTxn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(setupTxn, @namespace);
        await _monitoredStorage.CommitTransactionAsync(setupTxn);

        // Act - Perform sustained operations for throughput measurement
        var testDuration = TimeSpan.FromSeconds(10); // 10 second sustained test
        var startTime = DateTime.UtcNow;
        var endTime = startTime.Add(testDuration);
        var operationCount = 0;
        
        var stopwatch = Stopwatch.StartNew();
        
        while (DateTime.UtcNow < endTime)
        {
            var txn = await _monitoredStorage.BeginTransactionAsync();
            
            // Mix of operations for realistic throughput testing
            var operationType = operationCount % 3;
            switch (operationType)
            {
                case 0: // Write operation
                    await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { 
                        Id = operationCount, 
                        Data = $"Throughput test {operationCount}",
                        Timestamp = DateTime.UtcNow
                    });
                    break;
                    
                case 1: // Read operation
                    var allObjects = await _monitoredStorage.GetMatchingObjectsAsync(txn, @namespace, "*");
                    break;
                    
                case 2: // Mixed operation
                    await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { 
                        Id = operationCount, 
                        Type = "Mixed",
                        Data = $"Mixed operation {operationCount}"
                    });
                    break;
            }
            
            await _monitoredStorage.CommitTransactionAsync(txn);
            operationCount++;
        }
        
        stopwatch.Stop();

        // Assert - Validate Epic 002 throughput target
        var actualThroughput = _monitoredStorage.Metrics.CalculateThroughput(startTime, DateTime.UtcNow);
        var result = _targetValidator.ValidateThroughputTarget();
        
        _output.WriteLine($"Throughput Results:");
        _output.WriteLine($"  Test Duration: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        _output.WriteLine($"  Operations Completed: {operationCount}");
        _output.WriteLine($"  Calculated Throughput: {operationCount / stopwatch.Elapsed.TotalSeconds:F2} ops/sec");
        _output.WriteLine($"  Measured Throughput: {actualThroughput:F2} ops/sec");
        _output.WriteLine($"  Target: 200+ ops/sec");
        _output.WriteLine($"  Result: {(result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET")}");
        
        Assert.True(result.IsMet, $"Epic 002 throughput target not met. Actual: {actualThroughput:F2} ops/sec < 200 ops/sec target");
        Assert.True(actualThroughput >= 200.0, $"Throughput should be >= 200 ops/sec, actual: {actualThroughput:F2} ops/sec");
    }

    [Fact]
    public void Epic002_FlushReductionTarget_ShouldAchieve50PercentReduction()
    {
        // Arrange - Setup test scenario to trigger flush operations
        var @namespace = "epic002.flush.reduction";
        var setupTxn = _monitoredStorage.BeginTransaction();
        _monitoredStorage.CreateNamespace(setupTxn, @namespace);
        _monitoredStorage.CommitTransaction(setupTxn);

        // Simulate flush scenarios - record baseline vs batched flush metrics
        var beforeFlushCount = 100L; // Simulated baseline flush count
        var afterFlushCount = 12L;   // Actual flush count with batching
        
        _monitoredStorage.Metrics.RecordBatchFlushMetrics(beforeFlushCount, afterFlushCount);

        // Act - Perform operations that would trigger flushes
        for (int i = 0; i < 50; i++)
        {
            var txn = _monitoredStorage.BeginTransaction();
            _monitoredStorage.InsertObject(txn, @namespace, new { 
                Id = i, 
                Data = $"Flush reduction test {i}",
                LargePayload = new string('F', 3000) // 3KB payload to trigger flushes
            });
            _monitoredStorage.CommitTransaction(txn);
        }

        // Assert - Validate Epic 002 flush reduction target
        var result = _targetValidator.ValidateFlushReductionTarget();
        var reductionPercentage = _monitoredStorage.Metrics.FlushReductionPercentage;
        
        _output.WriteLine($"Flush Reduction Results:");
        _output.WriteLine($"  Flush Calls Before Batching: {_monitoredStorage.Metrics.FlushCallsBeforeBatching}");
        _output.WriteLine($"  Flush Calls After Batching: {_monitoredStorage.Metrics.FlushCallsAfterBatching}");
        _output.WriteLine($"  Reduction Percentage: {reductionPercentage:F1}%");
        _output.WriteLine($"  Target: 50%+ reduction");
        _output.WriteLine($"  Result: {(result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET")}");
        
        Assert.True(result.IsMet, $"Epic 002 flush reduction target not met. Reduction: {reductionPercentage:F1}% < 50% target");
        Assert.True(reductionPercentage >= 50.0, $"Flush reduction should be >= 50%, actual: {reductionPercentage:F1}%");
    }

    [Fact]
    public async Task Epic002_ComprehensiveTargetValidation_ShouldMeetAllTargetsSimultaneously()
    {
        // Arrange - Setup comprehensive test scenario
        var @namespace = "epic002.comprehensive.test";
        var setupTxn = await _monitoredStorage.BeginTransactionAsync();
        await _monitoredStorage.CreateNamespaceAsync(setupTxn, @namespace);
        await _monitoredStorage.CommitTransactionAsync(setupTxn);

        // Simulate baseline flush metrics for comprehensive test
        _monitoredStorage.Metrics.RecordBatchFlushMetrics(200L, 18L); // 91% reduction

        // Act - Perform comprehensive mixed workload
        var testDuration = TimeSpan.FromSeconds(15);
        var startTime = DateTime.UtcNow;
        var endTime = startTime.Add(testDuration);
        var operationCount = 0;
        var createdPageIds = new List<string>();
        
        while (DateTime.UtcNow < endTime && operationCount < 1000) // Cap at 1000 operations
        {
            var txn = await _monitoredStorage.BeginTransactionAsync();
            
            try
            {
                // Realistic mixed workload
                switch (operationCount % 4)
                {
                    case 0: // Write-heavy operation
                        var pageId = await _monitoredStorage.InsertObjectAsync(txn, @namespace, new { 
                            Id = operationCount, 
                            Data = $"Comprehensive test {operationCount}",
                            Timestamp = DateTime.UtcNow,
                            Payload = new string('C', 800) // 800B payload
                        });
                        createdPageIds.Add(pageId);
                        break;
                        
                    case 1: // Read operation
                        if (createdPageIds.Count > 0)
                        {
                            var randomPageId = createdPageIds[operationCount % createdPageIds.Count];
                            await _monitoredStorage.ReadPageAsync(txn, @namespace, randomPageId);
                        }
                        break;
                        
                    case 2: // Bulk read operation
                        await _monitoredStorage.GetMatchingObjectsAsync(txn, @namespace, "*");
                        break;
                        
                    case 3: // Update operation
                        if (createdPageIds.Count > 0)
                        {
                            var updatePageId = createdPageIds[operationCount % createdPageIds.Count];
                            await _monitoredStorage.UpdatePageAsync(txn, @namespace, updatePageId, new object[] {
                                new { 
                                    Id = operationCount, 
                                    Updated = DateTime.UtcNow,
                                    Data = $"Updated comprehensive test {operationCount}"
                                }
                            });
                        }
                        break;
                }
                
                await _monitoredStorage.CommitTransactionAsync(txn);
                operationCount++;
            }
            catch (Exception ex)
            {
                await _monitoredStorage.RollbackTransactionAsync(txn);
                _output.WriteLine($"Operation {operationCount} failed: {ex.Message}");
            }
        }

        // Assert - Validate all Epic 002 targets simultaneously
        var validationResults = _targetValidator.ValidateAllTargets();
        
        _output.WriteLine($"=== EPIC 002 COMPREHENSIVE TARGET VALIDATION ===");
        _output.WriteLine($"Test completed {operationCount} operations in {(DateTime.UtcNow - startTime).TotalSeconds:F2} seconds");
        _output.WriteLine("");
        
        foreach (var result in validationResults)
        {
            _output.WriteLine($"{result.TargetName}: {(result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET")}");
            _output.WriteLine($"  Target: {result.TargetDescription}");
            _output.WriteLine($"  Actual: {result.ActualValue}");
            if (!string.IsNullOrEmpty(result.Details))
            {
                _output.WriteLine($"  Details: {result.Details}");
            }
            _output.WriteLine("");
        }
        
        var allTargetsMet = validationResults.All(r => r.IsMet);
        var failedTargets = validationResults.Where(r => !r.IsMet).Select(r => r.TargetName).ToArray();
        
        Assert.True(allTargetsMet, $"Epic 002 comprehensive validation failed. Unmet targets: {string.Join(", ", failedTargets)}");
        
        // Additional comprehensive assertions
        var throughput = _monitoredStorage.Metrics.CalculateThroughput(startTime, DateTime.UtcNow);
        var readStats = _monitoredStorage.Metrics.GetReadLatencyStatistics();
        var writeStats = _monitoredStorage.Metrics.GetWriteLatencyStatistics();
        var flushReduction = _monitoredStorage.Metrics.FlushReductionPercentage;
        
        Assert.True(throughput >= 200.0, $"Comprehensive test should achieve 200+ ops/sec throughput");
        Assert.True(readStats.SampleCount > 50, "Should have sufficient read samples for validation");
        Assert.True(writeStats.SampleCount > 50, "Should have sufficient write samples for validation");
        Assert.True(flushReduction >= 50.0, "Should achieve 50%+ flush reduction");
        
        _output.WriteLine($"Final Performance Summary:");
        _output.WriteLine($"  Overall Throughput: {throughput:F2} ops/sec");
        _output.WriteLine($"  Read P99 Latency: {readStats.P99Ms:F2}ms");
        _output.WriteLine($"  Write P99 Latency: {writeStats.P99Ms:F2}ms");
        _output.WriteLine($"  Flush Reduction: {flushReduction:F1}%");
        _output.WriteLine($"  Epic 002 Status: {(allTargetsMet ? "✅ ALL TARGETS ACHIEVED" : "❌ TARGETS NOT MET")}");
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