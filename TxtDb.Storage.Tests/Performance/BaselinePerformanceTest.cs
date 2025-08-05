using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Performance;

/// <summary>
/// BASELINE PERFORMANCE TESTS - TDD Implementation for Epic 002
/// Measures current system performance to establish improvement targets:
/// - Current: ~5.5 ops/sec (from Epic documentation)
/// - Target: 200+ ops/sec (40x improvement)
/// 
/// ALL tests use real file I/O, no mocking, comprehensive measurements.
/// This establishes the performance baseline before async optimizations.
/// </summary>
public class BaselinePerformanceTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly StorageSubsystem _storage;
    private readonly ITestOutputHelper _output;

    public BaselinePerformanceTest(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_baseline_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = false  // Allow multiple objects per page for realistic testing
        });
    }

    [Fact]
    public void BaselinePerformance_SingleThreaded_ReadWriteOperations_ShouldMeasureCurrentThroughput()
    {
        // Arrange - Setup test namespace
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "baseline.singlethread.perf";
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        var operationCount = 0;
        var readOperations = 0;
        var writeOperations = 0;
        var stopwatch = new Stopwatch();

        // Act - Measure single-threaded performance for 30 seconds
        var testDuration = TimeSpan.FromSeconds(30);
        var endTime = DateTime.UtcNow.Add(testDuration);
        
        stopwatch.Start();
        
        while (DateTime.UtcNow < endTime)
        {
            try
            {
                var txn = _storage.BeginTransaction();
                
                // Write operation
                var pageId = _storage.InsertObject(txn, @namespace, new { 
                    Id = operationCount,
                    Data = $"Baseline test data {operationCount}",
                    Timestamp = DateTime.UtcNow,
                    Payload = new string('X', 1000) // 1KB payload per object
                });
                writeOperations++;
                
                // Read operation - read what we just wrote
                var readData = _storage.ReadPage(txn, @namespace, pageId);
                if (readData.Length > 0)
                {
                    readOperations++;
                }
                
                _storage.CommitTransaction(txn);
                operationCount += 2; // One write + one read
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Operation failed: {ex.Message}");
                // Continue testing despite individual failures
            }
        }
        
        stopwatch.Stop();

        // Calculate metrics
        var totalSeconds = stopwatch.Elapsed.TotalSeconds;
        var throughput = operationCount / totalSeconds;
        var avgOperationTime = stopwatch.Elapsed.TotalMilliseconds / operationCount;

        // Log detailed results
        _output.WriteLine($"=== BASELINE SINGLE-THREADED PERFORMANCE ===");
        _output.WriteLine($"Test Duration: {totalSeconds:F2} seconds");
        _output.WriteLine($"Total Operations: {operationCount}");
        _output.WriteLine($"Write Operations: {writeOperations}");
        _output.WriteLine($"Read Operations: {readOperations}");
        _output.WriteLine($"Throughput: {throughput:F2} ops/sec");
        _output.WriteLine($"Average Operation Time: {avgOperationTime:F2}ms");

        // Assert - Document current performance baseline
        Assert.True(operationCount > 0, "Should complete at least some operations");
        Assert.True(throughput > 0, "Should achieve some throughput");
        
        // Document the current performance for improvement tracking
        // Epic 002 states current performance is ~5.5 ops/sec, target is 200+ ops/sec
        _output.WriteLine($"Current baseline throughput: {throughput:F2} ops/sec");
        _output.WriteLine($"Target throughput: 200+ ops/sec");
        _output.WriteLine($"Required improvement factor: {200.0 / throughput:F2}x");
        
        // Test should pass regardless of current performance - this establishes baseline
        Assert.True(true, "Baseline test completed successfully");
    }

    [Fact]
    public void BaselinePerformance_ConcurrentOperations_ShouldMeasureMultiThreadedThroughput()
    {
        // Arrange - Setup test namespace
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "baseline.concurrent.perf";
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        var totalOperations = 0;
        var successfulOperations = 0;
        var conflictedOperations = 0;
        var exceptions = new ConcurrentBag<Exception>();
        var stopwatch = new Stopwatch();

        // Act - Measure concurrent performance with multiple threads
        var threadCount = Environment.ProcessorCount;
        var testDuration = TimeSpan.FromSeconds(60);
        var endTime = DateTime.UtcNow.Add(testDuration);
        
        stopwatch.Start();
        
        var concurrentTasks = Enumerable.Range(0, threadCount).Select(threadId =>
            Task.Run(async () =>
            {
                var threadOperations = 0;
                var random = new Random(threadId);
                
                while (DateTime.UtcNow < endTime)
                {
                    try
                    {
                        var txn = _storage.BeginTransaction();
                        
                        // Mixed read/write operations
                        var operationType = random.Next(0, 3);
                        
                        switch (operationType)
                        {
                            case 0: // Insert operation
                                var pageId = _storage.InsertObject(txn, @namespace, new { 
                                    ThreadId = threadId,
                                    Operation = threadOperations,
                                    Data = $"Concurrent test data {threadId}_{threadOperations}",
                                    Timestamp = DateTime.UtcNow,
                                    Payload = new string('Y', 500) // 500B payload
                                });
                                break;
                                
                            case 1: // Read operation
                                var allData = _storage.GetMatchingObjects(txn, @namespace, "*");
                                // Process read results to simulate real work
                                var objectCount = allData.Values.Sum(pages => pages.Length);
                                break;
                                
                            case 2: // Update operation (if data exists)
                                var existingData = _storage.GetMatchingObjects(txn, @namespace, "*");
                                if (existingData.Count > 0)
                                {
                                    var randomPageId = existingData.Keys.Skip(random.Next(existingData.Count)).First();
                                    var currentData = _storage.ReadPage(txn, @namespace, randomPageId);
                                    
                                    // Append new data to existing page
                                    var updatedContent = currentData.ToList();
                                    updatedContent.Add(new { 
                                        ThreadId = threadId,
                                        UpdateOperation = threadOperations,
                                        Timestamp = DateTime.UtcNow 
                                    });
                                    
                                    _storage.UpdatePage(txn, @namespace, randomPageId, updatedContent.ToArray());
                                }
                                break;
                        }
                        
                        _storage.CommitTransaction(txn);
                        Interlocked.Increment(ref successfulOperations);
                        threadOperations++;
                        
                        // Small delay to prevent tight loop
                        await Task.Delay(1);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                    {
                        Interlocked.Increment(ref conflictedOperations);
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                    
                    Interlocked.Increment(ref totalOperations);
                }
            })
        ).ToArray();
        
        Task.WaitAll(concurrentTasks);
        stopwatch.Stop();

        // Calculate metrics
        var totalSeconds = stopwatch.Elapsed.TotalSeconds;
        var throughput = totalOperations / totalSeconds;
        var successRate = (double)successfulOperations / totalOperations * 100;

        // Log detailed results
        _output.WriteLine($"=== BASELINE CONCURRENT PERFORMANCE ===");
        _output.WriteLine($"Test Duration: {totalSeconds:F2} seconds");
        _output.WriteLine($"Thread Count: {threadCount}");
        _output.WriteLine($"Total Operations: {totalOperations}");
        _output.WriteLine($"Successful Operations: {successfulOperations}");
        _output.WriteLine($"Conflicted Operations: {conflictedOperations}");
        _output.WriteLine($"Exceptions: {exceptions.Count}");
        _output.WriteLine($"Success Rate: {successRate:F2}%");
        _output.WriteLine($"Throughput: {throughput:F2} ops/sec");
        _output.WriteLine($"Successful Throughput: {successfulOperations / totalSeconds:F2} ops/sec");

        // Assert - Document concurrent performance
        Assert.True(totalOperations > 0, "Should complete operations");
        Assert.True(successfulOperations > 0, "Should have successful operations");
        Assert.True(exceptions.Count < totalOperations * 0.1, "Exception rate should be < 10%");
        
        // Document concurrent performance vs target
        var effectiveThroughput = successfulOperations / totalSeconds;
        _output.WriteLine($"Effective concurrent throughput: {effectiveThroughput:F2} ops/sec");
        _output.WriteLine($"Target throughput: 200+ ops/sec");
        _output.WriteLine($"Required improvement factor: {200.0 / effectiveThroughput:F2}x");
    }

    [Fact]
    public void BaselinePerformance_LatencyMeasurement_ShouldMeasureOperationLatencies()
    {
        // Arrange - Setup test namespace
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "baseline.latency.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        var readLatencies = new List<double>();
        var writeLatencies = new List<double>();
        var commitLatencies = new List<double>();
        var operationCount = 0;

        // Act - Measure individual operation latencies
        var testDuration = TimeSpan.FromSeconds(30);
        var endTime = DateTime.UtcNow.Add(testDuration);
        
        while (DateTime.UtcNow < endTime && operationCount < 1000)
        {
            try
            {
                // Measure transaction begin + write latency
                var txnStartTime = Stopwatch.StartNew();
                var txn = _storage.BeginTransaction();
                
                var writeStartTime = Stopwatch.StartNew();
                var pageId = _storage.InsertObject(txn, @namespace, new { 
                    Id = operationCount,
                    Data = $"Latency test {operationCount}",
                    Timestamp = DateTime.UtcNow
                });
                writeStartTime.Stop();
                writeLatencies.Add(writeStartTime.Elapsed.TotalMilliseconds);
                
                // Measure read latency
                var readStartTime = Stopwatch.StartNew();
                var readData = _storage.ReadPage(txn, @namespace, pageId);
                readStartTime.Stop();
                readLatencies.Add(readStartTime.Elapsed.TotalMilliseconds);
                
                // Measure commit latency
                var commitStartTime = Stopwatch.StartNew();
                _storage.CommitTransaction(txn);
                commitStartTime.Stop();
                commitLatencies.Add(commitStartTime.Elapsed.TotalMilliseconds);
                
                operationCount++;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Latency test operation failed: {ex.Message}");
            }
        }

        // Calculate latency statistics
        var avgReadLatency = readLatencies.Average();
        var avgWriteLatency = writeLatencies.Average();
        var avgCommitLatency = commitLatencies.Average();
        
        var p95ReadLatency = readLatencies.OrderBy(x => x).Skip((int)(readLatencies.Count * 0.95)).FirstOrDefault();
        var p95WriteLatency = writeLatencies.OrderBy(x => x).Skip((int)(writeLatencies.Count * 0.95)).FirstOrDefault();
        var p95CommitLatency = commitLatencies.OrderBy(x => x).Skip((int)(commitLatencies.Count * 0.95)).FirstOrDefault();

        // Log detailed latency results
        _output.WriteLine($"=== BASELINE LATENCY MEASUREMENTS ===");
        _output.WriteLine($"Sample Size: {operationCount} operations");
        _output.WriteLine($"Average Read Latency: {avgReadLatency:F2}ms");
        _output.WriteLine($"Average Write Latency: {avgWriteLatency:F2}ms");
        _output.WriteLine($"Average Commit Latency: {avgCommitLatency:F2}ms");
        _output.WriteLine($"95th Percentile Read Latency: {p95ReadLatency:F2}ms");
        _output.WriteLine($"95th Percentile Write Latency: {p95WriteLatency:F2}ms");
        _output.WriteLine($"95th Percentile Commit Latency: {p95CommitLatency:F2}ms");

        // Assert - Document latency baselines vs targets
        Assert.True(readLatencies.Count > 0, "Should measure read latencies");
        Assert.True(writeLatencies.Count > 0, "Should measure write latencies");
        Assert.True(commitLatencies.Count > 0, "Should measure commit latencies");

        // Epic 002 targets: <5ms read, <10ms write, <15ms commit
        _output.WriteLine($"=== LATENCY TARGETS COMPARISON ===");
        _output.WriteLine($"Current Read Latency: {avgReadLatency:F2}ms (Target: <5ms)");
        _output.WriteLine($"Current Write Latency: {avgWriteLatency:F2}ms (Target: <10ms)");
        _output.WriteLine($"Current Commit Latency: {avgCommitLatency:F2}ms (Target: <15ms)");
        
        var readImprovement = avgReadLatency / 5.0;
        var writeImprovement = avgWriteLatency / 10.0;
        var commitImprovement = avgCommitLatency / 15.0;
        
        _output.WriteLine($"Read latency improvement needed: {readImprovement:F2}x");
        _output.WriteLine($"Write latency improvement needed: {writeImprovement:F2}x");
        _output.WriteLine($"Commit latency improvement needed: {commitImprovement:F2}x");
    }

    [Fact]
    public void BaselinePerformance_FlushToDiskFrequency_ShouldMeasureCurrentFlushBehavior()
    {
        // Arrange - Setup test namespace
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "baseline.flush.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        var operationCount = 0;
        var stopwatch = Stopwatch.StartNew();

        // Act - Perform operations and monitor file system activity
        // Note: This test measures implied flush behavior through timing patterns
        var operationTimes = new List<double>();
        
        for (int i = 0; i < 100; i++)
        {
            var operationStart = Stopwatch.StartNew();
            
            var txn = _storage.BeginTransaction();
            var pageId = _storage.InsertObject(txn, @namespace, new { 
                Id = i,
                Data = $"Flush behavior test {i}",
                LargePayload = new string('Z', 5000) // 5KB to trigger potential flushes
            });
            _storage.CommitTransaction(txn);
            
            operationStart.Stop();
            operationTimes.Add(operationStart.Elapsed.TotalMilliseconds);
            operationCount++;
        }
        
        stopwatch.Stop();

        // Analyze timing patterns that might indicate flush behavior
        var avgOperationTime = operationTimes.Average();
        var maxOperationTime = operationTimes.Max();
        var minOperationTime = operationTimes.Min();
        var stdDeviation = Math.Sqrt(operationTimes.Select(x => Math.Pow(x - avgOperationTime, 2)).Average());

        // Count operations that took significantly longer (potential flush operations)
        var flushThreshold = avgOperationTime + (2 * stdDeviation);
        var potentialFlushOperations = operationTimes.Count(t => t > flushThreshold);

        // Log flush behavior analysis
        _output.WriteLine($"=== BASELINE FLUSH BEHAVIOR ANALYSIS ===");
        _output.WriteLine($"Total Operations: {operationCount}");
        _output.WriteLine($"Average Operation Time: {avgOperationTime:F2}ms");
        _output.WriteLine($"Min Operation Time: {minOperationTime:F2}ms");
        _output.WriteLine($"Max Operation Time: {maxOperationTime:F2}ms");
        _output.WriteLine($"Standard Deviation: {stdDeviation:F2}ms");
        _output.WriteLine($"Potential Flush Operations: {potentialFlushOperations} ({(double)potentialFlushOperations/operationCount*100:F1}%)");
        _output.WriteLine($"Flush Threshold: {flushThreshold:F2}ms");

        // Assert - Document current flush patterns
        Assert.True(operationTimes.Count == operationCount, "Should measure all operations");
        
        // Epic 002 goal: Reduce FlushToDisk calls by 50%
        _output.WriteLine($"=== FLUSH OPTIMIZATION TARGET ===");
        _output.WriteLine($"Current potential flush rate: {(double)potentialFlushOperations/operationCount*100:F1}%");
        _output.WriteLine($"Target flush reduction: 50%");
        _output.WriteLine($"Expected post-optimization flush rate: {(double)potentialFlushOperations/operationCount*50:F1}%");
    }

    public void Dispose()
    {
        try
        {
            _storage.Dispose();
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