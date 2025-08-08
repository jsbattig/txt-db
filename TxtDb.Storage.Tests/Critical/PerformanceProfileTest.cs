using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Critical;

/// <summary>
/// TDD Performance profiling test to identify specific bottlenecks in critical operations.
/// This test measures each step individually to guide optimization efforts.
/// 
/// TARGET PERFORMANCE METRICS:
/// - BeginTransaction: <2ms
/// - InsertObject (critical): <10ms  
/// - CommitTransaction (critical): <30ms
/// - Total end-to-end: <50ms
/// </summary>
public class PerformanceProfileTest : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public PerformanceProfileTest(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_perf_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    /// <summary>
    /// TDD TEST: Profile each step of a critical operation to identify bottlenecks.
    /// This test will fail initially and help guide the optimization implementation.
    /// </summary>
    [Fact]
    public async Task ProfileCriticalOperation_ShouldIdentifyBottlenecks()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 100, MaxDelayMs = 200 }
        });

        // Setup namespace
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "profile.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Warm-up run to exclude JIT/initialization overhead
        var warmupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.InsertObjectAsync(warmupTxn, "profile.test", new { Data = "Warmup" });
        await _asyncStorage.CommitTransactionAsync(warmupTxn, FlushPriority.Critical);

        // Act - Profile critical operation with detailed timing
        _output.WriteLine("=== CRITICAL OPERATION PERFORMANCE PROFILE ===");
        
        var totalStopwatch = Stopwatch.StartNew();
        
        // Step 1: BeginTransaction
        var beginTxnSW = Stopwatch.StartNew();
        var criticalTxn = await _asyncStorage.BeginTransactionAsync();
        beginTxnSW.Stop();
        
        // Step 2: InsertObject  
        var insertSW = Stopwatch.StartNew();
        await _asyncStorage.InsertObjectAsync(criticalTxn, "profile.test", new { 
            Data = "Critical data for performance testing",
            Timestamp = DateTime.UtcNow,
            ThreadId = Environment.CurrentManagedThreadId,
            ProcessId = Environment.ProcessId
        });
        insertSW.Stop();
        
        // Step 3: CommitTransaction with Critical Priority
        var commitSW = Stopwatch.StartNew(); 
        await _asyncStorage.CommitTransactionAsync(criticalTxn, FlushPriority.Critical);
        commitSW.Stop();
        
        totalStopwatch.Stop();

        // Report detailed timings
        _output.WriteLine($"BeginTransaction: {beginTxnSW.ElapsedMilliseconds}ms (target: <2ms)");
        _output.WriteLine($"InsertObject: {insertSW.ElapsedMilliseconds}ms (target: <10ms)");
        _output.WriteLine($"CommitTransaction: {commitSW.ElapsedMilliseconds}ms (target: <30ms)");
        _output.WriteLine($"Total: {totalStopwatch.ElapsedMilliseconds}ms (target: <50ms)");
        _output.WriteLine("==================================================");

        // Assert performance targets (these will fail initially)
        Assert.True(beginTxnSW.ElapsedMilliseconds < 2, 
            $"BeginTransaction too slow: {beginTxnSW.ElapsedMilliseconds}ms (target: <2ms)");
            
        Assert.True(insertSW.ElapsedMilliseconds < 10, 
            $"InsertObject too slow: {insertSW.ElapsedMilliseconds}ms (target: <10ms)");
            
        Assert.True(commitSW.ElapsedMilliseconds < 30, 
            $"CommitTransaction too slow: {commitSW.ElapsedMilliseconds}ms (target: <30ms)");
            
        Assert.True(totalStopwatch.ElapsedMilliseconds < 50, 
            $"Total operation too slow: {totalStopwatch.ElapsedMilliseconds}ms (target: <50ms)");
    }

    public void Dispose()
    {
        try
        {
            if (_asyncStorage is IDisposable disposable)
            {
                disposable.Dispose();
            }
            
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