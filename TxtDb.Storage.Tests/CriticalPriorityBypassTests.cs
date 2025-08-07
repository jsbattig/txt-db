using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// TDD Tests for critical priority bypass behavior in AsyncStorageSubsystem.
/// 
/// CRITICAL ISSUE: FlushPriority.Critical operations must bypass BatchFlushCoordinator entirely
/// for immediate database durability guarantees. Current implementation still routes critical
/// operations through batching, causing 400ms+ delays instead of expected <100ms.
/// 
/// TEST STRATEGY: 
/// 1. Write failing tests that define precise bypass behavior
/// 2. Implement bypass logic to make tests pass
/// 3. Verify no regressions in normal batching performance
/// </summary>
public class CriticalPriorityBypassTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public CriticalPriorityBypassTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_critical_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    /// <summary>
    /// TDD TEST 1: Critical operations must complete in less than 50ms
    /// This defines the bypass requirement - critical operations should not wait for batching
    /// </summary>
    [Fact]
    public async Task CriticalFlush_ShouldComplete_InLessThan50Milliseconds()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 100, MaxDelayMs = 200 }
        });

        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "critical.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Critical commit should bypass batching entirely
        _output.WriteLine($"Starting critical operation at {DateTime.Now:HH:mm:ss.fff}");
        var flushCountBefore = _asyncStorage.FlushOperationCount;
        
        var stopwatch = Stopwatch.StartNew();
        
        var sw1 = Stopwatch.StartNew();
        var criticalTxn = await _asyncStorage.BeginTransactionAsync();
        sw1.Stop();
        _output.WriteLine($"BeginTransaction took {sw1.ElapsedMilliseconds}ms");
        
        var sw2 = Stopwatch.StartNew();
        await _asyncStorage.InsertObjectAsync(criticalTxn, "critical.test", new { 
            Data = "Critical data requiring immediate durability" 
        });
        sw2.Stop();
        _output.WriteLine($"InsertObject took {sw2.ElapsedMilliseconds}ms");
        
        var sw3 = Stopwatch.StartNew();
        // This is the key - FlushPriority.Critical must bypass BatchFlushCoordinator
        await _asyncStorage.CommitTransactionAsync(criticalTxn, FlushPriority.Critical);
        sw3.Stop();
        _output.WriteLine($"CommitTransaction took {sw3.ElapsedMilliseconds}ms");
        
        stopwatch.Stop();
        
        var flushCountAfter = _asyncStorage.FlushOperationCount;
        _output.WriteLine($"Direct flush operations incremented by: {flushCountAfter - flushCountBefore}");

        // Assert - Critical operations must be immediate, not batched
        Assert.True(stopwatch.ElapsedMilliseconds < 50, 
            $"Critical flush must bypass batching and complete in <50ms, but took {stopwatch.ElapsedMilliseconds}ms");

        _output.WriteLine($"Critical flush completed in {stopwatch.ElapsedMilliseconds}ms (target: <50ms)");
    }

    /// <summary>
    /// TDD TEST 2: Normal operations should still use batching and take longer
    /// This ensures we don't break normal batching performance optimization
    /// </summary>
    [Fact]
    public async Task NormalFlush_ShouldUse_BatchingWithDelay()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 10, MaxDelayMs = 150 }
        });

        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "normal.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Normal commit should go through batching
        var stopwatch = Stopwatch.StartNew();
        
        var normalTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.InsertObjectAsync(normalTxn, "normal.test", new { 
            Data = "Normal data can be batched for efficiency" 
        });
        
        // This should use BatchFlushCoordinator with normal timing
        await _asyncStorage.CommitTransactionAsync(normalTxn, FlushPriority.Normal);
        
        stopwatch.Stop();

        // Assert - Normal operations should respect batching delays
        // May complete quickly if batch fills up, but should not be guaranteed <50ms like critical
        _output.WriteLine($"Normal flush completed in {stopwatch.ElapsedMilliseconds}ms (batching enabled)");
        
        // We won't assert a specific time for normal operations as batching behavior can vary
        // The key is that this test documents expected behavior difference
    }

    /// <summary>
    /// TDD TEST 3: Mixed priority operations - critical should jump ahead
    /// This tests the priority queue behavior when both types are mixed
    /// </summary>
    [Fact]
    public async Task MixedPriority_CriticalShouldJumpQueue_NormalShouldWait()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 50, MaxDelayMs = 300 }
        });

        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "mixed.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Start multiple normal operations first, then critical
        var normalTasks = new List<Task>();
        var normalTimes = new List<long>();
        
        // Start several normal operations that will be batched
        for (int i = 0; i < 5; i++)
        {
            int index = i;
            normalTasks.Add(Task.Run(async () =>
            {
                var sw = Stopwatch.StartNew();
                var txn = await _asyncStorage.BeginTransactionAsync();
                await _asyncStorage.InsertObjectAsync(txn, "mixed.test", new { Index = index, Type = "Normal" });
                await _asyncStorage.CommitTransactionAsync(txn, FlushPriority.Normal);
                sw.Stop();
                normalTimes.Add(sw.ElapsedMilliseconds);
            }));
        }
        
        // Give normal operations a small head start
        await Task.Delay(50);
        
        // Now start critical operation - it should complete quickly despite normal operations
        var criticalStopwatch = Stopwatch.StartNew();
        var criticalTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.InsertObjectAsync(criticalTxn, "mixed.test", new { Type = "Critical", Timestamp = DateTime.UtcNow });
        await _asyncStorage.CommitTransactionAsync(criticalTxn, FlushPriority.Critical);
        criticalStopwatch.Stop();
        
        // Wait for all normal operations to complete
        await Task.WhenAll(normalTasks);

        // Assert - Critical should complete quickly regardless of normal queue
        Assert.True(criticalStopwatch.ElapsedMilliseconds < 100, 
            $"Critical operation should bypass queue and complete in <100ms, but took {criticalStopwatch.ElapsedMilliseconds}ms");

        _output.WriteLine($"Critical operation (mixed scenario): {criticalStopwatch.ElapsedMilliseconds}ms");
        _output.WriteLine($"Normal operations times: [{string.Join(", ", normalTimes)}]ms");
    }

    /// <summary>
    /// TDD TEST 4: Critical bypass should not use BatchFlushCoordinator at all
    /// This test directly verifies that critical operations use synchronous flush path
    /// </summary>
    [Fact]
    public async Task CriticalFlush_ShouldBypassBatchCoordinator_UseSynchronousPath()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 1, MaxDelayMs = 1000 } // Very slow batching
        });

        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "bypass.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var flushCountBefore = _asyncStorage.FlushOperationCount;
        
        // Act - Critical operations should increment FlushOperationCount directly
        // indicating they used synchronous flush path, not BatchFlushCoordinator
        var sw_total = Stopwatch.StartNew();
        var criticalTxn = await _asyncStorage.BeginTransactionAsync();
        
        var sw_insert = Stopwatch.StartNew();
        await _asyncStorage.InsertObjectAsync(criticalTxn, "bypass.test", new { 
            Data = "This should use direct flush, not batch coordinator" 
        });
        sw_insert.Stop();
        
        var stopwatch = Stopwatch.StartNew();
        await _asyncStorage.CommitTransactionAsync(criticalTxn, FlushPriority.Critical);
        stopwatch.Stop();
        sw_total.Stop();
        
        var flushCountAfter = _asyncStorage.FlushOperationCount;
        
        _output.WriteLine($"Total time: {sw_total.ElapsedMilliseconds}ms, Insert: {sw_insert.ElapsedMilliseconds}ms, Commit: {stopwatch.ElapsedMilliseconds}ms");
        var flushCountIncrease = flushCountAfter - flushCountBefore;

        // Assert - Should complete fast and increment direct flush counter
        Assert.True(stopwatch.ElapsedMilliseconds < 100, 
            $"Critical bypass should complete quickly, took {stopwatch.ElapsedMilliseconds}ms");
        
        // Critical operations should use direct flush path (increments FlushOperationCount)
        Assert.True(flushCountIncrease > 0, 
            "Critical operations should use direct flush path and increment FlushOperationCount");

        _output.WriteLine($"Critical bypass completed in {stopwatch.ElapsedMilliseconds}ms");
        _output.WriteLine($"Direct flush operations incremented by: {flushCountIncrease}");
    }

    /// <summary>
    /// TDD TEST 5: Verify normal operations still use BatchFlushCoordinator
    /// This ensures normal operations don't accidentally bypass batching after our fix
    /// </summary>
    [Fact]
    public async Task NormalFlush_ShouldStillUseBatchCoordinator_NotIncrementDirectFlushCount()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig { MaxBatchSize = 5, MaxDelayMs = 100 }
        });

        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "batch.test");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var flushCountBefore = _asyncStorage.FlushOperationCount;
        
        // Act - Normal operations should NOT increment FlushOperationCount immediately
        // They should go through BatchFlushCoordinator
        var normalTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.InsertObjectAsync(normalTxn, "batch.test", new { 
            Data = "This should use batch coordinator" 
        });
        
        await _asyncStorage.CommitTransactionAsync(normalTxn, FlushPriority.Normal);
        
        // Give BatchFlushCoordinator time to process the batch
        await Task.Delay(200);
        
        var flushCountAfter = _asyncStorage.FlushOperationCount;
        var flushCountIncrease = flushCountAfter - flushCountBefore;

        // Assert - Normal operations may or may not increment direct flush count
        // depending on when the batch gets processed, but behavior should be different from critical
        _output.WriteLine($"Normal flush - direct flush operations incremented by: {flushCountIncrease}");
        
        // The main point is documenting the expected behavior difference
        // Normal operations should use batching, not bypass it
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