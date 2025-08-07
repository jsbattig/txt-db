using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// TDD Tests to isolate and fix critical ACID concurrency violations.
/// These tests identify the root cause of the failing ConcurrentObjectOperations test.
/// CRITICAL: Real database operations only (no mocking) to expose actual data integrity issues.
/// </summary>
public class ConcurrencyIsolationTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public ConcurrencyIsolationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_isolation_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task TwoConcurrentInserts_ShouldBothPersist_NoDataLoss()
    {
        // CRITICAL TDD TEST: This tests the most basic concurrency scenario
        // If this fails, the MVCC implementation has fundamental issues
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.two.concurrent");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var successCount = 0;
        var taskResults = new List<bool>();

        // Act - Two concurrent transactions
        var task1 = Task.Run(async () =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.two.concurrent", new { 
                    Id = 1, 
                    Thread = "Task1", 
                    Data = "First concurrent insert" 
                });
                
                // Verify we can read it back in the same transaction
                var readBack = await _asyncStorage.ReadPageAsync(txn, "test.two.concurrent", pageId);
                Assert.NotEmpty(readBack);
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successCount);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Task1 failed: {ex.Message}");
                return false;
            }
        });

        var task2 = Task.Run(async () =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.two.concurrent", new { 
                    Id = 2, 
                    Thread = "Task2", 
                    Data = "Second concurrent insert" 
                });
                
                // Verify we can read it back in the same transaction
                var readBack = await _asyncStorage.ReadPageAsync(txn, "test.two.concurrent", pageId);
                Assert.NotEmpty(readBack);
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successCount);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Task2 failed: {ex.Message}");
                return false;
            }
        });

        var results = await Task.WhenAll(task1, task2);
        
        // Assert - Both transactions should succeed
        Assert.True(results[0], "Task1 should succeed");
        Assert.True(results[1], "Task2 should succeed");
        Assert.Equal(2, successCount);

        // CRITICAL: Verify both objects are persisted and accessible
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.two.concurrent", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        Assert.Equal(2, totalObjects); // BOTH objects must be persisted

        _output.WriteLine($"SUCCESS: Both concurrent inserts persisted. Found {totalObjects} objects.");
    }

    [Fact]
    public async Task FiveConcurrentInserts_ShouldAllPersist_NoDataLoss()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.five.concurrent");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var successCount = 0;
        var concurrencyLevel = 5;

        // Act - Five concurrent transactions
        var tasks = Enumerable.Range(1, concurrencyLevel).Select(async taskId =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.five.concurrent", new { 
                    Id = taskId, 
                    Thread = $"Task{taskId}", 
                    Data = $"Concurrent insert #{taskId}",
                    Timestamp = DateTime.UtcNow
                });
                
                // Verify we can read it back in the same transaction
                var readBack = await _asyncStorage.ReadPageAsync(txn, "test.five.concurrent", pageId);
                Assert.NotEmpty(readBack);
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successCount);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Task{taskId} failed: {ex.Message}");
                return false;
            }
        });

        var results = await Task.WhenAll(tasks);
        
        // Assert - All transactions should succeed
        var successfulTasks = results.Count(r => r);
        Assert.Equal(concurrencyLevel, successfulTasks);
        Assert.Equal(concurrencyLevel, successCount);

        // CRITICAL: Verify all objects are persisted and accessible
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.five.concurrent", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        Assert.Equal(concurrencyLevel, totalObjects); // ALL objects must be persisted

        _output.WriteLine($"SUCCESS: All {concurrencyLevel} concurrent inserts persisted. Found {totalObjects} objects.");
    }

    [Fact]
    public async Task TenConcurrentInserts_ShouldAllPersist_NoDataLoss()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.ten.concurrent");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var successCount = 0;
        var concurrencyLevel = 10;

        // Act - Ten concurrent transactions
        var tasks = Enumerable.Range(1, concurrencyLevel).Select(async taskId =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.ten.concurrent", new { 
                    Id = taskId, 
                    Thread = $"Task{taskId}", 
                    Data = $"Concurrent insert #{taskId}",
                    Timestamp = DateTime.UtcNow,
                    Payload = new string((char)('A' + (taskId % 26)), 50) // Unique payload
                });
                
                // Verify we can read it back in the same transaction
                var readBack = await _asyncStorage.ReadPageAsync(txn, "test.ten.concurrent", pageId);
                Assert.NotEmpty(readBack);
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successCount);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Task{taskId} failed: {ex.Message}");
                return false;
            }
        });

        var results = await Task.WhenAll(tasks);
        
        // Assert - All transactions should succeed
        var successfulTasks = results.Count(r => r);
        Assert.Equal(concurrencyLevel, successfulTasks);
        Assert.Equal(concurrencyLevel, successCount);

        // CRITICAL: Verify all objects are persisted and accessible
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.ten.concurrent", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        Assert.Equal(concurrencyLevel, totalObjects); // ALL objects must be persisted

        _output.WriteLine($"SUCCESS: All {concurrencyLevel} concurrent inserts persisted. Found {totalObjects} objects.");
    }

    [Fact]
    public async Task TwentyConcurrentInserts_ShouldAllPersist_NoDataLoss()
    {
        // This replicates the failing original test conditions
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.twenty.concurrent");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var successCount = 0;
        var concurrencyLevel = 20;
        var sw = Stopwatch.StartNew();

        // Act - Twenty concurrent transactions
        var tasks = Enumerable.Range(1, concurrencyLevel).Select(async taskId =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.twenty.concurrent", new { 
                    Id = taskId, 
                    Thread = $"Task{taskId}", 
                    Data = $"Concurrent insert #{taskId}",
                    Timestamp = DateTime.UtcNow,
                    Payload = new string((char)('A' + (taskId % 26)), 100) // Unique payload per task
                });
                
                // Verify we can read it back in the same transaction
                var readBack = await _asyncStorage.ReadPageAsync(txn, "test.twenty.concurrent", pageId);
                Assert.NotEmpty(readBack);
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successCount);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Task{taskId} failed: {ex.Message}");
                return false;
            }
        });

        var results = await Task.WhenAll(tasks);
        sw.Stop();
        
        // Assert - All transactions should succeed
        var successfulTasks = results.Count(r => r);
        _output.WriteLine($"Completed {concurrencyLevel} concurrent operations in {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"Successful tasks: {successfulTasks}, Success count: {successCount}");

        Assert.Equal(concurrencyLevel, successfulTasks);
        Assert.Equal(concurrencyLevel, successCount);

        // CRITICAL: Verify all objects are persisted and accessible
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.twenty.concurrent", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        _output.WriteLine($"Final verification: Found {totalObjects} persisted objects out of {concurrencyLevel} expected");
        
        Assert.Equal(concurrencyLevel, totalObjects); // ALL objects must be persisted

        _output.WriteLine($"SUCCESS: All {concurrencyLevel} concurrent inserts persisted. Found {totalObjects} objects.");
    }

    [Fact]
    public async Task ConcurrentTransactionCommitOrder_ShouldMaintainIsolation()
    {
        // This tests if there are race conditions in the commit process itself
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.commit.order");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var commitResults = new List<(int TaskId, bool Success, Exception? Error)>();
        var commitLock = new object();

        // Act - Create transactions first, then commit them all simultaneously
        var transactions = new List<(int TaskId, long TxnId, string PageId)>();
        
        // Phase 1: Create all transactions and perform inserts
        for (int i = 1; i <= 10; i++)
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.commit.order", new { 
                Id = i, 
                Data = $"Transaction {i}" 
            });
            transactions.Add((i, txn, pageId));
        }

        // Phase 2: Commit all transactions simultaneously
        var commitTasks = transactions.Select(async t =>
        {
            try
            {
                await _asyncStorage.CommitTransactionAsync(t.TxnId);
                lock (commitLock)
                {
                    commitResults.Add((t.TaskId, true, null));
                }
                return true;
            }
            catch (Exception ex)
            {
                lock (commitLock)
                {
                    commitResults.Add((t.TaskId, false, ex));
                }
                _output.WriteLine($"Transaction {t.TaskId} commit failed: {ex.Message}");
                return false;
            }
        });

        await Task.WhenAll(commitTasks);

        // Assert
        var successfulCommits = commitResults.Count(r => r.Success);
        _output.WriteLine($"Successful commits: {successfulCommits}/10");
        
        foreach (var (taskId, success, error) in commitResults.Where(r => !r.Success))
        {
            _output.WriteLine($"Transaction {taskId} failed: {error?.Message}");
        }

        // Verify data integrity
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.commit.order", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        _output.WriteLine($"Final objects persisted: {totalObjects}");

        // CRITICAL: All successful commits should have their data persisted
        Assert.Equal(successfulCommits, totalObjects);
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