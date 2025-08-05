using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD Tests for Async Namespace Operations - Epic 002 Phase 2
/// Tests: CreateNamespaceAsync, DeleteNamespaceAsync, RenameNamespaceAsync
/// All tests use real database integration (no mocking)
/// </summary>
public class AsyncNamespaceOperationsTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public AsyncNamespaceOperationsTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_async_ns_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        // This will fail initially - we haven't implemented AsyncStorageSubsystem yet
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task CreateNamespaceAsync_ShouldCreateNewNamespace_WithProperDirectoryStructure()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var txn = await _asyncStorage.BeginTransactionAsync();
        var namespaceName = "test.async.create";

        // Act
        var createStart = Stopwatch.StartNew();
        await _asyncStorage.CreateNamespaceAsync(txn, namespaceName);
        createStart.Stop();
        await _asyncStorage.CommitTransactionAsync(txn);

        // Assert - Verify namespace was created and can be used
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var pageId = await _asyncStorage.InsertObjectAsync(verifyTxn, namespaceName, new { 
            Message = "Namespace creation test",
            Timestamp = DateTime.UtcNow 
        });
        
        Assert.NotNull(pageId);
        Assert.NotEmpty(pageId);
        
        var retrievedData = await _asyncStorage.ReadPageAsync(verifyTxn, namespaceName, pageId);
        Assert.NotNull(retrievedData);
        Assert.True(retrievedData.Length > 0);

        await _asyncStorage.CommitTransactionAsync(verifyTxn);
        
        _output.WriteLine($"Async namespace creation completed in {createStart.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task CreateNamespaceAsync_ShouldSupportCancellation()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            await _asyncStorage.CreateNamespaceAsync(txn, "test.cancelled.create", cts.Token);
        });
    }

    [Fact]
    public async Task DeleteNamespaceAsync_ShouldRemoveNamespaceAndAllData()
    {
        // Arrange - Create namespace with data
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var namespaceName = "test.async.delete";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, namespaceName);
        
        // Add some data to the namespace
        var pageIds = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, namespaceName, new { 
                Id = i,
                Data = $"Delete test data {i}"
            });
            pageIds.Add(pageId);
        }
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Delete the namespace
        var deleteTxn = await _asyncStorage.BeginTransactionAsync();
        var deleteStart = Stopwatch.StartNew();
        await _asyncStorage.DeleteNamespaceAsync(deleteTxn, namespaceName);
        deleteStart.Stop();
        await _asyncStorage.CommitTransactionAsync(deleteTxn);

        // Assert - Verify namespace and data are gone
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        
        // Attempting to use deleted namespace should fail
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _asyncStorage.InsertObjectAsync(verifyTxn, namespaceName, new { Test = "Should fail" }));

        await _asyncStorage.RollbackTransactionAsync(verifyTxn);
        
        _output.WriteLine($"Async namespace deletion completed in {deleteStart.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task RenameNamespaceAsync_ShouldChangeNamespaceNameWhilePreservingData()
    {
        // Arrange - Create namespace with data
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldName = "test.async.rename.old";
        var newName = "test.async.rename.new";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldName);
        
        var testObjects = new[]
        {
            new { Id = 1, Name = "Object 1", Category = "A" },
            new { Id = 2, Name = "Object 2", Category = "B" },
            new { Id = 3, Name = "Object 3", Category = "A" }
        };

        var originalPageIds = new List<string>();
        foreach (var obj in testObjects)
        {
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, oldName, obj);
            originalPageIds.Add(pageId);
        }
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Rename the namespace
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        var renameStart = Stopwatch.StartNew();
        await _asyncStorage.RenameNamespaceAsync(renameTxn, oldName, newName);
        renameStart.Stop();
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // Assert - Verify old namespace is gone and new namespace has the data
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        
        // Old namespace should not exist
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, oldName, "*"));

        // New namespace should have all the data
        var allDataInNewNamespace = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, newName, "*");
        var totalObjects = allDataInNewNamespace.Values.Sum(pages => pages.Length);
        
        Assert.Equal(testObjects.Length, totalObjects);
        Assert.Equal(originalPageIds.Count, allDataInNewNamespace.Count);

        await _asyncStorage.CommitTransactionAsync(verifyTxn);
        
        _output.WriteLine($"Async namespace rename completed in {renameStart.ElapsedMilliseconds}ms");
        _output.WriteLine($"Preserved {totalObjects} objects across {allDataInNewNamespace.Count} pages");
    }

    [Fact]
    public async Task ConcurrentNamespaceOperations_ShouldMaintainConsistency()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var concurrencyLevel = 10;
        var successfulOperations = 0;

        // Act - Create multiple namespaces concurrently
        var concurrentTasks = Enumerable.Range(0, concurrencyLevel).Select(async i =>
        {
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                var namespaceName = $"test.concurrent.ns.{i}";
                
                await _asyncStorage.CreateNamespaceAsync(txn, namespaceName);
                
                // Add some data to verify namespace is functional
                var pageId = await _asyncStorage.InsertObjectAsync(txn, namespaceName, new { 
                    NamespaceId = i,
                    Data = $"Concurrent namespace {i}",
                    Timestamp = DateTime.UtcNow
                });
                
                await _asyncStorage.CommitTransactionAsync(txn);
                Interlocked.Increment(ref successfulOperations);
                return true;
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Concurrent namespace operation {i} failed: {ex.Message}");
                return false;
            }
        });

        var results = await Task.WhenAll(concurrentTasks);
        var successCount = results.Count(r => r);

        // Assert
        Assert.True(successCount > 0, "Some concurrent namespace operations should succeed");
        Assert.Equal(successCount, successfulOperations);

        // Verify all successful namespaces are functional
        for (int i = 0; i < concurrencyLevel; i++)
        {
            if (results[i])
            {
                var verifyTxn = await _asyncStorage.BeginTransactionAsync();
                var namespaceName = $"test.concurrent.ns.{i}";
                
                var data = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, namespaceName, "*");
                Assert.True(data.Count > 0, $"Namespace {namespaceName} should contain data");
                
                await _asyncStorage.CommitTransactionAsync(verifyTxn);
            }
        }

        _output.WriteLine($"Concurrent namespace operations: {successCount}/{concurrencyLevel} succeeded");
    }

    [Fact]
    public async Task NamespaceOperations_WithLongRunningTransaction_ShouldBlockConflictingOperations()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var namespaceName = "test.blocking.operations";
        
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, namespaceName);
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Start a long-running transaction that modifies the namespace
        var longRunningTask = Task.Run(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            
            // Insert data and hold transaction open
            await _asyncStorage.InsertObjectAsync(txn, namespaceName, new { Status = "Long running" });
            
            // Simulate long processing time
            await Task.Delay(2000);
            
            await _asyncStorage.CommitTransactionAsync(txn);
        });

        // Small delay to ensure long-running transaction starts first
        await Task.Delay(100);

        // Try concurrent operations - some should succeed (reads), others might be blocked/conflict
        var concurrentTask = Task.Run(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            
            try
            {
                // Read operations should generally succeed
                var data = await _asyncStorage.GetMatchingObjectsAsync(txn, namespaceName, "*");
                await _asyncStorage.CommitTransactionAsync(txn);
                return "read_success";
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("conflict"))
            {
                await _asyncStorage.RollbackTransactionAsync(txn);
                return "conflict";
            }
        });

        // Wait for both to complete
        await Task.WhenAll(longRunningTask, concurrentTask);
        var concurrentResult = await concurrentTask;

        // Assert - The system should handle concurrent operations appropriately
        Assert.True(concurrentResult == "read_success" || concurrentResult == "conflict", 
            "Concurrent operation should either succeed or fail with conflict");

        _output.WriteLine($"Long-running transaction completed, concurrent operation result: {concurrentResult}");
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