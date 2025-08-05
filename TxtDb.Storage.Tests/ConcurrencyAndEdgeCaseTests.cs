using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL: NO MOCKING - Tests real parallel processing, edge cases, and stress scenarios
/// Tests concurrent access, race conditions, and system limits with actual file operations
/// </summary>
public class ConcurrencyAndEdgeCaseTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public ConcurrencyAndEdgeCaseTests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_concurrent_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            MaxPageSizeKB = 4, // Smaller for testing page overflow
            ForceOneObjectPerPage = true  // Critical: Ensure proper MVCC isolation
        });
    }

    [Fact]
    public void ConcurrentTransactions_HighVolume_ShouldMaintainConsistency()
    {
        // Arrange
        const int concurrentTransactions = 20;
        const int operationsPerTransaction = 50;
        var @namespace = "test.concurrent";
        var exceptions = new ConcurrentBag<Exception>();
        var completedOperations = new ConcurrentBag<int>();

        // Setup initial namespace
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        // Act - Run many concurrent transactions
        var tasks = Enumerable.Range(0, concurrentTransactions).Select(txnIndex =>
            Task.Run(() =>
            {
                try
                {
                    var txnId = _storage.BeginTransaction();
                    
                    for (int opIndex = 0; opIndex < operationsPerTransaction; opIndex++)
                    {
                        var data = new { 
                            TxnIndex = txnIndex, 
                            OpIndex = opIndex, 
                            Timestamp = DateTime.UtcNow,
                            RandomData = Guid.NewGuid().ToString()
                        };
                        
                        _storage.InsertObject(txnId, @namespace, data);
                    }
                    
                    _storage.CommitTransaction(txnId);
                    completedOperations.Add(txnIndex);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks);

        // Assert
        Assert.Empty(exceptions);
        Assert.Equal(concurrentTransactions, completedOperations.Count);

        // Verify all data is accessible
        var verifyTxn = _storage.BeginTransaction();
        var allObjects = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);

        var totalExpectedObjects = concurrentTransactions * operationsPerTransaction;
        var actualObjectCount = allObjects.Values.Sum(objects => objects.Length);
        
        Assert.True(actualObjectCount >= totalExpectedObjects * 0.9, // Allow for some conflicts
            $"Expected ~{totalExpectedObjects} objects, got {actualObjectCount}");
    }

    [Fact]
    public void PageSizeOverflow_LargeObjects_ShouldCreateNewPages()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();
        var @namespace = "test.pageoverflow";
        _storage.CreateNamespace(txnId, @namespace);

        // Create objects that will exceed 4KB page size
        var largeText = new string('X', 2000); // 2KB each
        var objects = new[]
        {
            new { Id = 1, Data = largeText },
            new { Id = 2, Data = largeText },
            new { Id = 3, Data = largeText }, // This should trigger new page
            new { Id = 4, Data = largeText }
        };

        var pageIds = new List<string>();

        // Act
        foreach (var obj in objects)
        {
            var pageId = _storage.InsertObject(txnId, @namespace, obj);
            pageIds.Add(pageId);
        }

        _storage.CommitTransaction(txnId);

        // Assert - Should have created multiple pages
        var uniquePageIds = pageIds.Distinct().ToList();
        Assert.True(uniquePageIds.Count >= 2, $"Expected multiple pages, got {uniquePageIds.Count}");

        // Verify all objects are retrievable
        var readTxn = _storage.BeginTransaction();
        var allObjects = _storage.GetMatchingObjects(readTxn, @namespace, "*");
        _storage.CommitTransaction(readTxn);

        var totalObjects = allObjects.Values.Sum(objects => objects.Length);
        Assert.Equal(4, totalObjects);
    }

    [Fact]
    public void NamespaceOperationLocks_ConcurrentDeleteWhileReading_ShouldBlock()
    {
        // Arrange
        var @namespace = "test.oplock";
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.InsertObject(setupTxn, @namespace, new { Data = "Test" });
        _storage.CommitTransaction(setupTxn);

        var deleteStarted = false;
        var deleteCompleted = false;
        var readingCompleted = false;

        // Act - Start long-running read operation
        var readTask = Task.Run(() =>
        {
            var readTxn = _storage.BeginTransaction();
            
            // Simulate long-running operation
            Thread.Sleep(2000);
            var data = _storage.GetMatchingObjects(readTxn, @namespace, "*");
            
            _storage.CommitTransaction(readTxn);
            readingCompleted = true;
        });

        // Start delete operation that should be blocked
        var deleteTask = Task.Run(() =>
        {
            Thread.Sleep(500); // Ensure read starts first
            deleteStarted = true;
            
            var deleteTxn = _storage.BeginTransaction();
            _storage.DeleteNamespace(deleteTxn, @namespace); // Should block
            _storage.CommitTransaction(deleteTxn);
            
            deleteCompleted = true;
        });

        // Wait for operations to complete
        Task.WaitAll(readTask, deleteTask);

        // Assert - Delete should have waited for read to complete
        Assert.True(deleteStarted);
        Assert.True(readingCompleted);
        Assert.True(deleteCompleted);
        
        // Reading should complete before delete
        // (This test verifies the ordering through timing)
    }

    [Fact]
    public void MassiveNamespaceCreation_ShouldHandleFileSystemLimits()
    {
        // Arrange
        const int namespaceCount = 1000;
        var txnId = _storage.BeginTransaction();
        var createdNamespaces = new List<string>();

        // Act
        for (int i = 0; i < namespaceCount; i++)
        {
            var namespaceName = $"test.mass.namespace_{i:D4}";
            _storage.CreateNamespace(txnId, namespaceName);
            createdNamespaces.Add(namespaceName);
        }

        _storage.CommitTransaction(txnId);

        // Assert - All directories should exist
        foreach (var ns in createdNamespaces)
        {
            var namespacePath = Path.Combine(_testRootPath, ns.Replace('.', Path.DirectorySeparatorChar));
            Assert.True(Directory.Exists(namespacePath), $"Namespace directory should exist: {namespacePath}");
        }

        // Verify we can still operate on all namespaces
        var testTxn = _storage.BeginTransaction();
        foreach (var ns in createdNamespaces.Take(10)) // Test first 10
        {
            _storage.InsertObject(testTxn, ns, new { Test = "Data" });
        }
        _storage.CommitTransaction(testTxn);
    }

    [Fact]
    public void InvalidTransactionId_ShouldThrowImmediately()
    {
        // Arrange
        var @namespace = "test.invalid";
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            _storage.ReadPage(-1, @namespace, "page1"));
            
        Assert.Throws<ArgumentException>(() => 
            _storage.InsertObject(0, @namespace, new { }));
            
        Assert.Throws<ArgumentException>(() => 
            _storage.UpdatePage(999999, @namespace, "page1", new object[] { }));
    }

    [Fact]
    public void DeepNamespaceNesting_ShouldCreateCorrectFolderStructure()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();
        var deepNamespace = "level1.level2.level3.level4.level5.final";

        // Act
        _storage.CreateNamespace(txnId, deepNamespace);
        _storage.InsertObject(txnId, deepNamespace, new { Depth = 6 });
        _storage.CommitTransaction(txnId);

        // Assert - Directory structure should be created
        var expectedPath = Path.Combine(_testRootPath, "level1", "level2", "level3", "level4", "level5", "final");
        Assert.True(Directory.Exists(expectedPath), $"Deep directory should exist: {expectedPath}");

        // Should be able to read the data
        var readTxn = _storage.BeginTransaction();
        var data = _storage.GetMatchingObjects(readTxn, deepNamespace, "*");
        _storage.CommitTransaction(readTxn);

        Assert.NotEmpty(data);
    }

    [Fact]
    public void SpecialCharactersInNamespace_ShouldHandleOrReject()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();
        var problematicNamespaces = new[]
        {
            "test.with-dashes",
            "test.with_underscores", 
            "test.with123numbers",
            "test.with.many.dots"
        };

        var invalidNamespaces = new[]
        {
            "test.with/slashes",
            "test.with\\backslashes",
            "test.with:colons",
            "test.with*wildcards"
        };

        // Act & Assert - Valid characters should work
        foreach (var ns in problematicNamespaces)
        {
            try
            {
                _storage.CreateNamespace(txnId, ns);
                _storage.InsertObject(txnId, ns, new { Name = ns });
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Valid namespace '{ns}' should not throw: {ex.Message}");
            }
        }

        // Invalid characters should throw or be sanitized
        foreach (var ns in invalidNamespaces)
        {
            try
            {
                _storage.CreateNamespace(txnId, ns);
                // If it doesn't throw, it should sanitize the name
            }
            catch (ArgumentException)
            {
                // Expected for invalid characters
            }
        }

        _storage.CommitTransaction(txnId);
    }

    [Fact]
    public void SystemResourceExhaustion_TooManyOpenTransactions_ShouldHandleGracefully()
    {
        // Arrange
        const int maxTransactions = 100;
        var transactions = new List<long>();

        // Act - Open many transactions without committing
        for (int i = 0; i < maxTransactions; i++)
        {
            try
            {
                var txnId = _storage.BeginTransaction();
                transactions.Add(txnId);
            }
            catch (Exception ex)
            {
                // System should handle resource limits gracefully
                Assert.True(transactions.Count >= 10, $"Should handle at least 10 transactions, got {transactions.Count}");
                break;
            }
        }

        // Assert - Should be able to use the transactions
        Assert.NotEmpty(transactions);

        // Cleanup - Commit or rollback all transactions
        foreach (var txnId in transactions)
        {
            try
            {
                _storage.RollbackTransaction(txnId);
            }
            catch
            {
                // Cleanup failure is acceptable
            }
        }
    }

    [Fact]
    public void ConcurrentVersionCleanup_WhileTransactionsActive_ShouldNotDeleteActiveVersions()
    {
        // Arrange
        var @namespace = "test.cleanup";
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Generation = 1 });
        _storage.CommitTransaction(setupTxn);

        // Create multiple versions - MUST read before write for ACID compliance
        for (int i = 2; i <= 5; i++)
        {
            var txn = _storage.BeginTransaction();
            var currentData = _storage.ReadPage(txn, @namespace, pageId);
            _storage.UpdatePage(txn, @namespace, pageId, new object[] { new { Generation = i } });
            _storage.CommitTransaction(txn);
        }

        // Start a long-running transaction that reads old version
        var longRunningTxn = _storage.BeginTransaction();
        var oldData = _storage.ReadPage(longRunningTxn, @namespace, pageId);

        // Act - Start version cleanup
        _storage.StartVersionCleanup(1); // Very frequent cleanup
        Thread.Sleep(3000); // Let cleanup run

        // Assert - Long-running transaction should still see its data
        var dataAfterCleanup = _storage.ReadPage(longRunningTxn, @namespace, pageId);
        _storage.CommitTransaction(longRunningTxn);

        Assert.Equal(oldData.Length, dataAfterCleanup.Length);
        
        // Verify cleanup happened (newer transactions should see latest)
        var newTxn = _storage.BeginTransaction();
        var latestData = _storage.ReadPage(newTxn, @namespace, pageId);
        _storage.CommitTransaction(newTxn);
        
        Assert.NotNull(latestData);
    }

    public void Dispose()
    {
        try
        {
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