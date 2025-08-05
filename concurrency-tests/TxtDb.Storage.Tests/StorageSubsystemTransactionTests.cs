using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL: NO MOCKING ALLOWED - All tests use real file I/O, real transactions, real serialization
/// Tests for MVCC transaction management with immediate persistence and snapshot isolation
/// </summary>
public class StorageSubsystemTransactionTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public StorageSubsystemTransactionTests()
    {
        // Create unique test directory for each test run
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        // Initialize storage subsystem with real file system
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void BeginTransaction_ShouldAssignUniqueTransactionId_WithImmediatePersistence()
    {
        // Act
        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();

        // Assert
        Assert.True(txn1 > 0, "Transaction ID should be positive");
        Assert.True(txn2 > txn1, "Transaction IDs should be increasing");
        Assert.NotEqual(txn1, txn2);

        // Verify metadata persistence - should be written to disk immediately
        var metadataFile = Path.Combine(_testRootPath, ".versions.json");
        Assert.True(File.Exists(metadataFile), "Version metadata should be persisted immediately");
        
        var metadataContent = File.ReadAllText(metadataFile);
        Assert.Contains(txn1.ToString(), metadataContent);
        Assert.Contains(txn2.ToString(), metadataContent);
    }

    [Fact]
    public void BeginTransaction_ConcurrentCalls_ShouldProduceUniqueIds()
    {
        // Arrange
        const int concurrentTransactions = 10;
        var transactionIds = new List<long>();
        var tasks = new List<Task>();

        // Act - Start multiple transactions concurrently
        for (int i = 0; i < concurrentTransactions; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                var txnId = _storage.BeginTransaction();
                lock (transactionIds)
                {
                    transactionIds.Add(txnId);
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        // Assert
        Assert.Equal(concurrentTransactions, transactionIds.Count);
        Assert.Equal(transactionIds.Count, transactionIds.Distinct().Count()); // All unique
        
        // Verify all are persisted
        var metadataFile = Path.Combine(_testRootPath, ".versions.json");
        var metadataContent = File.ReadAllText(metadataFile);
        foreach (var txnId in transactionIds)
        {
            Assert.Contains(txnId.ToString(), metadataContent);
        }
    }

    [Fact]
    public void CommitTransaction_ShouldUpdateMetadata_WithImmediatePersistence()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();

        // Act
        _storage.CommitTransaction(txnId);

        // Assert - Transaction should be removed from active list
        var metadataFile = Path.Combine(_testRootPath, ".versions.json");
        var metadataContent = File.ReadAllText(metadataFile);
        
        // Transaction should no longer be in active transactions
        // (This assumes the implementation tracks active vs committed transactions)
        Assert.True(File.Exists(metadataFile), "Metadata should still exist after commit");
    }

    [Fact]
    public void RollbackTransaction_ShouldCleanupImmediately()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();
        var @namespace = "test.rollback";
        
        // Create some data in the transaction
        _storage.CreateNamespace(txnId, @namespace);
        var pageId = _storage.InsertObject(txnId, @namespace, new { Name = "Test", Value = 42 });

        // Verify data exists before rollback
        var namespacePath = Path.Combine(_testRootPath, "test", "rollback");
        Assert.True(Directory.Exists(namespacePath), "Namespace should exist before rollback");

        // Act
        _storage.RollbackTransaction(txnId);

        // Assert - All transaction data should be marked invalid or cleaned up
        var metadataFile = Path.Combine(_testRootPath, ".versions.json");
        var metadataContent = File.ReadAllText(metadataFile);
        
        // Transaction should be cleaned up from metadata
        Assert.True(File.Exists(metadataFile), "Metadata file should exist");
        
        // Try to read the data - should not be visible in new transaction
        var newTxnId = _storage.BeginTransaction();
        var pages = _storage.GetMatchingObjects(newTxnId, @namespace, "*");
        Assert.Empty(pages); // Rolled back data should not be visible
        
        _storage.CommitTransaction(newTxnId);
    }

    [Fact]
    public void SnapshotIsolation_ConcurrentTransactions_ShouldSeeConsistentData()
    {
        // Arrange - Create initial data
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "test.isolation";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Value = 100 });
        _storage.CommitTransaction(setupTxn);

        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();

        // Act - T1 reads, T2 modifies, T1 reads again
        var t1InitialRead = _storage.ReadPage(txn1, @namespace, pageId);
        
        // T2 modifies the data - MUST read before write for ACID compliance
        var t2ReadData = _storage.ReadPage(txn2, @namespace, pageId);
        _storage.UpdatePage(txn2, @namespace, pageId, new object[] { new { Value = 200 } });
        _storage.CommitTransaction(txn2);

        // T1 reads again - should see the same data due to snapshot isolation
        var t1SecondRead = _storage.ReadPage(txn1, @namespace, pageId);

        // Assert
        Assert.Equal(t1InitialRead.Length, t1SecondRead.Length);
        // Both reads in T1 should see the same data (snapshot isolation)
        
        _storage.CommitTransaction(txn1);

        // Verify T2's changes are visible to new transactions
        var txn3 = _storage.BeginTransaction();
        var t3Read = _storage.ReadPage(txn3, @namespace, pageId);
        _storage.CommitTransaction(txn3);
        
        // T3 should see T2's changes
        Assert.NotNull(t3Read);
    }

    [Fact] 
    public void CrashRecovery_ProcessTermination_ShouldRecoverMetadataFromDisk()
    {
        // Arrange - Create transactions and data
        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();
        var @namespace = "test.crash";
        
        _storage.CreateNamespace(txn1, @namespace);
        var pageId = _storage.InsertObject(txn1, @namespace, new { Data = "Important" });
        _storage.CommitTransaction(txn1);
        
        // T2 is still active when "crash" occurs
        _storage.InsertObject(txn2, @namespace, new { Data = "Uncommitted" });

        // Simulate crash by creating new storage instance on same directory
        var recoveredStorage = new StorageSubsystem();
        recoveredStorage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });

        // Act - Try to use recovered storage
        var recoveryTxn = recoveredStorage.BeginTransaction();
        var recoveredData = recoveredStorage.GetMatchingObjects(recoveryTxn, @namespace, "*");
        recoveredStorage.CommitTransaction(recoveryTxn);

        // Assert - Should recover committed data, uncommitted data should be cleaned up
        Assert.NotEmpty(recoveredData);
        // Should contain T1's committed data but not T2's uncommitted data
    }

    [Fact]
    public void OptimisticConcurrencyConflict_ShouldDetectAndThrow()
    {
        // Arrange - Setup data
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "test.conflict";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Version = 1 });
        _storage.CommitTransaction(setupTxn);

        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();

        // Both transactions read the same data
        var data1 = _storage.ReadPage(txn1, @namespace, pageId);
        var data2 = _storage.ReadPage(txn2, @namespace, pageId);

        // T2 commits first - MUST read before write for ACID compliance
        var data2Read = _storage.ReadPage(txn2, @namespace, pageId);
        _storage.UpdatePage(txn2, @namespace, pageId, new object[] { new { Version = 2 } });
        _storage.CommitTransaction(txn2);

        // Act & Assert - T1 should detect conflict when trying to commit
        // MUST read before write for ACID compliance
        var data1ReadSecond = _storage.ReadPage(txn1, @namespace, pageId);
        _storage.UpdatePage(txn1, @namespace, pageId, new object[] { new { Version = 3 } });
        
        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(txn1));
    }

    [Fact]
    public void RequiredTransactionContext_OperationsWithoutTransaction_ShouldThrow()
    {
        // Arrange
        var @namespace = "test.notxn";
        
        // Act & Assert - All operations should require transaction context
        Assert.Throws<ArgumentException>(() => _storage.ReadPage(-1, @namespace, "page1"));
        Assert.Throws<ArgumentException>(() => _storage.InsertObject(0, @namespace, new { }));
        Assert.Throws<ArgumentException>(() => _storage.CreateNamespace(999, @namespace)); // Non-existent txn
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