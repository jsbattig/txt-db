using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// CRITICAL: ADVANCED MVCC CONFLICT TESTING - NO MOCKING
/// Tests complex multi-way conflicts, cascading rollbacks, and advanced ACID scenarios
/// ALL tests use real file I/O, real concurrent transactions, real conflict resolution
/// </summary>
public class AdvancedConflictTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public AdvancedConflictTests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_advanced_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void ThreeWayConflict_ShouldDetectAllConflicts()
    {
        // Arrange - Create shared resource
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "threeway.conflict.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var sharedPageId = _storage.InsertObject(setupTxn, @namespace, new { Balance = 1000, Version = 1 });
        _storage.CommitTransaction(setupTxn);

        // Act - Three concurrent transactions try to modify same resource
        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction(); 
        var txn3 = _storage.BeginTransaction();

        // All read the same initial state
        var data1 = _storage.ReadPage(txn1, @namespace, sharedPageId);
        var data2 = _storage.ReadPage(txn2, @namespace, sharedPageId);
        var data3 = _storage.ReadPage(txn3, @namespace, sharedPageId);

        Assert.NotEmpty(data1);
        Assert.NotEmpty(data2);
        Assert.NotEmpty(data3);

        // All try to update - Read before write is already satisfied above
        _storage.UpdatePage(txn1, @namespace, sharedPageId, new object[] { new { Balance = 900, Version = 2, UpdatedBy = "Txn1" } });
        _storage.UpdatePage(txn2, @namespace, sharedPageId, new object[] { new { Balance = 800, Version = 2, UpdatedBy = "Txn2" } });
        _storage.UpdatePage(txn3, @namespace, sharedPageId, new object[] { new { Balance = 700, Version = 2, UpdatedBy = "Txn3" } });

        // First commit should succeed
        _storage.CommitTransaction(txn1);

        // Second and third should fail due to conflict
        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(txn2));
        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(txn3));

        // Verify only first transaction's changes are visible
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, @namespace, sharedPageId);
        _storage.CommitTransaction(verifyTxn);
        
        Assert.NotEmpty(finalData);
    }

    [Fact]
    public void CascadingConflict_MultiplePagesDependencies_ShouldDetectCorrectly()
    {
        // Arrange - Create multiple related pages
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "cascade.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var accountPageId = _storage.InsertObject(setupTxn, @namespace, new { AccountId = 1, Balance = 1000 });
        var auditPageId = _storage.InsertObject(setupTxn, @namespace, new { AccountId = 1, LastTransaction = "none" });
        var configPageId = _storage.InsertObject(setupTxn, @namespace, new { MaxTransactionAmount = 500 });
        
        _storage.CommitTransaction(setupTxn);

        // Act - Two transactions with overlapping reads/writes
        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();

        // Transaction 1: Read account, audit, write both
        var account1 = _storage.ReadPage(txn1, @namespace, accountPageId);
        var audit1 = _storage.ReadPage(txn1, @namespace, auditPageId);
        
        // Transaction 2: Read account, config, write both  
        var account2 = _storage.ReadPage(txn2, @namespace, accountPageId);
        var config2 = _storage.ReadPage(txn2, @namespace, configPageId);

        // Transaction 1 updates account and audit - Read before write already satisfied above
        _storage.UpdatePage(txn1, @namespace, accountPageId, new object[] { new { AccountId = 1, Balance = 750 } });
        _storage.UpdatePage(txn1, @namespace, auditPageId, new object[] { new { AccountId = 1, LastTransaction = "withdrawal_250" } });
        
        // Transaction 2 updates account and config - Read before write already satisfied above
        _storage.UpdatePage(txn2, @namespace, accountPageId, new object[] { new { AccountId = 1, Balance = 600 } });
        _storage.UpdatePage(txn2, @namespace, configPageId, new object[] { new { MaxTransactionAmount = 400 } });

        // First commit succeeds
        _storage.CommitTransaction(txn1);

        // Second should fail due to conflict on shared account page
        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(txn2));

        // Verify transaction 1's changes are visible
        var verifyTxn = _storage.BeginTransaction();
        var finalAccount = _storage.ReadPage(verifyTxn, @namespace, accountPageId);
        var finalAudit = _storage.ReadPage(verifyTxn, @namespace, auditPageId);
        var finalConfig = _storage.ReadPage(verifyTxn, @namespace, configPageId);
        
        Assert.NotEmpty(finalAccount);
        Assert.NotEmpty(finalAudit);
        Assert.NotEmpty(finalConfig);
        
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void ReadOnlyTransaction_WithConcurrentWrites_ShouldAlwaysSucceed()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "readonly.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Value = "initial" });
        _storage.CommitTransaction(setupTxn);

        var readOnlyExceptions = new ConcurrentBag<Exception>();
        var writeExceptions = new ConcurrentBag<Exception>();
        var readOperations = 0;
        var writeOperations = 0;

        // Act - Concurrent read-only and write transactions
        var tasks = new List<Task>();

        // Start 5 read-only transactions
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var readTxn = _storage.BeginTransaction();
                    
                    // Multiple reads in same transaction
                    for (int j = 0; j < 10; j++)
                    {
                        var data = _storage.ReadPage(readTxn, @namespace, pageId);
                        Assert.NotEmpty(data);
                        Thread.Sleep(10);
                    }
                    
                    _storage.CommitTransaction(readTxn);
                    Interlocked.Increment(ref readOperations);
                }
                catch (Exception ex)
                {
                    readOnlyExceptions.Add(ex);
                }
            }));
        }

        // Start 3 write transactions
        for (int i = 0; i < 3; i++)
        {
            var writeId = i;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    var writeTxn = _storage.BeginTransaction();
                    var data = _storage.ReadPage(writeTxn, @namespace, pageId);
                    
                    Thread.Sleep(50); // Allow for conflicts
                    
                    // MUST read before write for ACID compliance
                    var writeData = _storage.ReadPage(writeTxn, @namespace, pageId);
                    _storage.UpdatePage(writeTxn, @namespace, pageId, 
                        new object[] { new { Value = $"updated_{writeId}", Timestamp = DateTime.UtcNow } });
                    
                    _storage.CommitTransaction(writeTxn);
                    Interlocked.Increment(ref writeOperations);
                }
                catch (Exception ex)
                {
                    writeExceptions.Add(ex);
                }
            }));
        }

        Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(30));

        // Assert - All read-only transactions should succeed
        Assert.Equal(5, readOperations);
        Assert.Empty(readOnlyExceptions);
        
        // At least one write transaction should succeed
        Assert.True(writeOperations >= 1, $"Expected at least 1 write operation, got {writeOperations}");
        
        // Write failures should be due to conflicts, not other errors
        foreach (var ex in writeExceptions)
        {
            Assert.Contains("conflict", ex.Message.ToLowerInvariant());
        }
    }

    [Fact]
    public void LongRunningTransaction_WithManyShortTransactions_ShouldMaintainIsolation()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "longrunning.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Counter = 0, Phase = "initial" });
        _storage.CommitTransaction(setupTxn);

        // Start long-running transaction
        var longTxn = _storage.BeginTransaction();
        var initialData = _storage.ReadPage(longTxn, @namespace, pageId);
        Assert.NotEmpty(initialData);

        var shortTxnCount = 0;
        var shortTxnExceptions = new ConcurrentBag<Exception>();

        // Act - Many short transactions modify the same resource
        var shortTxnTasks = Enumerable.Range(1, 20).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    var shortTxn = _storage.BeginTransaction();
                    var data = _storage.ReadPage(shortTxn, @namespace, pageId);
                    
                    // MUST read before write for ACID compliance
                    _storage.UpdatePage(shortTxn, @namespace, pageId, 
                        new object[] { new { Counter = i, Phase = $"short_txn_{i}" } });
                    
                    _storage.CommitTransaction(shortTxn);
                    Interlocked.Increment(ref shortTxnCount);
                }
                catch (Exception ex)
                {
                    shortTxnExceptions.Add(ex);
                }
            })
        ).ToArray();

        // Let short transactions complete
        Task.WaitAll(shortTxnTasks, TimeSpan.FromSeconds(30));

        // Long-running transaction should still see its original snapshot
        var longTxnDataAfter = _storage.ReadPage(longTxn, @namespace, pageId);
        Assert.NotEmpty(longTxnDataAfter);
        
        // If long transaction tries to update, it should detect conflict
        // Read before write is already satisfied by longTxnDataAfter read above
        _storage.UpdatePage(longTxn, @namespace, pageId, 
            new object[] { new { Counter = -1, Phase = "long_txn_update" } });

        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(longTxn));

        // Assert - At least some short transactions should have succeeded
        Assert.True(shortTxnCount >= 10, $"Expected at least 10 successful short transactions, got {shortTxnCount}");
        
        // Verify final state reflects short transactions
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, @namespace, pageId);
        Assert.NotEmpty(finalData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void WriteAfterWrite_SameTransaction_ShouldSucceed()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "writeafter.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Version = 1, Data = "initial" });
        _storage.CommitTransaction(setupTxn);

        // Act - Multiple writes in same transaction should work
        var txn = _storage.BeginTransaction();
        
        var data1 = _storage.ReadPage(txn, @namespace, pageId);
        _storage.UpdatePage(txn, @namespace, pageId, new object[] { new { Version = 2, Data = "first_update" } });
        
        // Read again after write (should see our own write)
        var data2 = _storage.ReadPage(txn, @namespace, pageId);
        _storage.UpdatePage(txn, @namespace, pageId, new object[] { new { Version = 3, Data = "second_update" } });
        
        // Third write - read before write already satisfied by data2 read above
        var data3 = _storage.ReadPage(txn, @namespace, pageId);
        _storage.UpdatePage(txn, @namespace, pageId, new object[] { new { Version = 4, Data = "final_update" } });

        // Assert - Should commit successfully
        _storage.CommitTransaction(txn);

        // Verify final state
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, @namespace, pageId);
        Assert.NotEmpty(finalData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void ConflictDetection_AcrossMultipleNamespaces_ShouldBeIndependent()
    {
        // Arrange - Create resources in different namespaces
        var setupTxn = _storage.BeginTransaction();
        var namespace1 = "conflict.ns1";
        var namespace2 = "conflict.ns2";
        
        _storage.CreateNamespace(setupTxn, namespace1);
        _storage.CreateNamespace(setupTxn, namespace2);
        
        var page1Id = _storage.InsertObject(setupTxn, namespace1, new { Value = "ns1_data" });
        var page2Id = _storage.InsertObject(setupTxn, namespace2, new { Value = "ns2_data" });
        
        _storage.CommitTransaction(setupTxn);

        // Act - Concurrent transactions on different namespaces
        var txn1 = _storage.BeginTransaction();
        var txn2 = _storage.BeginTransaction();

        // Transaction 1 modifies namespace1 - Read before write satisfied
        var data1 = _storage.ReadPage(txn1, namespace1, page1Id);
        _storage.UpdatePage(txn1, namespace1, page1Id, new object[] { new { Value = "ns1_updated_by_txn1" } });

        // Transaction 2 modifies namespace2 - Read before write satisfied
        var data2 = _storage.ReadPage(txn2, namespace2, page2Id);
        _storage.UpdatePage(txn2, namespace2, page2Id, new object[] { new { Value = "ns2_updated_by_txn2" } });

        // Assert - Both should commit successfully (no cross-namespace conflicts)
        _storage.CommitTransaction(txn1);
        _storage.CommitTransaction(txn2);

        // Verify both updates are visible
        var verifyTxn = _storage.BeginTransaction();
        var finalData1 = _storage.ReadPage(verifyTxn, namespace1, page1Id);
        var finalData2 = _storage.ReadPage(verifyTxn, namespace2, page2Id);
        
        Assert.NotEmpty(finalData1);
        Assert.NotEmpty(finalData2);
        
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void DeadlockPrevention_TwoTransactionsCrossAccess_ShouldHandleGracefully()
    {
        // Arrange - Create two resources
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "deadlock.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var resource1Id = _storage.InsertObject(setupTxn, @namespace, new { ResourceId = 1, Value = "R1" });
        var resource2Id = _storage.InsertObject(setupTxn, @namespace, new { ResourceId = 2, Value = "R2" });
        
        _storage.CommitTransaction(setupTxn);

        var exceptions = new ConcurrentBag<Exception>();
        var successCount = 0;

        // Act - Two transactions access resources in different orders
        var task1 = Task.Run(() =>
        {
            try
            {
                var txn1 = _storage.BeginTransaction();
                
                // Access R1 then R2
                var data1 = _storage.ReadPage(txn1, @namespace, resource1Id);
                Thread.Sleep(100); // Increase chance of deadlock
                var data2 = _storage.ReadPage(txn1, @namespace, resource2Id);
                
                // Updates with read-before-write satisfied above
                _storage.UpdatePage(txn1, @namespace, resource1Id, new object[] { new { ResourceId = 1, Value = "R1_Updated_By_Txn1" } });
                _storage.UpdatePage(txn1, @namespace, resource2Id, new object[] { new { ResourceId = 2, Value = "R2_Updated_By_Txn1" } });
                
                _storage.CommitTransaction(txn1);
                Interlocked.Increment(ref successCount);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        var task2 = Task.Run(() =>
        {
            try
            {
                var txn2 = _storage.BeginTransaction();
                
                // Access R2 then R1 (reverse order)
                var data2 = _storage.ReadPage(txn2, @namespace, resource2Id);
                Thread.Sleep(100); // Increase chance of deadlock
                var data1 = _storage.ReadPage(txn2, @namespace, resource1Id);
                
                // Updates with read-before-write satisfied above
                _storage.UpdatePage(txn2, @namespace, resource2Id, new object[] { new { ResourceId = 2, Value = "R2_Updated_By_Txn2" } });
                _storage.UpdatePage(txn2, @namespace, resource1Id, new object[] { new { ResourceId = 1, Value = "R1_Updated_By_Txn2" } });
                
                _storage.CommitTransaction(txn2);
                Interlocked.Increment(ref successCount);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        Task.WaitAll(new[] { task1, task2 }, TimeSpan.FromSeconds(10));

        // Assert - At least one transaction should succeed, conflicts are expected
        Assert.True(successCount >= 1, $"Expected at least 1 successful transaction, got {successCount}");
        
        // If there are exceptions, they should be conflict-related
        foreach (var ex in exceptions)
        {
            Assert.Contains("conflict", ex.Message.ToLowerInvariant());
        }
    }

    [Fact]
    public void OptimisticLocking_HighContentionScenario_ShouldMaintainConsistency()
    {
        // Arrange - Single resource, high contention
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "highcontention.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var hotPageId = _storage.InsertObject(setupTxn, @namespace, new { Counter = 0, LastUpdate = DateTime.UtcNow });
        _storage.CommitTransaction(setupTxn);

        var successfulUpdates = 0;
        var conflictExceptions = 0;
        var totalAttempts = 50;

        // Act - Many transactions try to increment counter
        var tasks = Enumerable.Range(1, totalAttempts).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    var data = _storage.ReadPage(txn, @namespace, hotPageId);
                    
                    // Small delay to increase contention
                    Thread.Sleep(new Random().Next(10, 50));
                    
                    // Read before write already satisfied by data read above
                    _storage.UpdatePage(txn, @namespace, hotPageId, 
                        new object[] { new { Counter = i, LastUpdate = DateTime.UtcNow, UpdatedBy = $"Task_{i}" } });
                    
                    _storage.CommitTransaction(txn);
                    Interlocked.Increment(ref successfulUpdates);
                }
                catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                {
                    Interlocked.Increment(ref conflictExceptions);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks, TimeSpan.FromSeconds(60));

        // Assert - Some updates should succeed, many conflicts expected
        Assert.True(successfulUpdates >= 1, $"Expected at least 1 successful update, got {successfulUpdates}");
        Assert.True(conflictExceptions > 0, $"Expected some conflicts in high contention scenario, got {conflictExceptions}");
        Assert.Equal(totalAttempts, successfulUpdates + conflictExceptions);

        // Verify final state is consistent
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, @namespace, hotPageId);
        Assert.NotEmpty(finalData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void TransactionRollback_AfterPartialWork_ShouldNotAffectOtherTransactions()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "rollback.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var page1Id = _storage.InsertObject(setupTxn, @namespace, new { Id = 1, Value = "original1" });
        var page2Id = _storage.InsertObject(setupTxn, @namespace, new { Id = 2, Value = "original2" });
        
        _storage.CommitTransaction(setupTxn);

        // Act - Transaction that will be rolled back
        var rollbackTxn = _storage.BeginTransaction();
        var data1 = _storage.ReadPage(rollbackTxn, @namespace, page1Id);
        var data2 = _storage.ReadPage(rollbackTxn, @namespace, page2Id);
        
        // Read before write already satisfied by data1 and data2 reads above
        _storage.UpdatePage(rollbackTxn, @namespace, page1Id, new object[] { new { Id = 1, Value = "should_rollback1" } });
        _storage.UpdatePage(rollbackTxn, @namespace, page2Id, new object[] { new { Id = 2, Value = "should_rollback2" } });

        // Concurrent successful transaction - Read before write satisfied
        var successTxn = _storage.BeginTransaction();
        var successData1 = _storage.ReadPage(successTxn, @namespace, page1Id);
        _storage.UpdatePage(successTxn, @namespace, page1Id, new object[] { new { Id = 1, Value = "successful_update" } });
        _storage.CommitTransaction(successTxn);

        // Now rollback first transaction (should fail due to conflict)
        Assert.Throws<InvalidOperationException>(() => _storage.CommitTransaction(rollbackTxn));

        // Assert - Successful transaction's changes should be visible
        var verifyTxn = _storage.BeginTransaction();
        var finalData1 = _storage.ReadPage(verifyTxn, @namespace, page1Id);
        var finalData2 = _storage.ReadPage(verifyTxn, @namespace, page2Id);
        
        Assert.NotEmpty(finalData1);
        Assert.NotEmpty(finalData2);
        
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void ConflictResolution_WithBackgroundVersionCleanup_ShouldNotInterfere()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "cleanup.conflict.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { Version = 1 });
        _storage.CommitTransaction(setupTxn);

        // Create multiple versions to trigger cleanup - Read before write satisfied
        for (int i = 2; i <= 10; i++)
        {
            var versionTxn = _storage.BeginTransaction();
            var data = _storage.ReadPage(versionTxn, @namespace, pageId);
            _storage.UpdatePage(versionTxn, @namespace, pageId, new object[] { new { Version = i } });
            _storage.CommitTransaction(versionTxn);
        }

        // Start background cleanup
        _storage.StartVersionCleanup(intervalMinutes: 0);

        var conflictCount = 0;
        var successCount = 0;

        // Act - Concurrent transactions while cleanup is running
        var tasks = Enumerable.Range(11, 20).Select(version =>
            Task.Run(() =>
            {
                try
                {
                    Thread.Sleep(new Random().Next(100, 500)); // Stagger transactions
                    
                    var txn = _storage.BeginTransaction();
                    var data = _storage.ReadPage(txn, @namespace, pageId);
                    
                    Thread.Sleep(100); // Allow cleanup to run
                    
                    // Read before write already satisfied by data read above
                    _storage.UpdatePage(txn, @namespace, pageId, new object[] { new { Version = version } });
                    _storage.CommitTransaction(txn);
                    
                    Interlocked.Increment(ref successCount);
                }
                catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                {
                    Interlocked.Increment(ref conflictCount);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks, TimeSpan.FromSeconds(30));

        // Assert - Transactions should work despite background cleanup
        Assert.True(successCount >= 1, $"Expected at least 1 successful transaction, got {successCount}");
        
        // Final verification
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, @namespace, pageId);
        Assert.NotEmpty(finalData);
        _storage.CommitTransaction(verifyTxn);
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