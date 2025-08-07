using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using TxtDb.Storage.Services.Async;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// PHASE 3: MVCC VERSION VISIBILITY TESTS
/// Tests that fresh Storage instances can see committed data from other instances
/// without requiring MVCC bypass mechanisms.
/// 
/// CRITICAL: These tests demonstrate the core issue - fresh instances cannot see
/// committed data because their SnapshotTSN only reflects their own TSN, not
/// the maximum committed TSN across all instances.
/// </summary>
public class MVCCVersionVisibilityTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly string _sharedStoragePath;

    public MVCCVersionVisibilityTests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_visibility_{Guid.NewGuid():N}");
        _sharedStoragePath = Path.Combine(_testRootPath, "shared_storage");
        Directory.CreateDirectory(_sharedStoragePath);
    }

    [Fact]
    public void FreshInstance_ShouldSeeCommittedDataFromOtherInstance_WithoutMVCCBypass()
    {
        // This test demonstrates the CORE ISSUE of Phase 3
        // Currently FAILS because fresh instances can't see committed data from others
        
        // Arrange - Instance 1 commits data
        var instance1 = new StorageSubsystem();
        instance1.Initialize(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn1 = instance1.BeginTransaction();
        var namespaceName = "test.visibility";
        instance1.CreateNamespace(txn1, namespaceName);
        var pageId = instance1.InsertObject(txn1, namespaceName, new { Id = 1, Message = "From Instance 1" });
        instance1.CommitTransaction(txn1);
        
        Console.WriteLine($"[TEST] Instance 1 committed data with TSN: {txn1}");
        instance1.Dispose();

        // Act - Fresh Instance 2 tries to read the committed data
        var instance2 = new StorageSubsystem();
        instance2.Initialize(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn2 = instance2.BeginTransaction();
        Console.WriteLine($"[TEST] Instance 2 new transaction TSN: {txn2}");
        
        // This should see the data committed by Instance 1, but currently doesn't
        // because Instance 2's SnapshotTSN doesn't include Instance 1's committed TSN
        var readData = instance2.ReadPage(txn2, namespaceName, pageId);
        
        // Assert - Fresh instance should see committed data
        Assert.Single(readData);
        var obj = readData[0] as dynamic;
        Assert.Equal(1, obj.Id);
        Assert.Equal("From Instance 1", obj.Message);
        
        instance2.CommitTransaction(txn2);
        instance2.Dispose();
    }

    [Fact]
    public async Task FreshAsyncInstance_ShouldSeeCommittedDataFromOtherInstance_WithoutMVCCBypass()
    {
        // Same test as above but using async version
        // This also currently FAILS due to the same SnapshotTSN visibility issue
        
        // Arrange - Instance 1 commits data
        var instance1 = new AsyncStorageSubsystem();
        await instance1.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn1 = await instance1.BeginTransactionAsync();
        var namespaceName = "test.async_visibility";
        await instance1.CreateNamespaceAsync(txn1, namespaceName);
        var pageId = await instance1.InsertObjectAsync(txn1, namespaceName, new { Id = 2, Message = "From Async Instance 1" });
        await instance1.CommitTransactionAsync(txn1);
        
        Console.WriteLine($"[TEST] Async Instance 1 committed data with TSN: {txn1}");
        (instance1 as IDisposable)?.Dispose();

        // Act - Fresh Instance 2 tries to read the committed data
        var instance2 = new AsyncStorageSubsystem();
        await instance2.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn2 = await instance2.BeginTransactionAsync();
        Console.WriteLine($"[TEST] Async Instance 2 new transaction TSN: {txn2}");
        
        // This should see the data committed by Instance 1, but currently doesn't
        var readData = await instance2.ReadPageAsync(txn2, namespaceName, pageId);
        
        // Assert - Fresh instance should see committed data
        Assert.Single(readData);
        var obj = readData[0] as dynamic;
        Assert.Equal(2, obj.Id);
        Assert.Equal("From Async Instance 1", obj.Message);
        
        await instance2.CommitTransactionAsync(txn2);
        (instance2 as IDisposable)?.Dispose();
    }

    [Fact]
    public void MultipleInstances_ShouldSeeEachOthersCommittedData_InSequence()
    {
        // Test that shows multiple instances creating data sequentially
        // Each fresh instance should see ALL previously committed data
        
        var data = new[]
        {
            new { Id = 1, Instance = "First", Message = "I am the first" },
            new { Id = 2, Instance = "Second", Message = "I see the first" },
            new { Id = 3, Instance = "Third", Message = "I see first and second" }
        };

        var namespaceName = "test.sequence";
        var pageIds = new List<string>();

        // Each instance commits one object
        for (int i = 0; i < data.Length; i++)
        {
            var instance = new StorageSubsystem();
            instance.Initialize(_sharedStoragePath, new StorageConfig { 
                Format = SerializationFormat.Json,
                ForceOneObjectPerPage = true
            });

            var txn = instance.BeginTransaction();
            
            // First instance creates namespace
            if (i == 0)
            {
                instance.CreateNamespace(txn, namespaceName);
            }
            
            var pageId = instance.InsertObject(txn, namespaceName, data[i]);
            pageIds.Add(pageId);
            instance.CommitTransaction(txn);
            
            Console.WriteLine($"[TEST] Instance {i + 1} committed object with TSN: {txn}");
            instance.Dispose();
        }

        // Final fresh instance should see ALL committed data
        var finalInstance = new StorageSubsystem();
        finalInstance.Initialize(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var finalTxn = finalInstance.BeginTransaction();
        Console.WriteLine($"[TEST] Final instance transaction TSN: {finalTxn}");

        // Should be able to read all three objects
        for (int i = 0; i < pageIds.Count; i++)
        {
            var readData = finalInstance.ReadPage(finalTxn, namespaceName, pageIds[i]);
            Assert.Single(readData);
            var obj = readData[0] as dynamic;
            Assert.Equal(data[i].Id, obj.Id);
            Assert.Equal(data[i].Instance, obj.Instance);
            Assert.Equal(data[i].Message, obj.Message);
        }

        finalInstance.CommitTransaction(finalTxn);
        finalInstance.Dispose();
    }

    [Fact]
    public void FreshInstance_ShouldUseCorrectMaxCommittedTSN_ForReadCommittedIsolation()
    {
        // This test specifically verifies the fix we're implementing
        // Fresh instances should use max committed TSN as their snapshot TSN
        
        // Arrange - Instance 1 commits with high TSN
        var instance1 = new StorageSubsystem();
        instance1.Initialize(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn1 = instance1.BeginTransaction();
        var namespaceName = "test.max_committed";
        instance1.CreateNamespace(txn1, namespaceName);
        var pageId = instance1.InsertObject(txn1, namespaceName, new { CommittedTSN = txn1, Data = "High TSN Data" });
        instance1.CommitTransaction(txn1);
        
        var committedTSN = txn1;
        Console.WriteLine($"[TEST] Instance 1 committed with TSN: {committedTSN}");
        instance1.Dispose();

        // Act - Fresh Instance 2 should use max committed TSN for its snapshot
        var instance2 = new StorageSubsystem();
        instance2.Initialize(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        });

        var txn2 = instance2.BeginTransaction();
        Console.WriteLine($"[TEST] Instance 2 transaction TSN: {txn2}");
        
        // The key test: Instance 2 should see data committed by Instance 1
        // This means Instance 2's SnapshotTSN must be >= Instance 1's committed TSN
        var readData = instance2.ReadPage(txn2, namespaceName, pageId);
        
        // Assert
        Assert.Single(readData);
        var obj = readData[0] as dynamic;
        Assert.Equal(committedTSN, obj.CommittedTSN);
        Assert.Equal("High TSN Data", obj.Data);
        
        instance2.CommitTransaction(txn2);
        instance2.Dispose();
    }

    public void Dispose()
    {
        if (Directory.Exists(_testRootPath))
        {
            Directory.Delete(_testRootPath, recursive: true);
        }
    }
}