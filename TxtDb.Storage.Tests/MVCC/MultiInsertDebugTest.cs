using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// DEBUG TEST - Understand multiple insert behavior
/// </summary>
public class MultiInsertDebugTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public MultiInsertDebugTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_multi_debug_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void DebugTest_ThreeObjectsSeparateTransactions_ShouldWork()
    {
        // Insert each object in separate transactions
        var @namespace = "debug.separate.txn";
        
        // Transaction 1
        var txn1 = _storage.BeginTransaction();
        _storage.CreateNamespace(txn1, @namespace);
        var pageId1 = _storage.InsertObject(txn1, @namespace, new { Id = 1, Name = "Object1" });
        _storage.CommitTransaction(txn1);
        
        // Transaction 2  
        var txn2 = _storage.BeginTransaction();
        var pageId2 = _storage.InsertObject(txn2, @namespace, new { Id = 2, Name = "Object2" });
        _storage.CommitTransaction(txn2);
        
        // Transaction 3
        var txn3 = _storage.BeginTransaction();
        var pageId3 = _storage.InsertObject(txn3, @namespace, new { Id = 3, Name = "Object3" });
        _storage.CommitTransaction(txn3);

        Console.WriteLine($"PageIds: {pageId1}, {pageId2}, {pageId3}");

        // Verify
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"Found {allData.Count} pages");
        Console.WriteLine($"Total objects: {allData.Values.Sum(pages => pages.Length)}");
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        Assert.Equal(3, allData.Values.Sum(pages => pages.Length));
    }

    [Fact]
    public void DebugTest_ThreeObjectsSameTransactionStepByStep_ShouldWork()
    {
        // Insert objects in same transaction but verify state after each
        var @namespace = "debug.same.txn";
        var txn = _storage.BeginTransaction();
        _storage.CreateNamespace(txn, @namespace);
        
        // Insert first object
        var pageId1 = _storage.InsertObject(txn, @namespace, new { Id = 1, Name = "Object1" });
        Console.WriteLine($"After insert 1, pageId: {pageId1}");
        
        // Read the page to see what's there
        var data1 = _storage.ReadPage(txn, @namespace, pageId1);
        Console.WriteLine($"After insert 1, page has {data1.Length} objects");
        
        // Insert second object
        var pageId2 = _storage.InsertObject(txn, @namespace, new { Id = 2, Name = "Object2" });
        Console.WriteLine($"After insert 2, pageId: {pageId2}");
        
        // Read the page again
        var data2 = _storage.ReadPage(txn, @namespace, pageId1);
        Console.WriteLine($"After insert 2, page {pageId1} has {data2.Length} objects");
        
        if (pageId2 != pageId1)
        {
            var data2b = _storage.ReadPage(txn, @namespace, pageId2);
            Console.WriteLine($"After insert 2, page {pageId2} has {data2b.Length} objects");
        }
        
        // Insert third object
        var pageId3 = _storage.InsertObject(txn, @namespace, new { Id = 3, Name = "Object3" });
        Console.WriteLine($"After insert 3, pageId: {pageId3}");
        
        // Read all pages
        var data3a = _storage.ReadPage(txn, @namespace, pageId1);
        Console.WriteLine($"After insert 3, page {pageId1} has {data3a.Length} objects");
        
        if (pageId2 != pageId1)
        {
            var data3b = _storage.ReadPage(txn, @namespace, pageId2);
            Console.WriteLine($"After insert 3, page {pageId2} has {data3b.Length} objects");
        }
        
        if (pageId3 != pageId1 && pageId3 != pageId2)
        {
            var data3c = _storage.ReadPage(txn, @namespace, pageId3);
            Console.WriteLine($"After insert 3, page {pageId3} has {data3c.Length} objects");
        }
        
        _storage.CommitTransaction(txn);

        // Final verification
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"\nFinal result: Found {allData.Count} pages");
        Console.WriteLine($"Total objects: {allData.Values.Sum(pages => pages.Length)}");
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        Assert.Equal(3, allData.Values.Sum(pages => pages.Length));
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