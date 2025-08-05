using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// Simple demonstration of how to fix the stress test data loss issue
/// </summary>
public class SimpleFixedStressTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public SimpleFixedStressTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_simple_fixed_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void SimpleFixed_UpdateWithProperPattern_NoDataLoss()
    {
        // Setup
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "simple.fixed.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create 20 objects - they'll all go to page001
        for (int i = 0; i < 20; i++)
        {
            _storage.InsertObject(setupTxn, @namespace, new { 
                Id = i, 
                Balance = 1000 
            });
        }
        _storage.CommitTransaction(setupTxn);

        // Verify initial state
        var verifyTxn = _storage.BeginTransaction();
        var initialData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        Assert.Equal(20, initialData.Values.Sum(pages => pages.Length));
        var pageId = initialData.Keys.First(); // "page001"

        // Update with PROPER pattern
        var updateTxn = _storage.BeginTransaction();
        
        // 1. READ entire page
        var currentContent = _storage.ReadPage(updateTxn, @namespace, pageId);
        Assert.Equal(20, currentContent.Length);
        
        // 2. MODIFY what we need while preserving everything
        var modifiedContent = new List<object>();
        foreach (var obj in currentContent)
        {
            dynamic item = obj;
            // Update balance for objects 5-9
            if ((int)item.Id >= 5 && (int)item.Id <= 9)
            {
                modifiedContent.Add(new {
                    Id = (int)item.Id,
                    Balance = 2000,
                    Updated = true
                });
            }
            else
            {
                // PRESERVE unchanged objects
                modifiedContent.Add(obj);
            }
        }
        
        // 3. WRITE entire modified content back
        _storage.UpdatePage(updateTxn, @namespace, pageId, modifiedContent.ToArray());
        _storage.CommitTransaction(updateTxn);

        // Verify NO data loss
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        _storage.CommitTransaction(finalTxn);
        
        Assert.Equal(20, finalData.Values.Sum(pages => pages.Length)); // All objects preserved!
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
            // Cleanup failed - not critical
        }
    }
}
