using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// ROOT CAUSE ANALYSIS - Understanding why UpdatePage causes data loss
/// </summary>
public class DataLossRootCauseTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public DataLossRootCauseTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_root_cause_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void RootCause_UpdatePageReplacesEntireContent_DataLoss()
    {
        // Arrange - Create multiple objects on same page
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "root.cause.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Insert 5 objects - they'll likely go to the same page
        var pageIds = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var pageId = _storage.InsertObject(setupTxn, @namespace, new { 
                Id = i, 
                Name = $"Object_{i}",
                Value = i * 100
            });
            pageIds.Add(pageId);
        }
        _storage.CommitTransaction(setupTxn);
        
        // Verify initial state
        var verifyTxn1 = _storage.BeginTransaction();
        var initialData = _storage.GetMatchingObjects(verifyTxn1, @namespace, "*");
        _storage.CommitTransaction(verifyTxn1);
        
        Assert.Equal(5, initialData.Values.Sum(pages => pages.Length));
        Assert.Single(initialData); // All objects on one page
        var thePageId = initialData.Keys.First();
        
        // Act - Update the page with ONLY ONE object (this is the problem\!)
        var updateTxn = _storage.BeginTransaction();
        
        // Read the page first (required by ACID)
        var currentContent = _storage.ReadPage(updateTxn, @namespace, thePageId);
        Assert.Equal(5, currentContent.Length); // Page has 5 objects
        
        // Update with ONLY ONE object - this REPLACES the entire page\!
        _storage.UpdatePage(updateTxn, @namespace, thePageId, new object[] { 
            new { 
                Id = 999,
                Name = "Single_Updated_Object",
                Value = 999,
                Note = "This single object REPLACES all 5 objects!"
            } 
        });
        
        _storage.CommitTransaction(updateTxn);
        
        // Assert - Verify data loss
        var verifyTxn2 = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(verifyTxn2, @namespace, "*");
        _storage.CommitTransaction(verifyTxn2);
        
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        
        // THIS IS THE BUG: UpdatePage replaces entire page content
        Assert.Equal(1, finalCount); // Lost 4 objects\!
        
        // Verify the single remaining object
        var remainingObject = finalData.Values.First().First();
        // Note: Using dynamic to handle Newtonsoft.Json objects
        dynamic obj = remainingObject;
        Assert.Equal(999, (int)obj.Id);
        Assert.Equal("Single_Updated_Object", (string)obj.Name);
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
