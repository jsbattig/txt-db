using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// SIMPLE DEBUG TEST - Minimal test to identify the data loss issue
/// </summary>
public class SimpleDebugTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public SimpleDebugTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_debug_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void DebugTest_InsertThreeObjects_ShouldFindThreeObjects()
    {
        // Arrange & Act - Create namespace and insert 3 objects
        var txn = _storage.BeginTransaction();
        var @namespace = "debug.simple";
        _storage.CreateNamespace(txn, @namespace);
        
        var pageId1 = _storage.InsertObject(txn, @namespace, new { Id = 1, Name = "Object1" });
        var pageId2 = _storage.InsertObject(txn, @namespace, new { Id = 2, Name = "Object2" });
        var pageId3 = _storage.InsertObject(txn, @namespace, new { Id = 3, Name = "Object3" });
        
        _storage.CommitTransaction(txn);

        // Assert - Verify we can find 3 objects
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        // Debug output
        Console.WriteLine($"Found {allData.Count} pages");
        Console.WriteLine($"Total objects: {allData.Values.Sum(pages => pages.Length)}");
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        Assert.Equal(3, allData.Values.Sum(pages => pages.Length));
    }

    [Fact]
    public void DebugTest_ExaminePageStructure_ShouldShowFileSystem()
    {
        // Arrange & Act - Create namespace and insert objects
        var txn = _storage.BeginTransaction();
        var @namespace = "debug.examine";
        _storage.CreateNamespace(txn, @namespace);
        
        var pageId1 = _storage.InsertObject(txn, @namespace, new { Id = 1, Data = "Test1" });
        var pageId2 = _storage.InsertObject(txn, @namespace, new { Id = 2, Data = "Test2" });
        
        _storage.CommitTransaction(txn);

        // Examine file system directly
        var namespacePath = Path.Combine(_testRootPath, "debug", "examine");
        Console.WriteLine($"Namespace path: {namespacePath}");
        
        if (Directory.Exists(namespacePath))
        {
            var files = Directory.GetFiles(namespacePath);
            Console.WriteLine($"Files in namespace: {files.Length}");
            foreach (var file in files)
            {
                Console.WriteLine($"  File: {Path.GetFileName(file)}");
                var content = File.ReadAllText(file);
                Console.WriteLine($"    Content length: {content.Length}");
                Console.WriteLine($"    Content preview: {content.Substring(0, Math.Min(100, content.Length))}...");
            }
        }
        else
        {
            Console.WriteLine("Namespace directory does not exist!");
        }

        // Test retrieval
        var retrieveTxn = _storage.BeginTransaction();
        var results = _storage.GetMatchingObjects(retrieveTxn, @namespace, "*");
        _storage.CommitTransaction(retrieveTxn);
        
        Console.WriteLine($"Retrieved {results.Count} pages with {results.Values.Sum(p => p.Length)} total objects");
        
        Assert.True(results.Values.Sum(p => p.Length) > 0, "Should find at least some objects");
    }

    [Fact]
    public void DebugTest_IndividualPageAccess_ShouldWork()
    {
        // Arrange & Act
        var txn = _storage.BeginTransaction();
        var @namespace = "debug.individual";
        _storage.CreateNamespace(txn, @namespace);
        
        var pageId = _storage.InsertObject(txn, @namespace, new { Id = 42, Message = "Hello World" });
        _storage.CommitTransaction(txn);

        Console.WriteLine($"Inserted into page: {pageId}");

        // Try to read the specific page
        var readTxn = _storage.BeginTransaction();
        var pageData = _storage.ReadPage(readTxn, @namespace, pageId);
        _storage.CommitTransaction(readTxn);
        
        Console.WriteLine($"Read page data: {pageData.Length} objects");
        
        // Try GetMatchingObjects
        var matchTxn = _storage.BeginTransaction();
        var matchData = _storage.GetMatchingObjects(matchTxn, @namespace, "*");
        _storage.CommitTransaction(matchTxn);
        
        Console.WriteLine($"GetMatchingObjects found: {matchData.Count} pages, {matchData.Values.Sum(p => p.Length)} objects");
        
        Assert.Single(pageData);
        Assert.Equal(1, matchData.Values.Sum(p => p.Length));
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