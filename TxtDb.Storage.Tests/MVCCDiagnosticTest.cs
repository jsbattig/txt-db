using TxtDb.Storage.Services;
using TxtDb.Storage.Models;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

public class MVCCDiagnosticTest : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly StorageSubsystem _storage;
    private readonly string _testRootPath;

    public MVCCDiagnosticTest(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), "mvcc_diagnostic_test", Guid.NewGuid().ToString());
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            MaxPageSizeKB = 64,
            ForceOneObjectPerPage = true  // For MVCC tests, each object should get its own page
        });
    }

    [Fact]
    public void DiagnosticTest_BasicInsertAndRead_ShouldShowWhatHappens()
    {
        // Create namespace and insert 3 objects
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "diagnostic.basic";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var page1Id = _storage.InsertObject(setupTxn, @namespace, new { Id = 1, Value = 100 });
        var page2Id = _storage.InsertObject(setupTxn, @namespace, new { Id = 2, Value = 200 });
        var page3Id = _storage.InsertObject(setupTxn, @namespace, new { Id = 3, Value = 300 });
        
        _output.WriteLine($"Inserted into pages: {page1Id}, {page2Id}, {page3Id}");
        _storage.CommitTransaction(setupTxn);

        // Read back each page individually
        var readTxn = _storage.BeginTransaction();
        var data1 = _storage.ReadPage(readTxn, @namespace, page1Id);
        var data2 = _storage.ReadPage(readTxn, @namespace, page2Id);
        var data3 = _storage.ReadPage(readTxn, @namespace, page3Id);
        
        _output.WriteLine($"Page {page1Id} has {data1.Length} objects");
        _output.WriteLine($"Page {page2Id} has {data2.Length} objects");
        _output.WriteLine($"Page {page3Id} has {data3.Length} objects");
        
        // Try to get all objects from namespace
        var allObjects = _storage.GetMatchingObjects(readTxn, @namespace, "*");
        _output.WriteLine($"GetMatchingObjects returned {allObjects.Count} pages");
        foreach (var kvp in allObjects)
        {
            _output.WriteLine($"  Page {kvp.Key} has {kvp.Value.Length} objects");
        }
        
        var totalObjects = allObjects.Values.Sum(page => page.Length);
        _output.WriteLine($"Total objects found: {totalObjects}");
        
        _storage.CommitTransaction(readTxn);

        // Assertions
        Assert.Equal(1, data1.Length);
        Assert.Equal(1, data2.Length);
        Assert.Equal(1, data3.Length);
        Assert.Equal(3, totalObjects);
    }

    public void Dispose()
    {
        _storage.Dispose();
        if (Directory.Exists(_testRootPath))
            Directory.Delete(_testRootPath, recursive: true);
    }
}