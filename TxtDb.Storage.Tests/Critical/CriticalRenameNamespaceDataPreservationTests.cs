using System.Diagnostics;
using System.Text.Json;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Critical;

/// <summary>
/// CRITICAL FAILING TESTS FIRST: Data loss issue in async RenameNamespaceAsync
/// 
/// PROBLEM: RenameNamespaceAsync preserves only 1 object out of 3 during namespace rename
/// Expected: All objects (3) preserved in new namespace
/// Actual: Only 1 object found after rename
/// 
/// ROOT CAUSE ANALYSIS:
/// - Test creates 3 objects in a namespace
/// - Calls RenameNamespaceAsync 
/// - Verifies ALL objects exist in new namespace
/// - FAILS: Only 1/3 objects preserved
/// 
/// THESE TESTS MUST FAIL FIRST, THEN BE FIXED
/// NO MOCKING - Real file I/O, real transactions, real data
/// </summary>
public class CriticalRenameNamespaceDataPreservationTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly AsyncStorageSubsystem _asyncStorage;
    private readonly ITestOutputHelper _output;

    public CriticalRenameNamespaceDataPreservationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_critical_rename_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task FAILING_RenameNamespaceAsync_SingleObject_ShouldPreserve()
    {
        // FAILING TEST FIRST - This should fail if the bug exists
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldNamespace = "critical.single.test";
        var newNamespace = "critical.single.renamed";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldNamespace);
        
        // Insert SINGLE object
        var testObject = new { Id = 1, Data = "Single test object", Category = "Test" };
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, oldNamespace, testObject);
        
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Verify object exists before rename
        var verifyBeforeTxn = await _asyncStorage.BeginTransactionAsync();
        var dataBeforeRename = await _asyncStorage.GetMatchingObjectsAsync(verifyBeforeTxn, oldNamespace, "*");
        var objectsBeforeRename = dataBeforeRename.Values.Sum(pages => pages.Length);
        await _asyncStorage.CommitTransactionAsync(verifyBeforeTxn);
        
        _output.WriteLine($"Objects before rename: {objectsBeforeRename}");
        Assert.Equal(1, objectsBeforeRename); // Should have 1 object

        // Act - Rename the namespace
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.RenameNamespaceAsync(renameTxn, oldNamespace, newNamespace);
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // Assert - Object should be preserved
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var dataAfterRename = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, newNamespace, "*");
        var objectsAfterRename = dataAfterRename.Values.Sum(pages => pages.Length);
        
        _output.WriteLine($"Objects after rename: {objectsAfterRename}");
        _output.WriteLine($"Pages after rename: {dataAfterRename.Count}");
        
        // THIS SHOULD PASS BUT MIGHT FAIL DUE TO BUG
        Assert.Equal(1, objectsAfterRename);
        Assert.Equal(1, dataAfterRename.Count);
        
        await _asyncStorage.CommitTransactionAsync(verifyTxn);
    }

    [Fact]
    public async Task FAILING_RenameNamespaceAsync_ThreeObjects_ShouldPreserveAll()
    {
        // FAILING TEST FIRST - This should definitely fail based on the bug report
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldNamespace = "critical.three.test";
        var newNamespace = "critical.three.renamed";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldNamespace);
        
        // Insert THREE objects (same as failing test)
        var testObjects = new[]
        {
            new { Id = 1, Name = "Object 1", Category = "A" },
            new { Id = 2, Name = "Object 2", Category = "B" },
            new { Id = 3, Name = "Object 3", Category = "A" }
        };

        var pageIds = new List<string>();
        foreach (var obj in testObjects)
        {
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, oldNamespace, obj);
            pageIds.Add(pageId);
            _output.WriteLine($"Created object {obj.Id} in page {pageId}");
        }
        
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Verify all objects exist before rename
        var verifyBeforeTxn = await _asyncStorage.BeginTransactionAsync();
        var dataBeforeRename = await _asyncStorage.GetMatchingObjectsAsync(verifyBeforeTxn, oldNamespace, "*");
        var objectsBeforeRename = dataBeforeRename.Values.Sum(pages => pages.Length);
        await _asyncStorage.CommitTransactionAsync(verifyBeforeTxn);
        
        _output.WriteLine($"Objects before rename: {objectsBeforeRename} across {dataBeforeRename.Count} pages");
        Assert.Equal(3, objectsBeforeRename); // Should have 3 objects

        // Act - Rename the namespace
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.RenameNamespaceAsync(renameTxn, oldNamespace, newNamespace);
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // Assert - ALL objects should be preserved
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var dataAfterRename = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, newNamespace, "*");
        var objectsAfterRename = dataAfterRename.Values.Sum(pages => pages.Length);
        
        _output.WriteLine($"DETAILED ANALYSIS:");
        _output.WriteLine($"dataAfterRename.Count: {dataAfterRename.Count}");
        _output.WriteLine($"dataAfterRename.Values.Count: {dataAfterRename.Values.Count}");
        _output.WriteLine($"objectsAfterRename calculation: {objectsAfterRename}");
        _output.WriteLine($"Expected pageIds.Count: {pageIds.Count}");
        
        foreach (var kvp in dataAfterRename)
        {
            _output.WriteLine($"Page {kvp.Key}: {kvp.Value.Length} objects");
            _output.WriteLine($"  Sum check: pages.Length = {kvp.Value.Length}");
        }
        
        // Debug the exact calculation
        var manualSum = 0;
        foreach (var pageData in dataAfterRename.Values)
        {
            manualSum += pageData.Length;
            _output.WriteLine($"Adding {pageData.Length} to sum, new total: {manualSum}");
        }
        _output.WriteLine($"Manual sum: {manualSum}");
        _output.WriteLine($"LINQ sum: {objectsAfterRename}");
        
        // FIXED: The test was incorrectly expecting one page per object
        // But InsertObjectAsync can put multiple objects in the same page for efficiency
        _output.WriteLine($"About to assert: Expected=3, Actual={objectsAfterRename}");
        Assert.Equal(3, objectsAfterRename);
        Assert.True(dataAfterRename.Count >= 1, "Should have at least one page with data");
        
        await _asyncStorage.CommitTransactionAsync(verifyTxn);
    }

    [Fact]
    public async Task FAILING_RenameNamespaceAsync_MultiplePages_ShouldPreserveAllPages()
    {
        // FAILING TEST FIRST - Test with forced multiple pages
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldNamespace = "critical.multipage.test";
        var newNamespace = "critical.multipage.renamed";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldNamespace);
        
        // Force multiple pages by creating larger objects
        var testObjects = new List<object>();
        var pageIds = new List<string>();
        
        for (int i = 1; i <= 5; i++)
        {
            var largeData = new string('X', 1000); // Large objects to potentially force page creation
            var obj = new { 
                Id = i, 
                Name = $"Large Object {i}", 
                Data = largeData,
                Timestamp = DateTime.UtcNow.AddSeconds(i)
            };
            testObjects.Add(obj);
            
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, oldNamespace, obj);
            pageIds.Add(pageId);
            _output.WriteLine($"Created large object {i} in page {pageId}");
        }
        
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Verify all objects and pages exist before rename
        var verifyBeforeTxn = await _asyncStorage.BeginTransactionAsync();
        var dataBeforeRename = await _asyncStorage.GetMatchingObjectsAsync(verifyBeforeTxn, oldNamespace, "*");
        var objectsBeforeRename = dataBeforeRename.Values.Sum(pages => pages.Length);
        await _asyncStorage.CommitTransactionAsync(verifyBeforeTxn);
        
        _output.WriteLine($"Objects before rename: {objectsBeforeRename} across {dataBeforeRename.Count} pages");
        Assert.Equal(5, objectsBeforeRename); // Should have 5 objects

        // Act - Rename the namespace
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.RenameNamespaceAsync(renameTxn, oldNamespace, newNamespace);
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // Assert - ALL objects and pages should be preserved
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var dataAfterRename = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, newNamespace, "*");
        var objectsAfterRename = dataAfterRename.Values.Sum(pages => pages.Length);
        
        _output.WriteLine($"Objects after rename: {objectsAfterRename} across {dataAfterRename.Count} pages");
        
        // THIS SHOULD FAIL IF BUG AFFECTS MULTIPLE PAGES
        Assert.Equal(5, objectsAfterRename);
        Assert.True(dataAfterRename.Count > 0, "Should have at least one page");
        
        await _asyncStorage.CommitTransactionAsync(verifyTxn);
    }

    [Fact]
    public async Task DEBUG_InspectNamespaceDirectoryStructure_AfterRename()
    {
        // DEBUG TEST - Inspect actual file structure
        
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldNamespace = "debug.inspect.old";
        var newNamespace = "debug.inspect.new";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldNamespace);
        
        var testObjects = new[]
        {
            new { Id = 1, Name = "Debug Object 1" },
            new { Id = 2, Name = "Debug Object 2" },
            new { Id = 3, Name = "Debug Object 3" }
        };

        foreach (var obj in testObjects)
        {
            await _asyncStorage.InsertObjectAsync(setupTxn, oldNamespace, obj);
        }
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Inspect directory before rename
        var oldPath = Path.Combine(_testRootPath, oldNamespace.Replace('.', Path.DirectorySeparatorChar));
        _output.WriteLine($"Old namespace path: {oldPath}");
        _output.WriteLine($"Old directory exists: {Directory.Exists(oldPath)}");
        if (Directory.Exists(oldPath))
        {
            var filesInOld = Directory.GetFiles(oldPath, "*", SearchOption.AllDirectories);
            _output.WriteLine($"Files in old namespace: {filesInOld.Length}");
            foreach (var file in filesInOld)
            {
                _output.WriteLine($"  File: {file} (Size: {new FileInfo(file).Length} bytes)");
            }
        }

        // Act - Rename
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.RenameNamespaceAsync(renameTxn, oldNamespace, newNamespace);
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // Inspect directory after rename
        var newPath = Path.Combine(_testRootPath, newNamespace.Replace('.', Path.DirectorySeparatorChar));
        _output.WriteLine($"New namespace path: {newPath}");
        _output.WriteLine($"New directory exists: {Directory.Exists(newPath)}");
        _output.WriteLine($"Old directory exists after rename: {Directory.Exists(oldPath)}");
        
        if (Directory.Exists(newPath))
        {
            var filesInNew = Directory.GetFiles(newPath, "*", SearchOption.AllDirectories);
            _output.WriteLine($"Files in new namespace: {filesInNew.Length}");
            foreach (var file in filesInNew)
            {
                _output.WriteLine($"  File: {file} (Size: {new FileInfo(file).Length} bytes)");
                
                // Read and inspect file contents if it's a data file
                if (file.EndsWith(".json") && !file.EndsWith(".namespace.json"))
                {
                    try
                    {
                        var content = await File.ReadAllTextAsync(file);
                        var jsonDoc = JsonDocument.Parse(content);
                        _output.WriteLine($"    Content preview: {content[..Math.Min(100, content.Length)]}");
                    }
                    catch (Exception ex)
                    {
                        _output.WriteLine($"    Failed to read file content: {ex.Message}");
                    }
                }
            }
        }

        // This test is for debugging - no assertions, just information
    }

    public void Dispose()
    {
        try
        {
            _asyncStorage?.Dispose();
            
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