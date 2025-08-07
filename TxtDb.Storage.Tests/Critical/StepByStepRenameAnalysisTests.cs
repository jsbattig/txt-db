using System.Text.Json;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Critical;

/// <summary>
/// STEP-BY-STEP ANALYSIS OF RENAME OPERATION DATA LOSS
/// 
/// Purpose: Isolate exactly where data is lost during RenameNamespaceAsync
/// Focus: Verify each step to understand if the issue is:
/// 1. File system movement (Directory.Move)
/// 2. Metadata update logic (PageVersions keys)
/// 3. GetMatchingObjectsAsync after rename
/// 4. Page reading logic after rename
/// 
/// NO MOCKING - Real file system operations only
/// </summary>
public class StepByStepRenameAnalysisTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly AsyncStorageSubsystem _asyncStorage;
    private readonly ITestOutputHelper _output;

    public StepByStepRenameAnalysisTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_step_analysis_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task StepByStep_AnalyzeRenameDataLoss_ThreeObjects()
    {
        // STEP 1: Setup namespace and insert 3 objects
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        var oldNamespace = "step.analysis.old";
        var newNamespace = "step.analysis.new";
        
        await _asyncStorage.CreateNamespaceAsync(setupTxn, oldNamespace);
        
        var testObjects = new[]
        {
            new { Id = 1, Name = "Object 1", Category = "A" },
            new { Id = 2, Name = "Object 2", Category = "B" },
            new { Id = 3, Name = "Object 3", Category = "A" }
        };

        _output.WriteLine("=== STEP 1: INSERTING OBJECTS ===");
        var pageIds = new List<string>();
        foreach (var obj in testObjects)
        {
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, oldNamespace, obj);
            pageIds.Add(pageId);
            _output.WriteLine($"Inserted object {obj.Id} into page: {pageId}");
        }
        
        await _asyncStorage.CommitTransactionAsync(setupTxn);
        _output.WriteLine($"Total page IDs created: {pageIds.Count}");
        _output.WriteLine($"Unique page IDs: {pageIds.Distinct().Count()}");

        // STEP 2: Verify objects exist before rename
        _output.WriteLine("\n=== STEP 2: VERIFY BEFORE RENAME ===");
        var beforeTxn = await _asyncStorage.BeginTransactionAsync();
        
        // Check each individual page
        foreach (var pageId in pageIds)
        {
            try
            {
                var pageData = await _asyncStorage.ReadPageAsync(beforeTxn, oldNamespace, pageId);
                _output.WriteLine($"Page {pageId}: {pageData.Length} objects");
                foreach (var obj in pageData)
                {
                    _output.WriteLine($"  Object: {JsonSerializer.Serialize(obj)}");
                }
            }
            catch (Exception ex)
            {
                _output.WriteLine($"ERROR reading page {pageId}: {ex.Message}");
            }
        }
        
        // Check GetMatchingObjectsAsync
        var allDataBefore = await _asyncStorage.GetMatchingObjectsAsync(beforeTxn, oldNamespace, "*");
        _output.WriteLine($"GetMatchingObjectsAsync returned {allDataBefore.Count} pages");
        var totalObjectsBefore = allDataBefore.Values.Sum(pages => pages.Length);
        _output.WriteLine($"Total objects before rename: {totalObjectsBefore}");
        
        await _asyncStorage.CommitTransactionAsync(beforeTxn);

        // STEP 3: Inspect file system before rename
        _output.WriteLine("\n=== STEP 3: FILE SYSTEM BEFORE RENAME ===");
        var oldPath = Path.Combine(_testRootPath, oldNamespace.Replace('.', Path.DirectorySeparatorChar));
        var filesBeforeRename = Directory.GetFiles(oldPath, "*", SearchOption.AllDirectories);
        _output.WriteLine($"Files in old namespace: {filesBeforeRename.Length}");
        foreach (var file in filesBeforeRename)
        {
            var fileName = Path.GetFileName(file);
            var fileSize = new FileInfo(file).Length;
            _output.WriteLine($"  {fileName} ({fileSize} bytes)");
            
            // If it's a page file, inspect its content
            if (fileName.StartsWith("page") && fileName.Contains(".json"))
            {
                try
                {
                    var content = await File.ReadAllTextAsync(file);
                    var jsonArray = JsonSerializer.Deserialize<object[]>(content);
                    _output.WriteLine($"    Contains {jsonArray?.Length} objects");
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"    ERROR reading content: {ex.Message}");
                }
            }
        }

        // STEP 4: Perform rename operation
        _output.WriteLine("\n=== STEP 4: PERFORMING RENAME ===");
        var renameTxn = await _asyncStorage.BeginTransactionAsync();
        
        try
        {
            await _asyncStorage.RenameNamespaceAsync(renameTxn, oldNamespace, newNamespace);
            _output.WriteLine("RenameNamespaceAsync completed successfully");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"ERROR during rename: {ex.Message}");
            await _asyncStorage.RollbackTransactionAsync(renameTxn);
            return;
        }
        
        await _asyncStorage.CommitTransactionAsync(renameTxn);

        // STEP 5: Inspect file system after rename
        _output.WriteLine("\n=== STEP 5: FILE SYSTEM AFTER RENAME ===");
        var newPath = Path.Combine(_testRootPath, newNamespace.Replace('.', Path.DirectorySeparatorChar));
        _output.WriteLine($"Old directory exists: {Directory.Exists(oldPath)}");
        _output.WriteLine($"New directory exists: {Directory.Exists(newPath)}");
        
        if (Directory.Exists(newPath))
        {
            var filesAfterRename = Directory.GetFiles(newPath, "*", SearchOption.AllDirectories);
            _output.WriteLine($"Files in new namespace: {filesAfterRename.Length}");
            foreach (var file in filesAfterRename)
            {
                var fileName = Path.GetFileName(file);
                var fileSize = new FileInfo(file).Length;
                _output.WriteLine($"  {fileName} ({fileSize} bytes)");
                
                // If it's a page file, inspect its content
                if (fileName.StartsWith("page") && fileName.Contains(".json"))
                {
                    try
                    {
                        var content = await File.ReadAllTextAsync(file);
                        var jsonArray = JsonSerializer.Deserialize<object[]>(content);
                        _output.WriteLine($"    Contains {jsonArray?.Length} objects");
                        for (int i = 0; i < jsonArray?.Length; i++)
                        {
                            _output.WriteLine($"    [{i}] {JsonSerializer.Serialize(jsonArray[i])}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _output.WriteLine($"    ERROR reading content: {ex.Message}");
                    }
                }
            }
        }

        // STEP 6: Try to read individual pages in new namespace
        _output.WriteLine("\n=== STEP 6: READ INDIVIDUAL PAGES IN NEW NAMESPACE ===");
        var afterRenameTxn = await _asyncStorage.BeginTransactionAsync();
        
        foreach (var pageId in pageIds)
        {
            try
            {
                var pageData = await _asyncStorage.ReadPageAsync(afterRenameTxn, newNamespace, pageId);
                _output.WriteLine($"Page {pageId} in new namespace: {pageData.Length} objects");
                foreach (var obj in pageData)
                {
                    _output.WriteLine($"  Object: {JsonSerializer.Serialize(obj)}");
                }
            }
            catch (Exception ex)
            {
                _output.WriteLine($"ERROR reading page {pageId} in new namespace: {ex.Message}");
            }
        }

        // STEP 7: Test GetMatchingObjectsAsync in new namespace
        _output.WriteLine("\n=== STEP 7: GetMatchingObjectsAsync IN NEW NAMESPACE ===");
        try
        {
            var allDataAfter = await _asyncStorage.GetMatchingObjectsAsync(afterRenameTxn, newNamespace, "*");
            _output.WriteLine($"GetMatchingObjectsAsync returned {allDataAfter.Count} pages");
            
            var totalObjectsAfter = 0;
            foreach (var kvp in allDataAfter)
            {
                _output.WriteLine($"Page {kvp.Key}: {kvp.Value.Length} objects");
                totalObjectsAfter += kvp.Value.Length;
                for (int i = 0; i < kvp.Value.Length; i++)
                {
                    _output.WriteLine($"  [{i}] {JsonSerializer.Serialize(kvp.Value[i])}");
                }
            }
            _output.WriteLine($"Total objects after rename: {totalObjectsAfter}");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"ERROR with GetMatchingObjectsAsync: {ex.Message}");
        }
        
        await _asyncStorage.CommitTransactionAsync(afterRenameTxn);

        // STEP 8: Try old namespace (should fail)
        _output.WriteLine("\n=== STEP 8: VERIFY OLD NAMESPACE IS GONE ===");
        var oldTxn = await _asyncStorage.BeginTransactionAsync();
        try
        {
            var oldData = await _asyncStorage.GetMatchingObjectsAsync(oldTxn, oldNamespace, "*");
            _output.WriteLine($"ERROR: Old namespace still accessible! Found {oldData.Count} pages");
        }
        catch (ArgumentException ex)
        {
            _output.WriteLine($"GOOD: Old namespace correctly throws ArgumentException: {ex.Message}");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"UNEXPECTED ERROR accessing old namespace: {ex.Message}");
        }
        await _asyncStorage.CommitTransactionAsync(oldTxn);
        
        _output.WriteLine("\n=== ANALYSIS COMPLETE ===");
        // This test is for analysis - we expect it to show the data loss issue
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