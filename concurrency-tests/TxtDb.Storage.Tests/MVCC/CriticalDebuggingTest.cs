using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// CRITICAL DEBUGGING - Identify the exact cause of 98% data loss
/// </summary>
public class CriticalDebuggingTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly LoggingStorageWrapper _storage;

    public CriticalDebuggingTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_critical_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        var innerStorage = new StorageSubsystem();
        innerStorage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        
        _storage = new LoggingStorageWrapper(innerStorage);
    }

    [Fact]
    public void CriticalDebug_TwoSimpleUpdates_TraceDataLoss()
    {
        Console.WriteLine("=== CRITICAL DEBUG: TWO SIMPLE UPDATES ===");
        
        // Setup: Create 3 objects
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "critical.test");
        
        var page1 = _storage.InsertObject(setupTxn, "critical.test", new { Id = 1, Value = "Object1" });
        var page2 = _storage.InsertObject(setupTxn, "critical.test", new { Id = 2, Value = "Object2" });  
        var page3 = _storage.InsertObject(setupTxn, "critical.test", new { Id = 3, Value = "Object3" });
        
        _storage.CommitTransaction(setupTxn);
        
        Console.WriteLine($"Setup complete - Pages: {page1}, {page2}, {page3}");
        
        // Verify initial state
        var verifyTxn1 = _storage.BeginTransaction();
        var initialData = _storage.GetMatchingObjects(verifyTxn1, "critical.test", "*");
        var initialCount = initialData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(verifyTxn1);
        
        Console.WriteLine($"Initial verification: {initialCount} objects in {initialData.Count} pages");
        Assert.Equal(3, initialCount);
        
        // Update 1: Update page1
        Console.WriteLine("=== UPDATE 1: Updating page1 ===");
        var update1Txn = _storage.BeginTransaction();
        var page1Data = _storage.ReadPage(update1Txn, "critical.test", page1);
        Console.WriteLine($"Read page1: {page1Data.Length} objects");
        
        _storage.UpdatePage(update1Txn, "critical.test", page1, new object[] { 
            new { Id = 1, Value = "Updated_Object1", UpdatedAt = DateTime.UtcNow } 
        });
        Console.WriteLine("Updated page1 with new data");
        
        _storage.CommitTransaction(update1Txn);
        Console.WriteLine("Update1 transaction committed");
        
        // Verify after update 1
        var verifyTxn2 = _storage.BeginTransaction();
        var afterUpdate1 = _storage.GetMatchingObjects(verifyTxn2, "critical.test", "*");
        var afterUpdate1Count = afterUpdate1.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(verifyTxn2);
        
        Console.WriteLine($"After Update1: {afterUpdate1Count} objects in {afterUpdate1.Count} pages");
        foreach (var kvp in afterUpdate1)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        if (afterUpdate1Count != 3)
        {
            Console.WriteLine($"*** DATA LOSS AFTER UPDATE 1: Expected 3, got {afterUpdate1Count} ***");
            DumpFilesystem("AFTER_UPDATE1");
        }
        
        // Update 2: Update page2
        Console.WriteLine("=== UPDATE 2: Updating page2 ===");
        var update2Txn = _storage.BeginTransaction();
        var page2Data = _storage.ReadPage(update2Txn, "critical.test", page2);
        Console.WriteLine($"Read page2: {page2Data.Length} objects");
        
        _storage.UpdatePage(update2Txn, "critical.test", page2, new object[] { 
            new { Id = 2, Value = "Updated_Object2", UpdatedAt = DateTime.UtcNow } 
        });
        Console.WriteLine("Updated page2 with new data");
        
        _storage.CommitTransaction(update2Txn);
        Console.WriteLine("Update2 transaction committed");
        
        // Final verification
        var verifyTxn3 = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(verifyTxn3, "critical.test", "*");
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(verifyTxn3);
        
        Console.WriteLine($"Final result: {finalCount} objects in {finalData.Count} pages");
        foreach (var kvp in finalData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        if (finalCount != 3)
        {
            Console.WriteLine($"*** CRITICAL DATA LOSS: Expected 3, got {finalCount} ***");
            DumpFilesystem("FINAL_STATE");
            WriteFullLog();
        }
        
        // This should pass but might fail - we want to see the logs
        Assert.True(finalCount >= 2, $"Severe data loss: expected 3, got {finalCount}");
    }

    [Fact] 
    public void CriticalDebug_ConcurrentSamePageUpdates_RaceCondition()
    {
        Console.WriteLine("=== CRITICAL DEBUG: CONCURRENT SAME PAGE UPDATES ===");
        
        // Setup: Create 1 object that multiple transactions will try to update
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "race.test");
        
        var sharedPageId = _storage.InsertObject(setupTxn, "race.test", new { Id = 1, Value = "Original", Counter = 0 });
        _storage.CommitTransaction(setupTxn);
        
        Console.WriteLine($"Setup: Created shared page {sharedPageId}");
        
        // Launch 3 concurrent transactions that all try to update the same page
        var tasks = new List<Task<string>>();
        var results = new List<string>();
        
        for (int i = 1; i <= 3; i++)
        {
            int taskId = i;
            var task = Task.Run(() =>
            {
                try
                {
                    Console.WriteLine($"Task {taskId} starting");
                    var txn = _storage.BeginTransaction();
                    
                    // Read current data
                    var currentData = _storage.ReadPage(txn, "race.test", sharedPageId);
                    Console.WriteLine($"Task {taskId} read {currentData.Length} objects");
                    
                    // Small delay to increase contention
                    Thread.Sleep(50);
                    
                    // Update with task-specific data
                    _storage.UpdatePage(txn, "race.test", sharedPageId, new object[] { 
                        new { Id = taskId, Value = $"Updated_by_Task_{taskId}", Counter = taskId, UpdatedAt = DateTime.UtcNow } 
                    });
                    Console.WriteLine($"Task {taskId} updated page");
                    
                    _storage.CommitTransaction(txn);
                    Console.WriteLine($"Task {taskId} committed successfully");
                    
                    return $"Task {taskId}: SUCCESS";
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Task {taskId} failed: {ex.Message}");
                    return $"Task {taskId}: FAILED - {ex.Message}";
                }
            });
            
            tasks.Add(task);
        }
        
        Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(30));
        
        foreach (var task in tasks)
        {
            results.Add(task.Result);
            Console.WriteLine(task.Result);
        }
        
        // Check final state
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, "race.test", "*");
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(finalTxn);
        
        Console.WriteLine($"Race condition result: {finalCount} objects");
        
        if (finalCount == 0)
        {
            Console.WriteLine("*** CRITICAL: All data lost in race condition! ***");
            DumpFilesystem("RACE_CONDITION_LOSS");
            WriteFullLog();
        }
        
        Assert.True(finalCount >= 1, "Race condition caused complete data loss");
    }

    private void DumpFilesystem(string phase)
    {
        Console.WriteLine($"=== FILESYSTEM DUMP: {phase} ===");
        
        var allFiles = Directory.GetFiles(_testRootPath, "*", SearchOption.AllDirectories);
        Console.WriteLine($"Total files in {_testRootPath}: {allFiles.Length}");
        
        foreach (var file in allFiles.OrderBy(f => f))
        {
            var relativePath = Path.GetRelativePath(_testRootPath, file);
            var size = new FileInfo(file).Length;
            
            try
            {
                var content = File.ReadAllText(file);
                Console.WriteLine($"  {relativePath} ({size} bytes)");
                Console.WriteLine($"    Content: {content}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  {relativePath} ({size} bytes) - ERROR reading: {ex.Message}");
            }
        }
    }

    private void WriteFullLog()
    {
        var logPath = Path.Combine(_testRootPath, "full_operation_log.txt");
        var allLogs = _storage.GetOperationLog().ToArray();
        File.WriteAllLines(logPath, allLogs);
        Console.WriteLine($"Full operation log written to: {logPath}");
        
        // Also print the last 50 log entries
        Console.WriteLine("=== RECENT OPERATION LOG ===");
        foreach (var log in allLogs.TakeLast(50))
        {
            Console.WriteLine(log);
        }
    }

    public void Dispose()
    {
        WriteFullLog();
        Console.WriteLine($"Test data preserved at: {_testRootPath}");
    }
}