using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// DEEP DEBUGGING - Comprehensive logging to identify data loss root cause
/// </summary>
public class DeepDebuggingStressTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;
    private readonly ConcurrentBag<string> _debugLog = new();
    private readonly object _logLock = new object();

    public DeepDebuggingStressTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_debug_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        
        Log("=== DEEP DEBUG SESSION STARTED ===");
    }

    private void Log(string message)
    {
        var timestamp = DateTime.UtcNow.ToString("HH:mm:ss.fff");
        var threadId = Thread.CurrentThread.ManagedThreadId;
        var logEntry = $"[{timestamp}] T{threadId:D2}: {message}";
        
        lock (_logLock)
        {
            _debugLog.Add(logEntry);
            Console.WriteLine(logEntry);
        }
    }

    private void DumpFileSystem(string @namespace, string phase)
    {
        Log($"=== FILESYSTEM DUMP: {phase} ===");
        var namespacePath = Path.Combine(_testRootPath, @namespace.Replace(".", Path.DirectorySeparatorChar.ToString()));
        
        if (Directory.Exists(namespacePath))
        {
            var files = Directory.GetFiles(namespacePath, "*", SearchOption.AllDirectories)
                .OrderBy(f => f)
                .ToArray();
            
            Log($"Directory: {namespacePath} - {files.Length} files");
            
            foreach (var file in files)
            {
                var fileName = Path.GetFileName(file);
                var size = new FileInfo(file).Length;
                var content = File.ReadAllText(file);
                var objectCount = content.Count(c => c == '{') - content.Count(c => c == '}') / 2; // Rough estimate
                
                Log($"  File: {fileName} ({size} bytes) ~{Math.Max(0, objectCount)} objects");
                Log($"    Content preview: {content.Substring(0, Math.Min(200, content.Length))}...");
            }
        }
        else
        {
            Log($"Directory does not exist: {namespacePath}");
        }
    }

    private void VerifyDataIntegrity(string @namespace, int expectedObjects, string phase)
    {
        Log($"=== DATA INTEGRITY CHECK: {phase} ===");
        
        try
        {
            var verifyTxn = _storage.BeginTransaction();
            Log($"Verification transaction started: {verifyTxn}");
            
            var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
            var actualObjects = allData.Values.Sum(pages => pages.Length);
            
            Log($"Expected objects: {expectedObjects}");
            Log($"Actual objects: {actualObjects}");
            Log($"Pages found: {allData.Count}");
            
            foreach (var kvp in allData)
            {
                Log($"  Page {kvp.Key}: {kvp.Value.Length} objects");
            }
            
            _storage.CommitTransaction(verifyTxn);
            Log($"Verification transaction committed: {verifyTxn}");
            
            if (actualObjects != expectedObjects)
            {
                Log($"*** DATA LOSS DETECTED: Missing {expectedObjects - actualObjects} objects! ***");
                DumpFileSystem(@namespace, $"DATA_LOSS_{phase}");
            }
        }
        catch (Exception ex)
        {
            Log($"*** VERIFICATION ERROR: {ex.Message} ***");
            Log($"Stack trace: {ex.StackTrace}");
        }
    }

    [Fact]
    public void DeepDebug_SmallScaleConcurrentUpdates_TraceEverything()
    {
        // Phase 1: Setup with detailed logging
        Log("=== PHASE 1: SETUP ===");
        var @namespace = "debug.deep.trace";
        var setupTxn = _storage.BeginTransaction();
        Log($"Setup transaction started: {setupTxn}");
        
        _storage.CreateNamespace(setupTxn, @namespace);
        Log($"Namespace created: {@namespace}");
        
        // Create 5 objects for testing
        var initialObjects = new List<object>();
        var pageIds = new List<string>();
        
        for (int i = 0; i < 5; i++)
        {
            var obj = new { 
                Id = i, 
                InitialValue = $"Object_{i}", 
                CreatedAt = DateTime.UtcNow,
                Phase = "SETUP"
            };
            initialObjects.Add(obj);
            
            var pageId = _storage.InsertObject(setupTxn, @namespace, obj);
            pageIds.Add(pageId);
            Log($"Inserted object {i} into page {pageId}");
        }
        
        _storage.CommitTransaction(setupTxn);
        Log($"Setup transaction committed: {setupTxn}");
        
        DumpFileSystem(@namespace, "AFTER_SETUP");
        VerifyDataIntegrity(@namespace, 5, "AFTER_SETUP");
        
        // Phase 2: Concurrent Updates with detailed tracing
        Log("=== PHASE 2: CONCURRENT UPDATES ===");
        
        var successful = 0;
        var conflicts = 0;
        var errors = 0;
        var operationLogs = new ConcurrentBag<string>();
        
        var tasks = Enumerable.Range(1, 10).Select(taskId =>
            Task.Run(() =>
            {
                var taskLog = new StringBuilder();
                
                try
                {
                    taskLog.AppendLine($"Task {taskId} starting");
                    var txn = _storage.BeginTransaction();
                    taskLog.AppendLine($"Task {taskId} transaction: {txn}");
                    
                    // Each task updates a different page to avoid conflicts
                    var targetPageIndex = taskId % pageIds.Count;
                    var targetPageId = pageIds[targetPageIndex];
                    taskLog.AppendLine($"Task {taskId} targeting page {targetPageId} (index {targetPageIndex})");
                    
                    // Read current data
                    var currentData = _storage.ReadPage(txn, @namespace, targetPageId);
                    taskLog.AppendLine($"Task {taskId} read {currentData.Length} objects from page {targetPageId}");
                    
                    // Small delay to increase chance of conflicts
                    Thread.Sleep(10);
                    
                    // PROPER Read-Modify-Write: preserve existing data and add new
                    var updatedContent = new List<object>();
                    
                    // Preserve ALL existing objects from the page
                    foreach (var existingObj in currentData)
                    {
                        updatedContent.Add(existingObj);
                    }
                    
                    // Add new object to the page
                    updatedContent.Add(new { 
                        Id = taskId * 1000,
                        UpdatedValue = $"Updated_by_Task_{taskId}",
                        UpdatedAt = DateTime.UtcNow,
                        Phase = "CONCURRENT_UPDATE",
                        OriginalData = currentData.FirstOrDefault()
                    });
                    
                    _storage.UpdatePage(txn, @namespace, targetPageId, updatedContent.ToArray());
                    taskLog.AppendLine($"Task {taskId} updated page {targetPageId} with {updatedContent.Count} total objects");
                    
                    _storage.CommitTransaction(txn);
                    taskLog.AppendLine($"Task {taskId} committed transaction {txn}");
                    
                    Interlocked.Increment(ref successful);
                    taskLog.AppendLine($"Task {taskId} completed successfully");
                }
                catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                {
                    Interlocked.Increment(ref conflicts);
                    taskLog.AppendLine($"Task {taskId} conflict: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errors);
                    taskLog.AppendLine($"Task {taskId} error: {ex.Message}");
                    taskLog.AppendLine($"Stack trace: {ex.StackTrace}");
                }
                
                operationLogs.Add(taskLog.ToString());
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromMinutes(1));
        
        // Log all operation details
        foreach (var opLog in operationLogs)
        {
            Log($"OPERATION LOG:\n{opLog}");
        }
        
        Log($"Concurrent phase completed: {successful} successful, {conflicts} conflicts, {errors} errors");
        
        DumpFileSystem(@namespace, "AFTER_CONCURRENT_UPDATES");
        VerifyDataIntegrity(@namespace, 5, "AFTER_CONCURRENT_UPDATES");
        
        // Phase 3: Detailed Analysis
        Log("=== PHASE 3: DETAILED ANALYSIS ===");
        
        // Check each page individually
        for (int i = 0; i < pageIds.Count; i++)
        {
            try
            {
                var analysisTxn = _storage.BeginTransaction();
                var pageData = _storage.ReadPage(analysisTxn, @namespace, pageIds[i]);
                Log($"Page {pageIds[i]} analysis: {pageData.Length} objects");
                
                for (int j = 0; j < pageData.Length; j++)
                {
                    Log($"  Object {j}: {System.Text.Json.JsonSerializer.Serialize(pageData[j])}");
                }
                
                _storage.CommitTransaction(analysisTxn);
            }
            catch (Exception ex)
            {
                Log($"Error analyzing page {pageIds[i]}: {ex.Message}");
            }
        }
        
        // Final verification
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(finalTxn);
        
        Log($"=== FINAL RESULTS ===");
        Log($"Initial objects: 5");
        Log($"Successful transactions: {successful}");
        Log($"Expected minimum: {5 + successful} objects (5 initial + successful adds)");
        Log($"Actual: {finalCount} objects");
        Log($"Success rate: {successful}/{successful + conflicts + errors} operations");
        
        // Write complete log to file for analysis
        WriteDebugLogToFile();
        
        // With proper Read-Modify-Write, we should see growth, not loss
        var expectedMinimum = 5 + successful;
        if (finalCount < expectedMinimum)
        {
            Log($"*** UNEXPECTED: Expected at least {expectedMinimum}, got {finalCount} ***");
        }
        
        Assert.True(finalCount >= 5, $"Data loss detected: expected at least 5 objects, got {finalCount}");
    }

    [Fact]
    public void DeepDebug_SequentialOperations_BaselineCheck()
    {
        Log("=== SEQUENTIAL BASELINE TEST ===");
        var @namespace = "debug.sequential.baseline";
        
        // Create initial data
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var pageIds = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var pageId = _storage.InsertObject(setupTxn, @namespace, new { Id = i, Value = $"Initial_{i}" });
            pageIds.Add(pageId);
            Log($"Sequential setup: inserted object {i} into page {pageId}");
        }
        _storage.CommitTransaction(setupTxn);
        
        VerifyDataIntegrity(@namespace, 5, "SEQUENTIAL_SETUP");
        
        // Sequential updates
        for (int i = 0; i < pageIds.Count; i++)
        {
            var txn = _storage.BeginTransaction();
            var currentData = _storage.ReadPage(txn, @namespace, pageIds[i]);
            Log($"Sequential update {i}: read {currentData.Length} objects from page {pageIds[i]}");
            
            // PROPER Read-Modify-Write: preserve existing objects and add new one
            var updatedContent = new List<object>();
            foreach (var existingObj in currentData)
            {
                updatedContent.Add(existingObj); // Preserve existing objects
            }
            
            // Add new object to the page
            updatedContent.Add(new { Id = i + 1000, Value = $"Updated_{i}", UpdatedAt = DateTime.UtcNow });
            
            _storage.UpdatePage(txn, @namespace, pageIds[i], updatedContent.ToArray());
            Log($"Sequential update {i}: updated page {pageIds[i]} with {updatedContent.Count} total objects");
            
            _storage.CommitTransaction(txn);
            
            // Verify after each update - expecting growth due to appends
            var expectedAfterUpdate = 5 + (i + 1); // Original 5 plus objects added so far
            VerifyDataIntegrity(@namespace, expectedAfterUpdate, $"SEQUENTIAL_UPDATE_{i}");
        }
        
        WriteDebugLogToFile();
        
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(finalTxn);
        
        var finalExpected = 5 + pageIds.Count; // Original 5 plus one added per page
        Assert.Equal(finalExpected, finalCount);
    }

    private void WriteDebugLogToFile()
    {
        var logFilePath = Path.Combine(_testRootPath, "debug_log.txt");
        var allLogs = _debugLog.OrderBy(log => log).ToArray();
        File.WriteAllLines(logFilePath, allLogs);
        Log($"Debug log written to: {logFilePath}");
    }

    public void Dispose()
    {
        try
        {
            WriteDebugLogToFile();
            
            if (Directory.Exists(_testRootPath))
            {
                // Don't delete - keep for analysis
                Log($"Test files preserved at: {_testRootPath}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Cleanup error: {ex.Message}");
        }
    }
}