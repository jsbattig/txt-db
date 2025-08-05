using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL DEBUG TEST - Identify root cause of data loss in ForceOneObjectPerPage mode
/// </summary>
public class DebugDataLossTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public DebugDataLossTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_debug_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true  // Critical: Test this specific configuration
        });
    }

    [Fact]
    public void SimpleSequentialInsert_ShouldPreserveAllData()
    {
        Console.WriteLine("=== SEQUENTIAL INSERT DEBUG ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "debug.test");
        _storage.CommitTransaction(setupTxn);
        
        var insertedObjects = new List<object>();
        
        // Sequential insertions - should be 100% reliable
        for (int i = 0; i < 10; i++)
        {
            var txn = _storage.BeginTransaction();
            var obj = new { Id = i, Value = $"Object_{i}", Timestamp = DateTime.UtcNow };
            insertedObjects.Add(obj);
            
            Console.WriteLine($"Inserting object {i} in transaction {txn}");
            var pageId = _storage.InsertObject(txn, "debug.test", obj);
            Console.WriteLine($"  -> Created page: {pageId}");
            
            _storage.CommitTransaction(txn);
            Console.WriteLine($"  -> Transaction {txn} committed successfully");
        }
        
        // Now verify all objects are readable
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, "debug.test", "*");
        _storage.CommitTransaction(verifyTxn);
        
        var totalObjects = allData.Values.Sum(pages => pages.Length);
        
        Console.WriteLine($"=== RESULTS ===");
        Console.WriteLine($"Inserted: {insertedObjects.Count} objects");
        Console.WriteLine($"Retrieved: {totalObjects} objects in {allData.Count} pages");
        
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
            foreach (var obj in kvp.Value)
            {
                dynamic item = obj;
                Console.WriteLine($"    Object Id={item.Id}, Value={item.Value}");
            }
        }
        
        // Check filesystem directly
        var namespacePath = Path.Combine(_testRootPath, "debug", "test");
        if (Directory.Exists(namespacePath))
        {
            var allFiles = Directory.GetFiles(namespacePath, "*", SearchOption.AllDirectories);
            Console.WriteLine($"Files on disk: {allFiles.Length}");
            foreach (var file in allFiles.OrderBy(f => f))
            {
                var fileName = Path.GetFileName(file);
                var size = new FileInfo(file).Length;
                Console.WriteLine($"  {fileName}: {size} bytes");
            }
        }
        
        Assert.Equal(insertedObjects.Count, totalObjects);
    }

    [Fact]
    public void SimpleConcurrentInsert_ShouldPreserveAllData()
    {
        Console.WriteLine("=== CONCURRENT INSERT DEBUG ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "concurrent.debug");
        _storage.CommitTransaction(setupTxn);
        
        var successful = 0;
        var exceptions = new ConcurrentBag<Exception>();
        
        // Just 5 concurrent transactions to start simple
        var tasks = Enumerable.Range(0, 5).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    var obj = new { Id = i, Value = $"Concurrent_{i}", ThreadId = Thread.CurrentThread.ManagedThreadId };
                    
                    Console.WriteLine($"Task {i}: Starting transaction {txn}");
                    var pageId = _storage.InsertObject(txn, "concurrent.debug", obj);
                    Console.WriteLine($"Task {i}: Created page {pageId}");
                    
                    _storage.CommitTransaction(txn);
                    Console.WriteLine($"Task {i}: Transaction {txn} committed");
                    
                    Interlocked.Increment(ref successful);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Task {i}: FAILED - {ex.Message}");
                    exceptions.Add(ex);
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromSeconds(10));
        
        // Verify all objects are readable
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, "concurrent.debug", "*");
        _storage.CommitTransaction(verifyTxn);
        
        var totalObjects = allData.Values.Sum(pages => pages.Length);
        
        Console.WriteLine($"=== CONCURRENT RESULTS ===");
        Console.WriteLine($"Successful transactions: {successful}");
        Console.WriteLine($"Exceptions: {exceptions.Count}");
        Console.WriteLine($"Retrieved objects: {totalObjects} in {allData.Count} pages");
        
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
            foreach (var obj in kvp.Value)
            {
                dynamic item = obj;
                Console.WriteLine($"    Object Id={item.Id}, Value={item.Value}, ThreadId={item.ThreadId}");
            }
        }
        
        if (exceptions.Any())
        {
            Console.WriteLine("Exceptions:");
            foreach (var ex in exceptions)
            {
                Console.WriteLine($"  {ex.GetType().Name}: {ex.Message}");
            }
        }
        
        // Check filesystem directly
        var namespacePath = Path.Combine(_testRootPath, "concurrent", "debug");
        if (Directory.Exists(namespacePath))
        {
            var allFiles = Directory.GetFiles(namespacePath, "*", SearchOption.AllDirectories);
            Console.WriteLine($"Files on disk: {allFiles.Length}");
            foreach (var file in allFiles.OrderBy(f => f))
            {
                var fileName = Path.GetFileName(file);
                var size = new FileInfo(file).Length;
                try
                {
                    var content = File.ReadAllText(file);
                    Console.WriteLine($"  {fileName}: {size} bytes - {content.Substring(0, Math.Min(50, content.Length))}...");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  {fileName}: {size} bytes - ERROR: {ex.Message}");
                }
            }
        }
        
        Assert.Equal(successful, totalObjects);
    }

    public void Dispose()
    {
        try
        {
            Console.WriteLine($"Debug files preserved at: {_testRootPath}");
            // Don't delete for analysis
        }
        catch
        {
            // Cleanup failed - not critical  
        }
    }
}