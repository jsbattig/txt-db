using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// RACE CONDITION ISOLATION - Identify the specific concurrent operation causing data loss
/// </summary>
public class RaceConditionIsolationTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public RaceConditionIsolationTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_race_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void RaceTest_SequentialInserts_ShouldPreserveAll()
    {
        Console.WriteLine("=== SEQUENTIAL BASELINE ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "sequential.test");
        _storage.CommitTransaction(setupTxn);
        
        // Sequential insertions - should work perfectly
        for (int i = 0; i < 50; i++)
        {
            var txn = _storage.BeginTransaction();
            _storage.InsertObject(txn, "sequential.test", new { Id = i, Value = $"Sequential_{i}" });
            _storage.CommitTransaction(txn);
        }
        
        // Verify
        var verifyTxn = _storage.BeginTransaction();
        var data = _storage.GetMatchingObjects(verifyTxn, "sequential.test", "*");
        var count = data.Values.Sum(p => p.Length);
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"Sequential result: {count}/50 objects preserved");
        Assert.Equal(50, count);
    }

    [Fact]
    public void RaceTest_ConcurrentSeparateTransactions_OneObjectEach()
    {
        Console.WriteLine("=== CONCURRENT SEPARATE TRANSACTIONS ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "concurrent.separate");
        _storage.CommitTransaction(setupTxn);
        
        var successful = 0;
        var failed = 0;
        var results = new ConcurrentBag<string>();
        
        // Each task creates its own transaction and inserts ONE object
        var tasks = Enumerable.Range(0, 50).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    var taskTxn = _storage.BeginTransaction();
                    var pageId = _storage.InsertObject(taskTxn, "concurrent.separate", new { 
                        Id = i, 
                        Value = $"Concurrent_{i}",
                        TaskId = i,
                        ThreadId = Thread.CurrentThread.ManagedThreadId,
                        Timestamp = DateTime.UtcNow
                    });
                    _storage.CommitTransaction(taskTxn);
                    
                    Interlocked.Increment(ref successful);
                    results.Add($"Task {i}: SUCCESS -> page {pageId}");
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failed);
                    results.Add($"Task {i}: FAILED -> {ex.Message}");
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromSeconds(30));
        
        // Print results
        foreach (var result in results.OrderBy(r => r))
        {
            Console.WriteLine(result);
        }
        
        // Verify final state
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(verifyTxn, "concurrent.separate", "*");
        var finalCount = finalData.Values.Sum(p => p.Length);
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"Concurrent separate result: {successful} successful, {failed} failed");
        Console.WriteLine($"Final data: {finalCount} objects in {finalData.Count} pages");
        
        foreach (var kvp in finalData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        // If transactions succeeded, objects should be preserved
        Assert.Equal(successful, finalCount);
    }

    [Fact]
    public void RaceTest_ConcurrentSameTransaction_MultipleInserts()
    {
        Console.WriteLine("=== CONCURRENT SAME TRANSACTION (INVALID BUT TESTING) ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "concurrent.same.txn");
        _storage.CommitTransaction(setupTxn);
        
        // DANGEROUS: Multiple threads using same transaction (this should fail or cause issues)
        var sharedTxn = _storage.BeginTransaction();
        var successful = 0;
        var failed = 0;
        var results = new ConcurrentBag<string>();
        
        var tasks = Enumerable.Range(0, 10).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    // UNSAFE: Using shared transaction from multiple threads
                    var pageId = _storage.InsertObject(sharedTxn, "concurrent.same.txn", new { 
                        Id = i, 
                        Value = $"SharedTxn_{i}",
                        ThreadId = Thread.CurrentThread.ManagedThreadId
                    });
                    
                    Interlocked.Increment(ref successful);
                    results.Add($"Task {i}: SUCCESS -> page {pageId}");
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failed);
                    results.Add($"Task {i}: FAILED -> {ex.Message}");
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromSeconds(10));
        
        try
        {
            _storage.CommitTransaction(sharedTxn);
            Console.WriteLine("Shared transaction committed successfully");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Shared transaction failed to commit: {ex.Message}");
        }
        
        foreach (var result in results.OrderBy(r => r))
        {
            Console.WriteLine(result);
        }
        
        Console.WriteLine($"Shared transaction test: {successful} successful, {failed} failed");
        
        // This test is just to see what happens - we don't assert specific results
        // since using same transaction from multiple threads is invalid
    }

    [Fact]
    public void RaceTest_FileSystemDirectAnalysis_CheckForCorruption()
    {
        Console.WriteLine("=== FILESYSTEM ANALYSIS ===");
        
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "filesystem.analysis");
        _storage.CommitTransaction(setupTxn);
        
        // Create 10 objects
        for (int i = 0; i < 10; i++)
        {
            var txn = _storage.BeginTransaction();
            _storage.InsertObject(txn, "filesystem.analysis", new { Id = i, Data = $"Object_{i}" });
            _storage.CommitTransaction(txn);
        }
        
        // Analyze filesystem directly
        var namespacePath = Path.Combine(_testRootPath, "filesystem", "analysis");
        if (Directory.Exists(namespacePath))
        {
            var allFiles = Directory.GetFiles(namespacePath, "*", SearchOption.AllDirectories);
            Console.WriteLine($"Files in namespace: {allFiles.Length}");
            
            foreach (var file in allFiles.OrderBy(f => f))
            {
                var fileName = Path.GetFileName(file);
                var size = new FileInfo(file).Length;
                
                try
                {
                    var content = File.ReadAllText(file);
                    var objectCount = content.Where(c => c == '[').Count(); // Rough JSON array count
                    
                    Console.WriteLine($"  {fileName}: {size} bytes, ~{objectCount} arrays");
                    
                    // Check for corruption patterns
                    if (content.Contains("}{"))
                    {
                        Console.WriteLine($"    *** POTENTIAL CORRUPTION: Adjacent objects without array structure ***");
                    }
                    
                    if (!content.StartsWith('[') || !content.EndsWith(']'))
                    {
                        Console.WriteLine($"    *** POTENTIAL CORRUPTION: Invalid JSON array structure ***");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  {fileName}: ERROR reading - {ex.Message}");
                }
            }
        }
        
        // Verify through storage API
        var verifyTxn = _storage.BeginTransaction();
        var apiData = _storage.GetMatchingObjects(verifyTxn, "filesystem.analysis", "*");
        var apiCount = apiData.Values.Sum(p => p.Length);
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"API reports: {apiCount} objects");
        Assert.Equal(10, apiCount);
    }

    public void Dispose()
    {
        try
        {
            Console.WriteLine($"Test files preserved at: {_testRootPath}");
            // Don't delete for analysis
        }
        catch
        {
            // Cleanup failed - not critical  
        }
    }
}