using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// CORRECTED STRESS TEST - Proper understanding of page-based architecture
/// UpdatePage replaces ENTIRE page content, not individual objects
/// </summary>
public class CorrectedStressTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public CorrectedStressTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_corrected_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true  // Critical: Ensure proper MVCC isolation
        });
    }

    [Fact]
    public void CorrectedStress_ProperPageUpdates_ShouldPreserveAllData()
    {
        Console.WriteLine("=== CORRECTED STRESS TEST - PROPER PAGE UPDATES ===");
        
        // Setup: Create multiple objects (they may go to same or different pages)
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "corrected.stress.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var initialObjects = new List<object>();
        for (int i = 0; i < 20; i++)
        {
            var obj = new { 
                Id = i, 
                Value = $"Object_{i}", 
                Balance = 1000,
                Version = 1,
                CreatedAt = DateTime.UtcNow 
            };
            initialObjects.Add(obj);
            _storage.InsertObject(setupTxn, @namespace, obj);
        }
        _storage.CommitTransaction(setupTxn);
        
        // Verify initial state
        var verifyTxn = _storage.BeginTransaction();
        var initialData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        var initialCount = initialData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"Initial setup: {initialCount} objects in {initialData.Count} pages");
        foreach (var kvp in initialData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        Assert.Equal(20, initialCount);
        
        // Concurrent updates - PROPER way: read entire page, modify objects, write entire page back
        var successful = 0;
        var conflicts = 0;
        var exceptions = new ConcurrentBag<Exception>();
        
        var tasks = Enumerable.Range(1, 50).Select(taskId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    
                    // Get all pages and pick one randomly to update
                    var allPages = _storage.GetMatchingObjects(txn, @namespace, "*");
                    if (allPages.Count == 0) return;
                    
                    var randomPage = allPages.Keys.Skip(new Random().Next(allPages.Count)).First();
                    
                    // Read the ENTIRE page content
                    var pageContent = _storage.ReadPage(txn, @namespace, randomPage);
                    
                    if (pageContent.Length == 0) 
                    {
                        _storage.CommitTransaction(txn);
                        return;
                    }
                    
                    // Modify ALL objects on the page (preserving existing data)
                    var updatedPageContent = new List<object>();
                    
                    foreach (var obj in pageContent)
                    {
                        // Use dynamic to handle different JSON object types
                        dynamic dynObj = obj;
                        var existingId = (int)dynObj.Id;
                        var existingValue = (string)dynObj.Value;
                        
                        // Create updated version of this object
                        var updatedObj = new {
                            Id = existingId,
                            Value = existingValue,
                            Balance = new Random().Next(500, 1500),
                            Version = taskId,
                            UpdatedAt = DateTime.UtcNow,
                            UpdatedBy = $"Task_{taskId}"
                        };
                        
                        updatedPageContent.Add(updatedObj);
                    }
                    
                    // Write the ENTIRE updated page content back (read before write satisfied by pageContent read above)
                    _storage.UpdatePage(txn, @namespace, randomPage, updatedPageContent.ToArray());
                    
                    _storage.CommitTransaction(txn);
                    Interlocked.Increment(ref successful);
                }
                catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                {
                    Interlocked.Increment(ref conflicts);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromMinutes(2));
        
        // Final verification
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(finalTxn);
        
        Console.WriteLine($"=== RESULTS ===");
        Console.WriteLine($"Operations: {successful} successful, {conflicts} conflicts, {exceptions.Count} errors");
        Console.WriteLine($"Final data: {finalCount} objects in {finalData.Count} pages");
        
        foreach (var kvp in finalData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        if (exceptions.Any())
        {
            Console.WriteLine("Exceptions:");
            foreach (var ex in exceptions.Take(3))
            {
                Console.WriteLine($"  {ex.GetType().Name}: {ex.Message}");
            }
        }
        
        // With proper page updates, ALL objects should be preserved
        Assert.Equal(20, finalCount);
        Assert.True(successful > 10, $"Should have some successful operations, got {successful}");
    }

    [Fact]
    public void CorrectedStress_IndividualObjectUpdates_UsingInsertInsteadOfUpdate()
    {
        Console.WriteLine("=== ALTERNATIVE APPROACH: USE INSERT FOR INDIVIDUAL OBJECTS ===");
        
        // If you want individual object updates, use InsertObject to append to pages
        // rather than UpdatePage which replaces entire pages
        
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "individual.updates.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);
        
        var successful = 0;
        var exceptions = new ConcurrentBag<Exception>();
        
        // Concurrent insertions (this should work fine with the InsertObject fix)
        var tasks = Enumerable.Range(1, 100).Select(taskId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    
                    // Insert individual objects - they'll be properly appended
                    _storage.InsertObject(txn, @namespace, new {
                        Id = taskId,
                        TaskId = taskId,
                        Value = $"ConcurrentObject_{taskId}",
                        CreatedAt = DateTime.UtcNow,
                        ThreadId = Thread.CurrentThread.ManagedThreadId
                    });
                    
                    _storage.CommitTransaction(txn);
                    Interlocked.Increment(ref successful);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks, TimeSpan.FromMinutes(1));
        
        // Verify all objects were preserved
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        var totalObjects = allData.Values.Sum(pages => pages.Length);
        _storage.CommitTransaction(verifyTxn);
        
        Console.WriteLine($"Individual insertions: {successful} successful, {exceptions.Count} errors");
        Console.WriteLine($"Total objects preserved: {totalObjects} (expected: {successful})");
        
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        // All successful insertions should be preserved
        Assert.Equal(successful, totalObjects);
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