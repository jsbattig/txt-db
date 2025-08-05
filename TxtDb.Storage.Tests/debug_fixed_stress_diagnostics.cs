using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// DEBUG: Analyze why FixedStressTest_HighConcurrency_MaintainsDataIntegrity is failing
/// </summary>
public class DebugFixedStressDiagnostics : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public DebugFixedStressDiagnostics()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_debug_fixed_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true  // Critical: Test this specific configuration
        });
    }

    [Fact]
    public void DebugHighConcurrency_CheckExpectations()
    {
        Console.WriteLine("=== FIXED STRESS TEST DEBUG ===");
        
        // Arrange - Same as the failing test
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "debug.fixed.high.concurrency";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create initial objects
        var objectCount = 100;
        for (int i = 0; i < objectCount; i++)
        {
            _storage.InsertObject(setupTxn, @namespace, new { 
                Id = i, 
                Counter = 0,
                LastUpdated = DateTime.UtcNow
            });
        }
        _storage.CommitTransaction(setupTxn);
        
        Console.WriteLine($"Setup: Created {objectCount} objects");

        // Act - 200 concurrent transactions (same as failing test)
        var successful = 0;
        var conflicts = 0;
        var exceptions = new ConcurrentBag<Exception>();
        
        var tasks = Enumerable.Range(1, 200).Select(taskId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    var pages = _storage.GetMatchingObjects(txn, @namespace, "*");
                    
                    Console.WriteLine($"Task {taskId}: Found {pages.Count} pages");
                    
                    // Update a random page
                    var pageId = pages.Keys.Skip(new Random().Next(pages.Count)).First();
                    var content = _storage.ReadPage(txn, @namespace, pageId);
                    
                    Console.WriteLine($"Task {taskId}: Updating page {pageId} with {content.Length} objects");
                    
                    // Properly update: increment counters while preserving all objects
                    var updated = content.Select(obj => {
                        dynamic item = obj;
                        var id = GetPropertyValue<int>(item, "Id", 0);
                        var counter = GetPropertyValue<int>(item, "Counter", 0);
                        return new {
                            Id = id,
                            Counter = counter + 1,
                            LastUpdated = DateTime.UtcNow,
                            UpdatedBy = $"Task_{taskId}"
                        };
                    }).ToArray();
                    
                    _storage.UpdatePage(txn, @namespace, pageId, updated);
                    _storage.CommitTransaction(txn);
                    
                    Interlocked.Increment(ref successful);
                    Console.WriteLine($"Task {taskId}: SUCCESS");
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("conflict"))
                {
                    Interlocked.Increment(ref conflicts);
                    Console.WriteLine($"Task {taskId}: CONFLICT - {ex.Message}");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    Console.WriteLine($"Task {taskId}: ERROR - {ex.Message}");
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks);
        
        // Assert
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        _storage.CommitTransaction(finalTxn);
        
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        
        Console.WriteLine($"=== RESULTS ===");
        Console.WriteLine($"Successful: {successful}");
        Console.WriteLine($"Conflicts: {conflicts}");
        Console.WriteLine($"Errors: {exceptions.Count}");
        Console.WriteLine($"Total: {successful + conflicts + exceptions.Count}");
        Console.WriteLine($"Final object count: {finalCount} (expected: {objectCount})");
        Console.WriteLine($"Final page count: {finalData.Count}");
        
        // Show what the failing test expects:
        Console.WriteLine($"=== FAILING TEST EXPECTATIONS ===");
        Console.WriteLine($"Assert.Equal({objectCount}, {finalCount}); // No data loss! --> {(objectCount == finalCount ? "PASS" : "FAIL")}");
        Console.WriteLine($"Assert.True({successful} > 50); // Many should succeed --> {(successful > 50 ? "PASS" : "FAIL")}");
        Console.WriteLine($"Assert.True({conflicts} > 50);  // Many should conflict --> {(conflicts > 50 ? "PASS" : "FAIL")}");
        Console.WriteLine($"Assert.Equal(200, {successful + conflicts}); // All accounted for --> {(200 == successful + conflicts ? "PASS" : "FAIL")}");
        
        if (exceptions.Any())
        {
            Console.WriteLine("Exceptions:");
            foreach (var ex in exceptions.Take(5))
            {
                Console.WriteLine($"  {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    private static T GetPropertyValue<T>(dynamic obj, string propertyName, T defaultValue = default(T))
    {
        try
        {
            var value = obj.GetType().GetProperty(propertyName)?.GetValue(obj);
            if (value != null && value is T)
                return (T)value;
            return defaultValue;
        }
        catch
        {
            return defaultValue;
        }
    }

    public void Dispose()
    {
        try
        {
            Console.WriteLine($"Debug files preserved at: {_testRootPath}");
        }
        catch
        {
            // Cleanup failed - not critical  
        }
    }
}