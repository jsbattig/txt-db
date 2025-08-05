using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// FIXED STRESS TESTS - Using proper Read-Modify-Write pattern
/// </summary>
public class FixedStressTestDiagnostics : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public FixedStressTestDiagnostics()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_fixed_stress_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void FixedStressTest_ProperReadModifyWrite_PreservesAllData()
    {
        // Arrange - Create test data
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "fixed.stress.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create 20 pages for testing
        var pageIds = new List<string>();
        for (int i = 0; i < 20; i++)
        {
            var pageId = _storage.InsertObject(setupTxn, @namespace, new { 
                Id = i, 
                Balance = 1000, 
                Version = 1,
                LastModified = DateTime.UtcNow 
            });
            pageIds.Add(pageId);
        }
        _storage.CommitTransaction(setupTxn);

        var successfulTransactions = 0;
        var conflictedTransactions = 0;
        var exceptions = new ConcurrentBag<Exception>();
        var random = new Random(42);

        // Act - Launch 50 concurrent transactions with PROPER update pattern
        var tasks = Enumerable.Range(1, 50).Select(txnId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    
                    // Get all pages to work with
                    var allPages = _storage.GetMatchingObjects(txn, @namespace, "*");
                    
                    // Pick 1-3 random pages to modify
                    var pagesToModify = allPages.Keys
                        .OrderBy(x => random.Next())
                        .Take(random.Next(1, 4))
                        .ToList();
                    
                    foreach (var pageId in pagesToModify)
                    {
                        // CRITICAL: Read-Modify-Write pattern
                        // 1. READ the entire page
                        var currentContent = _storage.ReadPage(txn, @namespace, pageId);
                        
                        // 2. MODIFY - update specific objects while preserving others
                        var modifiedContent = StressTestHelpers.UpdateObjectsOnPage(
                            currentContent,
                            selector: obj => true, // Update all objects on this page
                            updater: obj => new {
                                Id = StressTestHelpers.GetPropertyValue<int>(obj, "Id", 0),
                                Balance = random.Next(0, 2000),
                                Version = txnId,
                                LastModified = DateTime.UtcNow,
                                ModifiedBy = $"Transaction_{txnId}",
                                OriginalBalance = StressTestHelpers.GetPropertyValue<int>(obj, "Balance", 0)
                            }
                        );
                        
                        // 3. WRITE the entire modified content back
                        _storage.UpdatePage(txn, @namespace, pageId, modifiedContent);
                    }
                    
                    _storage.CommitTransaction(txn);
                    Interlocked.Increment(ref successfulTransactions);
                }
                catch (InvalidOperationException ex) when (ex.Message.ToLowerInvariant().Contains("conflict"))
                {
                    Interlocked.Increment(ref conflictedTransactions);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks, TimeSpan.FromMinutes(2));

        // Assert - Verify system state
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        var actualObjectCount = allData.Values.Sum(pages => pages.Length);
        
        // With proper Read-Modify-Write, ALL data should be preserved
        Assert.Equal(20, actualObjectCount);
        Assert.True(successfulTransactions > 0, "Should have successful transactions");
        Assert.True(conflictedTransactions > 0, "Should have some conflicts due to concurrency");
        Assert.Empty(exceptions);
    }

    [Fact]
    public void FixedStressTest_HighConcurrency_MaintainsDataIntegrity()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "fixed.high.concurrency";
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

        // Act - 200 concurrent transactions
        var successful = 0;
        var conflicts = 0;
        
        var tasks = Enumerable.Range(1, 200).Select(taskId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    var pages = _storage.GetMatchingObjects(txn, @namespace, "*");
                    
                    // Update a random page
                    var pageId = pages.Keys.Skip(new Random().Next(pages.Count)).First();
                    var content = _storage.ReadPage(txn, @namespace, pageId);
                    
                    // Properly update: increment counters while preserving all objects
                    var updated = content.Select(obj => {
                        dynamic item = obj;
                        return new {
                            Id = StressTestHelpers.GetPropertyValue<int>(item, "Id", 0),
                            Counter = StressTestHelpers.GetPropertyValue<int>(item, "Counter", 0) + 1,
                            LastUpdated = DateTime.UtcNow,
                            UpdatedBy = $"Task_{taskId}"
                        };
                    }).ToArray();
                    
                    _storage.UpdatePage(txn, @namespace, pageId, updated);
                    _storage.CommitTransaction(txn);
                    
                    Interlocked.Increment(ref successful);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("conflict"))
                {
                    Interlocked.Increment(ref conflicts);
                }
            })
        ).ToArray();
        
        Task.WaitAll(tasks);
        
        // Assert
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        _storage.CommitTransaction(finalTxn);
        
        var finalCount = finalData.Values.Sum(pages => pages.Length);
        
        Assert.Equal(objectCount, finalCount); // No data loss!
        Assert.True(successful > 50); // Many should succeed
        Assert.True(conflicts > 50);  // Many should conflict due to high concurrency
        Assert.Equal(200, successful + conflicts); // All accounted for
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
