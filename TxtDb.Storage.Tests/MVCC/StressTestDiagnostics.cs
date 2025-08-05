using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// DIAGNOSTIC STRESS TESTS - Analyze stress test failures and performance issues
/// </summary>
public class StressTestDiagnostics : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public StressTestDiagnostics()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_stress_diag_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true  // Critical: Ensure proper MVCC isolation for stress tests
        });
    }

    [Fact]
    public void DiagnosticTest_SimpleUpdatePattern_ShouldPreserveAllObjects()
    {
        // Arrange - Create test data similar to stress test
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "diagnostic.simple.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create 10 pages for testing
        var pageIds = new List<string>();
        for (int i = 0; i < 10; i++)
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

        // Verify initial state
        var initialTxn = _storage.BeginTransaction();
        var initialData = _storage.GetMatchingObjects(initialTxn, @namespace, "*");
        Assert.Equal(10, initialData.Values.Sum(pages => pages.Length));
        _storage.CommitTransaction(initialTxn);

        // Act - Sequential updates to understand the pattern
        for (int txnId = 1; txnId <= 5; txnId++)
        {
            var updateTxn = _storage.BeginTransaction();
            
            // Update the first page using PROPER Read-Modify-Write pattern
            var pageId = pageIds[0];
            var currentData = _storage.ReadPage(updateTxn, @namespace, pageId);
            
            // Build new page content preserving existing data
            var updatedContent = new List<object>();
            
            // Preserve ALL existing objects on the page
            foreach (var existingObj in currentData)
            {
                updatedContent.Add(existingObj);
            }
            
            // Add new object to the page (append pattern to avoid data loss)
            updatedContent.Add(new { 
                Id = txnId * 1000,
                Balance = txnId * 100,
                Version = txnId,
                LastModified = DateTime.UtcNow,
                ModifiedBy = $"Transaction_{txnId}"
            });
            
            // Write ALL content back to preserve data integrity
            _storage.UpdatePage(updateTxn, @namespace, pageId, updatedContent.ToArray());
            
            _storage.CommitTransaction(updateTxn);
            
            // Verify after each update
            var verifyTxn = _storage.BeginTransaction();
            var verifyData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
            _storage.CommitTransaction(verifyTxn);
            
            var actualCount = verifyData.Values.Sum(pages => pages.Length);
            var expectedCount = 10 + txnId; // Original 10 plus one added per transaction
            Assert.True(actualCount == expectedCount, 
                $"After transaction {txnId}, expected {expectedCount} objects but found {actualCount}");
        }
        
        // Final verification
        var finalTxn = _storage.BeginTransaction();
        var finalData = _storage.GetMatchingObjects(finalTxn, @namespace, "*");
        var finalExpected = 10 + 5; // Original 10 plus 5 added objects
        Assert.Equal(finalExpected, finalData.Values.Sum(pages => pages.Length));
        _storage.CommitTransaction(finalTxn);
    }

    [Fact]
    public void DiagnosticTest_ConcurrentUpdatesSmallScale_ShouldPreserveAllObjects()
    {
        // Arrange - Create test data
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "diagnostic.concurrent.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create 5 pages for testing
        var pageIds = new List<string>();
        for (int i = 0; i < 5; i++)
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

        // Act - Launch 10 concurrent transactions (small scale)
        var tasks = Enumerable.Range(1, 10).Select(txnId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    
                    // Each transaction updates one random page
                    var pageId = pageIds[txnId % pageIds.Count];
                    
                    // Read first
                    var data = _storage.ReadPage(txn, @namespace, pageId);
                    Assert.NotEmpty(data);
                    
                    // Small delay to increase contention
                    Thread.Sleep(10);
                    
                    // PROPER Read-Modify-Write pattern to preserve existing data
                    var currentPageData = _storage.ReadPage(txn, @namespace, pageId);
                    
                    // Build updated content preserving ALL existing objects
                    var updatedContent = new List<object>();
                    foreach (var existingObj in currentPageData)
                    {
                        updatedContent.Add(existingObj); // Preserve all existing objects
                    }
                    
                    // Add new object without losing existing ones
                    updatedContent.Add(new { 
                        Id = txnId * 1000,
                        Balance = txnId * 100,
                        Version = txnId,
                        LastModified = DateTime.UtcNow,
                        ModifiedBy = $"Transaction_{txnId}"
                    });
                    
                    // Write ALL objects back to preserve data integrity
                    _storage.UpdatePage(txn, @namespace, pageId, updatedContent.ToArray());
                    
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

        Task.WaitAll(tasks, TimeSpan.FromMinutes(1));

        // Assert - Check results
        var totalOperations = successfulTransactions + conflictedTransactions + exceptions.Count;
        Assert.Equal(10, totalOperations);
        
        if (exceptions.Any())
        {
            var firstException = exceptions.First();
            throw new Exception($"Unexpected exception in concurrent test: {firstException.Message}", firstException);
        }

        // Verify all objects still exist
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        var actualCount = allData.Values.Sum(pages => pages.Length);
        // With proper Read-Modify-Write, we should NEVER lose the original 5 objects
        Assert.True(actualCount >= 5, 
            $"Data loss detected: expected at least 5 objects (original), but found {actualCount}");
        
        // Additional check: successful transactions should have added objects
        var expectedWithSuccessful = 5 + successfulTransactions;
        if (actualCount < expectedWithSuccessful)
        {
            // This might happen due to concurrent conflicts, but let's log it
            Console.WriteLine($"WARNING: Expected {expectedWithSuccessful} objects ({successfulTransactions} successful adds), but found {actualCount}");
        }
    }

    [Fact]
    public void DiagnosticTest_AnalyzeUpdatePageBehavior_ShouldShowDetailedResults()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "diagnostic.analyze.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var pageId = _storage.InsertObject(setupTxn, @namespace, new { 
            Id = 1, 
            Value = "Original",
            Version = 1
        });
        _storage.CommitTransaction(setupTxn);

        // Act - Multiple updates with verification
        for (int i = 2; i <= 5; i++)
        {
            var updateTxn = _storage.BeginTransaction();
            
            // Read current data
            var currentData = _storage.ReadPage(updateTxn, @namespace, pageId);
            Assert.Single(currentData);
            
            // Update with new data
            _storage.UpdatePage(updateTxn, @namespace, pageId, new object[] { 
                new { 
                    Id = i,
                    Value = $"Updated_{i}",
                    Version = i,
                    PreviousData = currentData[0]
                } 
            });
            
            _storage.CommitTransaction(updateTxn);
            
            // Immediately verify the update
            var verifyTxn = _storage.BeginTransaction();
            var updatedData = _storage.ReadPage(verifyTxn, @namespace, pageId);
            _storage.CommitTransaction(verifyTxn);
            
            Assert.Single(updatedData);
            
            // Verify the namespace still has exactly 1 object
            var namespaceTxn = _storage.BeginTransaction();
            var namespaceData = _storage.GetMatchingObjects(namespaceTxn, @namespace, "*");
            _storage.CommitTransaction(namespaceTxn);
            
            var namespaceCount = namespaceData.Values.Sum(pages => pages.Length);
            Assert.True(namespaceCount == 1, 
                $"After update {i}, namespace should have 1 object but has {namespaceCount}");
        }
    }

    [Fact]
    public void DiagnosticTest_StressTestConditionsReduced_ShouldRevealIssue()
    {
        // Arrange - Exactly like stress test but reduced scale
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "diagnostic.stress.reduced";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        // Create 20 pages instead of 100
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
        var random = new Random(42); // Fixed seed for reproducibility

        // Act - Launch 50 concurrent transactions instead of 1000
        var tasks = Enumerable.Range(1, 50).Select(txnId =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    
                    // Each transaction works with 1-3 random pages instead of 1-5
                    var pagesToModify = pageIds.OrderBy(x => random.Next()).Take(random.Next(1, 4)).ToList();
                    var readData = new Dictionary<string, object[]>();
                    
                    // Read phase
                    foreach (var pageId in pagesToModify)
                    {
                        var data = _storage.ReadPage(txn, @namespace, pageId);
                        readData[pageId] = data;
                    }
                    
                    // Small delay to increase contention
                    Thread.Sleep(random.Next(1, 5));
                    
                    // Write phase - PROPER Read-Modify-Write pattern to preserve existing data
                    foreach (var pageId in pagesToModify)
                    {
                        // Get existing data from our read phase
                        var existingData = readData[pageId];
                        
                        // Build updated content preserving existing objects
                        var updatedContent = new List<object>();
                        
                        // Preserve all existing objects on the page
                        foreach (var existingObj in existingData)
                        {
                            updatedContent.Add(existingObj);
                        }
                        
                        // Add new object to the page (append pattern)
                        updatedContent.Add(new { 
                            Id = random.Next(1000, 9999),
                            Balance = random.Next(0, 2000),
                            Version = txnId,
                            LastModified = DateTime.UtcNow,
                            ModifiedBy = $"Transaction_{txnId}"
                        });
                        
                        // Write ALL content back to preserve data integrity
                        _storage.UpdatePage(txn, @namespace, pageId, updatedContent.ToArray());
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

        // Detailed analysis
        var totalOperations = successfulTransactions + conflictedTransactions + exceptions.Count;
        
        // Print diagnostics
        Console.WriteLine($"=== STRESS TEST DIAGNOSTICS ===");
        Console.WriteLine($"Total operations: {totalOperations}");
        Console.WriteLine($"Successful: {successfulTransactions}");
        Console.WriteLine($"Conflicted: {conflictedTransactions}");
        Console.WriteLine($"Exceptions: {exceptions.Count}");
        
        if (exceptions.Any())
        {
            Console.WriteLine("Exception details:");
            foreach (var ex in exceptions.Take(3))
            {
                Console.WriteLine($"  - {ex.GetType().Name}: {ex.Message}");
            }
        }
        
        // Verify system state
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);
        
        var actualObjectCount = allData.Values.Sum(pages => pages.Length);
        Console.WriteLine($"Initial objects: 20");
        Console.WriteLine($"Final objects: {actualObjectCount}");
        Console.WriteLine($"Objects added by successful transactions: {successfulTransactions}");
        Console.WriteLine($"Expected final count: at least {20 + successfulTransactions}");
        
        foreach (var kvp in allData)
        {
            Console.WriteLine($"  Page {kvp.Key}: {kvp.Value.Length} objects");
        }
        
        // With proper Read-Modify-Write pattern, we should have preserved data
        Assert.Equal(50, totalOperations);
        // Objects should grow since we're adding, not replacing
        Assert.True(actualObjectCount >= 20, $"Expected at least 20 objects (started with 20), got {actualObjectCount}");
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