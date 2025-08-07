using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// Critical race condition and TOCTOU tests for StorageSubsystem.
/// These tests specifically target the race conditions identified in page allocation logic.
/// 
/// RACE CONDITIONS EXPOSED:
/// 1. TOCTOU in ActiveTransactions.Count check vs page allocation
/// 2. Page space checking outside metadata lock scope
/// 3. Non-atomic page number allocation
/// </summary>
public class CriticalIssueStorageTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private string _testDataPath = string.Empty;
    private StorageSubsystem _storage = null!;
    
    public CriticalIssueStorageTests(ITestOutputHelper output)
    {
        _output = output;
        Setup();
    }
    
    private void Setup()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDataPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testDataPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = false, // Enable page reuse to trigger race conditions
            MaxPageSizeKB = 64
        });
    }
    
    public void Dispose()
    {
        _storage?.Dispose();
        if (Directory.Exists(_testDataPath))
        {
            Directory.Delete(_testDataPath, recursive: true);
        }
    }
    
    /// <summary>
    /// CRITICAL RACE CONDITION TEST: TOCTOU in transaction count check.
    /// 
    /// This test exposes the Time-of-Check-Time-of-Use race condition where:
    /// 1. Multiple threads check _metadata.ActiveTransactions.Count <= 1
    /// 2. All threads pass the check simultaneously 
    /// 3. All threads proceed to page space checking outside the metadata lock
    /// 4. Multiple threads select the same page causing MVCC conflicts
    /// 
    /// EXPECTED BEHAVIOR: Test should FAIL before fix, showing race condition
    /// AFTER FIX: Test should pass, showing atomic page allocation
    /// </summary>
    [Fact]
    public void ConcurrentInserts_TOCTOU_RaceCondition_ShouldExposeRaceWindow()
    {
        // This test will FAIL before the fix due to race conditions
        // After the fix, it should pass consistently
        
        const int threadCount = 50;
        const int insertsPerThread = 10;
        var exceptions = new ConcurrentBag<Exception>();
        var successfulInserts = new ConcurrentBag<string>();
        var pageDuplicates = new ConcurrentDictionary<string, List<long>>();
        
        var transactions = new long[threadCount];
        for (int i = 0; i < threadCount; i++)
        {
            transactions[i] = _storage.BeginTransaction();
        }
        
        var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];
        
        for (int t = 0; t < threadCount; t++)
        {
            var threadIndex = t;
            var transactionId = transactions[threadIndex];
            
            tasks[t] = Task.Run(() =>
            {
                try
                {
                    // Synchronize all threads to maximize race condition probability
                    barrier.SignalAndWait();
                    
                    for (int i = 0; i < insertsPerThread; i++)
                    {
                        var data = new { ThreadId = threadIndex, InsertIndex = i, Timestamp = DateTime.UtcNow.Ticks };
                        
                        // This call should trigger the race condition in FindOrCreatePageForInsertion
                        var pageId = _storage.InsertObject(transactionId, "test_namespace", data);
                        
                        successfulInserts.Add($"thread{threadIndex}_insert{i}_page{pageId}");
                        
                        // Track which transaction used which page
                        pageDuplicates.AddOrUpdate(pageId, 
                            new List<long> { transactionId },
                            (key, list) => { list.Add(transactionId); return list; });
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });
        }
        
        Task.WaitAll(tasks);
        
        // Attempt to commit all transactions
        var commitExceptions = new ConcurrentBag<Exception>();
        Parallel.For(0, threadCount, i =>
        {
            try
            {
                _storage.CommitTransaction(transactions[i]);
            }
            catch (Exception ex)
            {
                commitExceptions.Add(ex);
            }
        });
        
        // ANALYSIS: Before fix, this test should show:
        // 1. Multiple transactions selecting same page due to race condition
        // 2. MVCC conflicts during commit
        // 3. Lost data or inconsistent state
        
        _output.WriteLine($"Total successful inserts: {successfulInserts.Count}");
        _output.WriteLine($"Total exceptions during insert: {exceptions.Count}");
        _output.WriteLine($"Total commit exceptions: {commitExceptions.Count}");
        
        // Check for page reuse conflicts (the race condition symptom)
        var conflictedPages = pageDuplicates.Where(kvp => kvp.Value.Count > 1).ToList();
        _output.WriteLine($"Pages with multiple concurrent transactions: {conflictedPages.Count}");
        
        foreach (var conflict in conflictedPages.Take(5)) // Show first 5 conflicts
        {
            _output.WriteLine($"Page {conflict.Key} used by transactions: [{string.Join(", ", conflict.Value)}]");
        }
        
        // Before fix: This assertion should FAIL due to race conditions
        // After fix: This assertion should PASS consistently
        Assert.True(exceptions.Count == 0, 
            $"Race condition detected: {exceptions.Count} exceptions occurred during concurrent inserts. " +
            $"First exception: {exceptions.FirstOrDefault()?.Message}");
        
        Assert.True(commitExceptions.Count == 0,
            $"MVCC conflicts detected during commit: {commitExceptions.Count} commit failures. " +
            $"First commit exception: {commitExceptions.FirstOrDefault()?.Message}");
        
        // Expect consistent successful inserts
        Assert.Equal(threadCount * insertsPerThread, successfulInserts.Count);
    }
    
    /// <summary>
    /// CRITICAL TEST: Metadata lock scope vulnerability.
    /// 
    /// This test exposes the vulnerability where page space checking happens
    /// outside the metadata lock, allowing race conditions in page allocation.
    /// 
    /// The test uses timing to force the race condition window.
    /// </summary>
    [Fact]
    public void ConcurrentPageAllocation_MetadataLockScope_ShouldExposeVulnerability()
    {
        const int concurrentThreads = 20;
        var exceptions = new ConcurrentBag<Exception>();
        var allocatedPages = new ConcurrentBag<(long transactionId, string pageId)>();
        
        var transactions = new List<long>();
        for (int i = 0; i < concurrentThreads; i++)
        {
            transactions.Add(_storage.BeginTransaction());
        }
        
        var barrier = new Barrier(concurrentThreads);
        var tasks = new Task[concurrentThreads];
        
        for (int t = 0; t < concurrentThreads; t++)
        {
            var threadIndex = t;
            var transactionId = transactions[threadIndex];
            
            tasks[t] = Task.Run(() =>
            {
                try
                {
                    // Synchronize to maximize race condition probability
                    barrier.SignalAndWait();
                    
                    // Insert exactly one object to trigger page allocation race
                    var data = new { ThreadId = threadIndex, Data = "test_data_" + new string('x', 100) };
                    var pageId = _storage.InsertObject(transactionId, "race_test", data);
                    
                    allocatedPages.Add((transactionId, pageId));
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });
        }
        
        Task.WaitAll(tasks);
        
        // Analyze the race condition symptoms
        var pageUsage = allocatedPages
            .GroupBy(x => x.pageId)
            .ToDictionary(g => g.Key, g => g.Select(x => x.transactionId).ToList());
        
        var conflictedPages = pageUsage.Where(kvp => kvp.Value.Count > 1).ToList();
        
        _output.WriteLine($"Total threads: {concurrentThreads}");
        _output.WriteLine($"Successful allocations: {allocatedPages.Count}");
        _output.WriteLine($"Exceptions: {exceptions.Count}");
        _output.WriteLine($"Conflicted pages: {conflictedPages.Count}");
        
        foreach (var conflict in conflictedPages)
        {
            _output.WriteLine($"Page {conflict.Key} allocated by transactions: [{string.Join(", ", conflict.Value)}]");
        }
        
        // Clean up transactions
        foreach (var tid in transactions)
        {
            try
            {
                _storage.CommitTransaction(tid);
            }
            catch
            {
                // Expected due to conflicts
            }
        }
        
        // Before fix: Multiple transactions should get same page due to race condition
        // After fix: Each transaction should get unique page
        Assert.True(conflictedPages.Count == 0);
        
        Assert.True(exceptions.Count == 0);
    }
    
    /// <summary>
    /// ATOMIC DECISION MAKING TEST
    /// 
    /// This test verifies that the transaction count check and page allocation 
    /// decision are made atomically within the same metadata lock.
    /// </summary>
    [Fact]
    public void PageAllocation_AtomicDecisionMaking_ShouldPreventTOCTOUWindow()
    {
        const int threadCount = 30;
        var results = new ConcurrentBag<(long transactionId, string pageId, bool reuseDecision)>();
        var exceptions = new ConcurrentBag<Exception>();
        
        // Create transactions
        var transactions = Enumerable.Range(0, threadCount)
            .Select(_ => _storage.BeginTransaction())
            .ToList();
        
        var barrier = new Barrier(threadCount);
        
        // Execute concurrent inserts
        var tasks = transactions.Select((tid, index) => Task.Run(() =>
        {
            try
            {
                barrier.SignalAndWait();
                
                // The key is that ALL threads check ActiveTransactions.Count at nearly same time
                // Before fix: All see count <= 1 and try to reuse pages
                // After fix: Atomic decision prevents this race
                
                var data = new { ThreadIndex = index, Payload = "data_" + index };
                var pageId = _storage.InsertObject(tid, "atomic_test", data);
                
                // Try to determine if page reuse was attempted (heuristic)
                var reuseAttempted = pageId.StartsWith("page001") || pageId.StartsWith("page002");
                
                results.Add((tid, pageId, reuseAttempted));
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();
        
        Task.WaitAll(tasks);
        
        // Analyze results
        var pageUsage = results.GroupBy(r => r.pageId).ToDictionary(g => g.Key, g => g.ToList());
        var conflictPages = pageUsage.Where(kvp => kvp.Value.Count > 1).ToList();
        
        _output.WriteLine($"Total results: {results.Count}");
        _output.WriteLine($"Exceptions: {exceptions.Count}");
        _output.WriteLine($"Conflicted pages: {conflictPages.Count}");
        _output.WriteLine($"Unique pages allocated: {pageUsage.Count}");
        
        // Clean up
        foreach (var tid in transactions)
        {
            try { _storage.CommitTransaction(tid); } catch { /* Expected conflicts */ }
        }
        
        // ASSERTION: Atomic decision making should prevent TOCTOU race
        Assert.Equal(0, conflictPages.Count);
        
        Assert.Equal(0, exceptions.Count);
    }
    
    /// <summary>
    /// PAGE NUMBER ALLOCATION ATOMICITY TEST
    /// 
    /// This test specifically targets the page number allocation logic to ensure
    /// it's atomic and doesn't result in duplicate page numbers.
    /// </summary>
    [Fact]
    public void PageNumberAllocation_ShouldBeAtomic_PreventDuplicates()
    {
        const int concurrentTransactions = 40;
        var pageNumbers = new ConcurrentBag<(long transactionId, string pageId)>();
        var exceptions = new ConcurrentBag<Exception>();
        
        // Force page creation by using ForceOneObjectPerPage
        _storage.Dispose();
        _storage = new StorageSubsystem();
        _storage.Initialize(_testDataPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true, // Force new page creation
            MaxPageSizeKB = 64
        });
        
        var transactions = Enumerable.Range(0, concurrentTransactions)
            .Select(_ => _storage.BeginTransaction())
            .ToList();
        
        var barrier = new Barrier(concurrentTransactions);
        
        var tasks = transactions.Select((tid, index) => Task.Run(() =>
        {
            try
            {
                barrier.SignalAndWait();
                
                var data = new { Index = index };
                var pageId = _storage.InsertObject(tid, "page_number_test", data);
                
                pageNumbers.Add((tid, pageId));
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();
        
        Task.WaitAll(tasks);
        
        // Analyze page number allocation
        var allPageIds = pageNumbers.Select(p => p.pageId).ToList();
        var duplicatePages = allPageIds.GroupBy(p => p).Where(g => g.Count() > 1).ToList();
        
        _output.WriteLine($"Total page allocations: {allPageIds.Count}");
        _output.WriteLine($"Unique pages: {allPageIds.Distinct().Count()}");
        _output.WriteLine($"Duplicate pages: {duplicatePages.Count}");
        _output.WriteLine($"Exceptions: {exceptions.Count}");
        
        foreach (var dup in duplicatePages.Take(3))
        {
            _output.WriteLine($"Duplicate page: {dup.Key} allocated {dup.Count()} times");
        }
        
        // Clean up
        foreach (var tid in transactions)
        {
            try { _storage.CommitTransaction(tid); } catch { /* May have conflicts */ }
        }
        
        // ASSERTION: No duplicate page numbers should be allocated
        Assert.Equal(0, duplicatePages.Count);
        
        Assert.Equal(concurrentTransactions, allPageIds.Distinct().Count());
        
        Assert.Equal(0, exceptions.Count);
    }
}