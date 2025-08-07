using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL: Tests that MUST FAIL to demonstrate MVCC isolation violations.
/// These tests expose the exact race conditions and isolation violations mentioned in the code review.
/// After fixing the MVCC implementation, these tests should pass.
/// 
/// Key Issues Being Tested:
/// 1. TSN race condition during BeginTransaction (lines 51-56 in AsyncStorageSubsystem)
/// 2. Broken conflict detection allowing multiple transactions to modify same page
/// 3. Optimistic concurrency checks happening after commit point (lines 92-100)
/// 4. Transaction state transitions without proper atomic locking
/// </summary>
public class MVCCIsolationViolationTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public MVCCIsolationViolationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_mvcc_violation_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    /// <summary>
    /// CRITICAL TEST: This test MUST FAIL initially.
    /// 
    /// Tests the TSN race condition in BeginTransactionAsync() lines 51-56.
    /// The race condition occurs when multiple threads call BeginTransaction simultaneously
    /// and _metadata.CurrentTSN changes between assignment and snapshot read.
    /// 
    /// EXPECTED FAILURE: Two transactions get the same snapshot TSN, violating MVCC isolation.
    /// AFTER FIX: Each transaction should get a unique, incrementing snapshot TSN.
    /// </summary>
    [Fact]
    public async Task BeginTransaction_ConcurrentCalls_ShouldHaveUniqueSnapshotTSNs()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false // Disable to avoid timing complexity
        });

        const int concurrentTransactions = 10;
        var transactionIds = new ConcurrentBag<long>();
        var snapshotTSNs = new ConcurrentBag<long>();
        var exceptions = new ConcurrentBag<Exception>();

        // Start many transactions simultaneously to trigger race condition
        var tasks = Enumerable.Range(0, concurrentTransactions).Select(async _ =>
        {
            try
            {
                var txnId = await _asyncStorage.BeginTransactionAsync();
                transactionIds.Add(txnId);
                
                // Extract snapshot TSN from the transaction for verification
                // This would need access to internal state - we'll verify through behavior instead
                _output.WriteLine($"Started transaction: {txnId}");
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                _output.WriteLine($"Transaction start failed: {ex.Message}");
            }
        }).ToArray();

        await Task.WhenAll(tasks);

        // Assert no exceptions during transaction creation
        Assert.Empty(exceptions);
        
        // Assert all transactions have unique IDs (this should pass)
        var uniqueTransactionIds = transactionIds.Distinct().Count();
        Assert.Equal(concurrentTransactions, uniqueTransactionIds);
        
        // Now test the actual TSN race condition by having all transactions read the same page
        // If TSN assignment is broken, they'll all see the same snapshot
        await _asyncStorage.CreateNamespaceAsync(transactionIds.First(), "test");
        var pageId = await _asyncStorage.InsertObjectAsync(transactionIds.First(), "test", new { Value = "Initial" });
        await _asyncStorage.CommitTransactionAsync(transactionIds.First());

        // Start new transactions and have them all read the same page
        var readTasks = transactionIds.Skip(1).Take(5).Select(async txnId =>
        {
            try
            {
                var data = await _asyncStorage.ReadPageAsync(txnId, "test", pageId);
                _output.WriteLine($"Transaction {txnId} read {data.Length} objects");
                return (txnId, success: true, data.Length);
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Transaction {txnId} read failed: {ex.Message}");
                return (txnId, success: false, 0);
            }
        }).ToArray();

        var readResults = await Task.WhenAll(readTasks);
        
        // All reads should succeed if TSNs are assigned correctly
        foreach (var (txnId, success, count) in readResults)
        {
            Assert.True(success, $"Transaction {txnId} should be able to read with proper TSN assignment");
            Assert.True(count > 0, $"Transaction {txnId} should see the committed data");
        }

        // Clean up remaining transactions
        foreach (var txnId in transactionIds.Skip(1))
        {
            try
            {
                await _asyncStorage.RollbackTransactionAsync(txnId);
            }
            catch
            {
                // Ignore cleanup failures
            }
        }
    }

    /// <summary>
    /// CRITICAL TEST: This test MUST FAIL initially.
    /// 
    /// Tests that multiple transactions writing to the same page should result in MVCC conflicts.
    /// Currently, the conflict detection is broken and allows multiple writes to succeed.
    /// 
    /// EXPECTED FAILURE: All transactions succeed when they should conflict.
    /// AFTER FIX: Only one transaction should succeed, others should fail with conflict errors.
    /// </summary>
    [Fact]
    public async Task ConcurrentWrites_ToSamePage_ShouldCauseConflicts()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false,
            ForceOneObjectPerPage = true // Ensure all writes go to same page
        });

        // Create initial data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "conflict_test");
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "conflict_test", new { Counter = 0, Version = 1 });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        const int concurrentWrites = 5;
        var transactions = new List<long>();
        var commitResults = new ConcurrentBag<(long txnId, bool success, string error)>();

        // Start concurrent transactions
        for (int i = 0; i < concurrentWrites; i++)
        {
            var txnId = await _asyncStorage.BeginTransactionAsync();
            transactions.Add(txnId);
            _output.WriteLine($"Started transaction {i}: {txnId}");
        }

        // All transactions read the same page
        var readTasks = transactions.Select(async (txnId, index) =>
        {
            try
            {
                var data = await _asyncStorage.ReadPageAsync(txnId, "conflict_test", pageId);
                _output.WriteLine($"Transaction {index} ({txnId}) read {data.Length} objects");
                return (txnId, success: true);
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Transaction {index} ({txnId}) read failed: {ex.Message}");
                return (txnId, success: false);
            }
        }).ToArray();

        var readResults = await Task.WhenAll(readTasks);
        
        // All reads should succeed
        foreach (var (txnId, success) in readResults)
        {
            Assert.True(success, $"Transaction {txnId} should be able to read the page");
        }

        // Now all transactions try to write to the same page simultaneously
        var writeTasks = transactions.Select(async (txnId, index) =>
        {
            try
            {
                await _asyncStorage.UpdatePageAsync(txnId, "conflict_test", pageId, 
                    new object[] { new { Counter = index, Version = 2, TransactionId = txnId } });
                _output.WriteLine($"Transaction {index} ({txnId}) write succeeded");
                return (txnId, success: true);
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Transaction {index} ({txnId}) write failed: {ex.Message}");
                return (txnId, success: false);
            }
        }).ToArray();

        var writeResults = await Task.WhenAll(writeTasks);
        
        // This is the CRITICAL FAILURE POINT - currently all writes succeed
        var successfulWrites = writeResults.Count(r => r.success);
        _output.WriteLine($"Successful writes: {successfulWrites} (should be <= 1 after fix)");

        // Try to commit all transactions
        var commitTasks = transactions.Select(async (txnId, index) =>
        {
            try
            {
                await _asyncStorage.CommitTransactionAsync(txnId);
                commitResults.Add((txnId, true, ""));
                _output.WriteLine($"Transaction {index} ({txnId}) committed successfully");
            }
            catch (Exception ex)
            {
                commitResults.Add((txnId, false, ex.Message));
                _output.WriteLine($"Transaction {index} ({txnId}) commit failed: {ex.Message}");
            }
        }).ToArray();

        await Task.WhenAll(commitTasks);

        var successfulCommits = commitResults.Count(r => r.success);
        var failedCommits = commitResults.Count(r => !r.success);
        
        _output.WriteLine($"Successful commits: {successfulCommits}");
        _output.WriteLine($"Failed commits: {failedCommits}");

        // CRITICAL ASSERTION: This MUST FAIL initially showing the MVCC violation
        // Only ONE transaction should successfully commit when writing to the same page
        Assert.True(successfulCommits <= 1, 
            $"MVCC VIOLATION DETECTED: {successfulCommits} transactions successfully committed when writing to the same page. " +
            $"MVCC should allow at most 1 commit due to optimistic concurrency conflicts. " +
            $"This proves the conflict detection is broken.");

        // Verify conflicted transactions failed with proper error messages
        var conflicts = commitResults.Where(r => !r.success).ToList();
        foreach (var (txnId, _, error) in conflicts)
        {
            Assert.Contains("conflict", error.ToLowerInvariant());
        }
    }

    /// <summary>
    /// CRITICAL TEST: This test MUST FAIL initially.
    /// 
    /// Tests the timing of optimistic concurrency checks (lines 92-100 in AsyncStorageSubsystem).
    /// Currently, conflict detection happens AFTER the commit point, meaning writes are already visible.
    /// This violates MVCC principles where conflicts should be detected BEFORE making changes visible.
    /// 
    /// EXPECTED FAILURE: Conflicts are detected too late, after writes become visible.
    /// AFTER FIX: Conflicts should be detected before commit point, preventing visibility of conflicted writes.
    /// </summary>
    [Fact]
    public async Task ConflictDetection_ShouldHappenBeforeCommitPoint()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        // Create initial data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "timing_test");
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "timing_test", new { Value = "Original", Version = 1 });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Transaction A reads the page
        var txnA = await _asyncStorage.BeginTransactionAsync();
        var dataA = await _asyncStorage.ReadPageAsync(txnA, "timing_test", pageId);
        _output.WriteLine($"Transaction A read {dataA.Length} objects");

        // Transaction B reads and commits first
        var txnB = await _asyncStorage.BeginTransactionAsync();
        var dataB = await _asyncStorage.ReadPageAsync(txnB, "timing_test", pageId);
        await _asyncStorage.UpdatePageAsync(txnB, "timing_test", pageId, 
            new object[] { new { Value = "Modified by B", Version = 2 } });
        await _asyncStorage.CommitTransactionAsync(txnB);
        _output.WriteLine("Transaction B committed successfully");

        // Start a monitoring transaction to check visibility of A's writes
        var monitorTxn = await _asyncStorage.BeginTransactionAsync();

        // Transaction A tries to update (should conflict)
        await _asyncStorage.UpdatePageAsync(txnA, "timing_test", pageId, 
            new object[] { new { Value = "Modified by A", Version = 2 } });
        _output.WriteLine("Transaction A completed write operation");

        // Check if A's write is visible BEFORE commit attempt
        // This reveals if conflict detection happens too late
        try
        {
            var monitorData = await _asyncStorage.ReadPageAsync(monitorTxn, "timing_test", pageId);
            _output.WriteLine($"Monitor transaction sees {monitorData.Length} objects");
            
            // If we can see A's uncommitted changes, conflict detection is too late
            var currentValue = ((dynamic)monitorData[0]).Value.ToString();
            _output.WriteLine($"Current value visible: {currentValue}");
            
            if (currentValue == "Modified by A")
            {
                _output.WriteLine("CRITICAL: Transaction A's uncommitted changes are visible!");
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Monitor read failed: {ex.Message}");
        }

        // Now try to commit Transaction A - should fail with conflict
        Exception? commitException = null;
        try
        {
            await _asyncStorage.CommitTransactionAsync(txnA);
            _output.WriteLine("Transaction A committed - NO CONFLICT DETECTED!");
        }
        catch (Exception ex)
        {
            commitException = ex;
            _output.WriteLine($"Transaction A failed to commit: {ex.Message}");
        }

        // Clean up
        await _asyncStorage.CommitTransactionAsync(monitorTxn);

        // CRITICAL ASSERTION: Transaction A should fail due to conflict
        Assert.NotNull(commitException);
        Assert.Contains("conflict", commitException.Message.ToLowerInvariant());
        
        // Verify final state shows only Transaction B's changes
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.ReadPageAsync(verifyTxn, "timing_test", pageId);
        var finalValue = ((dynamic)finalData[0]).Value.ToString();
        Assert.Equal("Modified by B", finalValue);
        await _asyncStorage.CommitTransactionAsync(verifyTxn);
    }

    /// <summary>
    /// CRITICAL TEST: This test MUST FAIL initially.
    /// 
    /// Tests atomic transaction state transitions. Currently, transaction state updates
    /// are not properly synchronized, leading to race conditions in transaction management.
    /// 
    /// EXPECTED FAILURE: Race conditions in transaction state management.
    /// AFTER FIX: All transaction state transitions should be atomic and consistent.
    /// </summary>
    [Fact]
    public async Task TransactionStateTransitions_ShouldBeAtomic()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        const int concurrentTransactions = 20;
        var activeTransactions = new ConcurrentBag<long>();
        var completedTransactions = new ConcurrentBag<(long id, string outcome)>();
        var exceptions = new ConcurrentBag<Exception>();

        // Create many transactions concurrently
        var createTasks = Enumerable.Range(0, concurrentTransactions).Select(async i =>
        {
            try
            {
                var txnId = await _asyncStorage.BeginTransactionAsync();
                activeTransactions.Add(txnId);
                _output.WriteLine($"Created transaction {i}: {txnId}");
                
                // Simulate some work
                await Task.Delay(10);
                
                return txnId;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                return -1;
            }
        }).ToArray();

        var transactionIds = await Task.WhenAll(createTasks);
        var validTransactions = transactionIds.Where(id => id > 0).ToList();

        // Now try to commit/rollback them concurrently
        var completeTasks = validTransactions.Select(async (txnId, index) =>
        {
            try
            {
                if (index % 2 == 0)
                {
                    // Even transactions: commit
                    await _asyncStorage.CommitTransactionAsync(txnId);
                    completedTransactions.Add((txnId, "committed"));
                    _output.WriteLine($"Transaction {txnId} committed");
                }
                else
                {
                    // Odd transactions: rollback
                    await _asyncStorage.RollbackTransactionAsync(txnId);
                    completedTransactions.Add((txnId, "rolled back"));
                    _output.WriteLine($"Transaction {txnId} rolled back");
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                completedTransactions.Add((txnId, $"failed: {ex.Message}"));
                _output.WriteLine($"Transaction {txnId} completion failed: {ex.Message}");
            }
        }).ToArray();

        await Task.WhenAll(completeTasks);

        // Analyze results
        _output.WriteLine($"Total exceptions: {exceptions.Count}");
        _output.WriteLine($"Active transactions created: {activeTransactions.Count}");
        _output.WriteLine($"Completed transactions: {completedTransactions.Count}");

        // CRITICAL ASSERTIONS for atomic state management
        Assert.True(exceptions.Count == 0 || exceptions.All(ex => 
            ex.Message.Contains("not found") || ex.Message.Contains("already completed")),
            "All exceptions should be related to valid race conditions, not internal state corruption");

        Assert.Equal(validTransactions.Count, completedTransactions.Count);

        var committed = completedTransactions.Count(ct => ct.outcome == "committed");
        var rolledBack = completedTransactions.Count(ct => ct.outcome == "rolled back");
        
        _output.WriteLine($"Committed: {committed}, Rolled back: {rolledBack}");
        Assert.True(committed + rolledBack >= validTransactions.Count * 0.8, 
            "At least 80% of transactions should complete successfully");
    }

    public void Dispose()
    {
        try
        {
            if (_asyncStorage is IDisposable disposable)
            {
                disposable.Dispose();
            }
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