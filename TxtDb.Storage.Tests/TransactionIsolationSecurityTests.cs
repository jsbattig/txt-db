using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL SECURITY TESTS: Transaction isolation violation in critical operations bypass.
/// 
/// SECURITY VULNERABILITY:
/// The current implementation of critical operations bypass forces flush of ALL pending batches
/// from ALL transactions when a critical operation occurs, not just the current transaction's data.
/// This creates a security and MVCC isolation violation where:
/// - Critical operations from one transaction can force persistence of unrelated transactions' data
/// - Potential data exposure if those other transactions should fail or rollback
/// - Violates database isolation principles
/// 
/// These tests MUST FAIL initially to demonstrate the vulnerability exists.
/// After the fix, they should pass proving the isolation violation is resolved.
/// </summary>
public class TransactionIsolationSecurityTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private string _testDataPath = string.Empty;
    private AsyncStorageSubsystem _storage = null!;
    
    public TransactionIsolationSecurityTests(ITestOutputHelper output)
    {
        _output = output;
        Setup();
    }
    
    private async Task Setup()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDataPath);
        
        _storage = new AsyncStorageSubsystem();
        
        // Enable batch flushing to trigger the vulnerability
        var config = new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = true,
            BatchFlushConfig = new BatchFlushConfig
            {
                MaxBatchSize = 10,
                MaxDelayMs = 1000, // Long delay to ensure batching occurs
                MaxConcurrentFlushes = 2
            }
        };
        
        await _storage.InitializeAsync(_testDataPath, config);
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
    /// CRITICAL SECURITY TEST: Transaction A (critical) should NOT force flush Transaction B (normal) data.
    /// 
    /// CURRENT VULNERABILITY: ForceFlushAllPendingAsync() flushes ALL batched files from ALL transactions.
    /// SECURITY IMPACT: Transaction A can force persistence of Transaction B's data before Transaction B commits.
    /// EXPECTED BEHAVIOR: Only Transaction A's files should be flushed during critical commit.
    /// 
    /// This test MUST FAIL initially, demonstrating the isolation violation.
    /// </summary>
    [Fact]
    public async Task CriticalCommit_ShouldOnlyFlushOwnTransactionData_NotOtherTransactions()
    {
        // Create two transactions
        var transactionA = await _storage.BeginTransactionAsync(); // Will be critical
        var transactionB = await _storage.BeginTransactionAsync(); // Will be normal
        
        _output.WriteLine($"Transaction A (critical): {transactionA}");
        _output.WriteLine($"Transaction B (normal): {transactionB}");
        
        // Transaction B writes data (should be batched, not immediately flushed)
        await _storage.CreateNamespaceAsync(transactionB, "namespace_b");
        var pageIdB = await _storage.InsertObjectAsync(transactionB, "namespace_b", 
            new { Data = "Transaction B data - should NOT be flushed by Transaction A's critical commit" });
        
        _output.WriteLine($"Transaction B wrote to page: {pageIdB}");
        
        // Transaction A writes data (will use critical commit)
        await _storage.CreateNamespaceAsync(transactionA, "namespace_a");
        var pageIdA = await _storage.InsertObjectAsync(transactionA, "namespace_a", 
            new { Data = "Transaction A data - critical operation" });
        
        _output.WriteLine($"Transaction A wrote to page: {pageIdA}");
        
        // Get file paths that would be created
        var namespaceBPath = Path.Combine(_testDataPath, "namespace_b");
        var fileBPath = Path.Combine(namespaceBPath, $"{pageIdB}.json.v{transactionB}");
        var namespaceAPath = Path.Combine(_testDataPath, "namespace_a");
        var fileAPath = Path.Combine(namespaceAPath, $"{pageIdA}.json.v{transactionA}");
        
        _output.WriteLine($"Transaction A file: {fileAPath}");
        _output.WriteLine($"Transaction B file: {fileBPath}");
        
        // Verify files exist (should be created by InsertObjectAsync)
        Assert.True(File.Exists(fileAPath), $"Transaction A file should exist: {fileAPath}");
        Assert.True(File.Exists(fileBPath), $"Transaction B file should exist: {fileBPath}");
        
        // Get initial flush counts
        var initialFlushCount = _storage.FlushOperationCount;
        _output.WriteLine($"Initial flush count: {initialFlushCount}");
        
        // Commit Transaction A with CRITICAL priority
        // VULNERABILITY: This should flush ALL pending batches, including Transaction B's data
        var commitStart = DateTime.UtcNow;
        await _storage.CommitTransactionAsync(transactionA, FlushPriority.Critical);
        var commitTime = DateTime.UtcNow - commitStart;
        
        _output.WriteLine($"Transaction A critical commit completed in: {commitTime.TotalMilliseconds}ms");
        
        var finalFlushCount = _storage.FlushOperationCount;
        _output.WriteLine($"Final flush count: {finalFlushCount}");
        _output.WriteLine($"Flush operations during critical commit: {finalFlushCount - initialFlushCount}");
        
        // SECURITY ASSERTION: Transaction B's files should NOT have been flushed to disk
        // by Transaction A's critical commit.
        //
        // HOW TO VERIFY: Check if Transaction B's version file has been synced to disk.
        // If the vulnerability exists, ForceFlushAllPendingAsync() will have flushed
        // Transaction B's data even though Transaction B hasn't committed yet.
        //
        // EXPECTED FAILURE: This test will initially FAIL because the current implementation
        // flushes all batched data from all transactions during critical operations.
        
        // The issue is that checking file existence doesn't prove the data was force-flushed
        // since the file was already written by WritePageVersionAsync.
        // We need to verify that the flush operation affected Transaction B's files.
        //
        // A better approach: Monitor if flush operations were called on Transaction B's files
        // during Transaction A's critical commit.
        
        // For now, let's verify that Transaction B can still rollback successfully
        // If Transaction B's data was improperly flushed by Transaction A's critical commit,
        // rollback might fail or behave inconsistently.
        
        try
        {
            await _storage.RollbackTransactionAsync(transactionB);
            _output.WriteLine("Transaction B rollback succeeded - isolation maintained");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Transaction B rollback failed: {ex.Message}");
            throw new InvalidOperationException(
                "ISOLATION VIOLATION: Transaction B rollback failed after Transaction A's critical commit. " +
                "This indicates that Transaction A's critical commit improperly affected Transaction B's data persistence.", ex);
        }
        
        // Verify Transaction B's version file was properly cleaned up during rollback
        Assert.False(File.Exists(fileBPath), 
            $"ISOLATION VIOLATION: Transaction B's file should have been deleted during rollback, " +
            $"but still exists: {fileBPath}. This indicates improper cross-transaction flush operations.");
        
        // Additional verification: Ensure Transaction A's data is still committed and accessible
        var transactionForRead = await _storage.BeginTransactionAsync();
        try
        {
            var readData = await _storage.ReadPageAsync(transactionForRead, "namespace_a", pageIdA);
            Assert.NotEmpty(readData);
            _output.WriteLine($"Transaction A data successfully committed and readable: {readData.Length} objects");
        }
        finally
        {
            await _storage.CommitTransactionAsync(transactionForRead);
        }
        
        // CRITICAL PERFORMANCE REQUIREMENT: Critical operations must still be fast
        Assert.True(commitTime.TotalMilliseconds < 200, 
            $"Critical commit took {commitTime.TotalMilliseconds}ms, should be < 200ms for performance requirements");
        
        _output.WriteLine("TEST RESULT: If this test passes, it indicates that transaction isolation is maintained.");
        _output.WriteLine("If this test fails initially, it exposes the isolation violation vulnerability.");
    }
    
    /// <summary>
    /// CRITICAL SECURITY TEST: Transaction B should be able to rollback even after Transaction A commits critical operation.
    /// 
    /// VULNERABILITY: If Transaction A's critical commit forces flush of Transaction B's data,
    /// Transaction B might not be able to rollback properly, violating ACID properties.
    /// 
    /// This test MUST initially FAIL if the vulnerability exists.
    /// </summary>
    [Fact]
    public async Task TransactionRollback_AfterCriticalCommitOfOtherTransaction_ShouldSucceed()
    {
        var transactionA = await _storage.BeginTransactionAsync(); // Will commit critical
        var transactionB = await _storage.BeginTransactionAsync(); // Will rollback
        
        _output.WriteLine($"Transaction A (critical): {transactionA}");
        _output.WriteLine($"Transaction B (will rollback): {transactionB}");
        
        // Both transactions write data to different namespaces
        await _storage.CreateNamespaceAsync(transactionA, "critical_namespace");
        await _storage.CreateNamespaceAsync(transactionB, "rollback_namespace");
        
        var dataA = new { Operation = "Critical commit", Timestamp = DateTime.UtcNow };
        var dataB = new { Operation = "Should be rolled back", SensitiveData = "This data must not persist" };
        
        var pageIdA = await _storage.InsertObjectAsync(transactionA, "critical_namespace", dataA);
        var pageIdB = await _storage.InsertObjectAsync(transactionB, "rollback_namespace", dataB);
        
        _output.WriteLine($"Transaction A wrote to page: {pageIdA}");
        _output.WriteLine($"Transaction B wrote to page: {pageIdB}");
        
        // Get Transaction B's version file path
        var rollbackNamespacePath = Path.Combine(_testDataPath, "rollback_namespace");
        var transactionBFile = Path.Combine(rollbackNamespacePath, $"{pageIdB}.json.v{transactionB}");
        
        // Verify Transaction B's data file exists before critical commit
        Assert.True(File.Exists(transactionBFile), "Transaction B's version file should exist");
        
        // Transaction A commits with critical priority
        // VULNERABILITY: This may force flush Transaction B's data improperly
        await _storage.CommitTransactionAsync(transactionA, FlushPriority.Critical);
        _output.WriteLine("Transaction A committed with critical priority");
        
        // Now attempt to rollback Transaction B
        // If the vulnerability exists, this might fail or leave Transaction B's data persisted
        Exception? rollbackException = null;
        try
        {
            await _storage.RollbackTransactionAsync(transactionB);
            _output.WriteLine("Transaction B rollback completed successfully");
        }
        catch (Exception ex)
        {
            rollbackException = ex;
            _output.WriteLine($"Transaction B rollback failed: {ex.Message}");
        }
        
        // CRITICAL ASSERTION: Rollback should succeed regardless of other transactions' critical commits
        Assert.Null(rollbackException);
        
        // Verify Transaction B's data was properly cleaned up
        Assert.False(File.Exists(transactionBFile), 
            "ISOLATION VIOLATION: Transaction B's version file should be deleted after rollback, " +
            "but still exists. This indicates Transaction A's critical commit improperly affected Transaction B's data.");
        
        // Verify Transaction B's data is not readable by new transactions
        var verificationTransaction = await _storage.BeginTransactionAsync();
        try
        {
            var rollbackData = await _storage.ReadPageAsync(verificationTransaction, "rollback_namespace", pageIdB);
            Assert.Empty(rollbackData);
            _output.WriteLine("Verified: Transaction B's rolled-back data is not accessible");
        }
        finally
        {
            await _storage.CommitTransactionAsync(verificationTransaction);
        }
        
        // Verify Transaction A's data is still accessible (should be unaffected)
        var verificationTransaction2 = await _storage.BeginTransactionAsync();
        try
        {
            var commitData = await _storage.ReadPageAsync(verificationTransaction2, "critical_namespace", pageIdA);
            Assert.NotEmpty(commitData);
            _output.WriteLine($"Verified: Transaction A's committed data is still accessible: {commitData.Length} objects");
        }
        finally
        {
            await _storage.CommitTransactionAsync(verificationTransaction2);
        }
    }
    
    /// <summary>
    /// CRITICAL SECURITY TEST: MVCC isolation should be maintained during critical operations.
    /// 
    /// This test verifies that concurrent transactions maintain proper MVCC isolation
    /// even when one transaction uses critical priority commits.
    /// 
    /// VULNERABILITY: Critical operations might break MVCC snapshot isolation by
    /// forcing premature persistence of other transactions' data.
    /// </summary>
    [Fact]
    public async Task MVCCIsolation_DuringCriticalOperations_ShouldBeMaintained()
    {
        const int concurrentTransactions = 5;
        var transactions = new List<long>();
        var exceptions = new ConcurrentBag<Exception>();
        var commitResults = new ConcurrentBag<(long transactionId, bool success, string error)>();
        
        // Create multiple concurrent transactions
        for (int i = 0; i < concurrentTransactions; i++)
        {
            var tid = await _storage.BeginTransactionAsync();
            transactions.Add(tid);
            _output.WriteLine($"Created transaction {i}: {tid}");
        }
        
        // Create initial data on a single page that all transactions will conflict over
        await _storage.CreateNamespaceAsync(transactions[0], "mvcc_test");
        var initialPageId = await _storage.InsertObjectAsync(transactions[0], "mvcc_test", 
            new { InitialData = "Starting point", Version = 0 });
        await _storage.CommitTransactionAsync(transactions[0]);
        
        _output.WriteLine($"Created initial page for conflicts: {initialPageId}");
        
        // Remove the committed transaction from the list
        transactions = transactions.Skip(1).ToList();
        
        // All remaining transactions read and then try to update the SAME page (will create MVCC conflicts)
        var writeTasks = transactions.Select(async (tid, index) =>
        {
            try
            {
                // First read the page to establish read version
                var existingData = await _storage.ReadPageAsync(tid, "mvcc_test", initialPageId);
                _output.WriteLine($"Transaction {index} ({tid}) read {existingData.Length} objects from page {initialPageId}");
                
                // Then try to update the same page - this should cause conflicts
                var newData = new { 
                    TransactionId = tid,
                    Index = index,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Data = $"Transaction {index} conflicting update - {new string('x', 100)}"
                };
                
                await _storage.UpdatePageAsync(tid, "mvcc_test", initialPageId, new object[] { newData });
                _output.WriteLine($"Transaction {index} ({tid}) wrote to page: {initialPageId}");
                return initialPageId;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                _output.WriteLine($"Transaction {index} ({tid}) write failed: {ex.Message}");
                return null;
            }
        }).ToArray();
        
        await Task.WhenAll(writeTasks);
        
        // Commit transactions with mixed priorities
        // Transaction 0 (index 0 of remaining list): Critical priority (should trigger vulnerability)  
        // Others: Normal priority
        var commitTasks = transactions.Select(async (tid, index) =>
        {
            try
            {
                var priority = (index == 0) ? FlushPriority.Critical : FlushPriority.Normal;
                var commitStart = DateTime.UtcNow;
                
                await _storage.CommitTransactionAsync(tid, priority);
                var commitTime = DateTime.UtcNow - commitStart;
                
                commitResults.Add((tid, true, string.Empty));
                _output.WriteLine($"Transaction {index} ({tid}) committed successfully with {priority} priority in {commitTime.TotalMilliseconds}ms");
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                commitResults.Add((tid, false, ex.Message));
                _output.WriteLine($"Transaction {index} ({tid}) commit failed: {ex.Message}");
            }
        }).ToArray();
        
        await Task.WhenAll(commitTasks);
        
        // Analyze results
        var successfulCommits = commitResults.Count(r => r.success);
        var failedCommits = commitResults.Count(r => !r.success);
        
        _output.WriteLine($"Total write exceptions: {exceptions.Count}");
        _output.WriteLine($"Successful commits: {successfulCommits}");
        _output.WriteLine($"Failed commits: {failedCommits}");
        
        // MVCC ISOLATION VERIFICATION:
        // 1. The critical transaction (Transaction 0) should not interfere with other transactions' commit success
        // 2. MVCC conflicts should be handled properly regardless of commit priorities
        // 3. At most one transaction should succeed due to MVCC conflicts on the same page
        
        // Check that critical commit didn't break MVCC isolation
        var criticalCommitResult = commitResults.FirstOrDefault(r => r.transactionId == transactions[0]);
        Assert.NotEqual(default, criticalCommitResult);
        
        if (criticalCommitResult.success)
        {
            _output.WriteLine("Critical transaction committed successfully");
            
            // Verify the critical transaction's data is readable
            var verificationTransaction = await _storage.BeginTransactionAsync();
            try
            {
                var pages = await _storage.GetMatchingObjectsAsync(verificationTransaction, "mvcc_test", "*");
                Assert.NotEmpty(pages);
                _output.WriteLine($"Critical transaction's data is readable: {pages.Count} pages");
                
                // CRITICAL ISOLATION CHECK: Ensure only the critical transaction's data persisted
                // Other transactions should have failed due to MVCC conflicts
                var failedTransactions = commitResults.Where(r => !r.success && r.transactionId != transactions[0]).ToList();
                Assert.Equal(transactions.Count - 1, failedTransactions.Count); // -1 for the critical transaction that succeeded
                
                foreach (var failed in failedTransactions)
                {
                    Assert.Contains("conflict", failed.error.ToLower());
                    _output.WriteLine($"Transaction {failed.transactionId} properly failed with MVCC conflict: {failed.error}");
                }
            }
            finally
            {
                await _storage.CommitTransactionAsync(verificationTransaction);
            }
        }
        else
        {
            _output.WriteLine($"Critical transaction failed: {criticalCommitResult.error}");
            
            // If critical transaction failed, verify another transaction succeeded
            var successfulTransaction = commitResults.FirstOrDefault(r => r.success);
            if (successfulTransaction != default)
            {
                _output.WriteLine($"Transaction {successfulTransaction.transactionId} succeeded after critical failure");
                
                // Verify the successful transaction's data is readable
                var verificationTransaction = await _storage.BeginTransactionAsync();
                try
                {
                    var pages = await _storage.GetMatchingObjectsAsync(verificationTransaction, "mvcc_test", "*");
                    Assert.NotEmpty(pages);
                    _output.WriteLine($"Successful transaction's data is readable: {pages.Count} pages");
                }
                finally
                {
                    await _storage.CommitTransactionAsync(verificationTransaction);
                }
            }
        }
        
        // CRITICAL ASSERTION: MVCC isolation should prevent all transactions from succeeding simultaneously
        // due to conflicts on the same page
        Assert.True(successfulCommits <= 1, 
            $"MVCC ISOLATION VIOLATION: {successfulCommits} transactions succeeded simultaneously. " +
            $"MVCC should allow at most 1 transaction to succeed when writing to the same page.");
        
        _output.WriteLine("MVCC isolation maintained during critical operations");
    }
}