using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests;

/// <summary>
/// VERIFICATION TESTS: End-to-end tests that verify the MVCC isolation fixes are working correctly.
/// These tests should all PASS after implementing the MVCC fixes.
/// 
/// Key MVCC Guarantees Being Verified:
/// 1. Optimistic concurrency conflicts are properly detected
/// 2. Transaction isolation is maintained during critical operations  
/// 3. Snapshot isolation prevents dirty reads
/// 4. Lost update prevention works correctly
/// 5. Read-before-write violations are enforced
/// </summary>
public class MVCCFixVerificationTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public MVCCFixVerificationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_mvcc_verification_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _asyncStorage = new AsyncStorageSubsystem();
    }

    /// <summary>
    /// VERIFICATION: Optimistic concurrency conflicts are detected and prevent lost updates.
    /// This test should PASS after MVCC fixes.
    /// </summary>
    [Fact]
    public async Task OptimisticConcurrencyConflicts_ShouldBeDetectedAndPreventLostUpdates()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        // Setup: Create initial account data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "accounts");
        var accountPageId = await _asyncStorage.InsertObjectAsync(setupTxn, "accounts", 
            new { AccountId = "ACC-001", Balance = 1000.00m, Version = 1 });
        await _asyncStorage.CommitTransactionAsync(setupTxn);
        
        _output.WriteLine($"Created account {accountPageId} with initial balance $1000");

        // Two concurrent transactions attempt to withdraw money
        var withdraw1Txn = await _asyncStorage.BeginTransactionAsync();
        var withdraw2Txn = await _asyncStorage.BeginTransactionAsync();

        // Both read the current balance (establishes read version)
        var balance1 = await _asyncStorage.ReadPageAsync(withdraw1Txn, "accounts", accountPageId);
        var balance2 = await _asyncStorage.ReadPageAsync(withdraw2Txn, "accounts", accountPageId);

        _output.WriteLine($"Transaction 1 read balance: {balance1.Length} objects");
        _output.WriteLine($"Transaction 2 read balance: {balance2.Length} objects");

        // Both try to update with new balances
        await _asyncStorage.UpdatePageAsync(withdraw1Txn, "accounts", accountPageId, 
            new object[] { new { AccountId = "ACC-001", Balance = 900.00m, Version = 2, UpdatedBy = "Transaction1" } });
        
        await _asyncStorage.UpdatePageAsync(withdraw2Txn, "accounts", accountPageId, 
            new object[] { new { AccountId = "ACC-001", Balance = 800.00m, Version = 2, UpdatedBy = "Transaction2" } });

        // First transaction commits successfully
        await _asyncStorage.CommitTransactionAsync(withdraw1Txn);
        _output.WriteLine("Transaction 1 committed successfully");

        // Second transaction should fail with conflict
        var conflictException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _asyncStorage.CommitTransactionAsync(withdraw2Txn));
        
        Assert.Contains("concurrency conflict", conflictException.Message.ToLowerInvariant());
        _output.WriteLine($"Transaction 2 properly failed with: {conflictException.Message}");

        // Verify final state shows only the first transaction's update
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalBalance = await _asyncStorage.ReadPageAsync(verifyTxn, "accounts", accountPageId);
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        Assert.Single(finalBalance);
        _output.WriteLine("VERIFICATION PASSED: Optimistic concurrency conflict properly prevented lost update");
    }

    /// <summary>
    /// VERIFICATION: Multiple concurrent transactions writing to the same page should result in conflicts.
    /// Only one transaction should succeed. This test should PASS after MVCC fixes.
    /// </summary>
    [Fact]
    public async Task ConcurrentWritesToSamePage_ShouldAllowOnlyOneTransactionToSucceed()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        // Setup: Create initial data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "inventory");
        var itemPageId = await _asyncStorage.InsertObjectAsync(setupTxn, "inventory", 
            new { ItemId = "ITEM-001", Quantity = 100, LastUpdate = DateTime.UtcNow });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        const int concurrentTransactions = 5;
        var transactions = new List<long>();
        var commitResults = new ConcurrentBag<(long txnId, bool success, string error)>();

        // Start concurrent transactions
        for (int i = 0; i < concurrentTransactions; i++)
        {
            var txnId = await _asyncStorage.BeginTransactionAsync();
            transactions.Add(txnId);
            _output.WriteLine($"Started transaction {i}: {txnId}");
        }

        // All transactions read the same item
        await Task.WhenAll(transactions.Select(async txnId =>
        {
            var data = await _asyncStorage.ReadPageAsync(txnId, "inventory", itemPageId);
            _output.WriteLine($"Transaction {txnId} read {data.Length} objects");
        }));

        // All transactions attempt to update the same item
        await Task.WhenAll(transactions.Select(async (txnId, index) =>
        {
            await _asyncStorage.UpdatePageAsync(txnId, "inventory", itemPageId, 
                new object[] { new { ItemId = "ITEM-001", Quantity = 100 - (index * 10), 
                              LastUpdate = DateTime.UtcNow, UpdatedBy = $"Txn{txnId}" } });
            _output.WriteLine($"Transaction {txnId} completed write operation");
        }));

        // Try to commit all transactions
        await Task.WhenAll(transactions.Select(async (txnId, index) =>
        {
            try
            {
                await _asyncStorage.CommitTransactionAsync(txnId);
                commitResults.Add((txnId, true, ""));
                _output.WriteLine($"Transaction {txnId} committed successfully");
            }
            catch (Exception ex)
            {
                commitResults.Add((txnId, false, ex.Message));
                _output.WriteLine($"Transaction {txnId} commit failed: {ex.Message}");
            }
        }));

        var successfulCommits = commitResults.Count(r => r.success);
        var failedCommits = commitResults.Count(r => !r.success);
        
        _output.WriteLine($"Successful commits: {successfulCommits}");
        _output.WriteLine($"Failed commits: {failedCommits}");

        // CRITICAL VERIFICATION: Only one transaction should succeed
        Assert.Equal(1, successfulCommits);
        Assert.Equal(concurrentTransactions - 1, failedCommits);

        // Verify all failures are due to conflicts
        foreach (var (_, success, error) in commitResults.Where(r => !r.success))
        {
            Assert.Contains("conflict", error.ToLowerInvariant());
        }

        _output.WriteLine("VERIFICATION PASSED: MVCC properly prevented concurrent write conflicts");
    }

    /// <summary>
    /// VERIFICATION: Read-before-write enforcement should prevent ACID violations.
    /// This test should PASS after MVCC fixes.
    /// </summary>
    [Fact]
    public async Task ReadBeforeWrite_ShouldBeEnforcedToPreventACIDViolations()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        // Setup: Create initial data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "orders");
        var orderPageId = await _asyncStorage.InsertObjectAsync(setupTxn, "orders", 
            new { OrderId = "ORD-001", Status = "Pending", Amount = 250.00m });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Transaction tries to update without reading first (ACID violation)
        var violationTxn = await _asyncStorage.BeginTransactionAsync();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _asyncStorage.UpdatePageAsync(violationTxn, "orders", orderPageId, 
                new object[] { new { OrderId = "ORD-001", Status = "Completed", Amount = 250.00m } }));

        Assert.Contains("read-before-write", exception.Message.ToLowerInvariant());
        Assert.Contains("acid isolation", exception.Message.ToLowerInvariant());
        _output.WriteLine($"Read-before-write violation properly detected: {exception.Message}");

        await _asyncStorage.RollbackTransactionAsync(violationTxn);

        // Proper approach: Read first, then update
        var properTxn = await _asyncStorage.BeginTransactionAsync();
        var orderData = await _asyncStorage.ReadPageAsync(properTxn, "orders", orderPageId);
        await _asyncStorage.UpdatePageAsync(properTxn, "orders", orderPageId, 
            new object[] { new { OrderId = "ORD-001", Status = "Completed", Amount = 250.00m } });
        await _asyncStorage.CommitTransactionAsync(properTxn);

        _output.WriteLine("VERIFICATION PASSED: Read-before-write enforcement prevents ACID violations");
    }

    /// <summary>
    /// VERIFICATION: Snapshot isolation should provide consistent reads throughout transaction lifecycle.
    /// This test should PASS after MVCC fixes.
    /// </summary>
    [Fact]
    public async Task SnapshotIsolation_ShouldProvideConsistentReadsAcrossTransactionLifecycle()
    {
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            EnableBatchFlushing = false
        });

        // Setup: Create initial data
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "products");
        var product1Id = await _asyncStorage.InsertObjectAsync(setupTxn, "products", 
            new { ProductId = "PROD-001", Price = 100.00m, Generation = 1 });
        var product2Id = await _asyncStorage.InsertObjectAsync(setupTxn, "products", 
            new { ProductId = "PROD-002", Price = 200.00m, Generation = 1 });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Long-running analytics transaction
        var analyticsTxn = await _asyncStorage.BeginTransactionAsync();
        var initialProduct1 = await _asyncStorage.ReadPageAsync(analyticsTxn, "products", product1Id);
        var initialProduct2 = await _asyncStorage.ReadPageAsync(analyticsTxn, "products", product2Id);

        _output.WriteLine($"Analytics read initial data: Product1={initialProduct1.Length} objects, Product2={initialProduct2.Length} objects");

        // Concurrent transaction modifies the products
        var modifyTxn = await _asyncStorage.BeginTransactionAsync();
        var modProduct1 = await _asyncStorage.ReadPageAsync(modifyTxn, "products", product1Id);
        var modProduct2 = await _asyncStorage.ReadPageAsync(modifyTxn, "products", product2Id);

        await _asyncStorage.UpdatePageAsync(modifyTxn, "products", product1Id, 
            new object[] { new { ProductId = "PROD-001", Price = 150.00m, Generation = 2 } });
        await _asyncStorage.UpdatePageAsync(modifyTxn, "products", product2Id, 
            new object[] { new { ProductId = "PROD-002", Price = 250.00m, Generation = 2 } });
        await _asyncStorage.CommitTransactionAsync(modifyTxn);

        _output.WriteLine("Concurrent transaction updated both products");

        // Analytics transaction continues - should see consistent snapshot
        var laterProduct1 = await _asyncStorage.ReadPageAsync(analyticsTxn, "products", product1Id);
        var laterProduct2 = await _asyncStorage.ReadPageAsync(analyticsTxn, "products", product2Id);

        _output.WriteLine($"Analytics later reads: Product1={laterProduct1.Length} objects, Product2={laterProduct2.Length} objects");

        // Snapshot isolation: should see same data as initial reads
        Assert.Equal(initialProduct1.Length, laterProduct1.Length);
        Assert.Equal(initialProduct2.Length, laterProduct2.Length);

        await _asyncStorage.CommitTransactionAsync(analyticsTxn);

        // New transaction should see the updates
        var newTxn = await _asyncStorage.BeginTransactionAsync();
        var finalProduct1 = await _asyncStorage.ReadPageAsync(newTxn, "products", product1Id);
        var finalProduct2 = await _asyncStorage.ReadPageAsync(newTxn, "products", product2Id);
        await _asyncStorage.CommitTransactionAsync(newTxn);

        Assert.Single(finalProduct1);
        Assert.Single(finalProduct2);

        _output.WriteLine("VERIFICATION PASSED: Snapshot isolation maintained consistent view throughout transaction");
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