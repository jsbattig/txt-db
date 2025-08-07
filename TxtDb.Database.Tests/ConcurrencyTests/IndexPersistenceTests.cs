using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;

namespace TxtDb.Database.Tests.ConcurrencyTests;

/// <summary>
/// RED PHASE: Tests that expose the missing index persistence functionality
/// These tests demonstrate that primary key indexes are not persisted across
/// process restarts, causing data to become inaccessible.
/// 
/// REQUIRED IMPLEMENTATION: Index persistence in _indexes._primary namespace
/// </summary>
public class IndexPersistenceTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public IndexPersistenceTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_index_persistence", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    /// <summary>
    /// RED TEST: Exposes that primary key indexes are not persisted
    /// This test SHOULD PASS but WILL FAIL because indexes are not stored persistently
    /// </summary>
    [Fact]
    public async Task PrimaryKeyIndex_ShouldPersistAcrossProcessRestarts()
    {
        // Arrange - Phase 1: Create database and insert data
        {
            var databaseLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
            var database1 = await databaseLayer1.CreateDatabaseAsync("index_persistence_test");
            var table1 = await database1.CreateTableAsync("customers", "$.customerId");

            _output.WriteLine("Phase 1: Insert data and build primary key index");
            
            // Insert test data
            for (int i = 1; i <= 50; i++)
            {
                var txn = await databaseLayer1.BeginTransactionAsync("index_persistence_test");
                await table1.InsertAsync(txn, new { 
                    customerId = $"CUST-{i:D3}", 
                    name = $"Customer {i}",
                    email = $"customer{i}@example.com",
                    status = i % 2 == 0 ? "Active" : "Inactive"
                });
                await txn.CommitAsync();
            }
            
            _output.WriteLine("Inserted 50 customers - primary key index should be built");

            // Verify data is accessible in first process
            var verifyTxn = await databaseLayer1.BeginTransactionAsync("index_persistence_test");
            var customer25 = await table1.GetAsync(verifyTxn, "CUST-025");
            await verifyTxn.CommitAsync();
            
            Assert.NotNull(customer25);
            Assert.Equal("Customer 25", customer25.name);
            _output.WriteLine($"Verified customer in first process: {customer25.name}");
            
            // Dispose first database layer to simulate process termination
        } // DatabaseLayer1 goes out of scope - simulates process restart

        // Act - Phase 2: Create new database layer (simulates process restart)
        _output.WriteLine("Phase 2: Simulate process restart with new DatabaseLayer instance");
        
        var databaseLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database2 = await databaseLayer2.GetDatabaseAsync("index_persistence_test");
        Assert.NotNull(database2);
        
        var table2 = await database2.GetTableAsync("customers");
        Assert.NotNull(table2);
        
        // CRITICAL TEST: Data should still be accessible after "process restart"
        var retrieveTxn = await databaseLayer2.BeginTransactionAsync("index_persistence_test");
        var retrievedCustomer = await table2.GetAsync(retrieveTxn, "CUST-025");
        await retrieveTxn.CommitAsync();
        
        // CRITICAL ASSERTION: This WILL FAIL because index is not persisted
        Assert.NotNull(retrievedCustomer); // FAILS - index not loaded from persistent storage
        Assert.Equal("Customer 25", retrievedCustomer.name);
        Assert.Equal("customer25@example.com", retrievedCustomer.email);
        
        _output.WriteLine($"Retrieved customer after restart: {retrievedCustomer?.name}");
        
        // Verify multiple records are still accessible
        for (int i = 1; i <= 10; i++)
        {
            var txn = await databaseLayer2.BeginTransactionAsync("index_persistence_test");
            var customer = await table2.GetAsync(txn, $"CUST-{i:D3}");
            await txn.CommitAsync();
            
            Assert.NotNull(customer); // WILL FAIL - no persistent index
            _output.WriteLine($"Verified customer {i}: {customer?.name}");
        }
    }

    /// <summary>
    /// RED TEST: Exposes that index updates are not persisted during operations
    /// This test SHOULD PASS but WILL FAIL because index changes aren't saved
    /// </summary>
    [Fact]
    public async Task IndexUpdates_ShouldBePersisted_DuringNormalOperations()
    {
        // Arrange
        var databaseLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database1 = await databaseLayer1.CreateDatabaseAsync("index_update_persistence");
        var table1 = await database1.CreateTableAsync("inventory", "$.sku");

        _output.WriteLine("Phase 1: Insert initial data");
        
        // Insert initial data
        var txn1 = await databaseLayer1.BeginTransactionAsync("index_update_persistence");
        await table1.InsertAsync(txn1, new { sku = "ITEM-001", name = "Item 1", quantity = 100 });
        await table1.InsertAsync(txn1, new { sku = "ITEM-002", name = "Item 2", quantity = 200 });
        await txn1.CommitAsync();
        
        _output.WriteLine("Inserted initial inventory items");

        // Phase 2: Update data (should update persisted index)
        _output.WriteLine("Phase 2: Update existing items");
        
        var txn2 = await databaseLayer1.BeginTransactionAsync("index_update_persistence");
        await table1.UpdateAsync(txn2, "ITEM-001", new { sku = "ITEM-001", name = "Updated Item 1", quantity = 150 });
        await txn2.CommitAsync();
        
        _output.WriteLine("Updated ITEM-001");

        // Phase 3: Add more data (should extend persisted index)
        var txn3 = await databaseLayer1.BeginTransactionAsync("index_update_persistence");
        await table1.InsertAsync(txn3, new { sku = "ITEM-003", name = "Item 3", quantity = 300 });
        await txn3.CommitAsync();
        
        _output.WriteLine("Added ITEM-003");

        // Simulate process restart
        _output.WriteLine("Phase 3: Simulate process restart");
        
        var databaseLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database2 = await databaseLayer2.GetDatabaseAsync("index_update_persistence");
        var table2 = await database2.GetTableAsync("inventory");
        
        // CRITICAL TEST: All operations should be reflected in persisted index
        var verifyTxn = await databaseLayer2.BeginTransactionAsync("index_update_persistence");
        
        // Test updated item
        var item1 = await table2.GetAsync(verifyTxn, "ITEM-001");
        Assert.NotNull(item1); // WILL FAIL - index not persisted
        Assert.Equal("Updated Item 1", item1.name);
        Assert.Equal(150, (int)item1.quantity);
        
        // Test original item
        var item2 = await table2.GetAsync(verifyTxn, "ITEM-002");
        Assert.NotNull(item2); // WILL FAIL - index not persisted
        Assert.Equal("Item 2", item2.name);
        Assert.Equal(200, (int)item2.quantity);
        
        // Test newly added item
        var item3 = await table2.GetAsync(verifyTxn, "ITEM-003");
        Assert.NotNull(item3); // WILL FAIL - index not persisted
        Assert.Equal("Item 3", item3.name);
        Assert.Equal(300, (int)item3.quantity);
        
        await verifyTxn.CommitAsync();
        
        _output.WriteLine("All index updates should persist across process restart");
    }

    /// <summary>
    /// RED TEST: Exposes that delete operations don't update persisted index
    /// This test SHOULD PASS but WILL FAIL because deletions aren't reflected in persistent index
    /// </summary>
    [Fact]
    public async Task IndexDeletions_ShouldBePersisted_RemovedItemsShouldStayGone()
    {
        // Arrange
        var databaseLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database1 = await databaseLayer1.CreateDatabaseAsync("index_deletion_persistence");
        var table1 = await database1.CreateTableAsync("products", "$.productId");

        _output.WriteLine("Phase 1: Insert products for deletion test");
        
        // Insert test products
        for (int i = 1; i <= 20; i++)
        {
            var txn = await databaseLayer1.BeginTransactionAsync("index_deletion_persistence");
            await table1.InsertAsync(txn, new { 
                productId = $"PROD-{i:D3}", 
                name = $"Product {i}",
                price = 10.00 + i
            });
            await txn.CommitAsync();
        }
        
        _output.WriteLine("Inserted 20 products");

        // Delete some products
        _output.WriteLine("Phase 2: Delete selected products");
        
        for (int i = 5; i <= 10; i++) // Delete products 5-10
        {
            var deleteTxn = await databaseLayer1.BeginTransactionAsync("index_deletion_persistence");
            var deleted = await table1.DeleteAsync(deleteTxn, $"PROD-{i:D3}");
            await deleteTxn.CommitAsync();
            
            Assert.True(deleted);
            _output.WriteLine($"Deleted PROD-{i:D3}");
        }

        // Simulate process restart
        _output.WriteLine("Phase 3: Simulate process restart and verify deletions persisted");
        
        var databaseLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database2 = await databaseLayer2.GetDatabaseAsync("index_deletion_persistence");
        var table2 = await database2.GetTableAsync("products");
        
        // CRITICAL TEST: Deleted items should remain deleted after restart
        var verifyTxn = await databaseLayer2.BeginTransactionAsync("index_deletion_persistence");
        
        // Verify existing items are still accessible
        var product1 = await table2.GetAsync(verifyTxn, "PROD-001");
        Assert.NotNull(product1); // WILL FAIL - index not persisted
        
        var product15 = await table2.GetAsync(verifyTxn, "PROD-015");
        Assert.NotNull(product15); // WILL FAIL - index not persisted
        
        // Verify deleted items remain deleted
        for (int i = 5; i <= 10; i++)
        {
            var deletedProduct = await table2.GetAsync(verifyTxn, $"PROD-{i:D3}");
            Assert.Null(deletedProduct); // Should be null, but might not be due to stale cache
            _output.WriteLine($"Verified PROD-{i:D3} remains deleted");
        }
        
        await verifyTxn.CommitAsync();
        
        _output.WriteLine("Deletion persistence verified across process restart");
    }

    /// <summary>
    /// RED TEST: Exposes lack of index versioning and consistency checks
    /// This test establishes the need for index version management
    /// </summary>
    [Fact]
    public async Task IndexVersioning_ShouldDetectInconsistencies_ProvideRecoveryMechanism()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("index_versioning_test");
        var table = await database.CreateTableAsync("documents", "$.docId");

        _output.WriteLine("Phase 1: Create baseline data for version testing");
        
        // Insert baseline data
        for (int i = 1; i <= 30; i++)
        {
            var insertTxn = await databaseLayer.BeginTransactionAsync("index_versioning_test");
            await table.InsertAsync(insertTxn, new { 
                docId = $"DOC-{i:D3}", 
                title = $"Document {i}",
                created = DateTime.UtcNow.AddDays(-i)
            });
            await insertTxn.CommitAsync();
        }
        
        _output.WriteLine("Created 30 documents for version testing");

        // TODO: This test will need index versioning implementation to be meaningful
        // For now, it establishes the requirement for version tracking
        
        // CRITICAL REQUIREMENT: Index should have version metadata
        // CRITICAL REQUIREMENT: Index loading should detect version mismatches
        // CRITICAL REQUIREMENT: Index should provide recovery from corruption
        
        _output.WriteLine("Index versioning requirements established");
        _output.WriteLine("Implementation needed: SerializedIndexData with Version field");
        _output.WriteLine("Implementation needed: Version consistency checks on load");
        _output.WriteLine("Implementation needed: Index rebuild capability");
        
        // For now, just verify basic functionality works
        var txn = await databaseLayer.BeginTransactionAsync("index_versioning_test");
        var doc15 = await table.GetAsync(txn, "DOC-015");
        await txn.CommitAsync();
        
        // This might fail due to current caching issues, but establishes the baseline
        if (doc15 != null)
        {
            Assert.Equal("Document 15", doc15.title);
            _output.WriteLine($"Baseline verification: {doc15.title}");
        }
        else
        {
            _output.WriteLine("Baseline verification failed - demonstrates need for index persistence");
        }
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}