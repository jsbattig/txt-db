using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;

namespace TxtDb.Database.Tests.ConcurrencyTests;

/// <summary>
/// RED PHASE: Tests that expose the table caching issues
/// These tests demonstrate that cached table instances cause stale index state,
/// leading to null returns when data should be visible.
/// 
/// CRITICAL PROBLEM: Database.cs lines 157-158 return cached table instances
/// that have stale primary key indexes, breaking data visibility.
/// </summary>
public class TableCachingIssueExposureTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public TableCachingIssueExposureTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_caching_exposure", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    /// <summary>
    /// RED TEST: Exposes that cached table instances have stale indexes
    /// This test SHOULD PASS but WILL FAIL due to table caching bug
    /// </summary>
    [Fact]
    public async Task TableInstances_ShouldNotBeCached_DataAlwaysVisible()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("cache_test_db");
        var table1 = await database.CreateTableAsync("products", "$.id");

        _output.WriteLine("Phase 1: Insert data using first table instance");
        
        // Act - Phase 1: Insert data using first table instance
        var txn1 = await databaseLayer.BeginTransactionAsync("cache_test_db");
        var insertedKey = await table1.InsertAsync(txn1, new { id = "PROD-001", name = "Test Product", price = 99.99 });
        await txn1.CommitAsync();
        
        _output.WriteLine($"Inserted product with key: {insertedKey}");

        // Act - Phase 2: Get fresh table instance (should NOT be cached)
        var table2 = await database.GetTableAsync("products"); // DIFFERENT INSTANCE
        
        // CRITICAL ASSERTION: Table instances should not be cached
        Assert.NotSame(table1, table2); // This should pass - different instances
        
        _output.WriteLine("Phase 2: Retrieve data using second table instance (should not be cached)");
        
        var txn2 = await databaseLayer.BeginTransactionAsync("cache_test_db");
        var retrieved = await table2.GetAsync(txn2, "PROD-001");
        await txn2.CommitAsync();
        
        // CRITICAL ASSERTION: Data should be visible through fresh table instance
        Assert.NotNull(retrieved); // THIS WILL FAIL due to stale cached index
        Assert.Equal("PROD-001", retrieved.id);
        Assert.Equal("Test Product", retrieved.name);
        Assert.Equal(99.99, (double)retrieved.price);
        
        _output.WriteLine($"Retrieved product: {retrieved?.id} - {retrieved?.name} - ${retrieved?.price}");
    }

    /// <summary>
    /// RED TEST: Exposes that multiple table instances share stale cached state
    /// This test SHOULD PASS but WILL FAIL due to cached table sharing bug
    /// </summary>
    [Fact]
    public async Task MultipleTableInstances_ShouldNotShareState_IndexesShouldBeIndependent()
    {
        // Arrange
        var databaseLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var databaseLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        
        var database1 = await databaseLayer1.CreateDatabaseAsync("multi_instance_test");
        var table1 = await database1.CreateTableAsync("inventory", "$.sku");

        _output.WriteLine("Phase 1: Insert data through first database layer instance");
        
        // Act - Phase 1: Insert data through first instance
        var txn1 = await databaseLayer1.BeginTransactionAsync("multi_instance_test");
        await table1.InsertAsync(txn1, new { sku = "SKU-001", quantity = 100, location = "Warehouse A" });
        await txn1.CommitAsync();
        
        _output.WriteLine("Inserted inventory item through first instance");

        // Act - Phase 2: Access through completely different database layer instance
        var database2 = await databaseLayer2.GetDatabaseAsync("multi_instance_test");
        Assert.NotNull(database2);
        
        var table2 = await database2.GetTableAsync("inventory");
        Assert.NotNull(table2);
        
        // CRITICAL ASSERTION: Different instances should not share cached state
        Assert.NotSame(table1, table2); // Should pass - different instances
        
        _output.WriteLine("Phase 2: Retrieve data through second database layer instance");
        
        var txn2 = await databaseLayer2.BeginTransactionAsync("multi_instance_test");
        var retrieved = await table2.GetAsync(txn2, "SKU-001");
        await txn2.CommitAsync();
        
        // CRITICAL ASSERTION: Data should be visible across independent instances
        Assert.NotNull(retrieved); // THIS WILL FAIL due to independent cached indexes
        Assert.Equal("SKU-001", retrieved.sku);
        Assert.Equal(100, (int)retrieved.quantity);
        Assert.Equal("Warehouse A", retrieved.location);
        
        _output.WriteLine($"Retrieved inventory: {retrieved?.sku} - Qty: {retrieved?.quantity} - Location: {retrieved?.location}");
    }

    /// <summary>
    /// RED TEST: Exposes that cached indexes become stale across transaction boundaries
    /// This test SHOULD PASS but WILL FAIL due to index not reflecting committed data
    /// </summary>
    [Fact]
    public async Task TransactionBoundaries_ShouldNotBreakIndexVisibility_CommittedDataShouldBeVisible()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("transaction_boundary_test");
        var table = await database.CreateTableAsync("orders", "$.orderId");

        _output.WriteLine("Phase 1: Insert data in one transaction");
        
        // Act - Phase 1: Insert data in one transaction
        var insertTxn = await databaseLayer.BeginTransactionAsync("transaction_boundary_test");
        await table.InsertAsync(insertTxn, new { orderId = "ORD-001", customerId = "CUST-123", total = 250.00 });
        await insertTxn.CommitAsync(); // Data is now committed to storage
        
        _output.WriteLine("Committed order data to storage");

        // Act - Phase 2: Use SAME table instance in different transaction
        // The cached table instance should reflect committed data, but it won't due to stale index
        _output.WriteLine("Phase 2: Retrieve data using same table instance in new transaction");
        
        var retrieveTxn = await databaseLayer.BeginTransactionAsync("transaction_boundary_test");
        var retrieved = await table.GetAsync(retrieveTxn, "ORD-001");
        await retrieveTxn.CommitAsync();
        
        // CRITICAL ASSERTION: Committed data should be visible even with same table instance
        Assert.NotNull(retrieved); // THIS WILL FAIL because cached index doesn't reflect committed state
        Assert.Equal("ORD-001", retrieved.orderId);
        Assert.Equal("CUST-123", retrieved.customerId);
        Assert.Equal(250.00, (double)retrieved.total);
        
        _output.WriteLine($"Retrieved order: {retrieved?.orderId} - Customer: {retrieved?.customerId} - Total: ${retrieved?.total}");
    }

    /// <summary>
    /// RED TEST: Exposes that index loading performance is impacted by caching approach
    /// This test measures the current broken approach to establish baseline for improvement
    /// </summary>
    [Fact]
    public async Task IndexLoadingPerformance_CurrentCachingApproach_ShouldEstablishBaseline()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("performance_baseline_test");
        var table = await database.CreateTableAsync("products", "$.productId");

        const int recordCount = 1000;
        _output.WriteLine($"Phase 1: Insert {recordCount} records to establish baseline");
        
        // Act - Phase 1: Insert many records
        for (int i = 0; i < recordCount; i++)
        {
            var txn = await databaseLayer.BeginTransactionAsync("performance_baseline_test");
            await table.InsertAsync(txn, new { 
                productId = $"PROD-{i:D6}", 
                name = $"Product {i}", 
                category = $"Category {i % 10}",
                price = 10.00 + (i * 0.01)
            });
            await txn.CommitAsync();
        }
        
        _output.WriteLine($"Inserted {recordCount} records");

        // Act - Phase 2: Measure retrieval performance with cached table
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        for (int i = 0; i < 100; i++) // Sample 100 retrievals
        {
            var txn = await databaseLayer.BeginTransactionAsync("performance_baseline_test");
            var retrieved = await table.GetAsync(txn, $"PROD-{i:D6}");
            await txn.CommitAsync();
            
            // THIS WILL LIKELY FAIL - retrieved will be null due to stale cached index
            if (retrieved == null)
            {
                _output.WriteLine($"FAILED to retrieve PROD-{i:D6} - Index cache is stale!");
                Assert.NotNull(retrieved); // Force failure to expose the issue
            }
        }
        
        sw.Stop();
        var avgLatency = sw.ElapsedMilliseconds / 100.0;
        
        _output.WriteLine($"Average retrieval latency with current caching: {avgLatency:F2}ms");
        _output.WriteLine("This baseline will be improved after eliminating table caching");
        
        // Set a generous baseline - the real issue is correctness, not performance
        Assert.True(avgLatency < 1000, $"Baseline performance {avgLatency:F2}ms exceeds 1000ms");
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