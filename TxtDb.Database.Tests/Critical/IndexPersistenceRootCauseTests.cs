using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// PHASE 2 TDD: Root cause analysis tests for index persistence failure
/// 
/// PROBLEM STATEMENT: 
/// - Database layer creates indexes in memory during operations
/// - When a new Database instance is created, indexes are empty 
/// - InitializeIndexesFromStorageAsync() method may not be working correctly
/// - Index persistence to storage may have issues
/// 
/// EXPECTED BEHAVIOR:
/// - Indexes should be automatically persisted to storage during operations
/// - Fresh Database/Table instances should rebuild indexes from storage
/// - Index operations should be transparent across instance lifecycles
/// </summary>
public class IndexPersistenceRootCauseTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public IndexPersistenceRootCauseTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine("/tmp", "txtdb_phase2_debug", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Phase 2 Test Directory: {_testDirectory}");
        _output.WriteLine($"Storage Directory: {_storageDirectory}");
    }

    /// <summary>
    /// FAILING TEST: Demonstrates that index is built in memory but not persisted
    /// This test should fail in Phase 2, then pass after implementing index persistence
    /// </summary>
    [Fact]
    public async Task SingleRecord_IndexNotPersisted_FreshInstanceCannotFind()
    {
        const string testDbName = "phase2_single_record_test";
        const string testTableName = "users";
        const string testPrimaryKey = "USER-001";
        
        // PHASE 1: Create database, insert single record
        _output.WriteLine("=== PHASE 1: Insert single record and verify in-memory index works ===");
        
        IDatabaseTransaction? txn1 = null;
        try
        {
            var dbLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
            var database1 = await dbLayer1.CreateDatabaseAsync(testDbName);
            var table1 = await database1.CreateTableAsync(testTableName, "$.userId");
            
            _output.WriteLine("Created database and table");
            
            // Insert single test record
            txn1 = await dbLayer1.BeginTransactionAsync(testDbName);
            await table1.InsertAsync(txn1, new { 
                userId = testPrimaryKey, 
                name = "Test User", 
                email = "test@example.com" 
            });
            await txn1.CommitAsync();
            
            _output.WriteLine($"Inserted record with primary key: {testPrimaryKey}");
            
            // Verify record can be found with SAME table instance (in-memory index)
            var txn1Verify = await dbLayer1.BeginTransactionAsync(testDbName);
            var foundUser1 = await table1.GetAsync(txn1Verify, testPrimaryKey);
            await txn1Verify.CommitAsync();
            
            Assert.NotNull(foundUser1);
            Assert.Equal("Test User", foundUser1.name);
            _output.WriteLine("✓ Record found with same table instance (in-memory index works)");
        }
        finally
        {
            txn1?.Dispose();
        }
        
        // PHASE 2: Create fresh Database/Table instances (simulates process restart)  
        _output.WriteLine("\n=== PHASE 2: Create fresh instances and try to find same record ===");
        
        IDatabaseTransaction? txn2 = null;
        try
        {
            var dbLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
            var database2 = await dbLayer2.GetDatabaseAsync(testDbName);
            Assert.NotNull(database2);
            
            var table2 = await database2.GetTableAsync(testTableName);
            Assert.NotNull(table2);
            
            _output.WriteLine("Created fresh database and table instances");
            
            // CRITICAL TEST: Try to find the same record with fresh table instance
            txn2 = await dbLayer2.BeginTransactionAsync(testDbName);
            var foundUser2 = await table2.GetAsync(txn2, testPrimaryKey);
            await txn2.CommitAsync();
            
            // THIS ASSERTION WILL FAIL - demonstrating the index persistence problem
            Assert.NotNull(foundUser2); // FAILS: foundUser2 is null because index is empty
            Assert.Equal("Test User", foundUser2.name);
            _output.WriteLine("✓ Record found with fresh table instance (persistent index works)");
        }
        finally
        {
            txn2?.Dispose();
        }
    }
    
    /// <summary>
    /// FAILING TEST: Demonstrates index rebuilding doesn't happen automatically
    /// This test examines InitializeIndexesFromStorageAsync behavior
    /// </summary>
    [Fact]
    public async Task IndexRebuildingFromStorage_CurrentlyFails_ShouldWork()
    {
        const string testDbName = "phase2_index_rebuilding_test";
        const string testTableName = "products";
        
        // Insert multiple records to make index rebuilding more obvious
        _output.WriteLine("=== PHASE 1: Insert multiple records ===");
        
        var dbLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database1 = await dbLayer1.CreateDatabaseAsync(testDbName);
        var table1 = await database1.CreateTableAsync(testTableName, "$.productId");
        
        // Insert test records
        var testRecords = new[]
        {
            new { productId = "PROD-A", name = "Product A", price = 10.50 },
            new { productId = "PROD-B", name = "Product B", price = 20.75 },
            new { productId = "PROD-C", name = "Product C", price = 15.25 }
        };
        
        foreach (var record in testRecords)
        {
            var txn = await dbLayer1.BeginTransactionAsync(testDbName);
            await table1.InsertAsync(txn, record);
            await txn.CommitAsync();
            _output.WriteLine($"Inserted: {record.productId}");
        }
        
        // Verify all records are findable with original table instance
        foreach (var record in testRecords)
        {
            var txn = await dbLayer1.BeginTransactionAsync(testDbName);
            var found = await table1.GetAsync(txn, record.productId);
            await txn.CommitAsync();
            
            Assert.NotNull(found);
            Assert.Equal(record.name, found.name);
            _output.WriteLine($"✓ Verified {record.productId} findable in original instance");
        }
        
        // PHASE 2: Create fresh table instance and verify index rebuilding
        _output.WriteLine("\n=== PHASE 2: Test index rebuilding with fresh table instance ===");
        
        var dbLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database2 = await dbLayer2.GetDatabaseAsync(testDbName);
        var table2 = await database2.GetTableAsync(testTableName);
        
        Assert.NotNull(database2);
        Assert.NotNull(table2);
        _output.WriteLine("Created fresh database/table instances");
        
        // CRITICAL TEST: Verify all records are still findable with fresh instance
        // This depends on InitializeIndexesFromStorageAsync working correctly
        foreach (var record in testRecords)
        {
            var txn = await dbLayer2.BeginTransactionAsync(testDbName);
            var found = await table2.GetAsync(txn, record.productId);
            await txn.CommitAsync();
            
            // THESE ASSERTIONS WILL FAIL - demonstrating index rebuilding problem
            Assert.NotNull(found); // FAILS: found is null because index wasn't rebuilt
            Assert.Equal(record.name, found.name);
            _output.WriteLine($"✓ Verified {record.productId} findable in fresh instance");
        }
    }
    
    /// <summary>
    /// FAILING TEST: Demonstrates that InitializeIndexesFromStorageAsync is called but doesn't work
    /// This test verifies that the method exists but has implementation issues
    /// </summary>
    [Fact]
    public async Task InitializeIndexesFromStorageAsync_IsCalledButFails()
    {
        const string testDbName = "phase2_initialize_test";
        const string testTableName = "orders";
        const string testPrimaryKey = "ORDER-999";
        
        _output.WriteLine("=== PHASE 1: Create data for index initialization test ===");
        
        // Create initial data
        var dbLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database1 = await dbLayer1.CreateDatabaseAsync(testDbName);
        var table1 = await database1.CreateTableAsync(testTableName, "$.orderId");
        
        var txn1 = await dbLayer1.BeginTransactionAsync(testDbName);
        await table1.InsertAsync(txn1, new { 
            orderId = testPrimaryKey, 
            customer = "Test Customer",
            amount = 299.99,
            status = "Pending"
        });
        await txn1.CommitAsync();
        
        _output.WriteLine($"Created order: {testPrimaryKey}");
        
        // PHASE 2: Manually test InitializeIndexesFromStorageAsync
        _output.WriteLine("\n=== PHASE 2: Test InitializeIndexesFromStorageAsync directly ===");
        
        var dbLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database2 = await dbLayer2.GetDatabaseAsync(testDbName);
        var table2 = await database2.GetTableAsync(testTableName);
        
        Assert.NotNull(table2);
        
        // At this point, InitializeIndexesFromStorageAsync should have been called
        // by the Database.GetTableAsync method (line 199 in Database.cs)
        // Let's verify if it worked by trying to find our record
        
        var txn2 = await dbLayer2.BeginTransactionAsync(testDbName);
        var foundOrder = await table2.GetAsync(txn2, testPrimaryKey);
        await txn2.CommitAsync();
        
        // CRITICAL ASSERTION: This will fail if InitializeIndexesFromStorageAsync doesn't work
        Assert.NotNull(foundOrder); // FAILS: index not properly initialized
        Assert.Equal("Test Customer", foundOrder.customer);
        Assert.Equal(299.99, (double)foundOrder.amount);
        
        _output.WriteLine("✓ InitializeIndexesFromStorageAsync worked correctly");
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