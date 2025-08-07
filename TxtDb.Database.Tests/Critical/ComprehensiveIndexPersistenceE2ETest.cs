using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// PHASE 2 COMPLETE: Comprehensive End-to-End test demonstrating full index persistence
/// 
/// This test proves that Phase 2 "Fix index persistence and rebuilding mechanisms" is complete:
/// - Indexes are properly persisted across Database instance lifecycle
/// - Fresh Database instances can rebuild indexes from storage
/// - Multiple tables and records work correctly
/// - Complex scenarios work seamlessly
/// 
/// SUCCESS CRITERIA:
/// 1. Multiple tables with different data types
/// 2. Multiple Database instance restarts
/// 3. Data inserted in one instance is findable in fresh instances
/// 4. Index rebuilding is transparent and automatic
/// 5. No manual index rebuilding required by application code
/// </summary>
public class ComprehensiveIndexPersistenceE2ETest : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public ComprehensiveIndexPersistenceE2ETest(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine("/tmp", "txtdb_phase2_e2e", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Phase 2 E2E Test Directory: {_testDirectory}");
        _output.WriteLine($"Storage Directory: {_storageDirectory}");
    }

    /// <summary>
    /// COMPREHENSIVE E2E TEST: Multiple tables, multiple instance restarts, various data types
    /// This test demonstrates that Phase 2 index persistence fix is fully working
    /// </summary>
    [Fact]
    public async Task CompleteIndexPersistence_MultipleTablesAndRestarts_WorksSeamlessly()
    {
        const string dbName = "comprehensive_e2e_test";
        
        // === PHASE 1: Create database with multiple tables and various data ===
        _output.WriteLine("=== PHASE 1: Create database with multiple tables and insert test data ===");
        
        var testData = new
        {
            Users = new[]
            {
                new { userId = "USER-001", name = "Alice Johnson", email = "alice@test.com", age = 28 },
                new { userId = "USER-002", name = "Bob Smith", email = "bob@test.com", age = 35 },
                new { userId = "USER-003", name = "Carol Davis", email = "carol@test.com", age = 42 }
            },
            Products = new[]
            {
                new { productId = "PROD-A", name = "Laptop", price = 1299.99, category = "Electronics" },
                new { productId = "PROD-B", name = "Mouse", price = 25.50, category = "Electronics" },
                new { productId = "PROD-C", name = "Book", price = 15.75, category = "Education" }
            },
            Orders = new[]
            {
                new { orderId = 1001, customerId = "USER-001", productId = "PROD-A", quantity = 1, total = 1299.99 },
                new { orderId = 1002, customerId = "USER-002", productId = "PROD-B", quantity = 2, total = 51.00 },
                new { orderId = 1003, customerId = "USER-003", productId = "PROD-C", quantity = 3, total = 47.25 }
            }
        };
        
        // Create and populate database
        using (var dbLayer1 = await DatabaseLayer.CreateAsync(_storageDirectory))
        {
            var database1 = await dbLayer1.CreateDatabaseAsync(dbName);
            
            // Create tables with different primary key types
            var usersTable1 = await database1.CreateTableAsync("users", "$.userId");     // String keys
            var productsTable1 = await database1.CreateTableAsync("products", "$.productId"); // String keys  
            var ordersTable1 = await database1.CreateTableAsync("orders", "$.orderId");   // Integer keys
            
            _output.WriteLine("Created 3 tables: users, products, orders");
            
            // Insert users
            foreach (var user in testData.Users)
            {
                using var txn = await dbLayer1.BeginTransactionAsync(dbName);
                await usersTable1.InsertAsync(txn, user);
                await txn.CommitAsync();
                _output.WriteLine($"Inserted user: {user.userId}");
            }
            
            // Insert products
            foreach (var product in testData.Products)
            {
                using var txn = await dbLayer1.BeginTransactionAsync(dbName);
                await productsTable1.InsertAsync(txn, product);
                await txn.CommitAsync();
                _output.WriteLine($"Inserted product: {product.productId}");
            }
            
            // Insert orders
            foreach (var order in testData.Orders)
            {
                using var txn = await dbLayer1.BeginTransactionAsync(dbName);
                await ordersTable1.InsertAsync(txn, order);
                await txn.CommitAsync();
                _output.WriteLine($"Inserted order: {order.orderId}");
            }
            
            // Verify all data is accessible in original instance
            _output.WriteLine("\n--- Verifying data in original instance ---");
            await VerifyAllData(dbLayer1, dbName, testData, "original instance");
        } // DatabaseLayer1 disposed - simulating application restart
        
        // === PHASE 2: First restart - verify all data is still accessible ===
        _output.WriteLine("\n=== PHASE 2: First restart - fresh Database instance ===");
        
        using (var dbLayer2 = await DatabaseLayer.CreateAsync(_storageDirectory))
        {
            var database2 = await dbLayer2.GetDatabaseAsync(dbName);
            Assert.NotNull(database2);
            _output.WriteLine("Successfully loaded database from storage");
            
            await VerifyAllData(dbLayer2, dbName, testData, "first restart");
            
            // Add more data in this instance
            _output.WriteLine("\n--- Adding additional data in restarted instance ---");
            var usersTable2 = await database2.GetTableAsync("users");
            
            using var txn = await dbLayer2.BeginTransactionAsync(dbName);
            await usersTable2.InsertAsync(txn, new { userId = "USER-004", name = "David Wilson", email = "david@test.com", age = 29 });
            await txn.CommitAsync();
            _output.WriteLine("Added USER-004");
        } // DatabaseLayer2 disposed - simulating another application restart
        
        // === PHASE 3: Second restart - verify all data including newly added ===
        _output.WriteLine("\n=== PHASE 3: Second restart - verify data persistence continues working ===");
        
        using (var dbLayer3 = await DatabaseLayer.CreateAsync(_storageDirectory))
        {
            var database3 = await dbLayer3.GetDatabaseAsync(dbName);
            Assert.NotNull(database3);
            
            await VerifyAllData(dbLayer3, dbName, testData, "second restart");
            
            // Verify the additionally added user
            var usersTable3 = await database3.GetTableAsync("users");
            using var txn = await dbLayer3.BeginTransactionAsync(dbName);
            var david = await usersTable3.GetAsync(txn, "USER-004");
            await txn.CommitAsync();
            
            Assert.NotNull(david);
            Assert.Equal("David Wilson", david.name);
            _output.WriteLine("✓ USER-004 found after second restart - comprehensive persistence working");
        }
        
        // === PHASE 4: Update and delete operations across restarts ===
        _output.WriteLine("\n=== PHASE 4: Test update/delete operations with fresh instances ===");
        
        using (var dbLayer4 = await DatabaseLayer.CreateAsync(_storageDirectory))
        {
            var database4 = await dbLayer4.GetDatabaseAsync(dbName);
            var usersTable4 = await database4.GetTableAsync("users");
            
            // Update a user
            using var updateTxn = await dbLayer4.BeginTransactionAsync(dbName);
            await usersTable4.UpdateAsync(updateTxn, "USER-001", new { 
                userId = "USER-001", 
                name = "Alice Johnson-Smith", // Updated name
                email = "alice.smith@test.com", // Updated email
                age = 29 
            });
            await updateTxn.CommitAsync();
            _output.WriteLine("Updated USER-001");
            
            // Delete a user
            using var deleteTxn = await dbLayer4.BeginTransactionAsync(dbName);
            var deleted = await usersTable4.DeleteAsync(deleteTxn, "USER-002");
            await deleteTxn.CommitAsync();
            Assert.True(deleted);
            _output.WriteLine("Deleted USER-002");
        }
        
        // === PHASE 5: Final restart - verify updates and deletes persisted ===
        _output.WriteLine("\n=== PHASE 5: Final restart - verify updates/deletes persisted correctly ===");
        
        using (var dbLayer5 = await DatabaseLayer.CreateAsync(_storageDirectory))
        {
            var database5 = await dbLayer5.GetDatabaseAsync(dbName);
            var usersTable5 = await database5.GetTableAsync("users");
            
            // Verify update persisted
            using var verifyTxn = await dbLayer5.BeginTransactionAsync(dbName);
            var updatedAlice = await usersTable5.GetAsync(verifyTxn, "USER-001");
            Assert.NotNull(updatedAlice);
            Assert.Equal("Alice Johnson-Smith", updatedAlice.name);
            Assert.Equal("alice.smith@test.com", updatedAlice.email);
            _output.WriteLine("✓ USER-001 update persisted correctly");
            
            // Verify delete persisted
            var deletedBob = await usersTable5.GetAsync(verifyTxn, "USER-002");
            Assert.Null(deletedBob);
            _output.WriteLine("✓ USER-002 deletion persisted correctly");
            
            await verifyTxn.CommitAsync();
        }
        
        _output.WriteLine("\n🎉 PHASE 2 COMPLETE: Index persistence working perfectly across all scenarios!");
    }
    
    private async Task VerifyAllData(DatabaseLayer dbLayer, string dbName, dynamic testData, string instanceName)
    {
        var database = await dbLayer.GetDatabaseAsync(dbName);
        Assert.NotNull(database);
        
        var usersTable = await database.GetTableAsync("users");
        var productsTable = await database.GetTableAsync("products");
        var ordersTable = await database.GetTableAsync("orders");
        
        Assert.NotNull(usersTable);
        Assert.NotNull(productsTable);
        Assert.NotNull(ordersTable);
        
        // Verify users
        foreach (var user in testData.Users)
        {
            using var txn = await dbLayer.BeginTransactionAsync(dbName);
            var foundUser = await usersTable.GetAsync(txn, user.userId);
            await txn.CommitAsync();
            
            Assert.NotNull(foundUser);
            Assert.Equal(user.name, foundUser.name);
            Assert.Equal(user.email, foundUser.email);
        }
        
        // Verify products
        foreach (var product in testData.Products)
        {
            using var txn = await dbLayer.BeginTransactionAsync(dbName);
            var foundProduct = await productsTable.GetAsync(txn, product.productId);
            await txn.CommitAsync();
            
            Assert.NotNull(foundProduct);
            Assert.Equal(product.name, foundProduct.name);
            Assert.Equal(product.price, (double)foundProduct.price);
        }
        
        // Verify orders (integer keys)
        foreach (var order in testData.Orders)
        {
            using var txn = await dbLayer.BeginTransactionAsync(dbName);
            var foundOrder = await ordersTable.GetAsync(txn, order.orderId);
            await txn.CommitAsync();
            
            Assert.NotNull(foundOrder);
            Assert.Equal(order.customerId, foundOrder.customerId);
            Assert.Equal(order.total, (double)foundOrder.total);
        }
        
        _output.WriteLine($"✓ All data verified successfully in {instanceName}");
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