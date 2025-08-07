using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using TxtDb.Database.Services;
using TxtDb.Database.Models;
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Services;
using TxtDb.Storage.Models;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// CRITICAL: Tests for Database-Storage transaction coordination issues.
/// These tests reproduce the exact issue where:
/// 1. Database layer InsertAsync properly commits data to Storage layer
/// 2. Database layer GetAsync finds the correct index entry 
/// 3. But when GetAsync reads the page from Storage, it returns empty/null
/// 4. This suggests transaction isolation or version visibility issues
/// </summary>
public class TransactionCoordinationTests : IAsyncDisposable
{
    private string _testDirectory = null!;
    private AsyncStorageSubsystem _storage = null!;
    private DatabaseLayer _database = null!;

    private async Task SetUp()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"TxtDbTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        
        _storage = new AsyncStorageSubsystem();
        await _storage.InitializeAsync(_testDirectory, null);
        _database = new DatabaseLayer(_storage);
    }

    public async ValueTask DisposeAsync()
    {
        if (_storage != null)
        {
            _storage.Dispose();
        }
        
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public async Task InsertThenGet_SameTransaction_ShouldReturnInsertedData()
    {
        // Arrange
        await SetUp();
        var database = await _database.CreateDatabaseAsync("TestDb");
        var table = await database.CreateTableAsync("Products", "$.Id");
        
        var product = new { Id = 1, Name = "Test Product", Price = 99.99 };
        
        // Act & Assert - This test SHOULD PASS but currently FAILS
        using var transaction = await _database.BeginTransactionAsync("TestDb");
        
        // Insert succeeds
        var insertedId = await table.InsertAsync(transaction, product);
        Console.WriteLine($"[TEST] Inserted product with ID: {insertedId}");
        
        // Get should return the inserted data but currently returns NULL
        var retrievedProduct = await table.GetAsync(transaction, 1);
        Console.WriteLine($"[TEST] Retrieved product: {retrievedProduct}");
        
        // CRITICAL: This assertion FAILS due to transaction coordination issue
        Assert.NotNull(retrievedProduct);
        Assert.Equal(1, (int)retrievedProduct.Id);
        Assert.Equal("Test Product", (string)retrievedProduct.Name);
        
        await transaction.CommitAsync();
    }

    [Fact]
    public async Task InsertCommitThenGet_SeparateTransaction_ShouldReturnInsertedData()
    {
        // Arrange
        await SetUp();
        var database = await _database.CreateDatabaseAsync("TestDb2");
        var table = await database.CreateTableAsync("Products2", "$.Id");
        
        var product = new { Id = 2, Name = "Another Product", Price = 149.99 };
        
        // Act - Insert and commit
        using (var insertTxn = await _database.BeginTransactionAsync("TestDb2"))
        {
            await table.InsertAsync(insertTxn, product);
            await insertTxn.CommitAsync();
        }
        
        // Act & Assert - Read in separate transaction
        using var readTxn = await _database.BeginTransactionAsync("TestDb2");
        var retrievedProduct = await table.GetAsync(readTxn, 2);
        
        // This should work (committed data should be visible to new transactions)
        Assert.NotNull(retrievedProduct);
        Assert.Equal(2, (int)retrievedProduct.Id);
        Assert.Equal("Another Product", (string)retrievedProduct.Name);
        
        await readTxn.CommitAsync();
    }

    [Fact]
    public async Task MultipleInsertsAndGets_SameTransaction_AllShouldBeVisible()
    {
        // Arrange
        await SetUp();
        var database = await _database.CreateDatabaseAsync("TestDb3");
        var table = await database.CreateTableAsync("Products3", "$.Id");
        
        var products = new[]
        {
            new { Id = 10, Name = "Product A", Price = 10.0 },
            new { Id = 11, Name = "Product B", Price = 20.0 },
            new { Id = 12, Name = "Product C", Price = 30.0 }
        };
        
        // Act & Assert
        using var transaction = await _database.BeginTransactionAsync("TestDb3");
        
        // Insert all products
        foreach (var product in products)
        {
            await table.InsertAsync(transaction, product);
            Console.WriteLine($"[TEST] Inserted: {product.Name}");
        }
        
        // All products should be immediately visible within the same transaction
        foreach (var expectedProduct in products)
        {
            var retrieved = await table.GetAsync(transaction, expectedProduct.Id);
            Console.WriteLine($"[TEST] Retrieved for ID {expectedProduct.Id}: {retrieved}");
            
            Assert.NotNull(retrieved);
            Assert.Equal(expectedProduct.Id, (int)retrieved.Id);
            Assert.Equal(expectedProduct.Name, (string)retrieved.Name);
        }
        
        await transaction.CommitAsync();
    }

    [Fact]
    public async Task DebugTransactionFlow_ShowsStorageVersioning()
    {
        // Arrange
        await SetUp();
        var database = await _database.CreateDatabaseAsync("TestDb4");
        var table = await database.CreateTableAsync("DebugTable", "$.Id");
        
        var testObject = new { Id = 99, Data = "Debug Test" };
        
        // Act - Insert and immediately try to read
        using var transaction = await _database.BeginTransactionAsync("TestDb4");
        
        Console.WriteLine($"[DEBUG] Transaction ID: {transaction.TransactionId}");
        Console.WriteLine($"[DEBUG] Storage Transaction ID: {transaction.GetStorageTransactionId()}");
        
        // Insert
        await table.InsertAsync(transaction, testObject);
        Console.WriteLine($"[DEBUG] Insert completed");
        
        // Try to read - this is where the issue occurs
        var result = await table.GetAsync(transaction, 99);
        Console.WriteLine($"[DEBUG] Get result: {result}");
        
        // Check what the storage layer sees
        var storageTransactionId = transaction.GetStorageTransactionId();
        var tableNamespace = "TestDb4.DebugTable";
        
        // This will help us understand what's happening at the Storage layer
        try
        {
            var allObjects = await _storage.GetMatchingObjectsAsync(storageTransactionId, tableNamespace, "*");
            Console.WriteLine($"[DEBUG] Storage layer sees {allObjects.Count} pages");
            foreach (var kvp in allObjects)
            {
                Console.WriteLine($"[DEBUG] Page {kvp.Key}: {kvp.Value.Length} objects");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DEBUG] Error reading from storage: {ex.Message}");
        }
        
        // The assertion that should pass but currently fails
        Assert.NotNull(result);
        
        await transaction.CommitAsync();
    }
}