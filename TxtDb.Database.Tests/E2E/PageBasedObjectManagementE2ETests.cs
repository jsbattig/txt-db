using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Models;
using TxtDb.Database.Services;
using TxtDb.Database.Exceptions;
using TxtDb.Storage.Services;

namespace TxtDb.Database.Tests.E2E;

/// <summary>
/// Epic 004 Story 1: Page-Based Object Management E2E Tests
/// 
/// These tests validate page-based CRUD operations with primary key enforcement:
/// - Insert with primary key uniqueness across pages
/// - Update with sequential search and page rewrite
/// - Get with efficient page lookup and sequential search within page
/// - Delete with object removal and empty page cleanup
/// - Optimistic concurrency handling at page level
/// 
/// CRITICAL: All tests use real storage and real page operations
/// </summary>
public class PageBasedObjectManagementE2ETests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;
    private IDatabaseLayer? _databaseLayer;
    private IDatabase? _database;
    private ITable? _table;

    public PageBasedObjectManagementE2ETests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_object_management_e2e", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    #region TDD Phase 1: Red - Failing E2E Tests

    /// <summary>
    /// Test 1: Basic CRUD Operations E2E
    /// Insert, Get, Update, Delete with primary key enforcement
    /// </summary>
    [Fact]
    public async Task BasicCRUDOperations_WithPrimaryKeyEnforcement_ShouldWork()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        
        dynamic testProduct = new
        {
            productId = "PROD-001",
            name = "Test Product",
            price = 19.99m,
            category = "Electronics",
            inStock = true
        };

        // Act & Assert - Insert
        IDatabaseTransaction transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);
        
        var insertedKey = await _table!.InsertAsync(transaction, testProduct);
        Assert.Equal("PROD-001", insertedKey);
        
        await transaction.CommitAsync();
        _output.WriteLine($"Inserted product with key: {insertedKey}");

        // Act & Assert - Get
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var retrievedProduct = await _table.GetAsync(transaction, "PROD-001");
        Assert.NotNull(retrievedProduct);
        Assert.Equal("PROD-001", (string)retrievedProduct.productId);
        Assert.Equal("Test Product", (string)retrievedProduct.name);
        Assert.Equal(19.99m, (decimal)retrievedProduct.price);
        
        await transaction.CommitAsync();
        _output.WriteLine($"Retrieved product: {retrievedProduct.name}");

        // Act & Assert - Update
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        dynamic updatedProduct = new
        {
            productId = "PROD-001",
            name = "Updated Test Product",
            price = 24.99m,
            category = "Electronics",
            inStock = false
        };
        
        await _table.UpdateAsync(transaction, "PROD-001", updatedProduct);
        await transaction.CommitAsync();
        _output.WriteLine("Updated product");

        // Verify update
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        var verifyUpdated = await _table.GetAsync(transaction, "PROD-001");
        Assert.Equal("Updated Test Product", (string)verifyUpdated.name);
        Assert.Equal(24.99m, (decimal)verifyUpdated.price);
        Assert.False((bool)verifyUpdated.inStock);
        await transaction.CommitAsync();

        // Act & Assert - Delete
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var deleted = await _table.DeleteAsync(transaction, "PROD-001");
        Assert.True(deleted);
        
        await transaction.CommitAsync();
        _output.WriteLine("Deleted product");

        // Verify deletion
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        var verifyDeleted = await _table.GetAsync(transaction, "PROD-001");
        Assert.Null(verifyDeleted);
        await transaction.CommitAsync();
        
        _output.WriteLine("Basic CRUD operations completed successfully");
    }

    /// <summary>
    /// Test 2: Primary Key Uniqueness Enforcement Across Pages
    /// Insert multiple objects, ensure primary key uniqueness is enforced across all pages
    /// </summary>
    [Fact]
    public async Task PrimaryKeyUniqueness_AcrossMultiplePages_ShouldBeEnforced()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        const int objectCount = 1000; // Force multiple pages
        
        var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);

        // Act - Insert many objects to span multiple pages
        var insertedKeys = new List<string>();
        for (int i = 0; i < objectCount; i++)
        {
            dynamic product = new
            {
                productId = $"PROD-{i:D6}",
                name = $"Product {i}",
                price = 10.0m + i,
                category = i % 2 == 0 ? "Electronics" : "Books"
            };
            
            var key = await _table!.InsertAsync(transaction, product);
            insertedKeys.Add((string)key);
            
            if (i % 100 == 0)
            {
                _output.WriteLine($"Inserted {i + 1} products");
            }
        }
        
        await transaction.CommitAsync();
        _output.WriteLine($"Successfully inserted {objectCount} unique products");

        // Assert - Attempt to insert duplicate primary key should fail
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        dynamic duplicateProduct = new
        {
            productId = "PROD-000500", // Duplicate of existing key
            name = "Duplicate Product",
            price = 99.99m
        };
        
        await Assert.ThrowsAsync<DuplicatePrimaryKeyException>(
            async () => await _table!.InsertAsync(transaction, duplicateProduct));
        
        await transaction.RollbackAsync();
        _output.WriteLine("Primary key uniqueness properly enforced across pages");

        // Assert - Verify all original objects still retrievable
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var sampleKeys = insertedKeys.Where((_, i) => i % 100 == 0).ToList();
        foreach (var key in sampleKeys)
        {
            var product = await _table!.GetAsync(transaction, key);
            Assert.NotNull(product);
            Assert.Equal(key, (string)product.productId);
        }
        
        await transaction.CommitAsync();
        _output.WriteLine($"Verified {sampleKeys.Count} sample products still retrievable");
    }

    /// <summary>
    /// Test 3: Sequential Search and Page Rewrite on Update
    /// Update object in middle of page, verify sequential search and page rewrite behavior
    /// </summary>
    [Fact]
    public async Task UpdateOperation_SequentialSearchAndPageRewrite_ShouldWork()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        const int objectsPerPage = 100; // Force objects into same page
        
        var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);

        // Create objects that will be in the same page
        var targetKey = "TARGET-050"; // Object in middle of page
        for (int i = 0; i < objectsPerPage; i++)
        {
            dynamic product = new
            {
                productId = i == 50 ? targetKey : $"PROD-{i:D3}",
                name = $"Product {i}",
                price = 10.0m + i,
                version = 1,
                timestamp = DateTime.UtcNow
            };
            
            await _table!.InsertAsync(transaction, product);
        }
        
        await transaction.CommitAsync();
        _output.WriteLine($"Created {objectsPerPage} objects in same page");

        // Act - Update target object (should trigger sequential search and page rewrite)
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var updateStart = DateTime.UtcNow;
        
        dynamic updatedProduct = new
        {
            productId = targetKey,
            name = "Updated Target Product",
            price = 999.99m,
            version = 2,
            timestamp = DateTime.UtcNow,
            newField = "Added in update"
        };
        
        await _table!.UpdateAsync(transaction, targetKey, updatedProduct);
        
        var updateDuration = DateTime.UtcNow - updateStart;
        await transaction.CommitAsync();
        
        _output.WriteLine($"Update operation took: {updateDuration.TotalMilliseconds}ms");

        // Assert - Verify update performance target (realistic for file-based MVCC)
        Assert.True(updateDuration.TotalMilliseconds < 50,
            $"Update took {updateDuration.TotalMilliseconds}ms, should be < 50ms");

        // Assert - Verify target object was updated
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var updatedObj = await _table.GetAsync(transaction, targetKey);
        Assert.NotNull(updatedObj);
        Assert.Equal("Updated Target Product", (string)updatedObj.name);
        Assert.Equal(999.99m, (decimal)updatedObj.price);
        Assert.Equal(2, (int)updatedObj.version);
        Assert.Equal("Added in update", (string)updatedObj.newField);
        
        // Assert - Verify other objects in page remain unchanged
        var sampleKeys = new[] { "PROD-049", "PROD-051", "PROD-025", "PROD-075" };
        foreach (var key in sampleKeys)
        {
            var product = await _table.GetAsync(transaction, key);
            Assert.NotNull(product);
            Assert.Equal(1, (int)product.version); // Original version
            Assert.DoesNotContain("newField", (IDictionary<string, object>)product);
        }
        
        await transaction.CommitAsync();
        _output.WriteLine("Sequential search and page rewrite validated");
    }

    /// <summary>
    /// Test 4: Empty Page Cleanup on Delete
    /// Delete all objects from a page, verify page cleanup behavior
    /// </summary>
    [Fact]
    public async Task DeleteOperation_EmptyPageCleanup_ShouldWork()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        const int objectsInPage = 10; // Small page for easy deletion
        
        var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);

        // Create objects in same page
        var pageKeys = new List<string>();
        for (int i = 0; i < objectsInPage; i++)
        {
            var key = $"DELETE-{i:D3}";
            pageKeys.Add(key);
            
            dynamic product = new
            {
                productId = key,
                name = $"To Delete {i}",
                price = 5.0m + i
            };
            
            await _table!.InsertAsync(transaction, product);
        }
        
        await transaction.CommitAsync();
        _output.WriteLine($"Created {objectsInPage} objects for deletion test");

        // Act - Delete all objects from the page one by one
        foreach (var key in pageKeys)
        {
            transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
            
            var deleted = await _table!.DeleteAsync(transaction, key);
            Assert.True(deleted);
            
            await transaction.CommitAsync();
            _output.WriteLine($"Deleted object: {key}");
        }

        // Assert - Verify all objects are gone
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        foreach (var key in pageKeys)
        {
            var product = await _table!.GetAsync(transaction, key);
            Assert.Null(product);
        }
        
        await transaction.CommitAsync();
        _output.WriteLine("All objects successfully deleted");

        // Assert - Verify page cleanup occurred (attempt to delete non-existent key should return false)
        transaction = await _databaseLayer.BeginTransactionAsync(_database.Name);
        
        var deletedAgain = await _table!.DeleteAsync(transaction, pageKeys[0]);
        Assert.False(deletedAgain);
        
        await transaction.CommitAsync();
        _output.WriteLine("Empty page cleanup validated");
    }

    /// <summary>
    /// Test 5: Performance Benchmark for Page Operations
    /// Verify all page operations meet performance targets
    /// </summary>
    [Fact]
    public async Task PageOperations_PerformanceBenchmark_ShouldMeetTargets()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        const int benchmarkCount = 100;
        
        var insertTimes = new List<double>();
        var getTimes = new List<double>();
        var updateTimes = new List<double>();
        var deleteTimes = new List<double>();

        // Benchmark Insert Operations
        for (int i = 0; i < benchmarkCount; i++)
        {
            var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);
            
            dynamic product = new
            {
                productId = $"PERF-INSERT-{i:D4}",
                name = $"Performance Test Product {i}",
                price = 15.0m + i,
                category = "Performance"
            };
            
            var start = DateTime.UtcNow;
            await _table!.InsertAsync(transaction, product);
            var duration = DateTime.UtcNow - start;
            insertTimes.Add(duration.TotalMilliseconds);
            
            await transaction.CommitAsync();
        }

        // Benchmark Get Operations
        for (int i = 0; i < benchmarkCount; i++)
        {
            var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);
            
            var start = DateTime.UtcNow;
            var product = await _table!.GetAsync(transaction, $"PERF-INSERT-{i:D4}");
            var duration = DateTime.UtcNow - start;
            getTimes.Add(duration.TotalMilliseconds);
            
            Assert.NotNull(product);
            await transaction.CommitAsync();
        }

        // Benchmark Update Operations
        for (int i = 0; i < benchmarkCount; i++)
        {
            var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);
            
            dynamic updatedProduct = new
            {
                productId = $"PERF-INSERT-{i:D4}",
                name = $"Updated Performance Test Product {i}",
                price = 25.0m + i,
                category = "Performance",
                updated = true
            };
            
            var start = DateTime.UtcNow;
            await _table!.UpdateAsync(transaction, $"PERF-INSERT-{i:D4}", updatedProduct);
            var duration = DateTime.UtcNow - start;
            updateTimes.Add(duration.TotalMilliseconds);
            
            await transaction.CommitAsync();
        }

        // Benchmark Delete Operations
        for (int i = 0; i < benchmarkCount; i++)
        {
            var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);
            
            var start = DateTime.UtcNow;
            var deleted = await _table!.DeleteAsync(transaction, $"PERF-INSERT-{i:D4}");
            var duration = DateTime.UtcNow - start;
            deleteTimes.Add(duration.TotalMilliseconds);
            
            Assert.True(deleted);
            await transaction.CommitAsync();
        }

        // Calculate performance metrics
        var insertP95 = CalculateP95(insertTimes);
        var getP95 = CalculateP95(getTimes);
        var updateP95 = CalculateP95(updateTimes);
        var deleteP95 = CalculateP95(deleteTimes);

        _output.WriteLine($"Page Operations Performance (P95):");
        _output.WriteLine($"  Insert: {insertP95:F2}ms");
        _output.WriteLine($"  Get: {getP95:F2}ms");
        _output.WriteLine($"  Update: {updateP95:F2}ms");
        _output.WriteLine($"  Delete: {deleteP95:F2}ms");

        // Realistic performance targets for file-based MVCC operations
        // These targets account for file I/O overhead, MVCC coordination, and async operations
        Assert.True(insertP95 < 75, $"Insert P95 {insertP95:F2}ms exceeds 75ms target");
        Assert.True(getP95 < 35, $"Get P95 {getP95:F2}ms exceeds 35ms target");
        Assert.True(updateP95 < 75, $"Update P95 {updateP95:F2}ms exceeds 75ms target");
        Assert.True(deleteP95 < 50, $"Delete P95 {deleteP95:F2}ms exceeds 50ms target");

        _output.WriteLine("All page operations meet performance targets");
    }

    /// <summary>
    /// Test 6: Object Validation and Error Handling
    /// Test various error conditions and edge cases
    /// </summary>
    [Fact]
    public async Task ObjectValidation_ErrorHandling_ShouldWork()
    {
        // Arrange
        await InitializeTestEnvironmentAsync();
        var transaction = await _databaseLayer!.BeginTransactionAsync(_database!.Name);

        // Act & Assert - Missing Primary Key
        dynamic invalidProduct = new
        {
            name = "Product without ID",
            price = 10.0m
            // Missing productId field
        };
        
        await Assert.ThrowsAsync<MissingPrimaryKeyException>(
            async () => await _table!.InsertAsync(transaction, invalidProduct));

        // Act & Assert - Update Non-Existent Object
        dynamic nonExistentUpdate = new
        {
            productId = "NON-EXISTENT",
            name = "Does not exist",
            price = 50.0m
        };
        
        await Assert.ThrowsAsync<ObjectNotFoundException>(
            async () => await _table!.UpdateAsync(transaction, "NON-EXISTENT", nonExistentUpdate));

        // Act & Assert - Primary Key Mismatch in Update
        dynamic validProduct = new
        {
            productId = "VALID-001",
            name = "Valid Product",
            price = 20.0m
        };
        
        await _table!.InsertAsync(transaction, validProduct);
        
        dynamic mismatchProduct = new
        {
            productId = "DIFFERENT-001", // Different from update key
            name = "Mismatched Key",
            price = 30.0m
        };
        
        await Assert.ThrowsAsync<PrimaryKeyMismatchException>(
            async () => await _table!.UpdateAsync(transaction, "VALID-001", mismatchProduct));

        // Act & Assert - Delete Non-Existent Object Should Return False
        var deleteResult = await _table!.DeleteAsync(transaction, "NON-EXISTENT-DELETE");
        Assert.False(deleteResult);

        await transaction.RollbackAsync();
        _output.WriteLine("Object validation and error handling completed");
    }

    #endregion

    #region Helper Methods

    private async Task InitializeTestEnvironmentAsync()
    {
        // CRITICAL FIX: Use async factory pattern
        _databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        _database = await _databaseLayer.CreateDatabaseAsync("test_db");
        _table = await _database.CreateTableAsync("products", "$.productId");
        
        _output.WriteLine("Test environment initialized");
    }

    private static double CalculateP95(List<double> values)
    {
        values.Sort();
        int index = (int)Math.Ceiling(values.Count * 0.95) - 1;
        return values[Math.Max(0, Math.Min(index, values.Count - 1))];
    }

    #endregion

    public void Dispose()
    {
        (_databaseLayer as IDisposable)?.Dispose();
        
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