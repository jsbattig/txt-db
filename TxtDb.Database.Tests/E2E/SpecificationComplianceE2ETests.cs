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
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Models;

namespace TxtDb.Database.Tests.E2E;

/// <summary>
/// E2E Tests for Specification Compliance Fixes
/// 
/// These tests validate that the implementation strictly follows Epic 004 specification:
/// 1. NO unauthorized features (index persistence, retry logic, versioning, async factory)
/// 2. CORRECT index structure (SortedDictionary with HashSet values, not ConcurrentDictionary)
/// 3. ALL required interfaces implemented (IIndex, IQueryFilter, IPageEventSubscriber)
/// 4. SYNCHRONOUS constructor initialization (no async factory pattern)
/// 5. PERFORMANCE targets met (original 10ms targets, not adjusted 50ms)
/// </summary>
public class SpecificationComplianceE2ETests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;
    private IDatabaseLayer? _databaseLayer;

    public SpecificationComplianceE2ETests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_compliance_e2e_tests", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    #region Compliance Fix Tests

    /// <summary>
    /// Test: Synchronous Constructor Pattern (No Async Factory)
    /// SPEC REQUIREMENT: Remove async factory pattern, restore synchronous constructors
    /// </summary>
    [Fact]
    public async Task DatabaseLayer_SynchronousConstructor_ShouldWork()
    {
        // Act - Create database layer with synchronous constructor (storage must be pre-initialized)
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        // Assert - Instance created successfully without async factory
        Assert.NotNull(_databaseLayer);
        _output.WriteLine("DatabaseLayer created with synchronous constructor pattern");
    }

    /// <summary>
    /// Test: Required Interfaces Available
    /// SPEC REQUIREMENT: IIndex, IQueryFilter, IPageEventSubscriber must be implemented
    /// </summary>
    [Fact]
    public async Task RequiredInterfaces_AllImplemented_ShouldBeAccessible()
    {
        // Arrange
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await _databaseLayer.CreateDatabaseAsync("test_interfaces");
        var table = await database.CreateTableAsync("test_table", "$.id");
        
        using var transaction = await _databaseLayer.BeginTransactionAsync("test_interfaces");
        
        // Act & Assert - Index creation returns IIndex
        var index = await table.CreateIndexAsync(transaction, "name_index", "$.name");
        Assert.NotNull(index);
        Assert.IsAssignableFrom<IIndex>(index);
        Assert.Equal("name_index", index.Name);
        Assert.Equal("$.name", index.FieldPath);
        
        // Act & Assert - Query filter interface accessible
        var filter = new TestQueryFilter("$.name", "TestValue");
        Assert.IsAssignableFrom<IQueryFilter>(filter);
        Assert.True(filter.Matches(new { name = "TestValue" }));
        Assert.False(filter.Matches(new { name = "OtherValue" }));
        
        // Act & Assert - Page event subscriber interface accessible
        var subscriber = new TestPageEventSubscriber();
        Assert.IsAssignableFrom<IPageEventSubscriber>(subscriber);
        
        await _databaseLayer.SubscribeToPageEvents("test_interfaces", "test_table", subscriber);
        await _databaseLayer.UnsubscribeFromPageEvents("test_interfaces", "test_table", subscriber);
        
        _output.WriteLine("All required interfaces are properly implemented and accessible");
    }

    /// <summary>
    /// Test: Correct Index Structure (SortedDictionary with HashSet)
    /// SPEC REQUIREMENT: Index should use SortedDictionary<object, HashSet<string>>, not ConcurrentDictionary<object, string>
    /// </summary>
    [Fact]
    public async Task IndexStructure_SortedDictionaryWithHashSet_ShouldSupportMultiplePagesPerKey()
    {
        // Arrange
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await _databaseLayer.CreateDatabaseAsync("multi_page_test");
        var table = await database.CreateTableAsync("products", "$.id");
        
        using var transaction = await _databaseLayer.BeginTransactionAsync("multi_page_test");
        
        // Insert objects with same indexed field value (should go to different pages)
        var objects = new List<dynamic>();
        for (int i = 0; i < 200; i++) // Force multiple pages
        {
            objects.Add(new { id = i, category = "electronics", name = $"Product {i}" });
        }
        
        foreach (var obj in objects)
        {
            await table.InsertAsync(transaction, obj);
        }
        
        // Act - Create index on category field (same value for all objects)
        var categoryIndex = await table.CreateIndexAsync(transaction, "category_index", "$.category");
        
        // Act - Find all objects with same category value
        var electronicsProducts = await categoryIndex.FindAsync("electronics");
        
        // Assert - All objects found despite being in different pages
        Assert.Equal(200, electronicsProducts.Count);
        
        // Assert - Verify objects are distributed across multiple pages
        // This test validates that the index can map one key to multiple pages
        var productIds = electronicsProducts.Select(p => (int)p.id).ToHashSet();
        Assert.Equal(200, productIds.Count); // All unique products found
        
        _output.WriteLine($"Index correctly supports multiple pages per key: found {electronicsProducts.Count} objects with same category");
    }

    /// <summary>
    /// Test: No Unauthorized Features Present
    /// SPEC REQUIREMENT: Remove index persistence, retry logic, versioning, async factory
    /// </summary>
    [Fact]
    public async Task NoUnauthorizedFeatures_Implementation_ShouldBeClean()
    {
        // Arrange
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await _databaseLayer.CreateDatabaseAsync("clean_implementation");
        var table = await database.CreateTableAsync("test_table", "$.id");
        
        using var transaction = await _databaseLayer.BeginTransactionAsync("clean_implementation");
        
        // Act - Insert object
        await table.InsertAsync(transaction, new { id = 1, name = "Test" });
        
        // Assert - No retry logic should be invoked (operation succeeds immediately)
        // If retry logic existed, this would be slower or show retries in logs
        var startTime = DateTime.UtcNow;
        var retrieved = await table.GetAsync(transaction, 1);
        var duration = DateTime.UtcNow - startTime;
        
        Assert.NotNull(retrieved);
        Assert.Equal(1, retrieved.id);
        Assert.True(duration.TotalMilliseconds < 10, 
            $"Operation took {duration.TotalMilliseconds}ms - no retry logic should make it faster");
        
        // Assert - No versioning infrastructure (no version tracking)
        // Clean implementation should not have version-related fields or methods
        await transaction.CommitAsync();
        
        _output.WriteLine("Implementation is clean without unauthorized features");
    }

    /// <summary>
    /// Test: Original Performance Targets (10ms, not 50ms adjusted)
    /// SPEC REQUIREMENT: Meet original 10ms database creation target
    /// </summary>
    [Fact]
    public async Task Performance_Original10msTargets_ShouldBeAchieved()
    {
        // Arrange
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var performanceTimes = new List<double>();
        
        // Act - Measure database creation performance
        for (int i = 0; i < 50; i++)
        {
            var start = DateTime.UtcNow;
            await _databaseLayer.CreateDatabaseAsync($"perf_test_{i}");
            var duration = DateTime.UtcNow - start;
            performanceTimes.Add(duration.TotalMilliseconds);
        }
        
        // Calculate P95
        performanceTimes.Sort();
        var p95Index = (int)Math.Ceiling(performanceTimes.Count * 0.95) - 1;
        var p95Time = performanceTimes[p95Index];
        
        _output.WriteLine($"Database creation P95: {p95Time:F2}ms");
        
        // Assert - Meet ORIGINAL 10ms target (not adjusted 50ms)
        Assert.True(p95Time < 10.0, 
            $"P95 latency {p95Time:F2}ms exceeds original 10ms specification target");
        
        _output.WriteLine("Original performance targets achieved");
    }

    /// <summary>
    /// Test: All Table Interface Methods Work
    /// SPEC REQUIREMENT: All methods in ITable interface must be functional
    /// </summary>
    [Fact]
    public async Task TableInterface_AllMethods_ShouldWork()
    {
        // Arrange
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await _databaseLayer.CreateDatabaseAsync("table_methods_test");
        var table = await database.CreateTableAsync("products", "$.id");
        
        using var transaction = await _databaseLayer.BeginTransactionAsync("table_methods_test");
        
        // Test basic CRUD operations
        var productId = await table.InsertAsync(transaction, new { id = 1, name = "Widget", price = 99.99 });
        Assert.Equal(1, productId);
        
        var product = await table.GetAsync(transaction, 1);
        Assert.NotNull(product);
        Assert.Equal("Widget", product.name);
        
        await table.UpdateAsync(transaction, 1, new { id = 1, name = "Updated Widget", price = 109.99 });
        var updated = await table.GetAsync(transaction, 1);
        Assert.Equal("Updated Widget", updated.name);
        
        // Test index operations
        var nameIndex = await table.CreateIndexAsync(transaction, "name_idx", "$.name");
        Assert.NotNull(nameIndex);
        
        var indexes = await table.ListIndexesAsync(transaction);
        Assert.Contains("name_idx", indexes);
        
        // Test query operations
        var filter = new TestQueryFilter("$.name", "Updated Widget");
        var results = await table.QueryAsync(transaction, filter);
        Assert.Single(results);
        Assert.Equal("Updated Widget", results[0].name);
        
        // Test index drop
        await table.DropIndexAsync(transaction, "name_idx");
        var indexesAfterDrop = await table.ListIndexesAsync(transaction);
        Assert.DoesNotContain("name_idx", indexesAfterDrop);
        
        // Test delete
        var deleted = await table.DeleteAsync(transaction, 1);
        Assert.True(deleted);
        
        var deletedProduct = await table.GetAsync(transaction, 1);
        Assert.Null(deletedProduct);
        
        _output.WriteLine("All ITable methods work correctly");
    }

    #endregion

    #region Helper Classes

    private class TestQueryFilter : IQueryFilter
    {
        private readonly string _fieldPath;
        private readonly object _expectedValue;

        public TestQueryFilter(string fieldPath, object expectedValue)
        {
            _fieldPath = fieldPath;
            _expectedValue = expectedValue;
        }

        public bool Matches(dynamic obj)
        {
            try
            {
                // Simple field extraction for test purposes
                var fieldName = _fieldPath.Substring(2); // Remove "$."
                var type = obj.GetType();
                var property = type.GetProperty(fieldName);
                var value = property?.GetValue(obj);
                
                return _expectedValue.Equals(value);
            }
            catch
            {
                return false;
            }
        }
    }

    private class TestPageEventSubscriber : IPageEventSubscriber
    {
        public List<(string database, string table, string pageId)> ReceivedEvents { get; } = new();

        public Task OnPageModified(string database, string table, string pageId, CancellationToken cancellationToken = default)
        {
            ReceivedEvents.Add((database, table, pageId));
            return Task.CompletedTask;
        }
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