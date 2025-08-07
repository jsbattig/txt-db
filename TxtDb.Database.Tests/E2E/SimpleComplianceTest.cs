using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Services;
using TxtDb.Storage.Services;
using TxtDb.Storage.Services.Async;

namespace TxtDb.Database.Tests.E2E;

/// <summary>
/// Simple test to validate Epic 004 specification compliance
/// </summary>
public class SimpleComplianceTest : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public SimpleComplianceTest(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "simple_compliance_test", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
    }

    [Fact]
    public async Task SpecificationCompliance_SynchronousConstructor_ShouldWork()
    {
        // Test synchronous constructor (no async factory)
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        
        // Create database layer with synchronous constructor
        var databaseLayer = new DatabaseLayer(storageSubsystem);
        Assert.NotNull(databaseLayer);
        
        // Create a database
        var database = await databaseLayer.CreateDatabaseAsync("test_db");
        Assert.NotNull(database);
        Assert.Equal("test_db", database.Name);
        
        // Create a table
        var table = await database.CreateTableAsync("test_table", "$.id");
        Assert.NotNull(table);
        Assert.Equal("test_table", table.Name);
        Assert.Equal("$.id", table.PrimaryKeyField);
        
        _output.WriteLine("Synchronous constructor pattern works correctly");
        _output.WriteLine($"Created database: {database.Name}");
        _output.WriteLine($"Created table: {table.Name}");
    }

    [Fact]
    public async Task SpecificationCompliance_AllRequiredInterfacesExist_ShouldWork()
    {
        // Test that all required interfaces exist and are accessible
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        var databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await databaseLayer.CreateDatabaseAsync("interface_test");
        var table = await database.CreateTableAsync("test_table", "$.id");
        
        await using var transaction = await databaseLayer.BeginTransactionAsync("interface_test");
        
        // Test IIndex interface exists (create index should return IIndex)
        var index = await table.CreateIndexAsync(transaction, "name_index", "$.name");
        Assert.NotNull(index);
        Assert.IsAssignableFrom<IIndex>(index);
        Assert.Equal("name_index", index.Name);
        Assert.Equal("$.name", index.FieldPath);
        
        // Test IQueryFilter interface exists
        var filter = new TestQueryFilter("$.name", "test");
        Assert.IsAssignableFrom<IQueryFilter>(filter);
        
        // Test IPageEventSubscriber interface exists  
        var subscriber = new TestPageEventSubscriber();
        Assert.IsAssignableFrom<IPageEventSubscriber>(subscriber);
        
        // Test subscription methods exist on database layer
        await databaseLayer.SubscribeToPageEvents("interface_test", "test_table", subscriber);
        await databaseLayer.UnsubscribeFromPageEvents("interface_test", "test_table", subscriber);
        
        _output.WriteLine("All required interfaces are properly implemented");
    }

    [Fact]
    public async Task SpecificationCompliance_BasicTableOperations_ShouldWork()
    {
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        var databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var database = await databaseLayer.CreateDatabaseAsync("operations_test");
        var table = await database.CreateTableAsync("products", "$.id");
        
        await using var transaction = await databaseLayer.BeginTransactionAsync("operations_test");
        
        // Test insert
        var insertedId = await table.InsertAsync(transaction, new { id = 1, name = "Widget", price = 99.99 });
        Assert.Equal(1, insertedId);
        
        // Test get
        var retrieved = await table.GetAsync(transaction, 1);
        Assert.NotNull(retrieved);
        Assert.Equal(1, retrieved.id);
        Assert.Equal("Widget", retrieved.name);
        
        // Test update
        await table.UpdateAsync(transaction, 1, new { id = 1, name = "Updated Widget", price = 109.99 });
        var updated = await table.GetAsync(transaction, 1);
        Assert.Equal("Updated Widget", updated.name);
        
        // Test index operations
        var nameIndex = await table.CreateIndexAsync(transaction, "name_idx", "$.name");
        var indexes = await table.ListIndexesAsync(transaction);
        Assert.Contains("name_idx", indexes);
        
        // Test query
        var filter = new TestQueryFilter("$.name", "Updated Widget");
        var results = await table.QueryAsync(transaction, filter);
        Assert.Single(results);
        
        // Test delete
        var deleted = await table.DeleteAsync(transaction, 1);
        Assert.True(deleted);
        var deletedObj = await table.GetAsync(transaction, 1);
        Assert.Null(deletedObj);
        
        _output.WriteLine("All basic table operations work correctly");
    }

    // Helper classes for testing
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
        public Task OnPageModified(string database, string table, string pageId, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
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