using TxtDb.Storage.Services;
using TxtDb.Storage.Models;
using TxtDb.Storage.Interfaces;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// End-to-end test to verify that the JSON deserialization fix resolves the 
/// "insert succeeds, get returns null" issue caused by JValue deserialization problems.
/// </summary>
public class JsonDeserializationE2ETest
{
    [Fact]
    public void StorageSubsystem_InsertAndGet_ShouldWorkWithProperSerialization()
    {
        // Arrange
        using var tempDir = new TemporaryDirectory();
        var config = new StorageConfig
        {
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        };
        
        var storage = new StorageSubsystem();
        storage.Initialize(tempDir.Path, config);
        
        const string namespaceName = "test_namespace";
        
        // Create test objects with various data types that were problematic
        var testObjects = new[]
        {
            new { Id = 42, Name = "Test Object 1", IsActive = true, Version = 1.0 },
            new { Id = 123, Name = "Test Object 2", IsActive = false, Version = 2.5 },
            new { Id = 456, Name = "Test Object 3", IsActive = true, Version = 3.14159 }
        };

        // Setup namespace
        var setupTxn = storage.BeginTransaction();
        storage.CreateNamespace(setupTxn, namespaceName);
        storage.CommitTransaction(setupTxn);

        // Act 1: Insert objects
        var insertedKeys = new List<string>();
        var insertTxn = storage.BeginTransaction();
        foreach (var obj in testObjects)
        {
            var key = storage.InsertObject(insertTxn, namespaceName, obj);
            insertedKeys.Add(key);
            Assert.NotNull(key);
            Assert.NotEmpty(key);
        }
        storage.CommitTransaction(insertTxn);

        // Act 2: Retrieve objects
        var retrievedObjects = new List<object>();
        var readTxn = storage.BeginTransaction();
        foreach (var key in insertedKeys)
        {
            var pageData = storage.ReadPage(readTxn, namespaceName, key);
            Assert.NotNull(pageData);
            Assert.Single(pageData); // One object per page
            retrievedObjects.Add(pageData[0]);
        }
        storage.CommitTransaction(readTxn);

        // Assert: All objects should be retrieved successfully
        Assert.Equal(testObjects.Length, retrievedObjects.Count);
        
        for (int i = 0; i < testObjects.Length; i++)
        {
            var original = testObjects[i];
            var retrieved = retrievedObjects[i];
            
            Assert.NotNull(retrieved);
            
            // Convert to dynamic for property access
            dynamic originalDynamic = original;
            dynamic retrievedDynamic = retrieved;
            
            // Verify all properties are correctly deserialized
            Assert.Equal(originalDynamic.Id, retrievedDynamic.Id);
            Assert.Equal(originalDynamic.Name, retrievedDynamic.Name);
            Assert.Equal(originalDynamic.IsActive, retrievedDynamic.IsActive);
            Assert.Equal(originalDynamic.Version, retrievedDynamic.Version);
            
            // Critical assertion: Make sure Id is not a JValue but a proper int
            var idValue = retrievedDynamic.Id;
            Assert.True(idValue is int || idValue is long, 
                $"Id should be int or long, but was {idValue?.GetType().Name}");
            Assert.NotEqual("Newtonsoft.Json.Linq.JValue", idValue?.GetType().FullName);
        }
    }
    
    [Fact]
    public void StorageSubsystem_GetAll_ShouldReturnProperlyDeserializedObjects()
    {
        // Arrange
        using var tempDir = new TemporaryDirectory();
        var config = new StorageConfig
        {
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        };
        
        var storage = new StorageSubsystem();
        storage.Initialize(tempDir.Path, config);
        const string namespaceName = "getall_test";
        
        var testObjects = new[]
        {
            new { ProductId = 1, ProductName = "Widget A", Price = 19.99, InStock = true },
            new { ProductId = 2, ProductName = "Widget B", Price = 25.50, InStock = false },
            new { ProductId = 3, ProductName = "Widget C", Price = 30.00, InStock = true }
        };

        // Setup namespace
        var setupTxn = storage.BeginTransaction();
        storage.CreateNamespace(setupTxn, namespaceName);
        storage.CommitTransaction(setupTxn);

        // Act 1: Insert all objects
        var insertTxn = storage.BeginTransaction();
        foreach (var obj in testObjects)
        {
            var key = storage.InsertObject(insertTxn, namespaceName, obj);
            Assert.NotNull(key);
        }
        storage.CommitTransaction(insertTxn);

        // Act 2: Get all objects using pattern matching
        Dictionary<string, object[]> allPages;
        var readTxn = storage.BeginTransaction();
        allPages = storage.GetMatchingObjects(readTxn, namespaceName, "*"); // Get all pages
        storage.CommitTransaction(readTxn);

        // Assert
        Assert.NotNull(allPages);
        Assert.Equal(testObjects.Length, allPages.Count);
        
        // Verify each object is properly deserialized
        foreach (var kvp in allPages)
        {
            var pageData = kvp.Value;
            Assert.Single(pageData); // One object per page
            var retrievedObj = pageData[0];
            
            Assert.NotNull(retrievedObj);
            
            dynamic retrieved = retrievedObj;
            
            // Verify ProductId is a proper integer type, not JValue
            var productIdValue = retrieved.ProductId;
            Assert.True(productIdValue is int || productIdValue is long,
                $"ProductId should be int or long, but was {productIdValue?.GetType().Name}");
            Assert.NotEqual("Newtonsoft.Json.Linq.JValue", productIdValue?.GetType().FullName);
            
            // Verify Price is a proper number type, not JValue
            var priceValue = retrieved.Price;
            Assert.True(priceValue is double || priceValue is decimal || priceValue is float,
                $"Price should be numeric, but was {priceValue?.GetType().Name}");
            Assert.NotEqual("Newtonsoft.Json.Linq.JValue", priceValue?.GetType().FullName);
            
            // Verify InStock is proper boolean, not JValue
            var inStockValue = retrieved.InStock;
            Assert.IsType<bool>(inStockValue);
            Assert.NotEqual("Newtonsoft.Json.Linq.JValue", inStockValue?.GetType().FullName);
            
            // Verify ProductName is proper string, not JValue
            var nameValue = retrieved.ProductName;
            Assert.IsType<string>(nameValue);
            Assert.NotEqual("Newtonsoft.Json.Linq.JValue", nameValue?.GetType().FullName);
        }
    }
    
    [Fact] 
    public void StorageSubsystem_ComplexNestedObjects_ShouldDeserializeCorrectly()
    {
        // Arrange
        using var tempDir = new TemporaryDirectory();
        var config = new StorageConfig
        {
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true
        };
        
        var storage = new StorageSubsystem();
        storage.Initialize(tempDir.Path, config);
        const string namespaceName = "nested_test";
        
        var complexObject = new
        {
            OrderId = 12345,
            Customer = new
            {
                CustomerId = 67890,
                Name = "John Doe",
                IsVip = true
            },
            Items = new[]
            {
                new { ItemId = 1, Quantity = 2, Price = 10.50 },
                new { ItemId = 2, Quantity = 1, Price = 25.00 }
            },
            Metadata = new
            {
                CreatedAt = DateTime.UtcNow,
                ProcessedBy = "System",
                Version = 1
            }
        };

        // Setup namespace
        var setupTxn = storage.BeginTransaction();
        storage.CreateNamespace(setupTxn, namespaceName);
        storage.CommitTransaction(setupTxn);

        // Act
        string insertedKey;
        var insertTxn = storage.BeginTransaction();
        insertedKey = storage.InsertObject(insertTxn, namespaceName, complexObject);
        storage.CommitTransaction(insertTxn);
        
        object retrievedObject;
        var readTxn = storage.BeginTransaction();
        var pageData = storage.ReadPage(readTxn, namespaceName, insertedKey);
        retrievedObject = pageData[0];
        storage.CommitTransaction(readTxn);

        // Assert
        Assert.NotNull(retrievedObject);
        
        dynamic retrieved = retrievedObject;
        
        // Test top-level properties
        Assert.True(retrieved.OrderId is int || retrieved.OrderId is long);
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", retrieved.OrderId?.GetType().FullName);
        
        // Test nested Customer object
        Assert.NotNull(retrieved.Customer);
        dynamic customer = retrieved.Customer;
        Assert.True(customer.CustomerId is int || customer.CustomerId is long);
        Assert.IsType<string>(customer.Name);
        Assert.IsType<bool>(customer.IsVip);
        
        // Test array of items
        Assert.NotNull(retrieved.Items);
        var items = (object[])retrieved.Items;
        Assert.Equal(2, items.Length);
        
        foreach (var item in items)
        {
            dynamic itemDynamic = item;
            Assert.True(itemDynamic.ItemId is int || itemDynamic.ItemId is long);
            Assert.True(itemDynamic.Quantity is int || itemDynamic.Quantity is long);
            Assert.True(itemDynamic.Price is double || itemDynamic.Price is decimal || itemDynamic.Price is float);
        }
        
        // Test nested Metadata object  
        Assert.NotNull(retrieved.Metadata);
        dynamic metadata = retrieved.Metadata;
        Assert.IsType<string>(metadata.ProcessedBy);
        Assert.True(metadata.Version is int || metadata.Version is long);
    }
}

/// <summary>
/// Helper class for creating temporary directories for testing
/// </summary>
public class TemporaryDirectory : IDisposable
{
    public string Path { get; }
    
    public TemporaryDirectory()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.GetRandomFileName());
        Directory.CreateDirectory(Path);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(Path))
        {
            Directory.Delete(Path, true);
        }
    }
}