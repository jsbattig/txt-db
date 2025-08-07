using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Exceptions;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// GREEN PHASE: Tests that verify the type-safe primary key handling system works correctly.
/// These tests confirm that the critical type handling bugs have been fixed:
/// 
/// FIXED BEHAVIORS:
/// 1. Mixed primary key types no longer cause ArgumentException in SortedDictionary operations
/// 2. Type-safe comparison ensures int(123) != string("123") semantically
/// 3. Primary key operations use normalized keys for reliable dictionary operations
/// 4. Cross-type retrieval correctly returns null/false instead of crashing
/// </summary>
public class TypeSafePrimaryKeyTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public TypeSafePrimaryKeyTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_type_safe", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    /// <summary>
    /// GREEN TEST: Verifies that mixed primary key types are now supported without exceptions
    /// This test PASSES because we replaced SortedDictionary with Dictionary
    /// </summary>
    [Fact]
    public async Task MixedPrimaryKeyTypes_Dictionary_ShouldWorkWithoutExceptions()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("mixed_types_test");
        var table = await database.CreateTableAsync("mixed_keys", "$.id");

        _output.WriteLine("Phase 1: Insert string primary key");
        
        // Act - Phase 1: Insert object with string primary key
        var txn1 = await databaseLayer.BeginTransactionAsync("mixed_types_test");
        var stringResult = await table.InsertAsync(txn1, new { id = "STRING-001", data = "String key data" });
        await txn1.CommitAsync();
        
        _output.WriteLine($"Successfully inserted string primary key: {stringResult}");

        // Act - Phase 2: Insert object with integer primary key (this now works!)
        var txn2 = await databaseLayer.BeginTransactionAsync("mixed_types_test");
        var intResult = await table.InsertAsync(txn2, new { id = 123, data = "Integer key data" });
        await txn2.CommitAsync();
        
        _output.WriteLine($"Successfully inserted integer primary key: {intResult}");

        // Assert: Both insertions should succeed
        Assert.Equal("STRING-001", stringResult);
        Assert.Equal(123, intResult);
        
        // Verify both records can be retrieved
        var txn3 = await databaseLayer.BeginTransactionAsync("mixed_types_test");
        var stringRecord = await table.GetAsync(txn3, "STRING-001");
        var intRecord = await table.GetAsync(txn3, 123);
        await txn3.CommitAsync();
        
        Assert.NotNull(stringRecord);
        Assert.NotNull(intRecord);
        Assert.Equal("String key data", stringRecord.data);
        Assert.Equal("Integer key data", intRecord.data);
        
        _output.WriteLine("SUCCESS: Mixed primary key types now work without ArgumentException");
    }

    /// <summary>
    /// GREEN TEST: Verifies that type-safe comparison correctly treats different types as different keys
    /// This test PASSES because our normalized key system ensures type safety
    /// </summary>
    [Fact]
    public async Task TypeSafeComparison_DifferentTypesWithSameValue_ShouldBeTreatedAsDifferentKeys()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("type_safe_test");
        var table = await database.CreateTableAsync("products", "$.productId");

        _output.WriteLine("Phase 1: Insert product with integer primary key 123");
        
        // Act - Phase 1: Insert with integer key
        var txn1 = await databaseLayer.BeginTransactionAsync("type_safe_test");
        await table.InsertAsync(txn1, new { productId = 123, name = "Integer Product" });
        await txn1.CommitAsync();

        // Act - Phase 2: Insert with string key "123" - should be allowed as it's a different key
        var txn2 = await databaseLayer.BeginTransactionAsync("type_safe_test");
        await table.InsertAsync(txn2, new { productId = "123", name = "String Product" });
        await txn2.CommitAsync();
        
        _output.WriteLine("Successfully inserted both int(123) and string('123') as different keys");

        // Act - Phase 3: Verify cross-type retrieval returns null (correct type-safe behavior)
        var txn3 = await databaseLayer.BeginTransactionAsync("type_safe_test");
        var intRecordWithStringKey = await table.GetAsync(txn3, "123");   // Should find string record
        var stringRecordWithIntKey = await table.GetAsync(txn3, 123);     // Should find int record
        await txn3.CommitAsync();
        
        // Assert: Type-safe retrieval should work correctly
        Assert.NotNull(intRecordWithStringKey);
        Assert.NotNull(stringRecordWithIntKey);
        Assert.Equal("String Product", intRecordWithStringKey.name);    // Retrieved with string key
        Assert.Equal("Integer Product", stringRecordWithIntKey.name);   // Retrieved with int key
        
        _output.WriteLine("SUCCESS: Type-safe key handling works correctly");
        _output.WriteLine($"String key '123' retrieved: {intRecordWithStringKey.name}");
        _output.WriteLine($"Integer key 123 retrieved: {stringRecordWithIntKey.name}");
    }

    /// <summary>
    /// GREEN TEST: Verifies that UpdateAsync with correct type works, but mismatched types properly fail
    /// This test PASSES because our type-safe comparison now works correctly
    /// </summary>
    [Fact]
    public async Task TypeSafeUpdate_CorrectTypeWorks_MismatchedTypeFails()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("update_type_safe_test");
        var table = await database.CreateTableAsync("users", "$.userId");

        var userId = Guid.NewGuid();
        _output.WriteLine($"Phase 1: Insert user with GUID primary key: {userId}");
        
        // Act - Phase 1: Insert with GUID key
        var txn1 = await databaseLayer.BeginTransactionAsync("update_type_safe_test");
        await table.InsertAsync(txn1, new { userId = userId, username = "testuser", email = "test@example.com" });
        await txn1.CommitAsync();
        
        // Act - Phase 2: Update with correct type (GUID) - should work
        var txn2 = await databaseLayer.BeginTransactionAsync("update_type_safe_test");
        await table.UpdateAsync(txn2, userId, new { userId = userId, username = "updateduser", email = "updated@example.com" });
        await txn2.CommitAsync();
        
        _output.WriteLine("Successfully updated record using correct GUID type");

        // Act - Phase 3: Try to update using string representation of GUID - should fail with ObjectNotFoundException
        var txn3 = await databaseLayer.BeginTransactionAsync("update_type_safe_test");
        
        var exception = await Assert.ThrowsAsync<ObjectNotFoundException>(async () =>
        {
            await table.UpdateAsync(txn3, userId.ToString(), new { userId = userId.ToString(), username = "shouldfail", email = "fail@example.com" });
        });
        
        await txn3.RollbackAsync();
        
        _output.WriteLine($"Correctly failed with ObjectNotFoundException: {exception.Message}");
        
        // Verify the record still has the correct data
        var txn4 = await databaseLayer.BeginTransactionAsync("update_type_safe_test");
        var finalRecord = await table.GetAsync(txn4, userId);
        await txn4.CommitAsync();
        
        Assert.NotNull(finalRecord);
        Assert.Equal("updateduser", finalRecord.username);  // Should have the correct update
        Assert.Equal("updated@example.com", finalRecord.email);
        
        _output.WriteLine("SUCCESS: Type-safe update handling works correctly");
    }

    /// <summary>
    /// GREEN TEST: Verifies that DeleteAsync with correct type works, but mismatched types return false
    /// This test PASSES because our type-safe comparison now works correctly
    /// </summary>
    [Fact]
    public async Task TypeSafeDelete_CorrectTypeWorks_MismatchedTypeReturnsFalse()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("delete_type_safe_test");
        var table = await database.CreateTableAsync("orders", "$.orderId");

        _output.WriteLine("Phase 1: Insert order with double primary key");
        
        // Act - Phase 1: Insert with double key
        var txn1 = await databaseLayer.BeginTransactionAsync("delete_type_safe_test");
        await table.InsertAsync(txn1, new { orderId = 123.45, customerId = "CUST-001", total = 299.99 });
        await txn1.CommitAsync();
        
        _output.WriteLine("Successfully inserted order with double key: 123.45");

        // Act - Phase 2: Try to delete using integer representation - should return false (correct behavior)
        var txn2 = await databaseLayer.BeginTransactionAsync("delete_type_safe_test");
        var deleteResult = await table.DeleteAsync(txn2, 123);  // int instead of double
        await txn2.CommitAsync();
        
        Assert.False(deleteResult);  // Should return false - correct type-safe behavior
        _output.WriteLine("Correctly returned false when deleting with wrong type (int instead of double)");
        
        // Act - Phase 3: Delete using correct type - should work
        var txn3 = await databaseLayer.BeginTransactionAsync("delete_type_safe_test");
        var correctDeleteResult = await table.DeleteAsync(txn3, 123.45);  // correct double type
        await txn3.CommitAsync();
        
        Assert.True(correctDeleteResult);  // Should succeed with correct type
        _output.WriteLine("Successfully deleted record using correct type (double 123.45)");
        
        // Verify the record is actually deleted
        var txn4 = await databaseLayer.BeginTransactionAsync("delete_type_safe_test");
        var deletedRecord = await table.GetAsync(txn4, 123.45);
        await txn4.CommitAsync();
        
        Assert.Null(deletedRecord);  // Should be null after deletion
        _output.WriteLine("SUCCESS: Type-safe delete handling works correctly");
    }

    /// <summary>
    /// GREEN TEST: Verifies that the new dictionary-based system handles many mixed types without issues
    /// This test PASSES because we eliminated the SortedDictionary type comparison problem
    /// </summary>
    [Fact]
    public async Task HighVolumeTypeMixing_ShouldWorkWithoutExceptions()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("high_volume_mixed_test");
        var table = await database.CreateTableAsync("mixed_items", "$.itemId");

        _output.WriteLine("Phase 1: Insert many items with different key types");
        
        var keysToInsert = new object[] { 
            "STRING-001", 123, "STRING-002", 456, 789.123, 
            Guid.NewGuid(), "STRING-003", 999, DateTime.Now.Ticks, "STRING-004"
        };
        
        // Act: Insert all different types - should all work without exceptions
        for (int i = 0; i < keysToInsert.Length; i++)
        {
            var key = keysToInsert[i];
            var txn = await databaseLayer.BeginTransactionAsync("high_volume_mixed_test");
            
            var result = await table.InsertAsync(txn, new { itemId = key, description = $"Item {i}", index = i });
            await txn.CommitAsync();
            
            _output.WriteLine($"Successfully inserted key: {key} (Type: {key.GetType().Name})");
            Assert.Equal(key, result);
        }
        
        _output.WriteLine($"SUCCESS: Inserted {keysToInsert.Length} items with mixed types without any exceptions");

        // Phase 2: Retrieve all items to verify they're all accessible
        var retrievedCount = 0;
        foreach (var key in keysToInsert)
        {
            var txn = await databaseLayer.BeginTransactionAsync("high_volume_mixed_test");
            var retrieved = await table.GetAsync(txn, key);
            await txn.CommitAsync();
            
            Assert.NotNull(retrieved);
            retrievedCount++;
        }
        
        Assert.Equal(keysToInsert.Length, retrievedCount);
        _output.WriteLine($"SUCCESS: Retrieved all {retrievedCount} items with mixed types");
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