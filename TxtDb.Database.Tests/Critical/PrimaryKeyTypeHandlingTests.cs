using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;
using TxtDb.Database.Interfaces;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// RED PHASE: Tests that expose critical type handling bugs in Table operations
/// These tests reproduce the exact "System.ArgumentException: Object must be of type String" errors
/// that occur when primary keys of different types (int, string, guid) are compared.
/// 
/// CRITICAL BUGS TO EXPOSE:
/// 1. SortedDictionary<object, HashSet<string>> fails when mixing key types (lines 25, 43)
/// 2. String comparison using .ToString() fails for incompatible types (lines 141, 189, 226)
/// 3. Type unsafe primary key extraction and comparison throughout Table.cs
/// </summary>
public class PrimaryKeyTypeHandlingTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public PrimaryKeyTypeHandlingTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_type_handling", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    /// <summary>
    /// RED TEST: Exposes SortedDictionary<object, HashSet<string>> type comparison failure
    /// This test WILL FAIL with "System.ArgumentException: Object must be of type String"
    /// when trying to compare integer and string keys in the sorted dictionary
    /// </summary>
    [Fact]
    public async Task MixedPrimaryKeyTypes_SortedDictionary_ShouldThrowArgumentException()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("type_mixing_test");
        var table = await database.CreateTableAsync("mixed_keys", "$.id");

        _output.WriteLine("Phase 1: Insert string primary key");
        
        // Act - Phase 1: Insert object with string primary key
        var txn1 = await databaseLayer.BeginTransactionAsync("type_mixing_test");
        await table.InsertAsync(txn1, new { id = "STRING-001", data = "String key data" });
        await txn1.CommitAsync();
        
        _output.WriteLine("Successfully inserted string primary key: STRING-001");

        // Act - Phase 2: Insert object with integer primary key (this should trigger the bug)
        var txn2 = await databaseLayer.BeginTransactionAsync("type_mixing_test");
        
        _output.WriteLine("Phase 2: Insert integer primary key (expecting ArgumentException)");
        
        // CRITICAL ASSERTION: This WILL FAIL with ArgumentException due to SortedDictionary type comparison
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await table.InsertAsync(txn2, new { id = 123, data = "Integer key data" });
        });
        
        await txn2.RollbackAsync();
        
        _output.WriteLine($"Expected ArgumentException caught: {exception.Message}");
        Assert.Contains("Object must be of type", exception.Message);
    }

    /// <summary>
    /// RED TEST: Exposes type-unsafe primary key comparison failure in UpdateAsync
    /// This test WILL FAIL when trying to update using different primary key types
    /// </summary>
    [Fact]
    public async Task UpdateWithMismatchedPrimaryKeyTypes_ShouldThrowArgumentException()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("update_type_mismatch");
        var table = await database.CreateTableAsync("products", "$.productId");

        _output.WriteLine("Phase 1: Insert product with integer primary key");
        
        // Act - Phase 1: Insert with integer key
        var txn1 = await databaseLayer.BeginTransactionAsync("update_type_mismatch");
        await table.InsertAsync(txn1, new { productId = 12345, name = "Test Product", price = 99.99 });
        await txn1.CommitAsync();
        
        _output.WriteLine("Successfully inserted product with integer key: 12345");

        // Act - Phase 2: Try to update using string representation of the key
        var txn2 = await databaseLayer.BeginTransactionAsync("update_type_mismatch");
        
        _output.WriteLine("Phase 2: Attempt update with string key '12345' (expecting failure)");
        
        // CRITICAL ASSERTION: This WILL FAIL due to type comparison issues in UpdateAsync line 141
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await table.UpdateAsync(txn2, "12345", new { productId = "12345", name = "Updated Product", price = 149.99 });
        });
        
        await txn2.RollbackAsync();
        
        _output.WriteLine($"Expected ArgumentException caught: {exception.Message}");
        Assert.Contains("Object must be of type", exception.Message);
    }

    /// <summary>
    /// RED TEST: Exposes type-unsafe primary key comparison failure in GetAsync
    /// This test WILL FAIL when trying to retrieve using different primary key types
    /// </summary>
    [Fact]
    public async Task GetWithMismatchedPrimaryKeyTypes_ShouldReturnNullDueToTypeFailure()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("get_type_mismatch");
        var table = await database.CreateTableAsync("users", "$.userId");

        _output.WriteLine("Phase 1: Insert user with GUID primary key");
        
        // Act - Phase 1: Insert with GUID key
        var userId = Guid.NewGuid();
        var txn1 = await databaseLayer.BeginTransactionAsync("get_type_mismatch");
        await table.InsertAsync(txn1, new { userId = userId, username = "testuser", email = "test@example.com" });
        await txn1.CommitAsync();
        
        _output.WriteLine($"Successfully inserted user with GUID key: {userId}");

        // Act - Phase 2: Try to retrieve using string representation of GUID
        var txn2 = await databaseLayer.BeginTransactionAsync("get_type_mismatch");
        
        _output.WriteLine("Phase 2: Attempt get with string GUID (expecting null due to type failure)");
        
        // CRITICAL ASSERTION: This WILL return null due to type comparison failure in GetAsync line 189
        var retrieved = await table.GetAsync(txn2, userId.ToString());
        await txn2.CommitAsync();
        
        // The retrieval fails silently due to type comparison issues
        Assert.Null(retrieved); // This exposes the bug - should find the record but doesn't
        
        _output.WriteLine("Retrieved null due to type handling bug (should have found the record)");
    }

    /// <summary>
    /// RED TEST: Exposes type-unsafe primary key comparison failure in DeleteAsync
    /// This test WILL FAIL when trying to delete using different primary key types
    /// </summary>
    [Fact]
    public async Task DeleteWithMismatchedPrimaryKeyTypes_ShouldReturnFalseDueToTypeFailure()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("delete_type_mismatch");
        var table = await database.CreateTableAsync("orders", "$.orderId");

        _output.WriteLine("Phase 1: Insert order with double primary key");
        
        // Act - Phase 1: Insert with double key
        var txn1 = await databaseLayer.BeginTransactionAsync("delete_type_mismatch");
        await table.InsertAsync(txn1, new { orderId = 123.45, customerId = "CUST-001", total = 299.99 });
        await txn1.CommitAsync();
        
        _output.WriteLine("Successfully inserted order with double key: 123.45");

        // Act - Phase 2: Try to delete using integer representation
        var txn2 = await databaseLayer.BeginTransactionAsync("delete_type_mismatch");
        
        _output.WriteLine("Phase 2: Attempt delete with integer 123 (expecting false due to type failure)");
        
        // CRITICAL ASSERTION: This WILL return false due to type comparison failure in DeleteAsync line 226
        var deleted = await table.DeleteAsync(txn2, 123);
        await txn2.CommitAsync();
        
        // The deletion fails silently due to type comparison issues
        Assert.False(deleted); // This exposes the bug - should delete but doesn't due to type mismatch
        
        _output.WriteLine("Delete returned false due to type handling bug (should have deleted the record)");
        
        // Verify the record still exists using the correct type
        var txn3 = await databaseLayer.BeginTransactionAsync("delete_type_mismatch");
        var stillExists = await table.GetAsync(txn3, 123.45);
        await txn3.CommitAsync();
        
        Assert.NotNull(stillExists); // Record still exists because delete failed due to type mismatch
        _output.WriteLine("Verified: Record still exists because delete failed due to type handling bug");
    }

    /// <summary>
    /// RED TEST: Exposes SortedDictionary key insertion order issues with mixed types
    /// This test WILL FAIL when the SortedDictionary tries to sort incompatible key types
    /// </summary>
    [Fact]
    public async Task SortedDictionary_MixedTypes_ShouldFailOnKeyComparison()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("sorted_dict_failure");
        var table = await database.CreateTableAsync("mixed_sorting", "$.itemId");

        _output.WriteLine("Phase 1: Insert items with different key types in sequence");
        
        // This sequence will expose the SortedDictionary comparison failure
        var keysToInsert = new object[] { "ABC", 123, "DEF", 456, "GHI" };
        
        var txn = await databaseLayer.BeginTransactionAsync("sorted_dict_failure");
        
        for (int i = 0; i < keysToInsert.Length; i++)
        {
            var key = keysToInsert[i];
            _output.WriteLine($"Inserting item with key: {key} (Type: {key.GetType().Name})");
            
            if (i >= 1) // The second insertion should fail
            {
                // CRITICAL ASSERTION: This WILL FAIL with ArgumentException due to SortedDictionary type mixing
                var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                {
                    await table.InsertAsync(txn, new { itemId = key, description = $"Item {i}" });
                });
                
                _output.WriteLine($"Expected ArgumentException at key {key}: {exception.Message}");
                Assert.Contains("Object must be of type", exception.Message);
                break; // Stop after first failure
            }
            else
            {
                await table.InsertAsync(txn, new { itemId = key, description = $"Item {i}" });
                _output.WriteLine($"Successfully inserted first item with key: {key}");
            }
        }
        
        await txn.RollbackAsync();
    }

    /// <summary>
    /// RED TEST: Exposes that identical values of different types are treated as different keys
    /// This test shows the fundamental type handling inconsistency in the system
    /// </summary>
    [Fact]
    public async Task IdenticalValuesDifferentTypes_ShouldExposeInconsistentHandling()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("identical_values_test");
        
        // Use separate tables to avoid SortedDictionary type mixing error
        var stringTable = await database.CreateTableAsync("string_keys", "$.key");
        var intTable = await database.CreateTableAsync("int_keys", "$.key");

        _output.WriteLine("Phase 1: Insert identical values as different types in separate tables");
        
        // Act - Phase 1: Insert "123" as string in first table
        var txn1 = await databaseLayer.BeginTransactionAsync("identical_values_test");
        await stringTable.InsertAsync(txn1, new { key = "123", data = "String version", type = "string" });
        await txn1.CommitAsync();
        
        // Act - Phase 2: Insert 123 as integer in second table
        var txn2 = await databaseLayer.BeginTransactionAsync("identical_values_test");
        await intTable.InsertAsync(txn2, new { key = 123, data = "Integer version", type = "integer" });
        await txn2.CommitAsync();
        
        _output.WriteLine("Successfully inserted identical values as different types");

        // Act - Phase 3: Try cross-type retrieval (this exposes the type handling inconsistency)
        var txn3 = await databaseLayer.BeginTransactionAsync("identical_values_test");
        
        // Try to get string "123" using integer 123 - should work with proper type handling but won't
        var stringRecordWithIntKey = await stringTable.GetAsync(txn3, 123);
        var intRecordWithStringKey = await intTable.GetAsync(txn3, "123");
        
        await txn3.CommitAsync();
        
        // CRITICAL ASSERTION: These should logically find the records but won't due to type handling bugs
        Assert.Null(stringRecordWithIntKey); // Exposes bug: string "123" not found with int 123 key
        Assert.Null(intRecordWithStringKey); // Exposes bug: int 123 not found with string "123" key
        
        _output.WriteLine("Cross-type retrieval failed - exposes type handling inconsistency");
        _output.WriteLine("Records with identical semantic values are inaccessible due to type differences");
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