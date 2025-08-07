using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;

namespace TxtDb.Database.Tests.Critical;

/// <summary>
/// VERIFICATION TEST: Confirms that the critical type handling bugs have been fixed.
/// This test verifies that mixed primary key types can now be inserted without ArgumentExceptions.
/// </summary>
public class TypeSafeFixVerificationTest : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public TypeSafeFixVerificationTest(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "fix_verification", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
    }

    /// <summary>
    /// VERIFICATION TEST: Confirms that the ArgumentException bug has been fixed.
    /// Mixed primary key types should now work without throwing exceptions.
    /// </summary>
    [Fact]
    public async Task MixedPrimaryKeyTypes_ShouldWork_NoArgumentException()
    {
        // Arrange
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("fix_verification");
        var table = await database.CreateTableAsync("mixed_keys", "$.id");

        _output.WriteLine("Testing the fix: Mixed primary key types should work without ArgumentException");
        
        // Act & Assert: These operations should all succeed without exceptions
        var txn1 = await databaseLayer.BeginTransactionAsync("fix_verification");
        var stringResult = await table.InsertAsync(txn1, new { id = "STRING-001", data = "String key data" });
        var intResult = await table.InsertAsync(txn1, new { id = 123, data = "Integer key data" });
        var doubleResult = await table.InsertAsync(txn1, new { id = 456.78, data = "Double key data" });
        var guidResult = await table.InsertAsync(txn1, new { id = Guid.NewGuid(), data = "GUID key data" });
        await txn1.CommitAsync();
        
        // All insertions should succeed
        Assert.Equal("STRING-001", stringResult);
        Assert.Equal(123, intResult);
        Assert.Equal(456.78, doubleResult);
        Assert.IsType<Guid>(guidResult);
        
        _output.WriteLine("SUCCESS: All mixed primary key types inserted without ArgumentException");
        _output.WriteLine($"Inserted: {stringResult}, {intResult}, {doubleResult}, {guidResult}");
        _output.WriteLine("CONCLUSION: The critical type handling bugs have been fixed!");
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