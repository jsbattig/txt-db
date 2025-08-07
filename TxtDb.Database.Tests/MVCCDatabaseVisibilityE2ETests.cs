using TxtDb.Database.Interfaces;
using TxtDb.Database.Services;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;

namespace TxtDb.Database.Tests;

/// <summary>
/// PHASE 3: END-TO-END DATABASE LAYER MVCC VISIBILITY TESTS
/// 
/// These tests verify that the Database layer works correctly with proper MVCC
/// Storage layer calls instead of bypassing MVCC isolation.
/// 
/// CRITICAL: These are true end-to-end tests with no mocking whatsoever.
/// They use real storage, real file I/O, and real Database instances
/// to prove that fresh Database instances can see committed data from
/// other Database instances through proper MVCC visibility.
/// </summary>
public class MVCCDatabaseVisibilityE2ETests : IDisposable
{
    private readonly string _testRootPath;
    private readonly string _sharedStoragePath;

    public MVCCDatabaseVisibilityE2ETests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_db_visibility_{Guid.NewGuid():N}");
        _sharedStoragePath = Path.Combine(_testRootPath, "shared_db_storage");
        Directory.CreateDirectory(_sharedStoragePath);
    }

    [Fact]
    public async Task DatabaseLayer_FreshInstance_ShouldSeeDatabaseCreatedByOtherInstance()
    {
        // This test verifies that Database layer properly uses Storage layer MVCC
        // without bypassing version visibility
        
        // Arrange - Instance 1 creates a database
        var storage1 = new AsyncStorageSubsystem();
        await storage1.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json 
        });

        var dbLayer1 = new DatabaseLayer(storage1);

        var database1 = await dbLayer1.CreateDatabaseAsync("TestDatabase");
        Console.WriteLine($"[TEST] Instance 1 created database: {database1.Name}");
        
        // Dispose Instance 1 to simulate different process
        dbLayer1.Dispose();
        (storage1 as IDisposable)?.Dispose();

        // Act - Fresh Instance 2 should see the database created by Instance 1
        var storage2 = new AsyncStorageSubsystem();
        await storage2.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json 
        });

        var dbLayer2 = new DatabaseLayer(storage2);

        var foundDatabase = await dbLayer2.GetDatabaseAsync("TestDatabase");
        
        // Assert - Fresh instance should see database created by other instance
        Assert.NotNull(foundDatabase);
        Assert.Equal("TestDatabase", foundDatabase.Name);
        Console.WriteLine($"[TEST] Instance 2 successfully found database: {foundDatabase.Name}");
        
        // Cleanup
        dbLayer2.Dispose();
        (storage2 as IDisposable)?.Dispose();
    }

    [Fact]
    public async Task Database_FreshInstance_ShouldSeeTableCreatedByOtherInstance()
    {
        // This test verifies the full Database -> Table visibility chain
        
        // Arrange - Instance 1 creates database and table
        var storage1 = new AsyncStorageSubsystem();
        await storage1.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json 
        });

        var dbLayer1 = new DatabaseLayer(storage1);

        var database1 = await dbLayer1.CreateDatabaseAsync("TestDatabaseWithTable");
        var table1 = await database1.CreateTableAsync("Users", "$.Id");
        Console.WriteLine($"[TEST] Instance 1 created table: {table1.Name}");
        
        // Dispose Instance 1
        dbLayer1.Dispose();
        (storage1 as IDisposable)?.Dispose();

        // Act - Fresh Instance 2 should see the table created by Instance 1
        var storage2 = new AsyncStorageSubsystem();
        await storage2.InitializeAsync(_sharedStoragePath, new StorageConfig { 
            Format = SerializationFormat.Json 
        });

        var dbLayer2 = new DatabaseLayer(storage2);

        var foundDatabase2 = await dbLayer2.GetDatabaseAsync("TestDatabaseWithTable");
        Assert.NotNull(foundDatabase2);

        var foundTable2 = await foundDatabase2.GetTableAsync("Users");
        
        // Assert - Fresh instance should see table created by other instance
        Assert.NotNull(foundTable2);
        Assert.Equal("Users", foundTable2.Name);
        Assert.Equal("$.Id", foundTable2.PrimaryKeyField);
        Console.WriteLine($"[TEST] Instance 2 successfully found table: {foundTable2.Name}");
        
        // Cleanup
        dbLayer2.Dispose();
        (storage2 as IDisposable)?.Dispose();
    }

    // NOTE: Data insertion tests are complex and require transaction management
    // They are covered by existing critical tests in the Database.Tests project
    // This simplified test focuses on metadata visibility through MVCC

    // NOTE: Complex data insertion and transaction isolation tests
    // are covered extensively in existing Database.Tests.Critical namespace
    // This test suite focuses specifically on MVCC metadata visibility

    public void Dispose()
    {
        if (Directory.Exists(_testRootPath))
        {
            Directory.Delete(_testRootPath, recursive: true);
        }
    }
}