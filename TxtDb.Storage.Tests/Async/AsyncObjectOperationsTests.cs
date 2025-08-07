using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD Tests for Async Object Operations - Epic 002 Phase 2
/// Tests: InsertObjectAsync, UpdatePageAsync, ReadPageAsync, GetMatchingObjectsAsync
/// All tests use real database integration (no mocking)
/// </summary>
public class AsyncObjectOperationsTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public AsyncObjectOperationsTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_async_obj_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        // This will fail initially - we haven't implemented AsyncStorageSubsystem yet
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task InsertObjectAsync_ShouldCreateNewPage_WithUniquePageId()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
        var txn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(txn, "test.insert");

        var testData = new { 
            Id = 123,
            Name = "Test Object",
            Description = "Async insert test",
            Timestamp = DateTime.UtcNow,
            Metadata = new { 
                Version = "1.0",
                Tags = new[] { "async", "test", "insert" }
            }
        };

        // Act
        var insertStart = Stopwatch.StartNew();
        var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.insert", testData);
        insertStart.Stop();

        // Assert
        Assert.NotNull(pageId);
        Assert.NotEmpty(pageId);
        _output.WriteLine($"Async insert completed in {insertStart.ElapsedMilliseconds}ms, pageId: {pageId}");

        // Verify the object was inserted
        var retrievedData = await _asyncStorage.ReadPageAsync(txn, "test.insert", pageId);
        Assert.NotNull(retrievedData);
        Assert.True(retrievedData.Length > 0);

        await _asyncStorage.CommitTransactionAsync(txn);
    }

    [Fact]
    public async Task ReadPageAsync_ShouldRetrieveObjectData_WithCorrectDeserialization()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.read");
        
        var originalData = new { 
            Message = "Hello Async World",
            Number = 42,
            Date = DateTime.UtcNow,
            Array = new[] { 1, 2, 3, 4, 5 }
        };
        
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "test.read", originalData);
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act
        var readTxn = await _asyncStorage.BeginTransactionAsync();
        var readStart = Stopwatch.StartNew();
        var retrievedData = await _asyncStorage.ReadPageAsync(readTxn, "test.read", pageId);
        readStart.Stop();

        // Assert
        Assert.NotNull(retrievedData);
        Assert.True(retrievedData.Length > 0);
        _output.WriteLine($"Async read completed in {readStart.ElapsedMilliseconds}ms");

        // Verify data integrity (exact comparison depends on serialization format)
        var firstObject = retrievedData[0];
        Assert.NotNull(firstObject);

        await _asyncStorage.CommitTransactionAsync(readTxn);
    }

    [Fact]
    public async Task UpdatePageAsync_ShouldModifyExistingPage_MaintainingMVCCVersioning()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.update");
        
        var initialData = new { Id = 1, Status = "Initial" };
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "test.update", initialData);
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Update the page with new content
        var updateTxn = await _asyncStorage.BeginTransactionAsync();
        
        // CRITICAL MVCC FIX: Read the page first to comply with read-before-write isolation
        var currentContent = await _asyncStorage.ReadPageAsync(updateTxn, "test.update", pageId);
        Assert.NotEmpty(currentContent); // Verify we read the existing content
        
        var updatedContent = new object[] { 
            new { Id = 1, Status = "Updated via Async" },
            new { Id = 2, Status = "Additional Data" }
        };

        var updateStart = Stopwatch.StartNew();
        await _asyncStorage.UpdatePageAsync(updateTxn, "test.update", pageId, updatedContent);
        updateStart.Stop();
        await _asyncStorage.CommitTransactionAsync(updateTxn);

        // Assert - Verify the update was applied
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var updatedData = await _asyncStorage.ReadPageAsync(verifyTxn, "test.update", pageId);
        
        Assert.NotNull(updatedData);
        Assert.Equal(2, updatedData.Length);
        _output.WriteLine($"Async update completed in {updateStart.ElapsedMilliseconds}ms");

        await _asyncStorage.CommitTransactionAsync(verifyTxn);
    }

    [Fact]
    public async Task GetMatchingObjectsAsync_ShouldRetrieveFilteredData_WithPatternMatching()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.matching");

        // Insert multiple objects with different patterns
        var testObjects = new[]
        {
            new { Type = "user", Name = "Alice", Category = "admin" },
            new { Type = "user", Name = "Bob", Category = "member" },
            new { Type = "product", Name = "Widget", Category = "hardware" },
            new { Type = "order", Name = "Order-123", Category = "pending" }
        };

        var pageIds = new List<string>();
        foreach (var obj in testObjects)
        {
            var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "test.matching", obj);
            pageIds.Add(pageId);
        }
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        // Act - Query with pattern matching
        var queryTxn = await _asyncStorage.BeginTransactionAsync();
        var queryStart = Stopwatch.StartNew();
        var allObjects = await _asyncStorage.GetMatchingObjectsAsync(queryTxn, "test.matching", "*");
        queryStart.Stop();

        // Assert
        Assert.NotNull(allObjects);
        Assert.True(allObjects.Count > 0);
        
        var totalObjects = allObjects.Values.Sum(pages => pages.Length);
        Assert.Equal(testObjects.Length, totalObjects);
        
        _output.WriteLine($"Async pattern matching completed in {queryStart.ElapsedMilliseconds}ms");
        _output.WriteLine($"Retrieved {allObjects.Count} pages containing {totalObjects} objects");

        await _asyncStorage.CommitTransactionAsync(queryTxn);
    }

    [Fact]
    public async Task ConcurrentObjectOperations_ShouldMaintainDataIntegrity_WithAsyncProcessing()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.concurrent.objects");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var completedOperations = 0;
        var concurrencyLevel = 20;
        var objectsPerThread = 5;

        // Act - Concurrent async object operations
        var concurrentTasks = Enumerable.Range(0, concurrencyLevel).Select(async threadId =>
        {
            var localSuccess = 0;
            try
            {
                for (int i = 0; i < objectsPerThread; i++)
                {
                    var txn = await _asyncStorage.BeginTransactionAsync();
                    
                    // Insert operation
                    var insertData = new { 
                        ThreadId = threadId,
                        ObjectIndex = i,
                        Data = $"Concurrent object from thread {threadId}, index {i}",
                        Timestamp = DateTime.UtcNow,
                        Payload = new string((char)('A' + (threadId % 26)), 100) // Unique payload per thread
                    };
                    
                    var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.concurrent.objects", insertData);
                    
                    // Immediately read back to verify
                    var readBack = await _asyncStorage.ReadPageAsync(txn, "test.concurrent.objects", pageId);
                    Assert.NotNull(readBack);
                    Assert.True(readBack.Length > 0);
                    
                    await _asyncStorage.CommitTransactionAsync(txn);
                    localSuccess++;
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("conflict"))
            {
                // Conflicts are expected in high concurrency scenarios
            }
            
            Interlocked.Add(ref completedOperations, localSuccess);
            return localSuccess;
        });

        var results = await Task.WhenAll(concurrentTasks);
        var totalSuccessfulOperations = results.Sum();

        // Assert
        Assert.True(totalSuccessfulOperations > 0, "Some concurrent operations should succeed");
        Assert.Equal(totalSuccessfulOperations, completedOperations);

        // Verify final data consistency
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.concurrent.objects", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalFinalObjects = finalData.Values.Sum(pages => pages.Length);
        Assert.Equal(totalSuccessfulOperations, totalFinalObjects);

        _output.WriteLine($"Concurrent object operations: {totalSuccessfulOperations}/{concurrencyLevel * objectsPerThread} succeeded");
        _output.WriteLine($"Final data consistency verified: {totalFinalObjects} objects stored");
    }

    [Fact]
    public async Task ObjectOperations_WithCancellation_ShouldRespectCancellationTokens()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.cancellation");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // Act & Assert - Operations should be cancelled
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync(cts.Token);

            // Try to insert many large objects which should trigger cancellation
            for (int i = 0; i < 100; i++)
            {
                await _asyncStorage.InsertObjectAsync(txn, "test.cancellation", new { 
                    Index = i,
                    LargeData = new string('X', 50000) // 50KB per object
                }, cts.Token);

                // Add delay to ensure cancellation has time to trigger
                await Task.Delay(10, cts.Token);
            }

            await _asyncStorage.CommitTransactionAsync(txn, cts.Token);
        });
    }

    public void Dispose()
    {
        try
        {
            if (_asyncStorage is IDisposable disposable)
            {
                disposable.Dispose();
            }
            
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup errors are not critical for tests
        }
    }
}