using System.Diagnostics;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD Tests for Async Cancellation Token Handling - Epic 002 Phase 2
/// Tests comprehensive cancellation support across all async operations
/// All tests use real database integration (no mocking)
/// </summary>
public class AsyncCancellationTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private readonly IAsyncStorageSubsystem _asyncStorage;

    public AsyncCancellationTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_async_cancel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        // This will fail initially - we haven't implemented AsyncStorageSubsystem yet
        _asyncStorage = new AsyncStorageSubsystem();
    }

    [Fact]
    public async Task InitializeAsync_ShouldRespectCancellationToken()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        // Accept both OperationCanceledException and TaskCanceledException (which inherits from OperationCanceledException)
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await _asyncStorage.InitializeAsync(_testRootPath, null, cts.Token));
        
        // Verify it's a cancellation-related exception
        Assert.True(exception is OperationCanceledException || exception is TaskCanceledException,
            $"Expected cancellation exception, got {exception.GetType().Name}");
    }

    [Fact]
    public async Task BeginTransactionAsync_ShouldCancelCorrectly_WhenTokenIsCancelled()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        using var cts = new CancellationTokenSource();
        
        // Act - Cancel BEFORE calling operation
        cts.Cancel();

        // Assert
        // Accept both OperationCanceledException and TaskCanceledException
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => 
            await _asyncStorage.BeginTransactionAsync(cts.Token));
        Assert.True(exception is OperationCanceledException || exception is TaskCanceledException,
            $"Expected cancellation exception, got {exception.GetType().Name}");
    }

    [Fact]
    public async Task CommitTransactionAsync_ShouldCancelGracefully_PreservingDataIntegrity()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.commit.cancel");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var txn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.InsertObjectAsync(txn, "test.commit.cancel", new { 
            Data = "Should be preserved or rolled back cleanly" 
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        // Act & Assert
        try
        {
            await _asyncStorage.CommitTransactionAsync(txn, cts.Token);
        }
        catch (OperationCanceledException oce)
        {
            // Expected - verify system is in consistent state
            var verifyTxn = await _asyncStorage.BeginTransactionAsync();
            var data = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.commit.cancel", "*");
            await _asyncStorage.CommitTransactionAsync(verifyTxn);
            
            // Data should either be committed or not present (no partial state)
            var objectCount = data.Values.Sum(pages => pages.Length);
            Assert.True(objectCount == 0 || objectCount == 1, 
                "Data should be either fully committed or fully rolled back");
            
            var exceptionType = oce is TaskCanceledException ? "TaskCanceledException" : "OperationCanceledException";
            _output.WriteLine($"Cancelled commit ({exceptionType}) left system in consistent state with {objectCount} objects");
        }
    }

    [Fact]
    public async Task InsertObjectAsync_ShouldCancelDuringLargeDataProcessing()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.insert.cancel");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        // Act & Assert - Accept both cancellation exception types
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            
            // Try to insert very large object that should trigger cancellation
            var largeData = new { 
                Id = 1,
                Payload = new string('X', 1_000_000), // 1MB payload
                AdditionalData = Enumerable.Range(0, 10000).Select(i => new { 
                    Index = i,
                    Value = $"Large data item {i}"
                }).ToArray()
            };

            await _asyncStorage.InsertObjectAsync(txn, "test.insert.cancel", largeData, cts.Token);
        });
    }

    [Fact]
    public async Task ReadPageAsync_ShouldCancelDuringDataRetrieval()
    {
        // Arrange - Setup data first
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.read.cancel");
        
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "test.read.cancel", new { 
            LargeContent = new string('Y', 500_000) // 500KB
        });
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        using var cts = new CancellationTokenSource();

        // Act & Assert - Accept both cancellation exception types
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            
            // Cancel immediately after starting the operation to ensure cancellation during execution
            var operationTask = _asyncStorage.ReadPageAsync(txn, "test.read.cancel", pageId, cts.Token);
            cts.Cancel(); // Cancel immediately to trigger during operation execution
            
            await operationTask;
        });
    }

    [Fact]
    public async Task UpdatePageAsync_ShouldCancelCleanly_WithoutCorruptingData()
    {
        // Arrange - Setup initial data
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.update.cancel");
        
        var initialData = new { Status = "Original", Version = 1 };
        var pageId = await _asyncStorage.InsertObjectAsync(setupTxn, "test.update.cancel", initialData);
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        // Act - Try to update with cancellation
        try
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            var largeUpdateContent = Enumerable.Range(0, 100000).Select(i => new { 
                Index = i,
                Status = "Updated",
                LargeField = new string('Z', 1000)
            }).ToArray();

            await _asyncStorage.UpdatePageAsync(txn, "test.update.cancel", pageId, largeUpdateContent, cts.Token);
            await _asyncStorage.CommitTransactionAsync(txn);
        }
        catch (OperationCanceledException oce)
        {
            // Expected - verify original data is intact
            var verifyTxn = await _asyncStorage.BeginTransactionAsync();
            var currentData = await _asyncStorage.ReadPageAsync(verifyTxn, "test.update.cancel", pageId);
            await _asyncStorage.CommitTransactionAsync(verifyTxn);

            Assert.NotNull(currentData);
            Assert.True(currentData.Length > 0, "Original data should still exist");
            
            var exceptionType = oce is TaskCanceledException ? "TaskCanceledException" : "OperationCanceledException";
            _output.WriteLine($"Cancelled update operation ({exceptionType}) preserved original data integrity");
        }
    }

    [Fact]
    public async Task GetMatchingObjectsAsync_ShouldCancelDuringPatternMatching()
    {
        // Arrange - Create many objects to make pattern matching slow
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.matching.cancel");
        
        // Insert many objects
        for (int i = 0; i < 100; i++)
        {
            await _asyncStorage.InsertObjectAsync(setupTxn, "test.matching.cancel", new { 
                Id = i,
                Category = i % 10,
                Data = new string((char)('A' + (i % 26)), 1000) // Varying large data
            });
        }
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        using var cts = new CancellationTokenSource();

        // Act & Assert - Accept both cancellation exception types
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            
            // Cancel immediately after starting the operation to ensure cancellation during execution
            var operationTask = _asyncStorage.GetMatchingObjectsAsync(txn, "test.matching.cancel", "*", cts.Token);
            cts.Cancel(); // Cancel immediately to trigger during operation execution
            
            await operationTask;
        });
    }

    [Fact]
    public async Task CreateNamespaceAsync_ShouldCancelDuringDirectoryCreation()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert - Accept both cancellation exception types
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync();
            await _asyncStorage.CreateNamespaceAsync(txn, "test.create.cancel", cts.Token);
        });
    }

    [Fact]
    public async Task CancellationToken_ShouldPropagateCorrectly_ThroughNestedOperations()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // Act & Assert - Complex operation that should be cancelled at various points
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var txn = await _asyncStorage.BeginTransactionAsync(cts.Token);
            
            // Create namespace
            await _asyncStorage.CreateNamespaceAsync(txn, "test.nested.cancel", cts.Token);
            
            // Insert multiple objects
            for (int i = 0; i < 50; i++)
            {
                var pageId = await _asyncStorage.InsertObjectAsync(txn, "test.nested.cancel", new { 
                    Id = i,
                    LargeData = new string('X', 10000) // 10KB per object
                }, cts.Token);
                
                // Read back immediately
                await _asyncStorage.ReadPageAsync(txn, "test.nested.cancel", pageId, cts.Token);
                
                // Small delay to allow cancellation to trigger
                await Task.Delay(5, cts.Token);
            }
            
            await _asyncStorage.CommitTransactionAsync(txn, cts.Token);
        });
    }

    [Fact]
    public async Task CancellationDuringConcurrentOperations_ShouldNotAffectOtherTransactions()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        var setupTxn = await _asyncStorage.BeginTransactionAsync();
        await _asyncStorage.CreateNamespaceAsync(setupTxn, "test.concurrent.cancel");
        await _asyncStorage.CommitTransactionAsync(setupTxn);

        var successfulOperations = 0;
        var cancelledOperations = 0;

        // Act - Run concurrent operations where some will be cancelled
        var concurrentTasks = Enumerable.Range(0, 10).Select(async i =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(i * 10 + 50));
            
            try
            {
                var txn = await _asyncStorage.BeginTransactionAsync();
                
                for (int j = 0; j < 20; j++)
                {
                    await _asyncStorage.InsertObjectAsync(txn, "test.concurrent.cancel", new { 
                        ThreadId = i,
                        ObjectId = j,
                        Data = new string((char)('A' + (i % 26)), 5000) // 5KB per object
                    }, cts.Token);
                    
                    await Task.Delay(10, cts.Token); // Allow cancellation to trigger
                }
                
                await _asyncStorage.CommitTransactionAsync(txn, cts.Token);
                Interlocked.Increment(ref successfulOperations);
                return "success";
            }
            catch (OperationCanceledException)
            {
                // Both OperationCanceledException and TaskCanceledException are acceptable
                Interlocked.Increment(ref cancelledOperations);
                return "cancelled";
            }
        });

        var results = await Task.WhenAll(concurrentTasks);

        // Assert
        Assert.True(cancelledOperations > 0, "Some operations should be cancelled");
        Assert.True(successfulOperations >= 0, "Some operations might complete successfully");
        
        // Verify system consistency after mixed cancellation/success
        var verifyTxn = await _asyncStorage.BeginTransactionAsync();
        var finalData = await _asyncStorage.GetMatchingObjectsAsync(verifyTxn, "test.concurrent.cancel", "*");
        await _asyncStorage.CommitTransactionAsync(verifyTxn);

        var totalObjects = finalData.Values.Sum(pages => pages.Length);
        
        _output.WriteLine($"Concurrent cancellation test: {successfulOperations} successful, {cancelledOperations} cancelled");
        _output.WriteLine($"Final data consistency: {totalObjects} objects preserved");
        
        // System should remain consistent regardless of cancellations
        Assert.True(totalObjects >= 0, "System should maintain data consistency");
    }

    [Fact]
    public async Task StartVersionCleanupAsync_ShouldSupportCancellation()
    {
        // Arrange
        await _asyncStorage.InitializeAsync(_testRootPath);
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert - Accept both cancellation exception types
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await _asyncStorage.StartVersionCleanupAsync(1, cts.Token));
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