using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Services;
using TxtDb.Database.Exceptions;
using System.Collections.Concurrent;

namespace TxtDb.Database.Tests.ConcurrencyTests;

/// <summary>
/// TDD Phase 1: RED - Tests that expose critical thread safety and constructor issues
/// 
/// These tests are DESIGNED TO FAIL until fixes are implemented:
/// 1. Constructor deadlock risk in DatabaseLayer
/// 2. Thread safety violations in Table index operations  
/// 3. Collection modification during enumeration in Database metadata
/// 
/// CRITICAL: These tests will fail until the corresponding fixes are applied
/// </summary>
public class CriticalIssuesExposureTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;

    public CriticalIssuesExposureTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_critical_issues_tests", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    #region Issue #1: Constructor Deadlock Risk

    /// <summary>
    /// RED TEST: Exposes constructor deadlock risk in DatabaseLayer
    /// This test creates concurrent DatabaseLayer instances which should expose
    /// the synchronous blocking in constructor causing deadlocks in ASP.NET/UI contexts
    /// </summary>
    [Fact]
    public async Task ConstructorDeadlockRisk_ConcurrentDatabaseLayerCreation_ShouldExposeDeadlock()
    {
        // Arrange
        const int concurrentInstances = 10;
        var exceptions = new ConcurrentBag<Exception>();
        var creationTasks = new List<Task>();
        var createdInstances = new ConcurrentBag<IDatabaseLayer>();

        // Act - Create multiple DatabaseLayer instances concurrently
        // This should expose the deadlock risk from synchronous blocking in constructor
        for (int i = 0; i < concurrentInstances; i++)
        {
            var instanceIndex = i;
            var task = Task.Run(async () =>
            {
                try
                {
                    // Create a unique storage directory for each instance
                    var instanceStorageDir = Path.Combine(_storageDirectory, $"instance_{instanceIndex}");
                    Directory.CreateDirectory(instanceStorageDir);
                    
                    // FIXED: Use async factory pattern to avoid constructor deadlock
                    // This now properly awaits async initialization without blocking
                    var databaseLayer = await DatabaseLayer.CreateAsync(instanceStorageDir);
                    createdInstances.Add(databaseLayer);
                    
                    // Try to use the instance immediately
                    var databases = await databaseLayer.ListDatabasesAsync();
                    
                    _output.WriteLine($"Instance {instanceIndex} created successfully with {databases.Length} databases");
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"Instance {instanceIndex} failed: {ex.Message}");
                    exceptions.Add(ex);
                }
            });
            
            creationTasks.Add(task);
        }

        // Wait for all tasks with timeout
        var completed = await Task.WhenAll(creationTasks.Select(t => 
            Task.Run(async () =>
            {
                try
                {
                    await t.WaitAsync(TimeSpan.FromSeconds(30));
                    return true;
                }
                catch (TimeoutException)
                {
                    return false;
                }
            })));

        // Assert - This test exposes the deadlock/timeout issues
        var timeouts = completed.Count(c => !c);
        var totalExceptions = exceptions.Count;
        
        _output.WriteLine($"Results: {createdInstances.Count} successful, {totalExceptions} exceptions, {timeouts} timeouts");
        
        // FIXED: This test should now pass with async factory pattern
        // No more constructor deadlock with proper async initialization
        Assert.True(timeouts == 0, $"Constructor deadlock detected: {timeouts} operations timed out");
        Assert.True(totalExceptions == 0, $"Constructor exceptions detected: {totalExceptions} failures");
        Assert.Equal(concurrentInstances, createdInstances.Count);

        // Cleanup
        foreach (var instance in createdInstances)
        {
            (instance as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// RED TEST: Simulates ASP.NET context deadlock scenario
    /// This test simulates the specific deadlock scenario that occurs in ASP.NET
    /// when synchronous blocking occurs on async operations in constructors
    /// </summary>
    [Fact]
    public async Task ConstructorDeadlockRisk_ASPNETContextSimulation_ShouldExposeDeadlock()
    {
        // Arrange - Simulate ASP.NET synchronization context
        var originalContext = SynchronizationContext.Current;
        var aspnetContext = new AspNetSynchronizationContextMock();
        SynchronizationContext.SetSynchronizationContext(aspnetContext);

        Exception? caughtException = null;
        
        try
        {
            // Act - This should now work properly with async factory pattern
            var task = Task.Run(async () =>
            {
                // FIXED: Use async factory pattern - no more constructor deadlock
                var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
                return databaseLayer;
            });

            // Wait with timeout to detect deadlock
            var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
            
            if (completed == task)
            {
                var databaseLayer = await task;
                var databases = await databaseLayer.ListDatabasesAsync();
                _output.WriteLine($"Unexpectedly succeeded with {databases.Length} databases");
                (databaseLayer as IDisposable)?.Dispose();
            }
            else
            {
                // Expected: timeout due to deadlock
                throw new TimeoutException("Constructor deadlock detected in ASP.NET context simulation");
            }
        }
        catch (Exception ex)
        {
            caughtException = ex;
            _output.WriteLine($"Expected deadlock detected: {ex.Message}");
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(originalContext);
        }

        // Assert - This test should now pass with the async factory pattern fix
        // FIXED: No more deadlock with async factory pattern
        Assert.Null(caughtException); // This should now pass
    }

    #endregion

    #region Issue #2: Thread Safety Violations in Table

    /// <summary>
    /// RED TEST: Exposes thread safety violations in Table index operations
    /// This test performs concurrent insert/get operations that should expose
    /// race conditions in the Dictionary-based primary key index
    /// </summary>
    [Fact]
    public async Task ThreadSafetyViolation_ConcurrentTableIndexOperations_ShouldExposeRaceConditions()
    {
        // Arrange - Use async factory pattern (CRITICAL FIX)
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("concurrency_test_db");
        var table = await database.CreateTableAsync("test_table", "$.id");
        
        const int concurrentOperations = 100;
        const int operationsPerThread = 50;
        var exceptions = new ConcurrentBag<Exception>();
        var successfulInserts = new ConcurrentBag<string>();
        var successfulGets = new ConcurrentBag<object>();

        // Act - Perform concurrent insert and get operations
        // This should expose race conditions in Dictionary access
        var tasks = new List<Task>();
        
        for (int threadIndex = 0; threadIndex < concurrentOperations; threadIndex++)
        {
            var capturedIndex = threadIndex;
            
            // Insert task
            var insertTask = Task.Run(async () =>
            {
                try
                {
                    for (int i = 0; i < operationsPerThread; i++)
                    {
                        var transaction = await databaseLayer.BeginTransactionAsync(database.Name);
                        
                        dynamic obj = new
                        {
                            id = $"thread_{capturedIndex}_item_{i}",
                            value = $"Thread {capturedIndex} Item {i}",
                            timestamp = DateTime.UtcNow
                        };
                        
                        var insertedKey = await table.InsertAsync(transaction, obj);
                        successfulInserts.Add((string)insertedKey);
                        
                        await transaction.CommitAsync();
                        await transaction.DisposeAsync();
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    _output.WriteLine($"Insert thread {capturedIndex} failed: {ex.Message}");
                }
            });
            
            // Get task (runs concurrently with inserts)
            var getTask = Task.Run(async () =>
            {
                try
                {
                    // Small delay to allow some inserts to happen first
                    await Task.Delay(10);
                    
                    for (int i = 0; i < operationsPerThread / 2; i++)
                    {
                        var transaction = await databaseLayer.BeginTransactionAsync(database.Name);
                        
                        var key = $"thread_{capturedIndex}_item_{i}";
                        var obj = await table.GetAsync(transaction, key);
                        
                        if (obj != null)
                        {
                            successfulGets.Add(obj);
                        }
                        
                        await transaction.CommitAsync();
                        await transaction.DisposeAsync();
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    _output.WriteLine($"Get thread {capturedIndex} failed: {ex.Message}");
                }
            });
            
            tasks.Add(insertTask);
            tasks.Add(getTask);
        }

        // Wait for all operations to complete
        await Task.WhenAll(tasks);

        // Assert - This test exposes thread safety violations
        var totalExceptions = exceptions.Count;
        var totalSuccessfulInserts = successfulInserts.Count;
        var totalSuccessfulGets = successfulGets.Count;
        
        _output.WriteLine($"Results: {totalSuccessfulInserts} inserts, {totalSuccessfulGets} gets, {totalExceptions} exceptions");
        
        // Log any specific race condition exceptions
        foreach (var ex in exceptions)
        {
            if (ex.Message.Contains("Collection was modified") || 
                ex.Message.Contains("Index was outside") ||
                ex is KeyNotFoundException ||
                ex is InvalidOperationException)
            {
                _output.WriteLine($"Race condition detected: {ex.GetType().Name} - {ex.Message}");
            }
        }

        // FIXED: This test should now pass with ConcurrentDictionary
        // No more thread safety violations with proper concurrent collections
        Assert.True(totalExceptions == 0, $"Thread safety violations detected: {totalExceptions} exceptions occurred");
        
        // Clean up
        (databaseLayer as IDisposable)?.Dispose();
    }

    #endregion

    #region Issue #3: Collection Modification During Enumeration

    /// <summary>
    /// RED TEST: Exposes collection modification during enumeration in Database metadata
    /// This test performs concurrent table creation/deletion that should expose
    /// InvalidOperationException from modifying collections during enumeration
    /// </summary>
    [Fact]
    public async Task CollectionModificationError_ConcurrentMetadataOperations_ShouldExposeEnumerationException()
    {
        // Arrange - Use async factory pattern (CRITICAL FIX)
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.CreateDatabaseAsync("metadata_test_db");
        
        const int concurrentOperations = 20;
        var exceptions = new ConcurrentBag<Exception>();
        var createdTables = new ConcurrentBag<string>();
        var deletedTables = new ConcurrentBag<string>();

        // Act - Perform concurrent table operations that modify metadata during enumeration
        var tasks = new List<Task>();
        
        for (int i = 0; i < concurrentOperations; i++)
        {
            var capturedIndex = i;
            
            // Table creation task
            var createTask = Task.Run(async () =>
            {
                try
                {
                    var tableName = $"table_{capturedIndex}";
                    var table = await database.CreateTableAsync(tableName, "$.id");
                    createdTables.Add(tableName);
                    
                    // Immediately try to list tables (triggers enumeration)
                    var tables = await database.ListTablesAsync();
                    _output.WriteLine($"Created {tableName}, total tables: {tables.Length}");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    _output.WriteLine($"Create task {capturedIndex} failed: {ex.Message}");
                }
            });
            
            // Table deletion task (only if there are tables to delete)
            var deleteTask = Task.Run(async () =>
            {
                try
                {
                    // Small delay to ensure some tables exist first
                    await Task.Delay(50);
                    
                    var tablesToDelete = createdTables.Take(5).ToList();
                    if (tablesToDelete.Any())
                    {
                        var tableToDelete = tablesToDelete[capturedIndex % tablesToDelete.Count];
                        var deleted = await database.DeleteTableAsync(tableToDelete);
                        if (deleted)
                        {
                            deletedTables.Add(tableToDelete);
                            
                            // Immediately try to list tables (triggers enumeration)
                            var tables = await database.ListTablesAsync();
                            _output.WriteLine($"Deleted {tableToDelete}, remaining tables: {tables.Length}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    _output.WriteLine($"Delete task {capturedIndex} failed: {ex.Message}");
                }
            });
            
            tasks.Add(createTask);
            if (i < concurrentOperations / 2) // Only create half as many delete tasks
            {
                tasks.Add(deleteTask);
            }
        }

        // Wait for all operations to complete
        await Task.WhenAll(tasks);

        // Assert - This test exposes collection modification exceptions
        var totalExceptions = exceptions.Count;
        var totalCreated = createdTables.Count;
        var totalDeleted = deletedTables.Count;
        
        _output.WriteLine($"Results: {totalCreated} created, {totalDeleted} deleted, {totalExceptions} exceptions");
        
        // Log any collection modification exceptions
        foreach (var ex in exceptions)
        {
            if (ex is InvalidOperationException && ex.Message.Contains("Collection was modified"))
            {
                _output.WriteLine($"Collection modification during enumeration detected: {ex.Message}");
            }
        }

        // FIXED: This test should now pass with proper metadata synchronization
        // No more collection modification during enumeration with defensive copying
        Assert.True(totalExceptions == 0, $"Collection modification exceptions detected: {totalExceptions} failures occurred");
        
        // Clean up
        (databaseLayer as IDisposable)?.Dispose();
    }

    #endregion

    #region Helper Classes

    /// <summary>
    /// Mock ASP.NET synchronization context that can cause deadlocks
    /// when synchronous blocking occurs on async operations
    /// </summary>
    private class AspNetSynchronizationContextMock : SynchronizationContext
    {
        private readonly Queue<(SendOrPostCallback callback, object? state)> _queue = new();
        private readonly object _lock = new();
        private bool _isExecuting = false;

        public override void Post(SendOrPostCallback d, object? state)
        {
            lock (_lock)
            {
                _queue.Enqueue((d, state));
                
                if (!_isExecuting)
                {
                    _isExecuting = true;
                    Task.Run(ProcessQueue);
                }
            }
        }

        public override void Send(SendOrPostCallback d, object? state)
        {
            // In real ASP.NET, this can cause deadlocks when called from
            // within an async operation that's being waited on synchronously
            if (_isExecuting)
            {
                // Simulate deadlock - don't execute the callback
                Thread.Sleep(10000); // This will cause timeout
                return;
            }
            
            d(state);
        }

        private void ProcessQueue()
        {
            while (true)
            {
                (SendOrPostCallback callback, object? state) item;
                
                lock (_lock)
                {
                    if (_queue.Count == 0)
                    {
                        _isExecuting = false;
                        break;
                    }
                    
                    item = _queue.Dequeue();
                }
                
                try
                {
                    item.callback(item.state);
                }
                catch
                {
                    // Ignore callback exceptions
                }
            }
        }
    }

    #endregion

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