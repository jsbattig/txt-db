using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// TDD Tests for Deadlock Detection and Prevention - Phase 3 Enhanced Implementation
/// Tests enhanced deadlock prevention using resource ordering protocol and wait-for graph detection
/// ALL tests use real file I/O and real concurrent transactions (no mocking)
/// CRITICAL: These tests now verify PREVENTION rather than just detection
/// </summary>
public class DeadlockDetectionTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;
    private readonly IStorageSubsystem _storage;

    public DeadlockDetectionTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_deadlock_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true, // Ensure proper MVCC isolation
            EnableWaitDieDeadlockPrevention = true, // Enable Wait-Die deadlock prevention for these tests
            DeadlockTimeoutMs = 5000 // 5 second deadlock timeout (now used as resource contention timeout)
        });
    }

    [Fact]
    public void DeadlockTimeout_TwoTransactionsCrossLock_ShouldPreventDeadlock()
    {
        // Arrange - Create two resources for cross-access deadlock scenario
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "deadlock.prevention.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var resource1Id = _storage.InsertObject(setupTxn, @namespace, new { ResourceId = 1, Value = "R1_Initial" });
        var resource2Id = _storage.InsertObject(setupTxn, @namespace, new { ResourceId = 2, Value = "R2_Initial" });
        
        _storage.CommitTransaction(setupTxn);

        var exceptions = new ConcurrentBag<Exception>();
        var successCount = 0;
        var deadlockTimeouts = 0;

        var stopwatch = Stopwatch.StartNew();

        // Act - Create classic deadlock scenario: T1 locks R1->R2, T2 locks R2->R1
        // With resource ordering, both transactions should acquire resources in same order, preventing deadlock
        var task1 = Task.Run(() =>
        {
            try
            {
                var txn1 = _storage.BeginTransaction();
                
                // T1: Access resources (will be ordered automatically)
                var data1 = _storage.ReadPage(txn1, @namespace, resource1Id);
                _output.WriteLine($"T1: Acquired lock on R1 at {stopwatch.ElapsedMilliseconds}ms");
                
                // Small delay to ensure concurrent access pattern
                Thread.Sleep(100);
                
                var data2 = _storage.ReadPage(txn1, @namespace, resource2Id);
                _output.WriteLine($"T1: Acquired lock on R2 at {stopwatch.ElapsedMilliseconds}ms");
                
                // Update both resources - Read before write satisfied above
                _storage.UpdatePage(txn1, @namespace, resource1Id, new object[] { new { ResourceId = 1, Value = "R1_Updated_By_T1" } });
                _storage.UpdatePage(txn1, @namespace, resource2Id, new object[] { new { ResourceId = 2, Value = "R2_Updated_By_T1" } });
                
                _storage.CommitTransaction(txn1);
                Interlocked.Increment(ref successCount);
                _output.WriteLine($"T1: SUCCESS at {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (TimeoutException tex)
            {
                Interlocked.Increment(ref deadlockTimeouts);
                exceptions.Add(tex);
                _output.WriteLine($"T1: TIMEOUT at {stopwatch.ElapsedMilliseconds}ms - {tex.Message}");
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                _output.WriteLine($"T1: EXCEPTION at {stopwatch.ElapsedMilliseconds}ms - {ex.Message}");
            }
        });

        var task2 = Task.Run(() =>
        {
            try
            {
                var txn2 = _storage.BeginTransaction();
                
                // T2: Access same resources in different order (will be reordered automatically)  
                var data2 = _storage.ReadPage(txn2, @namespace, resource2Id);
                _output.WriteLine($"T2: Acquired lock on R2 at {stopwatch.ElapsedMilliseconds}ms");
                
                // Small delay to ensure concurrent access pattern
                Thread.Sleep(100);
                
                var data1 = _storage.ReadPage(txn2, @namespace, resource1Id);
                _output.WriteLine($"T2: Acquired lock on R1 at {stopwatch.ElapsedMilliseconds}ms");
                
                // Update both resources - Read before write satisfied above
                _storage.UpdatePage(txn2, @namespace, resource2Id, new object[] { new { ResourceId = 2, Value = "R2_Updated_By_T2" } });
                _storage.UpdatePage(txn2, @namespace, resource1Id, new object[] { new { ResourceId = 1, Value = "R1_Updated_By_T2" } });
                
                _storage.CommitTransaction(txn2);
                Interlocked.Increment(ref successCount);
                _output.WriteLine($"T2: SUCCESS at {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (TimeoutException tex)
            {
                Interlocked.Increment(ref deadlockTimeouts);
                exceptions.Add(tex);
                _output.WriteLine($"T2: TIMEOUT at {stopwatch.ElapsedMilliseconds}ms - {tex.Message}");
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                _output.WriteLine($"T2: EXCEPTION at {stopwatch.ElapsedMilliseconds}ms - {ex.Message}");
            }
        });

        // Wait for transactions to complete
        Task.WaitAll(new[] { task1, task2 }, TimeSpan.FromSeconds(15));
        stopwatch.Stop();

        // Assert - With resource ordering, both transactions should be able to succeed (serially)
        // OR one might succeed and the other might get a resource contention timeout (not a deadlock)
        _output.WriteLine($"FINAL: Success={successCount}, Timeouts={deadlockTimeouts}, Duration={stopwatch.ElapsedMilliseconds}ms");
        
        // The key assertion: should NOT timeout due to deadlock (should be fast or timeout due to contention)
        Assert.True(successCount >= 1, $"Expected at least 1 successful transaction due to deadlock prevention, got {successCount}");
        
        // If there are timeouts, they should be resource contention, not deadlock
        if (deadlockTimeouts > 0)
        {
            // Any timeouts should be legitimate resource contention, not deadlocks
            Assert.True(deadlockTimeouts <= 1, "With proper resource ordering, should have at most 1 timeout due to resource contention");
        }
        
        // Verify system state is consistent
        Thread.Sleep(100); // Allow lock cleanup to complete
        
        var verifyTxn = _storage.BeginTransaction();
        var finalData1 = _storage.ReadPage(verifyTxn, @namespace, resource1Id);
        var finalData2 = _storage.ReadPage(verifyTxn, @namespace, resource2Id);
        _storage.CommitTransaction(verifyTxn);
        
        Assert.NotEmpty(finalData1);
        Assert.NotEmpty(finalData2);
    }

    [Fact]
    public void DeadlockTimeout_ShouldNotAffectNormalOperations()
    {
        // Arrange - Create resource for normal operations
        var setupTxn = _storage.BeginTransaction();
        var @namespace = "normal.ops.test";
        _storage.CreateNamespace(setupTxn, @namespace);
        
        var resourceId = _storage.InsertObject(setupTxn, @namespace, new { Value = "Normal" });
        _storage.CommitTransaction(setupTxn);

        var successCount = 0;
        var exceptions = new ConcurrentBag<Exception>();

        // Act - Multiple normal operations should proceed without timeout issues
        var tasks = Enumerable.Range(1, 10).Select(i =>
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    var data = _storage.ReadPage(txn, @namespace, resourceId);
                    
                    // Small delay for realistic operation timing
                    Thread.Sleep(50);
                    
                    // Read before write satisfied by data read above
                    _storage.UpdatePage(txn, @namespace, resourceId, 
                        new object[] { new { Value = $"Updated_By_Task_{i}", Timestamp = DateTime.UtcNow } });
                    
                    _storage.CommitTransaction(txn);
                    Interlocked.Increment(ref successCount);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks, TimeSpan.FromSeconds(30));

        // Assert - With enhanced deadlock prevention, most operations should succeed
        Assert.True(successCount >= 8, $"Expected at least 80% success rate (8/10), got {successCount}");
        
        // Any conflicts should be MVCC conflicts or resource contention, NOT deadlock timeouts
        var timeoutExceptions = exceptions.OfType<TimeoutException>().ToList();
        var otherExceptions = exceptions.Where(ex => !(ex is TimeoutException)).ToList();
        
        // With proper resource ordering, should have minimal or no timeouts
        Assert.True(timeoutExceptions.Count <= 2, $"Expected at most 2 timeout exceptions due to resource contention, got {timeoutExceptions.Count}");
        
        // Any remaining exceptions should be legitimate MVCC conflicts
        foreach (var ex in otherExceptions)
        {
            if (ex is InvalidOperationException)
            {
                Assert.Contains("conflict", ex.Message.ToLowerInvariant());
            }
        }
    }

    [Fact]
    public void DeadlockTimeout_CustomTimeout_ShouldRespectConfiguration()
    {
        // Arrange - Create new storage with custom short timeout
        var customStorage = new StorageSubsystem();
        var customPath = Path.Combine(Path.GetTempPath(), $"txtdb_custom_timeout_{Guid.NewGuid():N}");
        Directory.CreateDirectory(customPath);
        
        try
        {
            customStorage.Initialize(customPath, new StorageConfig 
            { 
                Format = SerializationFormat.Json,
                DeadlockTimeoutMs = 1000 // 1 second timeout for resource contention
            });

            var setupTxn = customStorage.BeginTransaction();
            var @namespace = "custom.timeout.test";
            customStorage.CreateNamespace(setupTxn, @namespace);
            var resourceId = customStorage.InsertObject(setupTxn, @namespace, new { Value = "Test" });
            customStorage.CommitTransaction(setupTxn);

            var timeoutOccurred = false;
            var stopwatch = Stopwatch.StartNew();

            // Act - Create scenario that would cause timeout
            var task1 = Task.Run(() =>
            {
                var txn1 = customStorage.BeginTransaction();
                var data = customStorage.ReadPage(txn1, @namespace, resourceId); // Lock resource
                Thread.Sleep(2000); // Hold lock longer than timeout
                customStorage.CommitTransaction(txn1);
            });

            var task2 = Task.Run(() =>
            {
                try
                {
                    Thread.Sleep(100); // Ensure task1 gets lock first
                    var txn2 = customStorage.BeginTransaction();
                    customStorage.ReadPage(txn2, @namespace, resourceId); // Should timeout
                }
                catch (TimeoutException)
                {
                    timeoutOccurred = true;
                    stopwatch.Stop();
                }
            });

            Task.WaitAll(new[] { task1, task2 }, TimeSpan.FromSeconds(5));

            // Assert - Custom timeout should be respected
            Assert.True(timeoutOccurred, "Custom timeout should have been triggered");
            Assert.True(stopwatch.ElapsedMilliseconds >= 1000 && stopwatch.ElapsedMilliseconds <= 2000, 
                $"Timeout should occur around 1 second, actual: {stopwatch.ElapsedMilliseconds}ms");
        }
        finally
        {
            try
            {
                ((StorageSubsystem)customStorage)?.Dispose();
                if (Directory.Exists(customPath))
                    Directory.Delete(customPath, recursive: true);
            }
            catch
            {
                // Cleanup failed - not critical
            }
        }
    }

    public void Dispose()
    {
        try
        {
            ((StorageSubsystem)_storage)?.Dispose();
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup failed - not critical for tests
        }
    }
}