using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.Core;

/// <summary>
/// CRITICAL: TESTS FOR CONFIGURABLE DEADLOCK PREVENTION - NO MOCKING
/// Tests ensure that deadlock prevention can be configured for different use cases
/// Version cleanup tests should work with timeout-based locking
/// High-contention scenarios should still use Wait-Die prevention
/// </summary>
public class DeadlockPreventionConfigTests : IDisposable
{
    private readonly string _testRootPath;

    public DeadlockPreventionConfigTests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_deadlock_config_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
    }

    [Fact]
    public void StorageConfig_EnableWaitDieDeadlockPrevention_ShouldDefaultToFalse()
    {
        // Act
        var config = new StorageConfig();

        // Assert
        Assert.False(config.EnableWaitDieDeadlockPrevention);
        Assert.Equal(30000, config.DeadlockTimeoutMs); // Should still have default timeout
    }

    [Fact]
    public void StorageConfig_WithWaitDieDisabled_ShouldUseTimeoutBasedLocking()
    {
        // Arrange
        var config = new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true,
            EnableWaitDieDeadlockPrevention = false, // Use timeout-based locking
            DeadlockTimeoutMs = 5000
        };
        var storage = new StorageSubsystem();
        storage.Initialize(_testRootPath, config);

        // Create namespace and initial data
        var setupTxn = storage.BeginTransaction();
        var @namespace = "timeout.test";
        storage.CreateNamespace(setupTxn, @namespace);
        var pageId = storage.InsertObject(setupTxn, @namespace, new { Version = 1, Data = "Initial" });
        storage.CommitTransaction(setupTxn);

        // Act - Start long-running transaction
        var longTxn = storage.BeginTransaction();
        var data1 = storage.ReadPage(longTxn, @namespace, pageId);
        Assert.NotEmpty(data1);

        // Start second transaction that also needs same page
        // With Wait-Die disabled, this should use timeout-based locking instead of immediate abort
        var secondTxn = storage.BeginTransaction();
        
        // This should NOT throw DeadlockPreventionException
        // It should either succeed immediately or timeout after 5 seconds
        var exception = Record.Exception(() =>
        {
            var data2 = storage.ReadPage(secondTxn, @namespace, pageId);
        });

        // Assert - Should not get DeadlockPreventionException
        Assert.True(exception == null || exception is TimeoutException,
            $"Expected timeout or success, got: {exception?.GetType().Name}: {exception?.Message}");

        // Cleanup
        storage.CommitTransaction(longTxn);
        storage.CommitTransaction(secondTxn);
    }

    [Fact]
    public void StorageConfig_WithWaitDieEnabled_ShouldUseWaitDieAlgorithm()
    {
        // Arrange
        var config = new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            ForceOneObjectPerPage = true,
            EnableWaitDieDeadlockPrevention = true, // Use Wait-Die algorithm
            DeadlockTimeoutMs = 30000
        };
        var storage = new StorageSubsystem();
        storage.Initialize(_testRootPath, config);

        // Create namespace and initial data
        var setupTxn = storage.BeginTransaction();
        var @namespace = "waitdie.test";
        storage.CreateNamespace(setupTxn, @namespace);
        var pageId = storage.InsertObject(setupTxn, @namespace, new { Version = 1, Data = "Initial" });
        storage.CommitTransaction(setupTxn);

        // Act - Start transaction 3 (will be older/lower ID)
        var olderTxn = storage.BeginTransaction();
        var data1 = storage.ReadPage(olderTxn, @namespace, pageId);
        Assert.NotEmpty(data1);

        // Start transaction 4 (will be younger/higher ID)
        var youngerTxn = storage.BeginTransaction();
        
        // This SHOULD throw DeadlockPreventionException due to Wait-Die algorithm
        var exception = Assert.Throws<EnhancedDeadlockAwareLockManager.DeadlockPreventionException>(() =>
        {
            storage.ReadPage(youngerTxn, @namespace, pageId);
        });

        // Assert - Should get Wait-Die prevention message
        Assert.Contains("Wait-Die deadlock prevention", exception.Message);
        Assert.Contains("younger", exception.Message);
        Assert.Contains("aborted", exception.Message);

        // Cleanup
        storage.CommitTransaction(olderTxn);
        // Note: youngerTxn was aborted, no commit needed
    }

    public void Dispose()
    {
        try
        {
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