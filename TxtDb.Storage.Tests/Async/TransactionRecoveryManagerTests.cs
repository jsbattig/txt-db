using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for TransactionRecoveryManager - Phase 4 Infrastructure Hardening Component
/// 
/// CRITICAL REQUIREMENTS:
/// - Recover from partial transaction failures
/// - Handle crashed transactions during critical operations 
/// - Restore system to consistent state after failures
/// - Rollback incomplete transactions
/// - Journal-based recovery with durability guarantees
/// - Concurrent recovery support for multiple failed transactions
/// 
/// This component is essential for:
/// - Data integrity after system crashes or power failures
/// - Handling partial file writes that leave system in inconsistent state
/// - Automatic recovery during system startup
/// - Maintaining ACID properties even during failures
/// </summary>
public class TransactionRecoveryManagerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private TransactionRecoveryManager? _recoveryManager;

    public TransactionRecoveryManagerTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_recovery_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
    }

    [Fact]
    public void TransactionRecoveryManager_CreateWithValidConfig_ShouldSucceed()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "recovery.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000,
            AutoRecoveryOnStartup = true
        };

        // Act & Assert - This will fail until TransactionRecoveryManager is implemented
        _recoveryManager = new TransactionRecoveryManager(config);
        
        Assert.NotNull(_recoveryManager);
        Assert.Equal(config.JournalPath, _recoveryManager.JournalPath);
        Assert.Equal(config.MaxRecoveryAttempts, _recoveryManager.MaxRecoveryAttempts);
    }

    [Fact]
    public async Task BeginTransactionRecovery_WithValidTransactionId_ShouldCreateRecoveryContext()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "recovery.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var transactionId = Guid.NewGuid();
        var operationList = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = Path.Combine(_testRootPath, "test_file.txt"),
                OriginalContent = "original data",
                NewContent = "modified data"
            }
        };

        // Act
        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operationList);

        // Assert
        Assert.NotNull(context);
        Assert.Equal(transactionId, context.TransactionId);
        Assert.Equal(RecoveryState.InProgress, context.State);
        Assert.Single(context.Operations);
    }

    [Fact]
    public async Task RecoverTransaction_FromPartialFileWrite_ShouldRestoreOriginalContent()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "recovery.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var testFilePath = Path.Combine(_testRootPath, "partial_write_test.txt");
        var originalContent = "Original file content";
        var corruptedContent = "Partial corru"; // Simulates partial write

        // Create original file, then simulate partial write corruption
        await File.WriteAllTextAsync(testFilePath, originalContent);
        await File.WriteAllTextAsync(testFilePath, corruptedContent);

        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = testFilePath,
                OriginalContent = originalContent,
                NewContent = "Complete new content" // What it should have been
            }
        };

        // Act
        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);
        var result = await _recoveryManager.RecoverTransaction(context);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(RecoveryState.Completed, result.State);
        
        var recoveredContent = await File.ReadAllTextAsync(testFilePath);
        Assert.Equal(originalContent, recoveredContent); // Should be rolled back to original
    }

    [Fact]
    public async Task RecoverTransaction_FromMultipleFailedOperations_ShouldRecoverAll()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "multi_recovery.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var file1Path = Path.Combine(_testRootPath, "multi_file1.txt");
        var file2Path = Path.Combine(_testRootPath, "multi_file2.txt");
        var file1Original = "File 1 original";
        var file2Original = "File 2 original";

        // Create original files
        await File.WriteAllTextAsync(file1Path, file1Original);
        await File.WriteAllTextAsync(file2Path, file2Original);

        // Simulate partial transaction failure
        await File.WriteAllTextAsync(file1Path, "File 1 corrupted");
        await File.WriteAllTextAsync(file2Path, "File 2 partial");

        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = file1Path,
                OriginalContent = file1Original,
                NewContent = "File 1 complete update"
            },
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = file2Path,
                OriginalContent = file2Original,
                NewContent = "File 2 complete update"
            }
        };

        // Act
        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);
        var result = await _recoveryManager.RecoverTransaction(context);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(RecoveryState.Completed, result.State);
        
        var file1Content = await File.ReadAllTextAsync(file1Path);
        var file2Content = await File.ReadAllTextAsync(file2Path);
        
        Assert.Equal(file1Original, file1Content);
        Assert.Equal(file2Original, file2Content);
    }

    [Fact]
    public async Task RecoverTransaction_WithJournaling_ShouldPersistRecoveryState()
    {
        // Arrange
        var journalPath = Path.Combine(_testRootPath, "persistent.journal");
        var config = new TransactionRecoveryConfig
        {
            JournalPath = journalPath,
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var testFilePath = Path.Combine(_testRootPath, "journal_test.txt");
        var originalContent = "Original content for journaling";

        await File.WriteAllTextAsync(testFilePath, originalContent);

        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = testFilePath,
                OriginalContent = originalContent,
                NewContent = "New content"
            }
        };

        // Act
        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);

        // Assert - Journal file should be created and contain recovery information
        Assert.True(File.Exists(journalPath));
        
        var journalContent = await File.ReadAllTextAsync(journalPath);
        Assert.Contains(transactionId.ToString(), journalContent);
        Assert.Contains("FileWrite", journalContent);
        Assert.Contains(testFilePath, journalContent);
    }

    [Fact]
    public async Task RecoverFromJournal_OnStartup_ShouldRecoverIncompleteTransactions()
    {
        // Arrange - First create a recovery manager and simulate an incomplete transaction
        var journalPath = Path.Combine(_testRootPath, "startup_recovery.journal");
        var config1 = new TransactionRecoveryConfig
        {
            JournalPath = journalPath,
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000,
            AutoRecoveryOnStartup = false // Manual control for test
        };

        var manager1 = new TransactionRecoveryManager(config1);
        var testFilePath = Path.Combine(_testRootPath, "startup_test.txt");
        var originalContent = "Original before crash";

        await File.WriteAllTextAsync(testFilePath, originalContent);

        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.FileWrite,
                FilePath = testFilePath,
                OriginalContent = originalContent,
                NewContent = "New content after crash"
            }
        };

        // Begin recovery but don't complete it (simulate crash)
        var context = await manager1.BeginTransactionRecovery(transactionId, operations);
        
        // Corrupt the file to simulate partial failure
        await File.WriteAllTextAsync(testFilePath, "Corrupted partial");
        
        manager1.Dispose(); // Simulate process crash

        // Act - Create new recovery manager with auto recovery
        var config2 = new TransactionRecoveryConfig
        {
            JournalPath = journalPath,
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000,
            AutoRecoveryOnStartup = true
        };

        _recoveryManager = new TransactionRecoveryManager(config2);
        var recoveredTransactions = await _recoveryManager.RecoverFromJournal();

        // Assert
        Assert.Single(recoveredTransactions);
        Assert.True(recoveredTransactions[0].Success);
        
        var recoveredContent = await File.ReadAllTextAsync(testFilePath);
        Assert.Equal(originalContent, recoveredContent); // Should be restored to original
    }

    [Fact]
    public async Task RecoverTransaction_WithTimeout_ShouldFailGracefully()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "timeout.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 100 // Very short timeout
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.SlowOperation, // Custom slow operation for testing
                FilePath = Path.Combine(_testRootPath, "timeout_test.txt"),
                OriginalContent = "original",
                NewContent = "new"
            }
        };

        // Act
        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);
        var result = await _recoveryManager.RecoverTransaction(context);

        // Assert
        Assert.False(result.Success);
        Assert.Equal(RecoveryState.Failed, result.State);
        Assert.Contains("timed out", result.ErrorMessage.ToLower());
    }

    [Fact]
    public async Task GetRecoveryMetrics_AfterRecoveryOperations_ShouldProvideStatistics()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "metrics.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        // Execute several recovery operations
        var testFilePath = Path.Combine(_testRootPath, "metrics_test.txt");
        await File.WriteAllTextAsync(testFilePath, "original");

        for (int i = 0; i < 3; i++)
        {
            var transactionId = Guid.NewGuid();
            var operations = new List<RecoveryOperation>
            {
                new RecoveryOperation
                {
                    OperationType = RecoveryOperationType.FileWrite,
                    FilePath = testFilePath,
                    OriginalContent = "original",
                    NewContent = $"attempt_{i}"
                }
            };

            var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);
            await _recoveryManager.RecoverTransaction(context);
        }

        // Act
        var metrics = _recoveryManager.GetRecoveryMetrics();

        // Assert
        Assert.NotNull(metrics);
        Assert.Equal(3, metrics.TotalRecoveryAttempts);
        Assert.Equal(3, metrics.SuccessfulRecoveries);
        Assert.Equal(0, metrics.FailedRecoveries);
        Assert.True(metrics.AverageRecoveryTimeMs >= 0);
    }

    [Fact]
    public async Task RecoverTransaction_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        var config = new TransactionRecoveryConfig
        {
            JournalPath = Path.Combine(_testRootPath, "cancellation.journal"),
            MaxRecoveryAttempts = 3,
            RecoveryTimeoutMs = 30000
        };
        _recoveryManager = new TransactionRecoveryManager(config);

        var cancellationTokenSource = new CancellationTokenSource();
        var transactionId = Guid.NewGuid();
        var operations = new List<RecoveryOperation>
        {
            new RecoveryOperation
            {
                OperationType = RecoveryOperationType.SlowOperation,
                FilePath = Path.Combine(_testRootPath, "cancel_test.txt"),
                OriginalContent = "original",
                NewContent = "new"
            }
        };

        var context = await _recoveryManager.BeginTransactionRecovery(transactionId, operations);

        // Cancel after 50ms
        _ = Task.Delay(50).ContinueWith(_ => cancellationTokenSource.Cancel());

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            _recoveryManager.RecoverTransaction(context, cancellationTokenSource.Token));
    }

    public void Dispose()
    {
        _recoveryManager?.Dispose();
        
        try
        {
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, true);
            }
        }
        catch
        {
            // Best effort cleanup
        }
    }
}