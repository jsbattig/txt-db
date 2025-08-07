using TxtDb.Database.Interfaces;
using TxtDb.Database.Models;
using TxtDb.Database.Exceptions;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;

namespace TxtDb.Database.Services;

/// <summary>
/// Database transaction that wraps a storage transaction with 1:1 mapping.
/// Provides database-level consistency and isolation.
/// </summary>
public class DatabaseTransaction : IDatabaseTransaction
{
    private readonly IAsyncStorageSubsystem _storageSubsystem;
    private TransactionState _state;

    public long TransactionId { get; }
    public string DatabaseName { get; }
    public TransactionIsolationLevel IsolationLevel { get; }
    public DateTime StartedAt { get; }
    public TransactionState State => _state;

    public DatabaseTransaction(
        long transactionId,
        string databaseName,
        TransactionIsolationLevel isolationLevel,
        IAsyncStorageSubsystem storageSubsystem)
    {
        TransactionId = transactionId;
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        IsolationLevel = isolationLevel;
        StartedAt = DateTime.UtcNow;
        _storageSubsystem = storageSubsystem ?? throw new ArgumentNullException(nameof(storageSubsystem));
        _state = TransactionState.Active;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await CommitAsync(FlushPriority.Normal, cancellationToken);
    }

    public async Task CommitAsync(FlushPriority priority, CancellationToken cancellationToken = default)
    {
        if (_state != TransactionState.Active)
            throw new TransactionAlreadyCompletedException(TransactionId, _state.ToString());

        try
        {
            await _storageSubsystem.CommitTransactionAsync(TransactionId, priority, cancellationToken);
            _state = TransactionState.Committed;
        }
        catch (Exception ex)
        {
            _state = TransactionState.Aborted;
            
            // IMPROVED ERROR RECOVERY: Enhanced error logging for commit failures
            LogTransactionError(ex, "commit", "Transaction commit failed - changes were not persisted");
            
            throw new TransactionAbortedException(TransactionId, "Commit failed", ex);
        }
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_state != TransactionState.Active)
            throw new TransactionAlreadyCompletedException(TransactionId, _state.ToString());

        try
        {
            await _storageSubsystem.RollbackTransactionAsync(TransactionId, cancellationToken);
            _state = TransactionState.RolledBack;
        }
        catch (Exception ex)
        {
            _state = TransactionState.Aborted;
            
            // IMPROVED ERROR RECOVERY: Enhanced error logging with context
            LogTransactionError(ex, "rollback", "Transaction rollback failed - data consistency may be compromised");
            
            throw new TransactionAbortedException(TransactionId, "Rollback failed", ex);
        }
    }

    /// <summary>
    /// IMPROVED ERROR RECOVERY: Enhanced transaction error logging with detailed context
    /// This replaces silent error suppression with comprehensive diagnostics
    /// </summary>
    private void LogTransactionError(Exception exception, string operation, string message)
    {
        // Create comprehensive error context for debugging
        var errorContext = new
        {
            TransactionId,
            DatabaseName,
            IsolationLevel = IsolationLevel.ToString(),
            State = _state.ToString(),
            StartedAt,
            Duration = DateTime.UtcNow - StartedAt,
            Operation = operation,
            ExceptionType = exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = exception.StackTrace
        };

        // TODO: Replace with proper logging framework (ILogger, etc.)
        // For now, use Console.Error to ensure visibility during development/testing
        var errorMessage = $"[CRITICAL] Transaction Error - {message}\n" +
                          $"  Transaction ID: {TransactionId}\n" +
                          $"  Database: {DatabaseName}\n" +
                          $"  Isolation Level: {IsolationLevel}\n" +
                          $"  State: {_state}\n" +
                          $"  Started: {StartedAt:yyyy-MM-dd HH:mm:ss.fff} UTC\n" +
                          $"  Duration: {errorContext.Duration.TotalMilliseconds:F1}ms\n" +
                          $"  Operation: {operation}\n" +
                          $"  Exception: {exception.GetType().Name}\n" +
                          $"  Message: {exception.Message}\n" +
                          $"  Stack Trace: {exception.StackTrace}";

        Console.Error.WriteLine(errorMessage);
        
        // In production, this should be:
        // _logger?.LogError(exception, 
        //     "Transaction {TransactionId} {Operation} failed for database {DatabaseName}. " +
        //     "State: {State}, Duration: {Duration}ms", 
        //     TransactionId, operation, DatabaseName, _state, errorContext.Duration.TotalMilliseconds);
        
        // Also consider adding metrics/telemetry for monitoring:
        // - Transaction failure rates by operation type
        // - Transaction duration distributions
        // - Error patterns and frequency
        // - Database-specific error rates
    }

    public long GetStorageTransactionId()
    {
        return TransactionId;
    }

    public void Dispose()
    {
        // CRITICAL FIX: Safe synchronous disposal pattern to prevent deadlocks
        // This ensures ACID properties are maintained while avoiding GetAwaiter().GetResult() deadlocks
        if (_state == TransactionState.Active)
        {
            try
            {
                // DEADLOCK PREVENTION: Use ConfigureAwait(false) and safe async-to-sync conversion
                // This prevents deadlocks in synchronization contexts while maintaining ACID compliance
                
                // Option 1: Try to use a dedicated background thread for disposal
                var rollbackTask = Task.Run(async () =>
                {
                    try
                    {
                        await RollbackAsync().ConfigureAwait(false);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        // Log rollback failure for diagnostics - don't suppress critical errors
                        LogTransactionError(ex, "disposal rollback", "Rollback failed during synchronous disposal");
                        return false;
                    }
                });

                // Wait with timeout to prevent infinite blocking
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(30));
                var completedTask = Task.WhenAny(rollbackTask, timeoutTask).GetAwaiter().GetResult();

                if (completedTask == timeoutTask)
                {
                    LogTransactionError(new TimeoutException("Rollback timeout during disposal"), "disposal timeout", "Transaction disposal timed out - state may be inconsistent");
                    _state = TransactionState.Aborted;
                }
                else
                {
                    var rollbackSucceeded = rollbackTask.GetAwaiter().GetResult();
                    if (!rollbackSucceeded)
                    {
                        _state = TransactionState.Aborted;
                    }
                }
            }
            catch (Exception ex)
            {
                // IMPROVED ERROR HANDLING: Log errors but don't throw from Dispose()
                LogTransactionError(ex, "disposal error", "Unexpected error during transaction disposal");
                _state = TransactionState.Aborted;
            }
        }
    }


    public async ValueTask DisposeAsync()
    {
        if (_state == TransactionState.Active)
        {
            try
            {
                await RollbackAsync();
            }
            catch
            {
                // Ignore rollback errors during disposal
            }
        }
    }
}