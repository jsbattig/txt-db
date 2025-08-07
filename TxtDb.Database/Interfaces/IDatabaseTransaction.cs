using TxtDb.Database.Models;
using TxtDb.Storage.Models;

namespace TxtDb.Database.Interfaces;

/// <summary>
/// Database transaction that wraps a storage transaction with 1:1 mapping.
/// Provides database-level consistency and isolation.
/// Supports both synchronous and asynchronous disposal for compatibility.
/// </summary>
public interface IDatabaseTransaction : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Unique transaction identifier.
    /// </summary>
    long TransactionId { get; }
    
    /// <summary>
    /// Database this transaction operates on.
    /// </summary>
    string DatabaseName { get; }
    
    /// <summary>
    /// Transaction isolation level.
    /// </summary>
    TransactionIsolationLevel IsolationLevel { get; }
    
    /// <summary>
    /// Transaction start timestamp.
    /// </summary>
    DateTime StartedAt { get; }
    
    /// <summary>
    /// Current transaction state.
    /// </summary>
    TransactionState State { get; }
    
    /// <summary>
    /// Commits all changes made in this transaction.
    /// </summary>
    /// <exception cref="TransactionAlreadyCompletedException">If already committed/rolled back</exception>
    /// <exception cref="TransactionAbortedException">If transaction cannot be committed</exception>
    Task CommitAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commits with specified flush priority for performance tuning.
    /// </summary>
    /// <param name="priority">Flush priority for batch operations</param>
    Task CommitAsync(FlushPriority priority, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Rolls back all changes made in this transaction.
    /// </summary>
    /// <exception cref="TransactionAlreadyCompletedException">If already committed/rolled back</exception>
    Task RollbackAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the underlying storage transaction ID for internal operations.
    /// </summary>
    long GetStorageTransactionId();
}