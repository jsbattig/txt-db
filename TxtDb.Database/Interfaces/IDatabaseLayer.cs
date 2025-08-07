using TxtDb.Database.Models;

namespace TxtDb.Database.Interfaces;

/// <summary>
/// Main entry point for database layer operations.
/// Provides database lifecycle management and transaction coordination.
/// </summary>
public interface IDatabaseLayer
{
    /// <summary>
    /// Creates a new database with the specified name.
    /// </summary>
    /// <param name="name">Database name (must follow namespace conventions)</param>
    /// <returns>Database instance</returns>
    /// <exception cref="DatabaseAlreadyExistsException">If database exists</exception>
    /// <exception cref="InvalidDatabaseNameException">If name violates conventions</exception>
    Task<IDatabase> CreateDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets an existing database by name.
    /// </summary>
    /// <param name="name">Database name</param>
    /// <returns>Database instance or null if not found</returns>
    Task<IDatabase?> GetDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a database and all its tables.
    /// </summary>
    /// <param name="name">Database name</param>
    /// <returns>True if deleted, false if not found</returns>
    /// <exception cref="DatabaseInUseException">If active transactions exist</exception>
    Task<bool> DeleteDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all databases in the system.
    /// </summary>
    /// <returns>Array of database names</returns>
    Task<string[]> ListDatabasesAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Begins a database transaction that wraps a storage transaction.
    /// Maintains 1:1 mapping with underlying storage transactions.
    /// </summary>
    /// <param name="database">Target database</param>
    /// <param name="isolationLevel">Transaction isolation level</param>
    /// <returns>Database transaction handle</returns>
    Task<IDatabaseTransaction> BeginTransactionAsync(
        string database, 
        TransactionIsolationLevel isolationLevel = TransactionIsolationLevel.Snapshot,
        CancellationToken cancellationToken = default);
        
    /// <summary>
    /// Subscribes to page modification events for cache invalidation.
    /// </summary>
    /// <param name="database">Database name to subscribe to</param>
    /// <param name="table">Table name to subscribe to</param>
    /// <param name="subscriber">Event subscriber implementation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task SubscribeToPageEvents(string database, string table, IPageEventSubscriber subscriber, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Unsubscribes from page modification events.
    /// </summary>
    /// <param name="database">Database name to unsubscribe from</param>
    /// <param name="table">Table name to unsubscribe from</param>
    /// <param name="subscriber">Event subscriber to remove</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task UnsubscribeFromPageEvents(string database, string table, IPageEventSubscriber subscriber, CancellationToken cancellationToken = default);
}