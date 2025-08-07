namespace TxtDb.Database.Interfaces;

/// <summary>
/// Represents a table with primary key management and indexing.
/// Objects are stored in pages with multiple objects per page.
/// </summary>
public interface ITable
{
    /// <summary>
    /// Table name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// Primary key field JSON path (e.g., "$.id").
    /// </summary>
    string PrimaryKeyField { get; }
    
    /// <summary>
    /// Table creation timestamp.
    /// </summary>
    DateTime CreatedAt { get; }
    
    /// <summary>
    /// Inserts a new object into the table.
    /// The object will be added to an existing page or a new page will be created.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="obj">Object to insert (must contain primary key field)</param>
    /// <returns>Primary key value of inserted object</returns>
    /// <exception cref="MissingPrimaryKeyException">If object lacks primary key</exception>
    /// <exception cref="DuplicatePrimaryKeyException">If primary key already exists</exception>
    /// <exception cref="InvalidObjectException">If object cannot be serialized</exception>
    Task<object> InsertAsync(
        IDatabaseTransaction txn, 
        dynamic obj,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates an existing object by primary key (full replacement).
    /// This operation requires reading the containing page, finding the object,
    /// replacing it, and rewriting the entire page.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <param name="obj">New object (must contain same primary key)</param>
    /// <exception cref="ObjectNotFoundException">If primary key not found</exception>
    /// <exception cref="PrimaryKeyMismatchException">If object has different primary key</exception>
    Task UpdateAsync(
        IDatabaseTransaction txn, 
        object primaryKey, 
        dynamic obj,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Retrieves an object by primary key.
    /// Uses the primary key index to locate the page, then searches within the page.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <returns>Expando object or null if not found</returns>
    Task<dynamic?> GetAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes an object by primary key.
    /// This operation requires reading the containing page, removing the object,
    /// and rewriting the page (or deleting it if empty).
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <returns>True if deleted, false if not found</returns>
    Task<bool> DeleteAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default);
        
    /// <summary>
    /// Creates a secondary index on the specified field.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="indexName">Index name</param>
    /// <param name="fieldPath">JSON path to indexed field</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Index instance</returns>
    Task<IIndex> CreateIndexAsync(IDatabaseTransaction txn, string indexName, string fieldPath, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Drops a secondary index.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="indexName">Index name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if dropped, false if not found</returns>
    Task DropIndexAsync(IDatabaseTransaction txn, string indexName, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all indexes on the table.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Array of index names</returns>
    Task<IList<string>> ListIndexesAsync(IDatabaseTransaction txn, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Queries objects using filter.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="filter">Query filter specification</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Matching objects</returns>
    Task<IList<dynamic>> QueryAsync(IDatabaseTransaction txn, IQueryFilter filter, CancellationToken cancellationToken = default);
}