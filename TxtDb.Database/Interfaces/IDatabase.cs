namespace TxtDb.Database.Interfaces;

/// <summary>
/// Represents a database containing tables and indexes.
/// </summary>
public interface IDatabase
{
    /// <summary>
    /// Database name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// Database creation timestamp.
    /// </summary>
    DateTime CreatedAt { get; }
    
    /// <summary>
    /// Database metadata for extensibility.
    /// </summary>
    IDictionary<string, object> Metadata { get; }
    
    /// <summary>
    /// Creates a new table with primary key specification.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <param name="primaryKeyField">JSON path to primary key field (e.g., "$.id")</param>
    /// <returns>Table instance</returns>
    /// <exception cref="TableAlreadyExistsException">If table exists</exception>
    /// <exception cref="InvalidTableNameException">If name violates conventions</exception>
    /// <exception cref="InvalidPrimaryKeyPathException">If path is malformed</exception>
    Task<ITable> CreateTableAsync(
        string name, 
        string primaryKeyField,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets an existing table by name.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Table instance or null if not found</returns>
    Task<ITable?> GetTableAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a table and all its data and indexes.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>True if deleted, false if not found</returns>
    /// <exception cref="TableInUseException">If active operations exist</exception>
    Task<bool> DeleteTableAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all tables in the database.
    /// </summary>
    /// <returns>Array of table names</returns>
    Task<string[]> ListTablesAsync(CancellationToken cancellationToken = default);
}