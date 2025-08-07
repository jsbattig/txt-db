namespace TxtDb.Database.Models;

/// <summary>
/// Transaction isolation levels supported by the database layer.
/// </summary>
public enum TransactionIsolationLevel
{
    /// <summary>
    /// Snapshot isolation (default) - reads see consistent snapshot.
    /// </summary>
    Snapshot,
    
    /// <summary>
    /// Serializable - highest isolation level.
    /// </summary>
    Serializable
}