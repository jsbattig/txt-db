using TxtDb.Storage.Models;

namespace TxtDb.Database.Models;

/// <summary>
/// Database metadata stored in _system.databases namespace.
/// </summary>
public class DatabaseMetadata
{
    /// <summary>
    /// Database name (unique identifier).
    /// </summary>
    public string Name { get; set; } = "";
    
    /// <summary>
    /// Database creation timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }
    
    /// <summary>
    /// Last modification timestamp.
    /// </summary>
    public DateTime ModifiedAt { get; set; }
    
    /// <summary>
    /// Database version for schema evolution.
    /// </summary>
    public int Version { get; set; } = 1;
    
    /// <summary>
    /// Default serialization format for tables.
    /// </summary>
    public SerializationFormat DefaultFormat { get; set; } = SerializationFormat.Json;
    
    /// <summary>
    /// Custom database properties.
    /// </summary>
    public Dictionary<string, object> Properties { get; set; } = new();
    
    /// <summary>
    /// List of tables in this database.
    /// </summary>
    public List<string> Tables { get; set; } = new();
    
    /// <summary>
    /// Average objects per page for optimization.
    /// </summary>
    public int AverageObjectsPerPage { get; set; } = 100;
}