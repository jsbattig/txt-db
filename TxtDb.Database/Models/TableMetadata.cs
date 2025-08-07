using TxtDb.Storage.Models;

namespace TxtDb.Database.Models;

/// <summary>
/// Table metadata stored in {database}.{table}._metadata namespace.
/// </summary>
public class TableMetadata
{
    /// <summary>
    /// Table name.
    /// </summary>
    public string Name { get; set; } = "";
    
    /// <summary>
    /// Database name this table belongs to.
    /// </summary>
    public string DatabaseName { get; set; } = "";
    
    /// <summary>
    /// Primary key field JSON path (required).
    /// </summary>
    public string PrimaryKeyField { get; set; } = "";
    
    /// <summary>
    /// Table creation timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }
    
    /// <summary>
    /// Last modification timestamp.
    /// </summary>
    public DateTime ModifiedAt { get; set; }
    
    /// <summary>
    /// Serialization format for objects in this table.
    /// </summary>
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;
    
    /// <summary>
    /// Custom table properties.
    /// </summary>
    public Dictionary<string, object> Properties { get; set; } = new();
    
    /// <summary>
    /// Target objects per page - used by storage subsystem only.
    /// Database layer does not perform page splitting.
    /// </summary>
    public int TargetObjectsPerPage { get; set; } = 100;
}