namespace TxtDb.Sql.Models;

/// <summary>
/// Represents metadata for a column in a SQL result set.
/// Contains information about column name, data type, and constraints.
/// </summary>
public class SqlColumnInfo
{
    /// <summary>
    /// Column name.
    /// </summary>
    public string Name { get; init; } = string.Empty;
    
    /// <summary>
    /// SQL data type (e.g., "INT", "VARCHAR", "DECIMAL").
    /// </summary>
    public string DataType { get; init; } = string.Empty;
    
    /// <summary>
    /// Indicates if this column is the primary key.
    /// </summary>
    public bool IsPrimaryKey { get; init; }
    
    /// <summary>
    /// Indicates if this column allows NULL values.
    /// </summary>
    public bool IsNullable { get; init; } = true;
    
    /// <summary>
    /// Maximum length for string types (null if not applicable).
    /// </summary>
    public int? MaxLength { get; init; }
    
    /// <summary>
    /// Precision for numeric types (null if not applicable).
    /// </summary>
    public int? Precision { get; init; }
    
    /// <summary>
    /// Scale for decimal types (null if not applicable).
    /// </summary>
    public int? Scale { get; init; }
}