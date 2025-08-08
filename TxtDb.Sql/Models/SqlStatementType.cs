namespace TxtDb.Sql.Models;

/// <summary>
/// Enumeration of supported SQL statement types.
/// Used to categorize and handle different types of SQL operations.
/// </summary>
public enum SqlStatementType
{
    /// <summary>
    /// SELECT statement for querying data.
    /// </summary>
    Select,
    
    /// <summary>
    /// INSERT statement for adding new records.
    /// </summary>
    Insert,
    
    /// <summary>
    /// UPDATE statement for modifying existing records.
    /// </summary>
    Update,
    
    /// <summary>
    /// DELETE statement for removing records.
    /// </summary>
    Delete,
    
    /// <summary>
    /// CREATE TABLE statement for creating new tables.
    /// </summary>
    CreateTable,
    
    /// <summary>
    /// DROP TABLE statement for deleting tables.
    /// </summary>
    DropTable,
    
    /// <summary>
    /// ALTER TABLE statement for modifying table structure.
    /// </summary>
    AlterTable,
    
    /// <summary>
    /// Unknown or unsupported statement type.
    /// </summary>
    Unknown
}