using TxtDb.Sql.Models;

namespace TxtDb.Sql.Interfaces;

/// <summary>
/// Interface representing the result of a SQL execution.
/// Contains query results, metadata, and execution status information.
/// </summary>
public interface ISqlResult
{
    /// <summary>
    /// Type of SQL statement that was executed.
    /// </summary>
    SqlStatementType StatementType { get; }
    
    /// <summary>
    /// Column definitions for SELECT results.
    /// Empty for non-SELECT statements.
    /// </summary>
    IReadOnlyList<SqlColumnInfo> Columns { get; }
    
    /// <summary>
    /// Data rows for SELECT results.
    /// Empty for non-SELECT statements.
    /// Each row is an array of column values in the same order as Columns.
    /// </summary>
    IReadOnlyList<object[]> Rows { get; }
    
    /// <summary>
    /// Number of rows affected by INSERT, UPDATE, or DELETE statements.
    /// Zero for SELECT and CREATE TABLE statements.
    /// </summary>
    int AffectedRows { get; }
    
}