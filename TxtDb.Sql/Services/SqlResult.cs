using TxtDb.Sql.Interfaces;
using TxtDb.Sql.Models;

namespace TxtDb.Sql.Services;

/// <summary>
/// Implementation of ISqlResult representing the result of a SQL execution.
/// Contains query results, metadata, and execution status information.
/// </summary>
public class SqlResult : ISqlResult
{
    /// <summary>
    /// Type of SQL statement that was executed.
    /// </summary>
    public SqlStatementType StatementType { get; init; } = SqlStatementType.Unknown;
    
    /// <summary>
    /// Column definitions for SELECT results.
    /// </summary>
    public IReadOnlyList<SqlColumnInfo> Columns { get; init; } = Array.Empty<SqlColumnInfo>();
    
    /// <summary>
    /// Data rows for SELECT results.
    /// </summary>
    public IReadOnlyList<object[]> Rows { get; init; } = Array.Empty<object[]>();
    
    /// <summary>
    /// Number of rows affected by INSERT, UPDATE, or DELETE statements.
    /// </summary>
    public int AffectedRows { get; init; }
    
}