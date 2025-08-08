namespace TxtDb.Sql.Models;

/// <summary>
/// Represents a parsed SQL statement with all extracted information.
/// Contains statement type, table name, columns, values, and other parsed elements.
/// </summary>
public class SqlStatement
{
    /// <summary>
    /// Type of SQL statement.
    /// </summary>
    public SqlStatementType Type { get; init; } = SqlStatementType.Unknown;
    
    /// <summary>
    /// Table name referenced in the statement.
    /// </summary>
    public string TableName { get; init; } = string.Empty;
    
    /// <summary>
    /// Column definitions for CREATE TABLE statements.
    /// </summary>
    public IList<SqlColumnInfo> Columns { get; init; } = new List<SqlColumnInfo>();
    
    /// <summary>
    /// Column-value pairs for INSERT statements.
    /// </summary>
    public IDictionary<string, object> Values { get; init; } = new Dictionary<string, object>();
    
    /// <summary>
    /// List of column names for SELECT statements.
    /// </summary>
    public IList<string> SelectColumns { get; init; } = new List<string>();
    
    /// <summary>
    /// Indicates if SELECT statement uses * (all columns).
    /// </summary>
    public bool SelectAllColumns { get; init; }
    
    /// <summary>
    /// WHERE clause conditions (simplified representation for now).
    /// </summary>
    public string? WhereClause { get; init; }
    
    /// <summary>
    /// WHERE clause expression AST node from SqlParserCS (for proper evaluation).
    /// Used by UPDATE and DELETE operations for filtering rows.
    /// </summary>
    public SqlParser.Ast.Expression? WhereExpression { get; init; }
    
    /// <summary>
    /// SET clause assignments for UPDATE statements.
    /// Maps column names to their new values.
    /// </summary>
    public IDictionary<string, object> SetValues { get; init; } = new Dictionary<string, object>();
}