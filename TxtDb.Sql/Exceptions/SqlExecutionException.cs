namespace TxtDb.Sql.Exceptions;

/// <summary>
/// Exception thrown when SQL execution fails.
/// Provides detailed information about the failed SQL statement and error context.
/// </summary>
public class SqlExecutionException : Exception
{
    /// <summary>
    /// The SQL statement that failed to execute.
    /// </summary>
    public string? SqlStatement { get; }
    
    /// <summary>
    /// The type of SQL statement that failed (e.g., "SELECT", "INSERT").
    /// </summary>
    public string? StatementType { get; }
    
    /// <summary>
    /// Initializes a new instance with the specified message.
    /// </summary>
    /// <param name="message">Error message</param>
    public SqlExecutionException(string message) : base(message)
    {
    }
    
    /// <summary>
    /// Initializes a new instance with the specified message and inner exception.
    /// </summary>
    /// <param name="message">Error message</param>
    /// <param name="innerException">Inner exception that caused this error</param>
    public SqlExecutionException(string message, Exception innerException) : base(message, innerException)
    {
    }
    
    /// <summary>
    /// Initializes a new instance with the specified message and SQL statement.
    /// </summary>
    /// <param name="message">Error message</param>
    /// <param name="sqlStatement">SQL statement that failed</param>
    public SqlExecutionException(string message, string sqlStatement) : base($"{message}. SQL: {sqlStatement}")
    {
        SqlStatement = sqlStatement;
    }
    
    /// <summary>
    /// Initializes a new instance with the specified message, SQL statement, and statement type.
    /// </summary>
    /// <param name="message">Error message</param>
    /// <param name="sqlStatement">SQL statement that failed</param>
    /// <param name="statementType">Type of SQL statement</param>
    public SqlExecutionException(string message, string sqlStatement, string statementType) 
        : base($"{message}. Statement type: {statementType}. SQL: {sqlStatement}")
    {
        SqlStatement = sqlStatement;
        StatementType = statementType;
    }
}