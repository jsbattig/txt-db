using TxtDb.Database.Interfaces;

namespace TxtDb.Sql.Interfaces;

/// <summary>
/// Interface for executing SQL statements against a TxtDb database.
/// Provides a bridge between SQL commands and TxtDb's document-oriented storage.
/// </summary>
public interface ISqlExecutor
{
    /// <summary>
    /// Executes a SQL statement within the context of a database transaction.
    /// </summary>
    /// <param name="sql">SQL statement to execute</param>
    /// <param name="transaction">Active database transaction</param>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    /// <returns>Result of the SQL execution containing rows, metadata, and status information</returns>
    /// <exception cref="ArgumentNullException">Thrown when sql or transaction is null</exception>
    /// <exception cref="SqlExecutionException">Thrown when SQL cannot be parsed or executed</exception>
    Task<ISqlResult> ExecuteAsync(
        string sql, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken = default);
}