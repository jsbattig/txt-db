using SqlParser.Ast;
using TxtDb.Sql.Exceptions;
using TxtDb.Sql.Models;

namespace TxtDb.Sql.Visitors;

/// <summary>
/// Abstract base class for implementing the Visitor pattern over SQL statement ASTs.
/// 
/// This class provides a strongly-typed visitor interface that allows processing 
/// different SQL statement types with specific logic for each statement kind.
/// 
/// The visitor pattern enables clean separation of concerns:
/// - The AST structure remains unchanged
/// - Processing logic is encapsulated in visitor implementations
/// - New processing logic can be added without modifying existing AST classes
/// - Multiple different processors can work on the same AST
/// 
/// Concrete implementations should override the Visit* methods for the statement
/// types they need to handle.
/// </summary>
/// <typeparam name="TResult">The type returned by visitor methods</typeparam>
public abstract class SqlStatementVisitor<TResult>
{
    /// <summary>
    /// Visits a parsed SQL statement using the appropriate visitor method.
    /// 
    /// This is the main entry point for the visitor pattern. It dispatches
    /// to the specific Visit* method based on the statement type.
    /// </summary>
    /// <param name="parsedStatement">The parsed statement to visit</param>
    /// <param name="context">The execution context carrying state between methods</param>
    /// <returns>The result of visiting the statement</returns>
    /// <exception cref="SqlExecutionException">Thrown when the statement type is not supported</exception>
    public TResult Visit(ParsedStatement parsedStatement, ExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(parsedStatement);
        ArgumentNullException.ThrowIfNull(context);
        
        return parsedStatement.AstNode switch
        {
            Statement.CreateTable createTable => VisitCreateTable(createTable, context),
            Statement.Insert insert => VisitInsert(insert, context),
            Statement.Select select => VisitSelect(select, context),
            Statement.Update update => VisitUpdate(update, context),
            Statement.Delete delete => VisitDelete(delete, context),
            _ => throw new SqlExecutionException($"Unsupported statement type: {parsedStatement.AstNode.GetType().Name}", parsedStatement.OriginalSql)
        };
    }
    
    /// <summary>
    /// Visits a CREATE TABLE statement.
    /// 
    /// Override this method to provide custom logic for processing CREATE TABLE statements.
    /// The default implementation throws a SqlExecutionException indicating the statement
    /// type is not supported.
    /// </summary>
    /// <param name="createTable">The CREATE TABLE AST node</param>
    /// <param name="context">The execution context</param>
    /// <returns>The result of processing the CREATE TABLE statement</returns>
    protected virtual TResult VisitCreateTable(Statement.CreateTable createTable, ExecutionContext context)
    {
        throw new SqlExecutionException("CREATE TABLE statements are not supported by this visitor", string.Empty);
    }
    
    /// <summary>
    /// Visits an INSERT statement.
    /// 
    /// Override this method to provide custom logic for processing INSERT statements.
    /// The default implementation throws a SqlExecutionException indicating the statement
    /// type is not supported.
    /// </summary>
    /// <param name="insert">The INSERT AST node</param>
    /// <param name="context">The execution context</param>
    /// <returns>The result of processing the INSERT statement</returns>
    protected virtual TResult VisitInsert(Statement.Insert insert, ExecutionContext context)
    {
        throw new SqlExecutionException("INSERT statements are not supported by this visitor", string.Empty);
    }
    
    /// <summary>
    /// Visits a SELECT statement.
    /// 
    /// Override this method to provide custom logic for processing SELECT statements.
    /// The default implementation throws a SqlExecutionException indicating the statement
    /// type is not supported.
    /// </summary>
    /// <param name="select">The SELECT AST node</param>
    /// <param name="context">The execution context</param>
    /// <returns>The result of processing the SELECT statement</returns>
    protected virtual TResult VisitSelect(Statement.Select select, ExecutionContext context)
    {
        throw new SqlExecutionException("SELECT statements are not supported by this visitor", string.Empty);
    }
    
    /// <summary>
    /// Visits an UPDATE statement.
    /// 
    /// Override this method to provide custom logic for processing UPDATE statements.
    /// The default implementation throws a SqlExecutionException indicating the statement
    /// type is not supported.
    /// </summary>
    /// <param name="update">The UPDATE AST node</param>
    /// <param name="context">The execution context</param>
    /// <returns>The result of processing the UPDATE statement</returns>
    protected virtual TResult VisitUpdate(Statement.Update update, ExecutionContext context)
    {
        throw new SqlExecutionException("UPDATE statements are not supported by this visitor", string.Empty);
    }
    
    /// <summary>
    /// Visits a DELETE statement.
    /// 
    /// Override this method to provide custom logic for processing DELETE statements.
    /// The default implementation throws a SqlExecutionException indicating the statement
    /// type is not supported.
    /// </summary>
    /// <param name="delete">The DELETE AST node</param>
    /// <param name="context">The execution context</param>
    /// <returns>The result of processing the DELETE statement</returns>
    protected virtual TResult VisitDelete(Statement.Delete delete, ExecutionContext context)
    {
        throw new SqlExecutionException("DELETE statements are not supported by this visitor", string.Empty);
    }
}