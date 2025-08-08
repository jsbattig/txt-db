using SqlParser.Ast;
using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Models;

/// <summary>
/// Wrapper class that bridges SqlParserCS AST nodes to TxtDb's domain model.
/// Provides a strongly-typed interface over the generic SqlParserCS Statement AST.
/// 
/// This class replaces the regex-based parsing with proper AST-based parsing
/// while maintaining compatibility with existing TxtDb SQL execution logic.
/// </summary>
public class ParsedStatement
{
    /// <summary>
    /// The original SQL string that was parsed.
    /// </summary>
    public string OriginalSql { get; }
    
    /// <summary>
    /// The SqlParserCS AST node representing the parsed statement.
    /// </summary>
    public Statement AstNode { get; }
    
    /// <summary>
    /// The TxtDb statement type derived from the AST node.
    /// </summary>
    public SqlStatementType StatementType { get; }
    
    /// <summary>
    /// Table name referenced in the statement (if applicable).
    /// </summary>
    public string? TableName { get; }
    
    /// <summary>
    /// Initializes a new ParsedStatement from a SqlParserCS AST node.
    /// </summary>
    /// <param name="astNode">The AST node from SqlParserCS</param>
    /// <param name="originalSql">The original SQL string</param>
    /// <exception cref="SqlExecutionException">Thrown when the AST node type is not supported</exception>
    public ParsedStatement(Statement astNode, string originalSql)
    {
        AstNode = astNode ?? throw new ArgumentNullException(nameof(astNode));
        OriginalSql = originalSql ?? throw new ArgumentNullException(nameof(originalSql));
        
        StatementType = DetermineStatementType(astNode);
        TableName = ExtractTableName(astNode);
    }
    
    /// <summary>
    /// Determines the TxtDb statement type from the SqlParserCS AST node.
    /// </summary>
    private static SqlStatementType DetermineStatementType(Statement astNode)
    {
        return astNode switch
        {
            Statement.CreateTable => SqlStatementType.CreateTable,
            Statement.Insert => SqlStatementType.Insert,
            Statement.Update => SqlStatementType.Update,
            Statement.Delete => SqlStatementType.Delete,
            Statement.Select => SqlStatementType.Select,
            Statement.AlterTable => SqlStatementType.AlterTable,
            Statement.Drop drop when IsDropTable(drop) => SqlStatementType.DropTable,
            Statement.Drop drop when IsDropIndex(drop) => SqlStatementType.DropIndex,
            Statement.CreateIndex => SqlStatementType.CreateIndex,
            _ => throw new SqlExecutionException($"Unsupported statement type: {astNode.GetType().Name}", string.Empty)
        };
    }
    
    /// <summary>
    /// Extracts the primary table name from the AST node.
    /// </summary>
    private static string? ExtractTableName(Statement astNode)
    {
        return astNode switch
        {
            Statement.CreateTable createTable => ExtractTableNameFromCreateTable(createTable),
            Statement.Insert insert => ExtractTableNameFromInsert(insert),
            Statement.Update update => ExtractTableNameFromUpdate(update),
            Statement.Delete delete => ExtractTableNameFromDelete(delete),
            Statement.Select select => ExtractTableNameFromSelect(select),
            Statement.AlterTable alterTable => ExtractTableNameFromAlterTable(alterTable),
            Statement.Drop drop => ExtractTableNameFromDrop(drop),
            Statement.CreateIndex createIndex => ExtractTableNameFromCreateIndex(createIndex),
            _ => null
        };
    }
    
    /// <summary>
    /// Extracts table name from CREATE TABLE statement.
    /// </summary>
    private static string? ExtractTableNameFromCreateTable(Statement.CreateTable createTable)
    {
        // The table name is in createTable.Element.Name as an ObjectName
        return createTable.Element.Name?.ToString();
    }
    
    /// <summary>
    /// Extracts table name from INSERT statement.
    /// </summary>
    private static string? ExtractTableNameFromInsert(Statement.Insert insert)
    {
        // The table name is in insert.InsertOperation.Name as an ObjectName
        return insert.InsertOperation.Name?.ToString();
    }
    
    /// <summary>
    /// Extracts table name from UPDATE statement.
    /// </summary>
    private static string? ExtractTableNameFromUpdate(Statement.Update update)
    {
        // The table name is in update.Table as a TableWithJoins
        if (update.Table.Relation is TableFactor.Table table)
        {
            return table.Name?.ToString();
        }
        return null;
    }
    
    /// <summary>
    /// Extracts table name from DELETE statement.
    /// </summary>
    private static string? ExtractTableNameFromDelete(Statement.Delete delete)
    {
        // The table name is in delete.DeleteOperation.From
        if (delete.DeleteOperation.From is FromTable.WithFromKeyword withFrom)
        {
            if (withFrom.From?.Count > 0)
            {
                var firstTable = withFrom.From[0];
                if (firstTable.Relation is TableFactor.Table table)
                {
                    return table.Name?.ToString();
                }
            }
        }
        return null;
    }
    
    /// <summary>
    /// Extracts table name from SELECT statement (simplified - gets first table).
    /// </summary>
    private static string? ExtractTableNameFromSelect(Statement.Select select)
    {
        // The table name is in select.Query.Body
        if (select.Query.Body is SetExpression.SelectExpression selectExpr)
        {
            var selectQuery = selectExpr.Select;
            if (selectQuery.From?.Count > 0)
            {
                var firstTable = selectQuery.From[0];
                if (firstTable.Relation is TableFactor.Table table)
                {
                    return table.Name?.ToString();
                }
            }
        }
        return null;
    }
    
    /// <summary>
    /// Extracts table name from ALTER TABLE statement.
    /// </summary>
    private static string? ExtractTableNameFromAlterTable(Statement.AlterTable alterTable)
    {
        return alterTable.Name?.ToString();
    }
    
    /// <summary>
    /// Extracts table name from DROP statement.
    /// </summary>
    private static string? ExtractTableNameFromDrop(Statement.Drop drop)
    {
        if (IsDropIndex(drop))
        {
            // For DROP INDEX, we may need to extract table name differently
            // Some SQL dialects don't include table name in DROP INDEX
            // We'll need to handle this at execution time by looking up the index
            return null;
        }
        
        // For DROP TABLE
        if (drop.Names?.Count > 0)
        {
            return drop.Names[0]?.ToString();
        }
        return null;
    }
    
    /// <summary>
    /// Extracts table name from CREATE INDEX statement.
    /// </summary>
    private static string? ExtractTableNameFromCreateIndex(Statement.CreateIndex createIndex)
    {
        // Based on debug output, the table name is in createIndex.Element.TableName
        try
        {
            var element = createIndex.Element;
            if (element != null)
            {
                var tableNameProperty = element.GetType().GetProperty("TableName");
                if (tableNameProperty != null)
                {
                    var tableNameValue = tableNameProperty.GetValue(element);
                    if (tableNameValue != null)
                    {
                        return tableNameValue.ToString();
                    }
                }
            }
        }
        catch
        {
            // Fallback to null
        }
        
        return null;
    }
    
    /// <summary>
    /// Determines if a DROP statement is dropping a table.
    /// </summary>
    private static bool IsDropTable(Statement.Drop drop)
    {
        // Check if the object type being dropped is a table
        return drop.ObjectType == ObjectType.Table;
    }
    
    /// <summary>
    /// Determines if a DROP statement is dropping an index.
    /// </summary>
    private static bool IsDropIndex(Statement.Drop drop)
    {
        // Check if the object type being dropped is an index
        return drop.ObjectType == ObjectType.Index;
    }
}