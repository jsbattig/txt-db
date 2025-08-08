using SqlParser;
using SqlParser.Ast;
using TxtDb.Sql.Models;
using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Services;

/// <summary>
/// Service for parsing SQL statements using SqlParserCS for proper AST-based parsing.
/// Converts SQL text into structured SqlStatement objects for execution.
/// 
/// CRITICAL: This class has been completely rewritten to use SqlParserCS instead of regex.
/// This provides proper SQL parsing with full AST support, replacing the previous
/// fragile regex-based approach that was prone to parsing errors.
/// </summary>
public class SqlStatementParser
{
    private readonly SqlQueryParser _parser = new();
    
    /// <summary>
    /// Parses a SQL statement string into a structured SqlStatement object using SqlParserCS.
    /// </summary>
    /// <param name="sql">SQL statement to parse</param>
    /// <returns>Parsed SqlStatement with type, table name, and other extracted information</returns>
    /// <exception cref="SqlExecutionException">Thrown when SQL cannot be parsed or is unsupported</exception>
    public SqlStatement Parse(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new SqlExecutionException("SQL statement cannot be empty", sql);
            
        try
        {
            var statements = _parser.Parse(sql);
            
            if (statements.Count == 0)
                throw new SqlExecutionException("No statements found in SQL", sql);
                
            if (statements.Count > 1)
                throw new SqlExecutionException("Multiple statements are not supported", sql);
            
            var statement = statements[0];
            var parsedStatement = new ParsedStatement(statement, sql);
            
            return ConvertToLegacySqlStatement(parsedStatement);
        }
        catch (ParserException ex)
        {
            throw new SqlExecutionException($"Failed to parse SQL statement: {ex.Message}", sql);
        }
        catch (SqlExecutionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new SqlExecutionException($"Failed to parse SQL statement: {ex.Message}", sql);
        }
    }
    
    /// <summary>
    /// Converts a ParsedStatement (from SqlParserCS) to the legacy SqlStatement format
    /// for backward compatibility with existing code.
    /// 
    /// This method bridges the gap between the new SqlParserCS-based parsing and the
    /// existing SqlStatement interface that the rest of the system expects.
    /// </summary>
    /// <param name="parsedStatement">The ParsedStatement from SqlParserCS</param>
    /// <returns>Legacy SqlStatement object</returns>
    private SqlStatement ConvertToLegacySqlStatement(ParsedStatement parsedStatement)
    {
        return parsedStatement.StatementType switch
        {
            SqlStatementType.CreateTable => ConvertCreateTableStatement(parsedStatement),
            SqlStatementType.Insert => ConvertInsertStatement(parsedStatement),
            SqlStatementType.Select => ConvertSelectStatement(parsedStatement),
            SqlStatementType.Update => ConvertUpdateStatement(parsedStatement),
            SqlStatementType.Delete => ConvertDeleteStatement(parsedStatement),
            SqlStatementType.AlterTable => ConvertAlterTableStatement(parsedStatement),
            SqlStatementType.DropTable => ConvertDropTableStatement(parsedStatement),
            _ => throw new SqlExecutionException($"Unsupported statement type: {parsedStatement.StatementType}", parsedStatement.OriginalSql)
        };
    }
    
    private SqlStatement ConvertCreateTableStatement(ParsedStatement parsedStatement)
    {
        if (parsedStatement.AstNode is not Statement.CreateTable createTable)
            throw new SqlExecutionException("Expected CREATE TABLE statement", parsedStatement.OriginalSql);
        
        var tableName = parsedStatement.TableName ?? string.Empty;
        var columns = ExtractColumnDefinitions(createTable);
        
        return new SqlStatement
        {
            Type = SqlStatementType.CreateTable,
            TableName = tableName,
            Columns = columns
        };
    }
    
    private SqlStatement ConvertInsertStatement(ParsedStatement parsedStatement)
    {
        if (parsedStatement.AstNode is not Statement.Insert insert)
            throw new SqlExecutionException("Expected INSERT statement", parsedStatement.OriginalSql);
        
        var tableName = parsedStatement.TableName ?? string.Empty;
        var values = ExtractInsertValues(insert);
        
        return new SqlStatement
        {
            Type = SqlStatementType.Insert,
            TableName = tableName,
            Values = values
        };
    }
    
    private SqlStatement ConvertSelectStatement(ParsedStatement parsedStatement)
    {
        if (parsedStatement.AstNode is not Statement.Select select)
            throw new SqlExecutionException("Expected SELECT statement", parsedStatement.OriginalSql);
        
        var tableName = parsedStatement.TableName ?? string.Empty;
        var (columns, isSelectAll) = ExtractSelectColumns(select);
        var whereExpression = ExtractSelectWhereExpression(select);
        var whereClause = whereExpression?.ToString();
        
        return new SqlStatement
        {
            Type = SqlStatementType.Select,
            TableName = tableName,
            SelectColumns = columns,
            SelectAllColumns = isSelectAll,
            WhereExpression = whereExpression,
            WhereClause = whereClause
        };
    }
    
    private SqlStatement ConvertUpdateStatement(ParsedStatement parsedStatement)
    {
        if (parsedStatement.AstNode is not Statement.Update update)
            throw new SqlExecutionException("Expected UPDATE statement", parsedStatement.OriginalSql);
        
        var tableName = parsedStatement.TableName ?? string.Empty;
        var setValues = ExtractUpdateSetValues(update);
        var whereExpression = ExtractUpdateWhereExpression(update);
        var whereClause = whereExpression?.ToString();
        
        return new SqlStatement
        {
            Type = SqlStatementType.Update,
            TableName = tableName,
            SetValues = setValues,
            WhereExpression = whereExpression,
            WhereClause = whereClause
        };
    }
    
    private SqlStatement ConvertDeleteStatement(ParsedStatement parsedStatement)
    {
        if (parsedStatement.AstNode is not Statement.Delete delete)
            throw new SqlExecutionException("Expected DELETE statement", parsedStatement.OriginalSql);
        
        var tableName = parsedStatement.TableName ?? string.Empty;
        var whereExpression = ExtractDeleteWhereExpression(delete);
        var whereClause = whereExpression?.ToString();
        
        return new SqlStatement
        {
            Type = SqlStatementType.Delete,
            TableName = tableName,
            WhereExpression = whereExpression,
            WhereClause = whereClause
        };
    }
    
    private SqlStatement ConvertAlterTableStatement(ParsedStatement parsedStatement)
    {
        // ALTER TABLE support for parsing but not execution
        return new SqlStatement
        {
            Type = SqlStatementType.AlterTable,
            TableName = parsedStatement.TableName ?? string.Empty
        };
    }
    
    private SqlStatement ConvertDropTableStatement(ParsedStatement parsedStatement)
    {
        // DROP TABLE support for parsing but not execution
        return new SqlStatement
        {
            Type = SqlStatementType.DropTable,
            TableName = parsedStatement.TableName ?? string.Empty
        };
    }
    
    private List<SqlColumnInfo> ExtractColumnDefinitions(Statement.CreateTable createTable)
    {
        var columns = new List<SqlColumnInfo>();
        
        foreach (var column in createTable.Element.Columns)
        {
            var isPrimaryKey = column.Options?.Any(opt => 
                opt.Option is ColumnOption.Unique unique && unique.IsPrimary) ?? false;
            
            var columnInfo = new SqlColumnInfo
            {
                Name = column.Name.ToString(),
                DataType = ExtractDataTypeName(column.DataType),
                IsPrimaryKey = isPrimaryKey,
                IsNullable = !isPrimaryKey // Simplified logic
            };
            
            columns.Add(columnInfo);
        }
        
        return columns;
    }
    
    private Dictionary<string, object> ExtractInsertValues(Statement.Insert insert)
    {
        var values = new Dictionary<string, object>();
        
        // Get column names from INSERT statement
        var columns = insert.InsertOperation.Columns?.Select(c => c.ToString()).ToList() ?? new List<string>();
        
        // Extract values from INSERT statement
        if (insert.InsertOperation.Source is Statement.Select selectSource &&
            selectSource.Query.Body is SetExpression.ValuesExpression valuesExpr)
        {
            if (valuesExpr.Values.Rows.Count > 0)
            {
                var firstRow = valuesExpr.Values.Rows[0];
                for (int i = 0; i < Math.Min(columns.Count, firstRow.Count); i++)
                {
                    var columnName = columns[i];
                    var valueExpression = firstRow[i];
                    var extractedValue = ExtractLiteralValue(valueExpression);
                    values[columnName] = extractedValue;
                }
            }
        }
        
        return values;
    }
    
    private (List<string> columns, bool isSelectAll) ExtractSelectColumns(Statement.Select select)
    {
        if (select.Query.Body is not SetExpression.SelectExpression selectExpr)
            return (new List<string>(), false);
        
        var projections = selectExpr.Select.Projection;
        var columns = new List<string>();
        bool isSelectAll = false;
        
        foreach (var projection in projections)
        {
            switch (projection)
            {
                case SelectItem.Wildcard:
                    isSelectAll = true;
                    columns.Clear(); // Clear any previously added columns
                    break;
                case SelectItem.UnnamedExpression unnamedExpr:
                    if (unnamedExpr.Expression is Expression.Identifier identifier)
                    {
                        columns.Add(identifier.Ident.ToString());
                    }
                    break;
            }
        }
        
        return (columns, isSelectAll);
    }
    
    private Expression? ExtractSelectWhereExpression(Statement.Select select)
    {
        // Extract WHERE clause expression from SELECT statement
        if (select.Query.Body is SetExpression.SelectExpression selectExpr)
        {
            return selectExpr.Select.Selection;
        }
        return null;
    }
    
    private object ExtractLiteralValue(Expression expression)
    {
        return expression switch
        {
            Expression.LiteralValue literal => literal.Value switch
            {
                Value.Number number => int.TryParse(number.Value, out var intVal) ? (object)intVal : long.Parse(number.Value),
                Value.SingleQuotedString str => str.Value,
                Value.DoubleQuotedString str => str.Value,
                Value.Boolean boolean => boolean.Value,
                Value.Null => null,
                _ => expression.ToString()
            },
            _ => expression.ToString()
        } ?? string.Empty;
    }
    
    /// <summary>
    /// Extracts a simple data type name from SqlParserCS DataType objects.
    /// </summary>
    private string ExtractDataTypeName(DataType dataType)
    {
        return dataType switch
        {
            DataType.Int => "INT",
            DataType.BigInt => "BIGINT",
            DataType.SmallInt => "SMALLINT",
            DataType.TinyInt => "TINYINT",
            DataType.Varchar => "VARCHAR",
            DataType.Char => "CHAR",
            DataType.Text => "TEXT",
            DataType.Boolean => "BOOLEAN",
            DataType.Float => "FLOAT",
            DataType.Double => "DOUBLE",
            DataType.Decimal => "DECIMAL",
            DataType.Date => "DATE",
            DataType.Time => "TIME",
            DataType.Timestamp => "TIMESTAMP",
            _ => dataType.ToString().Split(' ')[0].ToUpperInvariant()
        };
    }
    
    /// <summary>
    /// Extracts SET clause values from UPDATE statement.
    /// </summary>
    private Dictionary<string, object> ExtractUpdateSetValues(Statement.Update update)
    {
        var setValues = new Dictionary<string, object>();
        
        foreach (var assignment in update.Assignments)
        {
            var columnName = ExtractAssignmentTargetName(assignment.Target);
            var value = ExtractLiteralValue(assignment.Value);
            setValues[columnName] = value;
        }
        
        return setValues;
    }
    
    /// <summary>
    /// Extracts WHERE expression from UPDATE statement.
    /// </summary>
    private Expression? ExtractUpdateWhereExpression(Statement.Update update)
    {
        return update.Selection;
    }
    
    /// <summary>
    /// Extracts WHERE expression from DELETE statement.
    /// </summary>
    private Expression? ExtractDeleteWhereExpression(Statement.Delete delete)
    {
        return delete.DeleteOperation.Selection;
    }
    
    /// <summary>
    /// Extracts identifier name from various identifier expressions.
    /// </summary>
    private string ExtractIdentifierName(Expression expression)
    {
        return expression switch
        {
            Expression.Identifier identifier => identifier.Ident.ToString(),
            Expression.CompoundIdentifier compound => compound.Idents.Last().ToString(),
            _ => expression.ToString()
        };
    }
    
    /// <summary>
    /// Extracts column name from assignment target.
    /// </summary>
    private string ExtractAssignmentTargetName(AssignmentTarget target)
    {
        return target switch
        {
            AssignmentTarget.ColumnName columnName => columnName.ToString(),
            _ => target.ToString()
        };
    }
}