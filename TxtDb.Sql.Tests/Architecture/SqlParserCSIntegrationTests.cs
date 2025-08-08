using SqlParser;
using SqlParser.Ast;
using SqlParser.Dialects;
using TxtDb.Sql.Models;
using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Tests.Architecture;

/// <summary>
/// Test suite to verify proper SqlParserCS integration according to architectural requirements.
/// These tests define how the SqlParserCS should be used to replace the regex-based parser.
/// 
/// CRITICAL: These tests are failing by design until SqlParserCS integration is implemented.
/// They define the EXACT contract required by the architecture plan.
/// </summary>
public class SqlParserCSIntegrationTests
{
    /// <summary>
    /// Test that SqlQueryParser can parse a basic CREATE TABLE statement into AST.
    /// This replaces the regex-based parsing with proper AST-based parsing.
    /// </summary>
    [Fact]
    public void SqlQueryParser_ShouldParse_CreateTableStatement()
    {
        // Arrange
        var sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        var parser = new SqlQueryParser();
        
        // Act
        var statements = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statements);
        Assert.Single(statements);
        
        var statement = statements[0];
        Assert.NotNull(statement);
        // Should be a Statement.CreateTable variant based on the AST structure
    }
    
    /// <summary>
    /// Test that SqlQueryParser can parse a basic INSERT statement into AST.
    /// </summary>
    [Fact]
    public void SqlQueryParser_ShouldParse_InsertStatement()
    {
        // Arrange
        var sql = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        var parser = new SqlQueryParser();
        
        // Act
        var statements = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statements);
        Assert.Single(statements);
        
        var statement = statements[0];
        Assert.NotNull(statement);
    }
    
    /// <summary>
    /// Test that SqlQueryParser can parse a basic SELECT statement into AST.
    /// </summary>
    [Fact]
    public void SqlQueryParser_ShouldParse_SelectStatement()
    {
        // Arrange
        var sql = "SELECT id, name FROM users";
        var parser = new SqlQueryParser();
        
        // Act
        var statements = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statements);
        Assert.Single(statements);
        
        var statement = statements[0];
        Assert.NotNull(statement);
    }
    
    /// <summary>
    /// Test that SqlQueryParser can parse a basic UPDATE statement into AST.
    /// </summary>
    [Fact]
    public void SqlQueryParser_ShouldParse_UpdateStatement()
    {
        // Arrange
        var sql = "UPDATE users SET name = 'Jane Doe' WHERE id = 1";
        var parser = new SqlQueryParser();
        
        // Act
        var statements = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statements);
        Assert.Single(statements);
        
        var statement = statements[0];
        Assert.NotNull(statement);
    }
    
    /// <summary>
    /// Test that SqlQueryParser can parse a basic DELETE statement into AST.
    /// </summary>
    [Fact]
    public void SqlQueryParser_ShouldParse_DeleteStatement()
    {
        // Arrange
        var sql = "DELETE FROM users WHERE id = 1";
        var parser = new SqlQueryParser();
        
        // Act
        var statements = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statements);
        Assert.Single(statements);
        
        var statement = statements[0];
        Assert.NotNull(statement);
    }
    
    /// <summary>
    /// Test that SqlQueryParser handles syntax errors gracefully.
    /// The parsing errors should be used to throw SqlExecutionException.
    /// </summary>
    [Fact]
    public void SqlQueryParser_WithInvalidSql_ShouldThrowParserException()
    {
        // Arrange
        var invalidSql = "SELECT FROM WHERE"; // Invalid syntax
        var parser = new SqlQueryParser();
        
        // Act & Assert
        var exception = Assert.Throws<ParserException>(() => parser.Parse(invalidSql));
        Assert.NotNull(exception.Message);
        Assert.Contains("expected", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that ParsedStatement wrapper can be created from SqlQueryParser results.
    /// This defines the wrapper class needed to bridge SqlParserCS AST to our domain model.
    /// </summary>
    [Fact]
    public void ParsedStatement_ShouldWrap_SqlParserCSResults()
    {
        // Arrange
        var sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        var parser = new SqlQueryParser();
        var statements = parser.Parse(sql);
        
        // Act
        var parsedStatement = new ParsedStatement(statements[0], sql);
        
        // Assert
        Assert.NotNull(parsedStatement);
        Assert.Equal(sql, parsedStatement.OriginalSql);
        Assert.NotNull(parsedStatement.AstNode);
        Assert.Equal(SqlStatementType.CreateTable, parsedStatement.StatementType);
    }
}