using TxtDb.Sql.Services;
using TxtDb.Sql.Models;
using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Tests.Services;

/// <summary>
/// Test suite for SQL parsing integration using SqlParserCS library.
/// These tests will fail initially until the SQL parsing logic is implemented.
/// Tests focus on proper parsing and statement type identification.
/// </summary>
public class SqlParserIntegrationTests
{
    /// <summary>
    /// Test that SqlParser can identify CREATE TABLE statements correctly.
    /// </summary>
    [Fact]
    public void ParseSql_WithCreateTableStatement_ShouldReturnCreateTableType()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.Equal(SqlStatementType.CreateTable, statement.Type);
        Assert.Equal("users", statement.TableName);
    }
    
    /// <summary>
    /// Test that SqlParser can identify INSERT statements correctly.
    /// </summary>
    [Fact]
    public void ParseSql_WithInsertStatement_ShouldReturnInsertType()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.Equal(SqlStatementType.Insert, statement.Type);
        Assert.Equal("users", statement.TableName);
    }
    
    /// <summary>
    /// Test that SqlParser can identify SELECT statements correctly.
    /// </summary>
    [Fact]
    public void ParseSql_WithSelectStatement_ShouldReturnSelectType()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "SELECT id, name FROM users WHERE id = 1";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.Equal(SqlStatementType.Select, statement.Type);
        Assert.Equal("users", statement.TableName);
    }
    
    /// <summary>
    /// Test that SqlParser can extract column definitions from CREATE TABLE.
    /// </summary>
    [Fact]
    public void ParseSql_WithCreateTableColumns_ShouldExtractColumnDefinitions()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(200), price DECIMAL(10,2))";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.NotNull(statement.Columns);
        Assert.Equal(3, statement.Columns.Count);
        
        var idColumn = statement.Columns[0];
        Assert.Equal("id", idColumn.Name);
        Assert.Equal("INT", idColumn.DataType);
        Assert.True(idColumn.IsPrimaryKey);
    }
    
    /// <summary>
    /// Test that SqlParser can extract values from INSERT statements.
    /// </summary>
    [Fact]
    public void ParseSql_WithInsertValues_ShouldExtractValues()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "INSERT INTO users (id, name, email) VALUES (123, 'Alice Smith', 'alice@example.com')";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.NotNull(statement.Values);
        Assert.Equal(3, statement.Values.Count);
        Assert.Equal(123, statement.Values["id"]);
        Assert.Equal("Alice Smith", statement.Values["name"]);
        Assert.Equal("alice@example.com", statement.Values["email"]);
    }
    
    /// <summary>
    /// Test that SqlParser can extract column list from SELECT statements.
    /// </summary>
    [Fact]
    public void ParseSql_WithSelectColumns_ShouldExtractColumnList()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "SELECT id, name, email FROM users";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.NotNull(statement.SelectColumns);
        Assert.Equal(3, statement.SelectColumns.Count);
        Assert.Contains("id", statement.SelectColumns);
        Assert.Contains("name", statement.SelectColumns);
        Assert.Contains("email", statement.SelectColumns);
    }
    
    /// <summary>
    /// Test that SqlParser handles SELECT * correctly.
    /// </summary>
    [Fact]
    public void ParseSql_WithSelectStar_ShouldIndicateAllColumns()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "SELECT * FROM users";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.True(statement.SelectAllColumns);
    }
    
    /// <summary>
    /// Test that SqlParser throws exception for unsupported SQL statements.
    /// </summary>
    [Fact]
    public void ParseSql_WithUnsupportedStatement_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "GRANT SELECT ON users TO role1"; // Truly unsupported by our parser
        
        // Act & Assert
        var exception = Assert.Throws<SqlExecutionException>(() => parser.Parse(sql));
        Assert.Contains("unsupported", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that SqlParser throws exception for malformed SQL.
    /// </summary>
    [Fact]
    public void ParseSql_WithMalformedSql_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "SELECT FROM WHERE";
        
        // Act & Assert
        var exception = Assert.Throws<SqlExecutionException>(() => parser.Parse(sql));
        Assert.Contains("parse", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that SqlParser handles case-insensitive SQL keywords.
    /// </summary>
    [Fact]
    public void ParseSql_WithMixedCaseKeywords_ShouldParseCorrectly()
    {
        // Arrange
        var parser = new SqlStatementParser();
        var sql = "select ID, Name from Users where ID = 1";
        
        // Act
        var statement = parser.Parse(sql);
        
        // Assert
        Assert.NotNull(statement);
        Assert.Equal(SqlStatementType.Select, statement.Type);
        Assert.Equal("Users", statement.TableName);
    }
}