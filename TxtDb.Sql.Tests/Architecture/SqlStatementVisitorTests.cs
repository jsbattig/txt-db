using SqlParser.Ast;
using TxtDb.Sql.Models;
using TxtDb.Sql.Visitors;

namespace TxtDb.Sql.Tests.Architecture;

/// <summary>
/// Test suite to verify the AST Visitor pattern implementation.
/// These tests define how the visitor pattern should work for processing SQL statements.
/// 
/// CRITICAL: These tests are failing by design until the visitor pattern is implemented.
/// They define the EXACT visitor contract required by the architecture plan.
/// </summary>
public class SqlStatementVisitorTests
{
    /// <summary>
    /// Test that SqlStatementVisitor can be implemented with custom behavior.
    /// </summary>
    [Fact]
    public void SqlStatementVisitor_ShouldAllow_CustomImplementation()
    {
        // Arrange
        var sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        var parser = new SqlParser.SqlQueryParser();
        var statements = parser.Parse(sql);
        var parsedStatement = new ParsedStatement(statements[0], sql);
        
        var visitor = new TestSqlStatementVisitor();
        var context = new TxtDb.Sql.Visitors.ExecutionContext();
        
        // Act
        var result = visitor.Visit(parsedStatement, context);
        
        // Assert
        Assert.NotNull(result);
        Assert.True(visitor.CreateTableVisited);
        Assert.Equal("users", visitor.LastTableName);
    }
    
    /// <summary>
    /// Test that SqlStatementVisitor supports all required statement types.
    /// </summary>
    [Theory]
    [InlineData("CREATE TABLE test (id INT)", SqlStatementType.CreateTable)]
    [InlineData("INSERT INTO test (id) VALUES (1)", SqlStatementType.Insert)]
    [InlineData("SELECT id FROM test", SqlStatementType.Select)]
    [InlineData("UPDATE test SET id = 2", SqlStatementType.Update)]
    [InlineData("DELETE FROM test", SqlStatementType.Delete)]
    public void SqlStatementVisitor_ShouldSupport_AllStatementTypes(string sql, SqlStatementType expectedType)
    {
        // Arrange
        var parser = new SqlParser.SqlQueryParser();
        var statements = parser.Parse(sql);
        var parsedStatement = new ParsedStatement(statements[0], sql);
        
        var visitor = new TestSqlStatementVisitor();
        var context = new TxtDb.Sql.Visitors.ExecutionContext();
        
        // Act
        var result = visitor.Visit(parsedStatement, context);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(expectedType, parsedStatement.StatementType);
    }
    
    /// <summary>
    /// Test that ExecutionContext can carry state between visitor methods.
    /// </summary>
    [Fact]
    public void ExecutionContext_ShouldCarry_StateBetweenMethods()
    {
        // Arrange
        var context = new TxtDb.Sql.Visitors.ExecutionContext();
        
        // Act
        context.SetValue("test_key", "test_value");
        var retrievedValue = context.GetValue<string>("test_key");
        
        // Assert
        Assert.Equal("test_value", retrievedValue);
    }
}

/// <summary>
/// Test implementation of SqlStatementVisitor for testing purposes.
/// </summary>
internal class TestSqlStatementVisitor : SqlStatementVisitor<object>
{
    public bool CreateTableVisited { get; private set; }
    public string? LastTableName { get; private set; }
    
    protected override object VisitCreateTable(Statement.CreateTable createTable, TxtDb.Sql.Visitors.ExecutionContext context)
    {
        CreateTableVisited = true;
        LastTableName = createTable.Element.Name?.ToString();
        return new object();
    }
    
    protected override object VisitInsert(Statement.Insert insert, TxtDb.Sql.Visitors.ExecutionContext context)
    {
        return new object();
    }
    
    protected override object VisitSelect(Statement.Select select, TxtDb.Sql.Visitors.ExecutionContext context)
    {
        return new object();
    }
    
    protected override object VisitUpdate(Statement.Update update, TxtDb.Sql.Visitors.ExecutionContext context)
    {
        return new object();
    }
    
    protected override object VisitDelete(Statement.Delete delete, TxtDb.Sql.Visitors.ExecutionContext context)
    {
        return new object();
    }
}