using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Tests.Exceptions;

/// <summary>
/// Test suite for SqlExecutionException error handling.
/// These tests will fail initially until the SqlExecutionException class is created.
/// Tests focus on proper exception construction and error message handling.
/// </summary>
public class SqlExecutionExceptionTests
{
    /// <summary>
    /// Test that SqlExecutionException can be created with a basic message.
    /// </summary>
    [Fact]
    public void Constructor_WithMessage_ShouldCreateException()
    {
        // Arrange
        var message = "SQL execution failed";
        
        // Act
        var exception = new SqlExecutionException(message);
        
        // Assert
        Assert.NotNull(exception);
        Assert.Equal(message, exception.Message);
        Assert.Null(exception.InnerException);
    }
    
    /// <summary>
    /// Test that SqlExecutionException can be created with message and inner exception.
    /// </summary>
    [Fact]
    public void Constructor_WithMessageAndInnerException_ShouldCreateException()
    {
        // Arrange
        var message = "SQL execution failed";
        var innerException = new InvalidOperationException("Database error");
        
        // Act
        var exception = new SqlExecutionException(message, innerException);
        
        // Assert
        Assert.NotNull(exception);
        Assert.Equal(message, exception.Message);
        Assert.Equal(innerException, exception.InnerException);
    }
    
    /// <summary>
    /// Test that SqlExecutionException can include SQL statement details.
    /// </summary>
    [Fact]
    public void Constructor_WithSqlStatement_ShouldIncludeSqlInMessage()
    {
        // Arrange
        var sql = "SELECT * FROM nonexistent_table";
        var message = "Table not found";
        
        // Act
        var exception = new SqlExecutionException(message, sql);
        
        // Assert
        Assert.NotNull(exception);
        Assert.Contains(message, exception.Message);
        Assert.Contains(sql, exception.Message);
        Assert.Equal(sql, exception.SqlStatement);
    }
    
    /// <summary>
    /// Test that SqlExecutionException can include statement type information.
    /// </summary>
    [Fact]
    public void Constructor_WithStatementType_ShouldIncludeTypeInformation()
    {
        // Arrange
        var sql = "INSERT INTO users VALUES (1, 'John')";
        var message = "Duplicate primary key";
        var statementType = "INSERT";
        
        // Act
        var exception = new SqlExecutionException(message, sql, statementType);
        
        // Assert
        Assert.NotNull(exception);
        Assert.Contains(message, exception.Message);
        Assert.Contains(statementType, exception.Message);
        Assert.Equal(sql, exception.SqlStatement);
        Assert.Equal(statementType, exception.StatementType);
    }
    
    /// <summary>
    /// Test that SqlExecutionException inherits from Exception properly.
    /// </summary>
    [Fact]
    public void SqlExecutionException_ShouldInheritFromException()
    {
        // Arrange
        var exception = new SqlExecutionException("Test message");
        
        // Act & Assert
        Assert.IsAssignableFrom<Exception>(exception);
    }
    
    /// <summary>
    /// Test that SqlExecutionException can be thrown and caught correctly.
    /// </summary>
    [Fact]
    public void ThrowSqlExecutionException_ShouldBeCatchableAsException()
    {
        // Arrange
        var message = "SQL execution error";
        
        // Act & Assert
        Action action = () => throw new SqlExecutionException(message);
        var exception = Assert.Throws<SqlExecutionException>(action);
        
        Assert.Equal(message, exception.Message);
    }
    
    /// <summary>
    /// Test that SqlExecutionException can be caught as base Exception type.
    /// </summary>
    [Fact]
    public void ThrowSqlExecutionException_ShouldBeCatchableAsBaseException()
    {
        // Arrange
        var message = "SQL execution error";
        Exception caughtException = null!;
        
        // Act
        try
        {
            throw new SqlExecutionException(message);
        }
        catch (Exception ex)
        {
            caughtException = ex;
        }
        
        // Assert
        Assert.NotNull(caughtException);
        Assert.IsType<SqlExecutionException>(caughtException);
        Assert.Equal(message, caughtException.Message);
    }
    
    /// <summary>
    /// Test that SqlExecutionException provides meaningful error context.
    /// </summary>
    [Fact]
    public void SqlExecutionException_ShouldProvideErrorContext()
    {
        // Arrange
        var sql = "SELECT * FROM invalid_table";
        var message = "Table 'invalid_table' does not exist";
        var statementType = "SELECT";
        
        // Act
        var exception = new SqlExecutionException(message, sql, statementType);
        
        // Assert
        Assert.NotNull(exception.SqlStatement);
        Assert.NotNull(exception.StatementType);
        Assert.Contains("invalid_table", exception.Message);
        Assert.Contains("SELECT", exception.Message);
    }
}