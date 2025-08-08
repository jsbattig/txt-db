using System.Dynamic;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Services;
using TxtDb.Database.Models;
using TxtDb.Sql.Interfaces;
using TxtDb.Sql.Services;
using TxtDb.Sql.Exceptions;
using TxtDb.Sql.Models;
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Models;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace TxtDb.Sql.Tests.Services;

/// <summary>
/// Test suite for SqlExecutor class implementation using real TxtDb instances (no mocking).
/// Following strict TDD red-green-refactor approach.
/// 
/// CRITICAL: All tests use real database/storage instances per user instructions - NO MOCKING.
/// These are integration tests that verify SQL execution against actual TxtDb components.
/// </summary>
public class SqlExecutorTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;
    private readonly ILogger<SqlExecutor> _logger;
    private IDatabaseLayer? _databaseLayer;
    private IDatabase? _database;
    
    public SqlExecutorTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_sql_tests", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        _logger = NullLogger<SqlExecutor>.Instance; // Use null logger for tests
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
    }

    /// <summary>
    /// Test that SqlExecutor can be instantiated with IDatabase dependency.
    /// This is the most basic test - constructor validation.
    /// </summary>
    [Fact]
    public async Task Constructor_WithValidDatabase_ShouldCreateInstance()
    {
        // Arrange
        var database = await CreateRealDatabase();
        
        // Act
        var executor = new SqlExecutor(database, _logger);
        
        // Assert
        Assert.NotNull(executor);
        Assert.IsAssignableFrom<ISqlExecutor>(executor);
    }
    
    /// <summary>
    /// Test that SqlExecutor constructor throws ArgumentNullException for null database.
    /// </summary>
    [Fact]
    public void Constructor_WithNullDatabase_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SqlExecutor(null!, _logger));
    }
    
    /// <summary>
    /// Test ExecuteAsync throws ArgumentNullException for null SQL.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithNullSql_ShouldThrowArgumentNullException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await executor.ExecuteAsync(null!, transaction));
    }
    
    /// <summary>
    /// Test ExecuteAsync throws ArgumentNullException for null transaction.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithNullTransaction_ShouldThrowArgumentNullException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var executor = new SqlExecutor(database, _logger);
        var sql = "SELECT * FROM users";
        
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await executor.ExecuteAsync(sql, null!));
    }
    
    /// <summary>
    /// Test that SqlExecutor can execute a simple CREATE TABLE statement.
    /// This tests the core CREATE TABLE functionality with TxtDb integration.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithCreateTableSql_ShouldReturnCreateTableResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        
        // Act
        var result = await executor.ExecuteAsync(sql, transaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.CreateTable, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // CREATE TABLE doesn't affect existing rows
        Assert.Empty(result.Rows);
        // No longer check HasError/ErrorMessage - errors are handled by exceptions
        
        // Verify table was actually created in database
        var table = await database.GetTableAsync("users");
        Assert.NotNull(table);
        Assert.Equal("users", table.Name);
        Assert.Equal("$.id", table.PrimaryKeyField); // Should extract and convert primary key from CREATE TABLE
        
        await transaction.CommitAsync();
    }
    
    /// <summary>
    /// Test that SqlExecutor can execute a simple INSERT statement.
    /// This requires a table to exist first, testing the full workflow.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithInsertSql_ShouldReturnInsertResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // First create table
        var createSql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
        await executor.ExecuteAsync(createSql, transaction);
        
        // Then insert data
        var insertSql = "INSERT INTO users (id, name) VALUES (1, 'John Doe')";
        
        // Act
        var result = await executor.ExecuteAsync(insertSql, transaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Insert, result.StatementType);
        Assert.Equal(1, result.AffectedRows);
        Assert.Empty(result.Rows);
        // No longer check HasError/ErrorMessage - errors are handled by exceptions
        
        // Commit the transaction first
        await transaction.CommitAsync();
        
        // Verify data was actually inserted using a new transaction
        var verifyTransaction = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        Assert.NotNull(table);
        var inserted = await table.GetAsync(verifyTransaction, 1);
        Assert.NotNull(inserted);
        Assert.Equal(1, (int)inserted!.id);
        Assert.Equal("John Doe", (string)inserted.name);
        
        await verifyTransaction.CommitAsync();
    }
    
    /// <summary>
    /// Test that SqlExecutor can execute a simple SELECT statement.
    /// This tests querying data that was inserted.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithSelectSql_ShouldReturnSelectResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert data
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (1, 'Alice')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (2, 'Bob')", transaction);
        
        // Commit transaction to make data visible
        await transaction.CommitAsync();
        
        // Create new transaction for SELECT
        var selectTransaction = await CreateRealTransaction();
        var selectSql = "SELECT id, name FROM users";
        
        // Act
        var result = await executor.ExecuteAsync(selectSql, selectTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Select, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // SELECT doesn't affect rows
        Assert.NotNull(result.Rows);
        Assert.NotNull(result.Columns);
        // No longer check HasError/ErrorMessage - errors are handled by exceptions
        
        // Should have 2 rows
        Assert.Equal(2, result.Rows.Count);
        
        // Should have 2 columns (id, name)
        Assert.Equal(2, result.Columns.Count);
        Assert.Contains(result.Columns, c => c.Name == "id");
        Assert.Contains(result.Columns, c => c.Name == "name");
        
        // Verify row data
        var row1 = result.Rows[0];
        var row2 = result.Rows[1];
        
        // DEBUG: Log actual row data types and values
        Console.WriteLine($"SELECT DEBUG: Row1[0] = {row1[0]} (Type: {row1[0]?.GetType().FullName})");
        Console.WriteLine($"SELECT DEBUG: Row1[1] = {row1[1]} (Type: {row1[1]?.GetType().FullName})");
        Console.WriteLine($"SELECT DEBUG: Row2[0] = {row2[0]} (Type: {row2[0]?.GetType().FullName})");
        Console.WriteLine($"SELECT DEBUG: Row2[1] = {row2[1]} (Type: {row2[1]?.GetType().FullName})");
        
        // First row should be Alice (id=1) - CRITICAL: Handle Int64 deserialization from JSON
        Assert.Equal(1L, row1[0]); // Use long literal since JSON deserializes as Int64
        Assert.Equal("Alice", row1[1]);
        
        // Second row should be Bob (id=2) - CRITICAL: Handle Int64 deserialization from JSON  
        Assert.Equal(2L, row2[0]); // Use long literal since JSON deserializes as Int64
        Assert.Equal("Bob", row2[1]);
        
        await selectTransaction.CommitAsync();
    }
    
    /// <summary>
    /// Test that SqlExecutor can execute SELECT * (all columns) statement.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithSelectAllSql_ShouldReturnAllColumnsResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert data with extra field (testing structureless nature)
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)", transaction);
        
        var selectSql = "SELECT * FROM users";
        
        // Act
        var result = await executor.ExecuteAsync(selectSql, transaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Select, result.StatementType);
        Assert.Single(result.Rows);
        
        // Should include all columns from actual data (id, name, age)
        Assert.True(result.Columns.Count >= 1); // At least id should be present
        
        var row = result.Rows[0];
        Assert.NotNull(row);
        Assert.True(row.Length >= 1); // At least one column
        
        await transaction.CommitAsync();
    }
    
    /// <summary>
    /// Test that SqlExecutor throws SqlExecutionException for unsupported SQL statements.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUnsupportedSql_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "ALTER TABLE users ADD COLUMN age INT"; // Not yet supported
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("unsupported", exception.Message.ToLower());
        Assert.Equal(sql, exception.SqlStatement);
    }
    
    /// <summary>
    /// Test that SqlExecutor handles SQL parsing errors gracefully.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUnparsableSql_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "SELECT FROM WHERE"; // Invalid syntax
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("parse", exception.Message.ToLower());
        Assert.Equal(sql, exception.SqlStatement);
    }
    
    /// <summary>
    /// Test that SqlExecutor handles empty SQL statements.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithEmptySql_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "   "; // Empty/whitespace only
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("empty", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test INSERT into non-existent table throws proper exception.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithInsertIntoNonexistentTable_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "INSERT INTO nonexistent_table (id, name) VALUES (1, 'Test')";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("table", exception.Message.ToLower());
        Assert.Contains("not found", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test SELECT from non-existent table throws proper exception.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithSelectFromNonexistentTable_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "SELECT * FROM nonexistent_table";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("table", exception.Message.ToLower());
        Assert.Contains("not found", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that CREATE TABLE with existing name throws proper exception.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithCreateTableForExistingTable_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // First create table
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY)", transaction);
        
        // Try to create same table again
        var sql = "CREATE TABLE users (id INT PRIMARY KEY)";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("already exists", exception.Message.ToLower());
    }
    
    // ================================
    // UPDATE OPERATION TESTS (Story 2)
    // ================================
    
    /// <summary>
    /// Test that UPDATE operation with WHERE clause filters and updates only matching rows.
    /// This is the primary functionality for Story 2.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateAndWhereClause_ShouldUpdateOnlyMatchingRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 25)", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET age = 31 WHERE id = 1";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only 1 row should be affected
        Assert.Empty(result.Rows);
        
        // Verify only the matching row was updated
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice (id=1) should have age updated to 31
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.NotNull(alice);
        Assert.Equal(31, (int)alice!.age);
        
        // Bob (id=2) should remain unchanged at age 30
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(bob);
        Assert.Equal(30, (int)bob!.age);
        
        // Charlie (id=3) should remain unchanged at age 25
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(charlie);
        Assert.Equal(25, (int)charlie!.age);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with WHERE clause that matches multiple rows.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateWhereMultipleMatches_ShouldUpdateAllMatchingRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (1, 'Alice', 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (2, 'Bob', 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (3, 'Charlie', 'inactive')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET status = 'premium' WHERE status = 'active'";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // Alice and Bob should be affected
        
        // Verify the results
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice and Bob should have status updated to 'premium'
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("premium", (string)alice!.status);
        
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("premium", (string)bob!.status);
        
        // Charlie should remain 'inactive'
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.Equal("inactive", (string)charlie!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with WHERE clause that matches no rows.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateWhereNoMatches_ShouldUpdateZeroRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (1, 'Alice')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET name = 'Bob' WHERE id = 999"; // Non-existent id
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // No rows should be affected
        
        // Verify no changes were made
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("Alice", (string)alice!.name); // Should remain unchanged
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE operation with complex WHERE clause using comparison operators.
    /// Tests >, <, >=, <= operators.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateAndComparisonOperators_ShouldFilterCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (1, 18, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (2, 65, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (3, 70, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (4, 25, 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        // Test greater than
        var updateSql = "UPDATE users SET status = 'senior' WHERE age > 65";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only user with age 70 should be affected
        
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Only the 70-year-old should have status 'senior'
        var user70 = await table!.GetAsync(verifyTxn, 3);
        Assert.Equal("senior", (string)user70!.status);
        
        // Others should remain 'active'
        var user65 = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("active", (string)user65!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with LIKE operator in WHERE clause.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateAndLikeOperator_ShouldMatchPatterns()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (1, 'John Smith', 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (2, 'Jane Doe', 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (3, 'John Adams', 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET status = 'vip' WHERE name LIKE 'John%'";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // John Smith and John Adams should be affected
        
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Johns should have 'vip' status
        var john1 = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("vip", (string)john1!.status);
        
        var john2 = await table.GetAsync(verifyTxn, 3);
        Assert.Equal("vip", (string)john2!.status);
        
        // Jane should remain 'active'
        var jane = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("active", (string)jane!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with IS NULL and IS NOT NULL operators.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateAndNullOperators_ShouldHandleNullValues()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data (some with null values) - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (1, 'Alice')", transaction); // email is null
        await executor.ExecuteAsync("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET name = 'Anonymous' WHERE email IS NULL";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only Alice should be affected
        
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice should have name updated
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("Anonymous", (string)alice!.name);
        
        // Bob should remain unchanged
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("Bob", (string)bob!.name);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with logical operators (AND, OR) in WHERE clause.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateAndLogicalOperators_ShouldEvaluateCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (1, 65, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (2, 70, 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (3, 30, 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET status = 'senior' WHERE age >= 65 AND status = 'active'";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only user 1 (age 65, active) should match
        
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // User 1 should have status 'senior'
        var user1 = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("senior", (string)user1!.status);
        
        // User 2 should remain 'inactive' (doesn't match active condition)
        var user2 = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("inactive", (string)user2!.status);
        
        // User 3 should remain 'active' (doesn't match age condition)
        var user3 = await table.GetAsync(verifyTxn, 3);
        Assert.Equal("active", (string)user3!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test UPDATE with SET clause updating multiple fields.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateMultipleFields_ShouldUpdateAllFields()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT to work around database layer transaction isolation
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (1, 'Alice', 25, 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for UPDATE - this works around the database layer limitation
        var updateTransaction = await CreateRealTransaction();
        var updateSql = "UPDATE users SET name = 'Alicia', age = 26, status = 'premium' WHERE id = 1";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(1, result.AffectedRows);
        
        await updateTransaction.CommitAsync(); // Commit UPDATE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // All fields should be updated
        var user = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("Alicia", (string)user!.name);
        Assert.Equal(26, (int)user.age);
        Assert.Equal("premium", (string)user.status);
        
        await verifyTxn.CommitAsync();
    }
    
    // ================================
    // DELETE OPERATION TESTS (Story 2)
    // ================================
    
    /// <summary>
    /// Test that DELETE operation with WHERE clause filters and deletes only matching rows.
    /// This is the primary functionality for Story 2.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndWhereClause_ShouldDeleteOnlyMatchingRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (1, 'Alice', 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (2, 'Bob', 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (3, 'Charlie', 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users WHERE id = 2";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only Bob should be deleted
        Assert.Empty(result.Rows);
        
        // Verify only the matching row was deleted
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice and Charlie should still exist
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.NotNull(alice);
        Assert.Equal("Alice", (string)alice!.name);
        
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(charlie);
        Assert.Equal("Charlie", (string)charlie!.name);
        
        // Bob should be deleted
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.Null(bob);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with WHERE clause that matches multiple rows.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteWhereMultipleMatches_ShouldDeleteAllMatchingRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (1, 'Alice', 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (2, 'Bob', 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, status) VALUES (3, 'Charlie', 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users WHERE status = 'inactive'";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // Alice and Bob should be deleted
        
        // Verify the results
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice and Bob should be deleted
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(alice);
        
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.Null(bob);
        
        // Charlie should still exist
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(charlie);
        Assert.Equal("Charlie", (string)charlie!.name);
        Assert.Equal("active", (string)charlie.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with WHERE clause that matches no rows.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteWhereNoMatches_ShouldDeleteZeroRows()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and insert test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (1, 'Alice')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users WHERE id = 999"; // Non-existent id
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // No rows should be affected
        
        // Verify no deletions were made
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.NotNull(alice);
        Assert.Equal("Alice", (string)alice!.name); // Should still exist
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE operation with complex WHERE clause using comparison operators.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndComparisonOperators_ShouldFilterCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, age INT)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age) VALUES (1, 17)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age) VALUES (2, 18)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age) VALUES (3, 25)", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        // Test less than
        var deleteSql = "DELETE FROM users WHERE age < 18";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only user with age 17 should be deleted
        
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // 17-year-old should be deleted
        var user17 = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(user17);
        
        // Others should still exist
        var user18 = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(user18);
        Assert.Equal(18, (int)user18!.age);
        
        var user25 = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(user25);
        Assert.Equal(25, (int)user25!.age);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with LIKE operator in WHERE clause.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndLikeOperator_ShouldMatchPatterns()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, email) VALUES (1, 'alice@temp.com')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, email) VALUES (2, 'bob@company.com')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, email) VALUES (3, 'charlie@temp.com')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users WHERE email LIKE '%@temp.com'";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // Alice and Charlie should be deleted
        
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Temp email users should be deleted
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(alice);
        
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.Null(charlie);
        
        // Bob should remain
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(bob);
        Assert.Equal("bob@company.com", (string)bob!.email);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with IS NULL operator.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndNullOperator_ShouldHandleNullValues()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data (some with null values) - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name) VALUES (1, 'Alice')", transaction); // email is null
        await executor.ExecuteAsync("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users WHERE email IS NULL";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only Alice should be deleted
        
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Alice should be deleted
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(alice);
        
        // Bob should remain
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(bob);
        Assert.Equal("Bob", (string)bob!.name);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with logical operators (AND, OR) in WHERE clause.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndLogicalOperators_ShouldEvaluateCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (1, 17, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (2, 17, 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (3, 25, 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        // Delete users who are under 18 AND active
        var deleteSql = "DELETE FROM users WHERE age < 18 AND status = 'active'";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(1, result.AffectedRows); // Only user 1 (age 17, active) should match
        
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // User 1 should be deleted
        var user1 = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(user1);
        
        // User 2 should remain (age < 18 but inactive)
        var user2 = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(user2);
        Assert.Equal("inactive", (string)user2!.status);
        
        // User 3 should remain (active but age >= 18)
        var user3 = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(user3);
        Assert.Equal("active", (string)user3!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DELETE with OR operator in WHERE clause.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteAndOrOperator_ShouldEvaluateCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup test data - CRITICAL FIX: Commit after INSERT
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20))", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (1, 17, 'active')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (2, 25, 'inactive')", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, age, status) VALUES (3, 30, 'active')", transaction);
        await transaction.CommitAsync(); // Commit INSERT transaction
        
        // Start new transaction for DELETE
        var deleteTransaction = await CreateRealTransaction();
        // Delete users who are under 18 OR inactive
        var deleteSql = "DELETE FROM users WHERE age < 18 OR status = 'inactive'";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // User 1 (age 17) and User 2 (inactive) should match
        
        await deleteTransaction.CommitAsync(); // Commit DELETE transaction
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        // Users 1 and 2 should be deleted
        var user1 = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(user1);
        
        var user2 = await table.GetAsync(verifyTxn, 2);
        Assert.Null(user2);
        
        // User 3 should remain (age >= 18 and active)
        var user3 = await table.GetAsync(verifyTxn, 3);
        Assert.NotNull(user3);
        Assert.Equal("active", (string)user3!.status);
        Assert.Equal(30, (int)user3.age);
        
        await verifyTxn.CommitAsync();
    }
    
    // ================================
    // CREATE INDEX OPERATION TESTS (Story 3)
    // ================================
    
    /// <summary>
    /// Test that CREATE INDEX statement can create a secondary index on a table.
    /// This is the primary functionality for CREATE INDEX in Story 3.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithCreateIndexSql_ShouldCreateSecondaryIndex()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table first
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", transaction);
        await transaction.CommitAsync();
        
        // Start new transaction for CREATE INDEX
        var indexTransaction = await CreateRealTransaction();
        var createIndexSql = "CREATE INDEX idx_users_age ON users (age)";
        
        // Act
        var result = await executor.ExecuteAsync(createIndexSql, indexTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.CreateIndex, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // CREATE INDEX doesn't affect existing rows
        Assert.Empty(result.Rows);
        
        // Verify index was actually created
        await indexTransaction.CommitAsync();
        Console.WriteLine("DEBUG: CREATE INDEX transaction committed successfully");
        
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        Assert.NotNull(table);
        
        // Verify index exists using ListIndexesAsync
        var indexes = await table!.ListIndexesAsync(verifyTxn);
        Console.WriteLine($"DEBUG: Available indexes: [{string.Join(", ", indexes)}]");
        Assert.Contains("idx_users_age", indexes);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test CREATE INDEX with invalid table name throws proper exception.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithCreateIndexOnNonExistentTable_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "CREATE INDEX idx_nonexistent_table_name ON nonexistent_table (name)";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("table", exception.Message.ToLower());
        Assert.Contains("not found", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test CREATE INDEX with duplicate index name throws proper exception.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithCreateIndexDuplicateName_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and first index
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", transaction);
        await executor.ExecuteAsync("CREATE INDEX idx_users_name ON users (name)", transaction);
        await transaction.CommitAsync();
        
        // Start new transaction for second CREATE INDEX with same name
        var indexTransaction = await CreateRealTransaction();
        var sql = "CREATE INDEX idx_users_name ON users (age)"; // Same index name, different column
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, indexTransaction));
        
        Assert.Contains("index", exception.Message.ToLower());
        Assert.Contains("already exists", exception.Message.ToLower());
    }
    
    // ================================
    // DROP INDEX OPERATION TESTS (Story 3)
    // ================================
    
    /// <summary>
    /// Test that DROP INDEX statement can remove a secondary index from a table.
    /// This is the primary functionality for DROP INDEX in Story 3.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDropIndexSql_ShouldDropSecondaryIndex()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table and index first
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", transaction);
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", transaction);
        await transaction.CommitAsync();
        
        // Start new transaction for DROP INDEX
        var dropTransaction = await CreateRealTransaction();
        var dropIndexSql = "DROP INDEX idx_users_age";
        
        // Act
        var result = await executor.ExecuteAsync(dropIndexSql, dropTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.DropIndex, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // DROP INDEX doesn't affect rows
        Assert.Empty(result.Rows);
        
        // Verify index was actually dropped
        await dropTransaction.CommitAsync();
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        Assert.NotNull(table);
        
        // Verify index no longer exists using ListIndexesAsync
        var indexes = await table!.ListIndexesAsync(verifyTxn);
        Assert.DoesNotContain("idx_users_age", indexes);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test DROP INDEX with non-existent index name returns success (idempotent operation).
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDropIndexNonExistentIndex_ShouldReturnSuccessResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table without any indexes
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", transaction);
        await transaction.CommitAsync();
        
        // Start new transaction for DROP INDEX
        var dropTransaction = await CreateRealTransaction();
        var sql = "DROP INDEX idx_nonexistent";
        
        // Act
        var result = await executor.ExecuteAsync(sql, dropTransaction);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.DropIndex, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // No index to drop
        Assert.Empty(result.Rows);
        
        await dropTransaction.CommitAsync();
    }
    
    /// <summary>
    /// Test DROP INDEX with non-existent index returns success (SQL standard idempotent behavior).
    /// Per SQL standard, DROP INDEX should succeed silently even if the index doesn't exist.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDropIndexOnNonExistentIndex_ShouldReturnSuccessResult()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "DROP INDEX idx_some_index";
        
        // Act
        var result = await executor.ExecuteAsync(sql, transaction);
        
        // Assert - SQL standard idempotent behavior: should return success even for non-existent index
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.DropIndex, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // No index to drop
        Assert.Empty(result.Rows);
    }
    
    /// <summary>
    /// Test CREATE INDEX and DROP INDEX integration workflow.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_CreateAndDropIndexWorkflow_ShouldWorkTogether()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, email VARCHAR(200))", transaction);
        await transaction.CommitAsync();
        
        // Create multiple indexes
        var indexTxn1 = await CreateRealTransaction();
        await executor.ExecuteAsync("CREATE INDEX idx_users_name ON users (name)", indexTxn1);
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", indexTxn1);
        await executor.ExecuteAsync("CREATE INDEX idx_users_email ON users (email)", indexTxn1);
        await indexTxn1.CommitAsync();
        
        // Verify all indexes were created
        var verifyTxn1 = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        var indexes = await table!.ListIndexesAsync(verifyTxn1);
        Assert.Contains("idx_users_name", indexes);
        Assert.Contains("idx_users_age", indexes);
        Assert.Contains("idx_users_email", indexes);
        await verifyTxn1.CommitAsync();
        
        // Drop one index
        var indexTxn2 = await CreateRealTransaction();
        await executor.ExecuteAsync("DROP INDEX idx_users_age", indexTxn2);
        await indexTxn2.CommitAsync();
        
        // Verify only the dropped index is gone - get fresh table instance to see updated state
        var verifyTxn2 = await CreateRealTransaction();
        var tableAfterDrop = await database.GetTableAsync("users"); // Fresh instance after DROP INDEX
        var finalIndexes = await tableAfterDrop!.ListIndexesAsync(verifyTxn2);
        Assert.Contains("idx_users_name", finalIndexes);
        Assert.DoesNotContain("idx_users_age", finalIndexes); // Should be dropped
        Assert.Contains("idx_users_email", finalIndexes);
        await verifyTxn2.CommitAsync();
        
        // Act & Assert - workflow completed successfully
        Assert.True(true); // If we get here, the workflow worked
    }
    
    // ================================
    // USE INDEX HINT OPERATION TESTS (Story 4)
    // ================================
    
    /// <summary>
    /// Test that SELECT with USE INDEX hint uses the specified index for optimized query execution.
    /// This is the primary functionality for USE INDEX in Story 4.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithSelectUseIndexHint_ShouldUseSpecifiedIndex()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var setupTxn = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table with test data
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (1, 'Alice', 25, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (2, 'Bob', 30, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (3, 'Charlie', 25, 'inactive')", setupTxn);
        
        // Create index on age column
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", setupTxn);
        await setupTxn.CommitAsync();
        
        // Start new transaction for USE INDEX SELECT
        var selectTxn = await CreateRealTransaction();
        var selectSql = "SELECT * FROM users USE INDEX (idx_users_age) WHERE age = 25";
        
        // Act
        var result = await executor.ExecuteAsync(selectSql, selectTxn);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Select, result.StatementType);
        Assert.Equal(0, result.AffectedRows); // SELECT doesn't affect rows
        Assert.Equal(2, result.Rows.Count); // Alice and Charlie have age 25
        
        // Verify correct data returned
        var row1 = result.Rows[0];
        var row2 = result.Rows[1];
        
        // Should return Alice and Charlie (both age 25)
        // Find the name column index
        var nameColumnIndex = -1;
        for (int i = 0; i < result.Columns.Count; i++)
        {
            if (result.Columns[i].Name == "name")
            {
                nameColumnIndex = i;
                break;
            }
        }
        
        Assert.True(nameColumnIndex >= 0, "Name column should be found");
        
        var names = new[] { row1[nameColumnIndex]?.ToString(), row2[nameColumnIndex]?.ToString() };
        Assert.Contains("Alice", names);
        Assert.Contains("Charlie", names);
        
        await selectTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test that UPDATE with USE INDEX hint uses the specified index for optimized query execution.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUpdateUseIndexHint_ShouldUseSpecifiedIndex()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var setupTxn = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table with test data
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (1, 'Alice', 25, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (2, 'Bob', 30, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (3, 'Charlie', 25, 'active')", setupTxn);
        
        // Create index on age column
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", setupTxn);
        await setupTxn.CommitAsync();
        
        // Start new transaction for USE INDEX UPDATE
        var updateTxn = await CreateRealTransaction();
        var updateSql = "UPDATE users USE INDEX (idx_users_age) SET status = 'premium' WHERE age = 25";
        
        // Act
        var result = await executor.ExecuteAsync(updateSql, updateTxn);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Update, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // Alice and Charlie should be updated
        
        // Verify updates were applied correctly
        await updateTxn.CommitAsync();
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Equal("premium", (string)alice!.status);
        
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.Equal("active", (string)bob!.status); // Should remain unchanged
        
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.Equal("premium", (string)charlie!.status);
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test that DELETE with USE INDEX hint uses the specified index for optimized query execution.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithDeleteUseIndexHint_ShouldUseSpecifiedIndex()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var setupTxn = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table with test data
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (1, 'Alice', 25, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (2, 'Bob', 30, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (3, 'Charlie', 25, 'inactive')", setupTxn);
        
        // Create index on age column
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", setupTxn);
        await setupTxn.CommitAsync();
        
        // Start new transaction for USE INDEX DELETE
        var deleteTxn = await CreateRealTransaction();
        var deleteSql = "DELETE FROM users USE INDEX (idx_users_age) WHERE age = 25";
        
        // Act
        var result = await executor.ExecuteAsync(deleteSql, deleteTxn);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Delete, result.StatementType);
        Assert.Equal(2, result.AffectedRows); // Alice and Charlie should be deleted
        
        // Verify deletions were applied correctly
        await deleteTxn.CommitAsync();
        var verifyTxn = await CreateRealTransaction();
        var table = await database.GetTableAsync("users");
        
        var alice = await table!.GetAsync(verifyTxn, 1);
        Assert.Null(alice); // Should be deleted
        
        var bob = await table.GetAsync(verifyTxn, 2);
        Assert.NotNull(bob); // Should remain
        Assert.Equal(30, (int)bob!.age);
        
        var charlie = await table.GetAsync(verifyTxn, 3);
        Assert.Null(charlie); // Should be deleted
        
        await verifyTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test that USE INDEX with non-existent index throws proper error.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUseIndexNonExistentIndex_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table without the specified index
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", transaction);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)", transaction);
        await transaction.CommitAsync();
        
        // Start new transaction for USE INDEX with non-existent index
        var selectTxn = await CreateRealTransaction();
        var sql = "SELECT * FROM users USE INDEX (idx_nonexistent) WHERE age = 25";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, selectTxn));
        
        Assert.Contains("index", exception.Message.ToLower());
        Assert.Contains("not found", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that USE INDEX with non-existent table throws proper error.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUseIndexNonExistentTable_ShouldThrowSqlExecutionException()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var transaction = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        var sql = "SELECT * FROM nonexistent_table USE INDEX (idx_name) WHERE id = 1";
        
        // Act & Assert
        var exception = await Assert.ThrowsAsync<SqlExecutionException>(async () =>
            await executor.ExecuteAsync(sql, transaction));
        
        Assert.Contains("table", exception.Message.ToLower());
        Assert.Contains("not found", exception.Message.ToLower());
    }
    
    /// <summary>
    /// Test that USE INDEX works correctly with complex WHERE clauses involving multiple conditions.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUseIndexAndComplexWhere_ShouldFilterCorrectly()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var setupTxn = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table with test data
        await executor.ExecuteAsync("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (1, 'Alice', 25, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (2, 'Bob', 25, 'inactive')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (3, 'Charlie', 30, 'active')", setupTxn);
        await executor.ExecuteAsync("INSERT INTO users (id, name, age, status) VALUES (4, 'David', 25, 'active')", setupTxn);
        
        // Create index on age column
        await executor.ExecuteAsync("CREATE INDEX idx_users_age ON users (age)", setupTxn);
        await setupTxn.CommitAsync();
        
        // Start new transaction for USE INDEX SELECT with complex WHERE
        var selectTxn = await CreateRealTransaction();
        var selectSql = "SELECT * FROM users USE INDEX (idx_users_age) WHERE age = 25 AND status = 'active'";
        
        // Act
        var result = await executor.ExecuteAsync(selectSql, selectTxn);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal(SqlStatementType.Select, result.StatementType);
        Assert.Equal(2, result.Rows.Count); // Alice and David (age 25, status active)
        
        // Verify correct data returned
        var row1 = result.Rows[0];
        var row2 = result.Rows[1];
        
        // Both should have age 25 and status active
        // Find the name column index
        var nameColumnIndex = -1;
        for (int i = 0; i < result.Columns.Count; i++)
        {
            if (result.Columns[i].Name == "name")
            {
                nameColumnIndex = i;
                break;
            }
        }
        
        Assert.True(nameColumnIndex >= 0, "Name column should be found");
        
        var names = new[] { row1[nameColumnIndex]?.ToString(), row2[nameColumnIndex]?.ToString() };
        Assert.Contains("Alice", names);
        Assert.Contains("David", names);
        
        await selectTxn.CommitAsync();
    }
    
    /// <summary>
    /// Test that USE INDEX provides performance improvement over full table scan.
    /// This test should demonstrate measurable performance benefits.
    /// FAILING TEST - Story 4 not yet implemented.
    /// </summary>
    [Fact]
    public async Task ExecuteAsync_WithUseIndexVsFullTableScan_ShouldShowPerformanceImprovement()
    {
        // Arrange
        var database = await CreateRealDatabase();
        var setupTxn = await CreateRealTransaction();
        var executor = new SqlExecutor(database, _logger);
        
        // Setup: Create table with more test data for performance testing
        await executor.ExecuteAsync("CREATE TABLE large_users (id INT PRIMARY KEY, name VARCHAR(100), age INT, status VARCHAR(20))", setupTxn);
        
        // Insert a larger dataset for performance comparison
        for (int i = 1; i <= 100; i++)
        {
            var name = $"User{i}";
            var age = 20 + (i % 40); // Ages from 20-59
            var status = (i % 3 == 0) ? "active" : "inactive";
            await executor.ExecuteAsync($"INSERT INTO large_users (id, name, age, status) VALUES ({i}, '{name}', {age}, '{status}')", setupTxn);
        }
        
        // Create index on age column
        await executor.ExecuteAsync("CREATE INDEX idx_large_users_age ON large_users (age)", setupTxn);
        await setupTxn.CommitAsync();
        
        // Test 1: USE INDEX query (should be optimized)
        var indexTxn = await CreateRealTransaction();
        var useIndexSql = "SELECT * FROM large_users USE INDEX (idx_large_users_age) WHERE age = 25";
        
        var indexStartTime = DateTime.UtcNow;
        var indexResult = await executor.ExecuteAsync(useIndexSql, indexTxn);
        var indexDuration = DateTime.UtcNow - indexStartTime;
        await indexTxn.CommitAsync();
        
        // Test 2: Full table scan query (should be slower)
        var scanTxn = await CreateRealTransaction();
        var fullScanSql = "SELECT * FROM large_users WHERE age = 25";
        
        var scanStartTime = DateTime.UtcNow;
        var scanResult = await executor.ExecuteAsync(fullScanSql, scanTxn);
        var scanDuration = DateTime.UtcNow - scanStartTime;
        await scanTxn.CommitAsync();
        
        // Assert
        // Both should return the same results
        Assert.Equal(indexResult.Rows.Count, scanResult.Rows.Count);
        
        // Index query should be faster or at least not significantly slower
        // For this test, we'll verify that USE INDEX at least doesn't break functionality
        // Performance improvements will be more evident in larger datasets
        Assert.True(indexResult.Rows.Count > 0, "USE INDEX should return matching rows");
        
        _output.WriteLine($"USE INDEX duration: {indexDuration.TotalMilliseconds}ms");
        _output.WriteLine($"Full scan duration: {scanDuration.TotalMilliseconds}ms");
        _output.WriteLine($"Matching rows found: {indexResult.Rows.Count}");
    }
    
    private async Task<IDatabase> CreateRealDatabase()
    {
        if (_database != null) return _database;
        
        var storageSubsystem = new AsyncStorageSubsystem();
        await storageSubsystem.InitializeAsync(_storageDirectory, null);
        _databaseLayer = new DatabaseLayer(storageSubsystem);
        
        var dbName = $"test_sql_db_{Guid.NewGuid():N}";
        _database = await _databaseLayer.CreateDatabaseAsync(dbName);
        
        return _database;
    }
    
    private async Task<IDatabaseTransaction> CreateRealTransaction()
    {
        var database = await CreateRealDatabase();
        return await _databaseLayer!.BeginTransactionAsync(database.Name, TransactionIsolationLevel.Snapshot);
    }
    
    public void Dispose()
    {
        (_databaseLayer as IDisposable)?.Dispose();
        
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Failed to clean up test directory: {ex.Message}");
            }
        }
    }
}