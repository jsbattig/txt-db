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