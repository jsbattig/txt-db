using System.Dynamic;
using TxtDb.Database.Interfaces;
using TxtDb.Database.Exceptions;
using TxtDb.Database.Models;
using TxtDb.Sql.Interfaces;
using TxtDb.Sql.Exceptions;
using TxtDb.Sql.Models;
using TxtDb.Sql.Filters;
using Microsoft.Extensions.Logging;

namespace TxtDb.Sql.Services;

/// <summary>
/// SQL executor implementation that integrates with TxtDb database operations.
/// Provides a bridge between SQL statements and TxtDb's document-oriented storage.
/// 
/// CRITICAL: TxtDb tables are STRUCTURELESS except for primary key requirements.
/// SQL CREATE TABLE only needs to identify the primary key field - other column definitions are ignored.
/// INSERT/SELECT can work with any JSON fields, not limited by CREATE TABLE definitions.
/// </summary>
public class SqlExecutor : ISqlExecutor
{
    private readonly IDatabase _database;
    private readonly SqlStatementParser _parser;
    private readonly ILogger<SqlExecutor> _logger;
    
    /// <summary>
    /// Initializes a new instance of SqlExecutor with the specified database and logger.
    /// </summary>
    /// <param name="database">TxtDb database instance</param>
    /// <param name="logger">Logger for tracking SQL execution operations</param>
    /// <exception cref="ArgumentNullException">Thrown when database or logger is null</exception>
    public SqlExecutor(IDatabase database, ILogger<SqlExecutor> logger)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _parser = new SqlStatementParser();
    }
    
    /// <summary>
    /// Executes a SQL statement within the context of a database transaction.
    /// </summary>
    /// <param name="sql">SQL statement to execute</param>
    /// <param name="transaction">Active database transaction</param>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    /// <returns>Result of the SQL execution</returns>
    /// <exception cref="ArgumentNullException">Thrown when sql or transaction is null</exception>
    /// <exception cref="SqlExecutionException">Thrown when SQL cannot be parsed or executed</exception>
    public async Task<ISqlResult> ExecuteAsync(
        string sql, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken = default)
    {
        if (sql == null)
            throw new ArgumentNullException(nameof(sql));
        if (transaction == null)
            throw new ArgumentNullException(nameof(transaction));
        
        try
        {
            // Parse SQL statement using existing parser
            var statement = _parser.Parse(sql);
            Console.WriteLine($"EXECUTOR DEBUG: Parsed SQL '{sql}' as type {statement.Type}");
            
            // Execute based on statement type
            if (statement.Type == SqlStatementType.Update)
            {
                Console.WriteLine("EXECUTOR DEBUG: Routing to ExecuteUpdateAsync");
                return await ExecuteUpdateAsync(statement, transaction, cancellationToken);
            }
            
            return statement.Type switch
            {
                SqlStatementType.CreateTable => await ExecuteCreateTableAsync(statement, transaction, cancellationToken),
                SqlStatementType.Insert => await ExecuteInsertAsync(statement, transaction, cancellationToken),
                SqlStatementType.Select => await ExecuteSelectAsync(statement, transaction, cancellationToken),
                SqlStatementType.Delete => await ExecuteDeleteAsync(statement, transaction, cancellationToken),
                SqlStatementType.AlterTable => throw new SqlExecutionException("ALTER TABLE statements are unsupported", sql, statement.Type.ToString()),
                SqlStatementType.DropTable => throw new SqlExecutionException("DROP TABLE statements are not yet supported", sql, statement.Type.ToString()),
                _ => throw new SqlExecutionException($"Unsupported SQL statement type: {statement.Type}", sql, statement.Type.ToString())
            };
        }
        catch (SqlExecutionException)
        {
            // Re-throw SQL execution exceptions as-is
            throw;
        }
        catch (Exception ex)
        {
            // Wrap any other exceptions in SqlExecutionException
            throw new SqlExecutionException($"Failed to execute SQL: {ex.Message}", sql);
        }
    }
    
    /// <summary>
    /// Executes CREATE TABLE statement.
    /// CRITICAL: In TxtDb, CREATE TABLE only needs to identify the primary key.
    /// All other column definitions are ignored (structureless storage).
    /// </summary>
    private async Task<ISqlResult> ExecuteCreateTableAsync(
        SqlStatement statement, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Find primary key column from parsed definition
            var primaryKeyColumn = statement.Columns.FirstOrDefault(c => c.IsPrimaryKey);
            if (primaryKeyColumn == null)
            {
                throw new SqlExecutionException(
                    "CREATE TABLE requires a PRIMARY KEY column", 
                    $"CREATE TABLE {statement.TableName}");
            }
            
            // Convert primary key column name to JSON path (TxtDb format)
            var primaryKeyPath = $"$.{primaryKeyColumn.Name}";
            
            // Create table in database
            await _database.CreateTableAsync(statement.TableName, primaryKeyPath, cancellationToken);
            
            return new SqlResult
            {
                StatementType = SqlStatementType.CreateTable,
                AffectedRows = 0, // CREATE TABLE doesn't affect existing rows
                Columns = Array.Empty<SqlColumnInfo>(),
                Rows = Array.Empty<object[]>()
            };
        }
        catch (TableAlreadyExistsException)
        {
            throw new SqlExecutionException(
                $"Table '{statement.TableName}' already exists", 
                $"CREATE TABLE {statement.TableName}",
                "CREATE TABLE");
        }
        catch (Exception ex) when (!(ex is SqlExecutionException))
        {
            throw new SqlExecutionException(
                $"Failed to create table '{statement.TableName}': {ex.Message}", 
                $"CREATE TABLE {statement.TableName}",
                "CREATE TABLE");
        }
    }
    
    /// <summary>
    /// Executes INSERT statement.
    /// CRITICAL: TxtDb allows inserting any JSON structure as long as primary key exists.
    /// </summary>
    private async Task<ISqlResult> ExecuteInsertAsync(
        SqlStatement statement, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Get table
            var table = await _database.GetTableAsync(statement.TableName, cancellationToken);
            if (table == null)
            {
                throw new SqlExecutionException(
                    $"Table '{statement.TableName}' not found", 
                    $"INSERT INTO {statement.TableName}");
            }
            
            // Create dynamic object from values
            var obj = new ExpandoObject() as IDictionary<string, object>;
            foreach (var kvp in statement.Values)
            {
                obj[kvp.Key] = kvp.Value;
            }
            
            // Insert object into table
            await table.InsertAsync(transaction, obj as dynamic, cancellationToken);
            
            return new SqlResult
            {
                StatementType = SqlStatementType.Insert,
                AffectedRows = 1,
                Columns = Array.Empty<SqlColumnInfo>(),
                Rows = Array.Empty<object[]>()
            };
        }
        catch (Exception ex) when (!(ex is SqlExecutionException))
        {
            throw new SqlExecutionException(
                $"Failed to insert into table '{statement.TableName}': {ex.Message}", 
                $"INSERT INTO {statement.TableName}");
        }
    }
    
    /// <summary>
    /// Executes SELECT statement.
    /// CRITICAL: TxtDb stores structureless objects, so SELECT can access any JSON fields.
    /// </summary>
    private async Task<ISqlResult> ExecuteSelectAsync(
        SqlStatement statement, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Get table
            var table = await _database.GetTableAsync(statement.TableName, cancellationToken);
            if (table == null)
            {
                throw new SqlExecutionException(
                    $"Table '{statement.TableName}' not found", 
                    $"SELECT FROM {statement.TableName}");
            }
            
            // For now, implement simple full table scan
            // TODO: Add WHERE clause support and index usage
            var allObjects = await GetAllObjectsFromTable(table, transaction, cancellationToken);
            
            _logger.LogDebug("SELECT: Found {ObjectCount} objects from table '{TableName}'", allObjects.Count, statement.TableName);
            
            // Build column info and rows
            var columns = new List<SqlColumnInfo>();
            var rows = new List<object[]>();
            
            if (allObjects.Count > 0)
            {
                // Determine columns based on SELECT statement and actual data
                var columnNames = DetermineColumnNames(statement, allObjects);
                
                _logger.LogDebug("SELECT: Column names: [{ColumnNames}]", string.Join(", ", columnNames));
                
                // Build column info
                foreach (var columnName in columnNames)
                {
                    columns.Add(new SqlColumnInfo
                    {
                        Name = columnName,
                        DataType = "DYNAMIC", // TxtDb is dynamically typed
                        IsPrimaryKey = columnName == table.PrimaryKeyField,
                        IsNullable = true
                    });
                }
                
                // Build rows
                foreach (var obj in allObjects)
                {
                    var row = new object[columnNames.Count];
                    var objDict = obj as IDictionary<string, object>;
                    
                    // DEBUG: Log object structure
                    if (objDict != null)
                    {
                        var objKeys = string.Join(", ", objDict.Keys);
                        _logger.LogDebug("SELECT: Object keys: [{ObjectKeys}]", objKeys);
                    }
                    
                    for (int i = 0; i < columnNames.Count; i++)
                    {
                        var columnName = columnNames[i];
                        row[i] = objDict?.ContainsKey(columnName) == true ? objDict[columnName] : null!;
                    }
                    
                    rows.Add(row);
                }
            }
            
            var result = new SqlResult
            {
                StatementType = SqlStatementType.Select,
                AffectedRows = 0, // SELECT doesn't affect rows
                Columns = columns.AsReadOnly(),
                Rows = rows.AsReadOnly()
            };
            
            _logger.LogDebug("SELECT: Returning {RowCount} rows, {ColumnCount} columns", result.Rows.Count, result.Columns.Count);
            
            return result;
        }
        catch (Exception ex) when (!(ex is SqlExecutionException))
        {
            throw new SqlExecutionException(
                $"Failed to select from table '{statement.TableName}': {ex.Message}", 
                $"SELECT FROM {statement.TableName}");
        }
    }
    
    /// <summary>
    /// Gets all objects from a table using QueryAsync with a match-all filter.
    /// </summary>
    private async Task<List<dynamic>> GetAllObjectsFromTable(
        ITable table, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        // Use QueryAsync with a match-all filter
        var matchAllFilter = new MatchAllQueryFilter();
        var results = await table.QueryAsync(transaction, matchAllFilter, cancellationToken);
        return results.ToList();
    }
    
    /// <summary>
    /// Determines column names for SELECT based on statement and actual data.
    /// </summary>
    private List<string> DetermineColumnNames(SqlStatement statement, List<dynamic> objects)
    {
        var columnNames = new List<string>();
        
        if (statement.SelectAllColumns)
        {
            // For SELECT *, discover all columns from actual data
            var allColumnNames = new HashSet<string>();
            foreach (var obj in objects)
            {
                if (obj is IDictionary<string, object> dict)
                {
                    foreach (var key in dict.Keys)
                    {
                        allColumnNames.Add(key);
                    }
                }
            }
            columnNames.AddRange(allColumnNames.OrderBy(c => c));
        }
        else
        {
            // Use specified columns
            columnNames.AddRange(statement.SelectColumns);
        }
        
        return columnNames;
    }
    
    /// <summary>
    /// Executes UPDATE statement with WHERE clause filtering.
    /// Updates only objects that match the WHERE conditions.
    /// </summary>
    private async Task<ISqlResult> ExecuteUpdateAsync(
        SqlStatement statement, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        Console.WriteLine($"EXECUTOR DEBUG: ExecuteUpdateAsync called for table '{statement.TableName}' with {statement.SetValues.Count} SET values");
        try
        {
            // Get table
            var table = await _database.GetTableAsync(statement.TableName, cancellationToken);
            if (table == null)
            {
                throw new SqlExecutionException(
                    $"Table '{statement.TableName}' not found", 
                    $"UPDATE {statement.TableName}");
            }
            
            // Create WHERE clause filter
            var whereFilter = new WhereClauseFilter(statement.WhereExpression, $"UPDATE {statement.TableName}");
            
            // Query for objects that match the WHERE clause
            var matchingObjects = await table.QueryAsync(transaction, whereFilter, cancellationToken);
            
            _logger.LogDebug("UPDATE: Found {ObjectCount} objects matching WHERE clause from table '{TableName}'", 
                matchingObjects.Count, statement.TableName);
            
            int affectedRows = 0;
            
            // Update each matching object
            foreach (var obj in matchingObjects)
            {
                // Extract primary key from existing object
                var objDict = obj as IDictionary<string, object>;
                if (objDict == null) continue;
                
                var primaryKeyField = table.PrimaryKeyField.TrimStart('$', '.');
                if (!objDict.TryGetValue(primaryKeyField, out var rawPrimaryKeyValue)) continue;
                
                // CRITICAL FIX: Handle type conversion for primary keys that get deserialized as Int64 but were stored as Int32
                // This is the same logic as Table.ExtractFieldValue to ensure consistent type handling
                object primaryKeyValue = rawPrimaryKeyValue;
                if (rawPrimaryKeyValue is long longValue && longValue >= int.MinValue && longValue <= int.MaxValue)
                {
                    primaryKeyValue = (int)longValue;
                    _logger.LogDebug("UPDATE: Converted primary key from Int64({LongValue}) to Int32({PrimaryKeyValue})", longValue, primaryKeyValue);
                    // Update the object to have the correct type as well
                    objDict[primaryKeyField] = primaryKeyValue;
                }
                
                _logger.LogDebug("UPDATE: Processing primary key {PrimaryKeyValue} (Type: {PrimaryKeyType})", 
                    primaryKeyValue, primaryKeyValue?.GetType().FullName);
                
                // Apply SET clause values directly to the existing object to preserve all type information
                // This avoids any type conversion issues that might occur when copying to a new ExpandoObject
                Console.WriteLine($"UPDATE DEBUG: Processing {statement.SetValues.Count} SET values");
                foreach (var setKvp in statement.SetValues)
                {
                    // CRITICAL FIX: Extract the actual field name from the SqlParserCS key format
                    string fieldName = ExtractFieldNameFromKey(setKvp.Key);
                    
                    // DEBUG: Console output for immediate visibility
                    Console.WriteLine($"UPDATE DEBUG: Raw key: '{setKvp.Key}' (Type: {setKvp.Key?.GetType().FullName}) -> Field name: '{fieldName}', Value: '{setKvp.Value}' (Type: {setKvp.Value?.GetType().FullName})");
                    
                    // Skip primary key field to avoid type conversion issues
                    if (fieldName == primaryKeyField)
                    {
                        Console.WriteLine($"UPDATE DEBUG: Skipping primary key field '{primaryKeyField}' in SET clause");
                        _logger.LogWarning("UPDATE: Skipping primary key field '{PrimaryKeyField}' in SET clause", primaryKeyField);
                        continue;
                    }
                    
                    // DEBUG: Console output for before and after values
                    var oldValue = objDict.ContainsKey(fieldName) ? objDict[fieldName] : "<not found>";
                    Console.WriteLine($"UPDATE DEBUG: Field '{fieldName}' BEFORE: '{oldValue}'");
                    objDict[fieldName] = setKvp.Value;
                    Console.WriteLine($"UPDATE DEBUG: Field '{fieldName}' AFTER: '{objDict[fieldName]}'");
                    
                    _logger.LogDebug("UPDATE: Field '{FieldName}' changed from '{OldValue}' to '{NewValue}'", 
                        fieldName, oldValue, setKvp.Value);
                }
                
                // IMPLEMENTATION: Call table.UpdateAsync() to persist the updated object
                // Handle the known database layer index consistency issue gracefully
                Console.WriteLine($"UPDATE DEBUG: About to call table.UpdateAsync with primaryKeyValue: {primaryKeyValue}");
                Console.WriteLine($"UPDATE DEBUG: Object properties after modification: {string.Join(", ", objDict.Select(kvp => $"{kvp.Key}={kvp.Value}"))}");
                try
                {
                    if (primaryKeyValue != null)
                    {
                        await table.UpdateAsync(transaction, primaryKeyValue, obj as dynamic, cancellationToken);
                        Console.WriteLine($"UPDATE DEBUG: Successfully called table.UpdateAsync for primary key {primaryKeyValue}");
                        _logger.LogDebug("UPDATE: Successfully updated object with primary key {PrimaryKeyValue}", primaryKeyValue);
                        affectedRows++;
                    }
                    else
                    {
                        Console.WriteLine("UPDATE DEBUG: Primary key value is null, skipping object");
                        _logger.LogWarning("UPDATE: Primary key value is null, skipping object");
                    }
                }
                catch (TxtDb.Database.Exceptions.ObjectNotFoundException ex)
                {
                    // CRITICAL FIX: Do not count failed updates as successful
                    // This was masking the real issue - updates are actually failing
                    Console.WriteLine($"UPDATE DEBUG: ObjectNotFoundException for primary key {primaryKeyValue}: {ex.Message}");
                    _logger.LogError("UPDATE: Object with primary key {PrimaryKeyValue} not found for update: {UpdateError}", 
                        primaryKeyValue, ex.Message);
                    
                    // DO NOT increment affectedRows for failed updates - this was the core bug
                    // affectedRows++;  // REMOVED - this was causing false success reports
                }
                catch (Exception updateEx)
                {
                    _logger.LogError("UPDATE: Failed to update object with primary key {PrimaryKeyValue}: {UpdateError}", 
                        primaryKeyValue, updateEx.Message);
                    
                    // Don't count as successful for unexpected errors
                }
            }
            
            _logger.LogDebug("UPDATE: Updated {AffectedRows} rows in table '{TableName}'", affectedRows, statement.TableName);
            
            return new SqlResult
            {
                StatementType = SqlStatementType.Update,
                AffectedRows = affectedRows,
                Columns = Array.Empty<SqlColumnInfo>(),
                Rows = Array.Empty<object[]>()
            };
        }
        catch (Exception ex) when (!(ex is SqlExecutionException))
        {
            throw new SqlExecutionException(
                $"Failed to update table '{statement.TableName}': {ex.Message}", 
                $"UPDATE {statement.TableName}");
        }
    }
    
    /// <summary>
    /// Executes DELETE statement with WHERE clause filtering.
    /// Deletes only objects that match the WHERE conditions.
    /// </summary>
    private async Task<ISqlResult> ExecuteDeleteAsync(
        SqlStatement statement, 
        IDatabaseTransaction transaction, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Get table
            var table = await _database.GetTableAsync(statement.TableName, cancellationToken);
            if (table == null)
            {
                throw new SqlExecutionException(
                    $"Table '{statement.TableName}' not found", 
                    $"DELETE FROM {statement.TableName}");
            }
            
            // Create WHERE clause filter
            var whereFilter = new WhereClauseFilter(statement.WhereExpression, $"DELETE FROM {statement.TableName}");
            
            // Query for objects that match the WHERE clause
            var matchingObjects = await table.QueryAsync(transaction, whereFilter, cancellationToken);
            
            Console.WriteLine($"DELETE DEBUG: Found {matchingObjects.Count} objects matching WHERE clause from table '{statement.TableName}'");
            _logger.LogDebug("DELETE: Found {ObjectCount} objects matching WHERE clause from table '{TableName}'", 
                matchingObjects.Count, statement.TableName);
            
            int affectedRows = 0;
            
            // Delete each matching object
            foreach (var obj in matchingObjects)
            {
                // Extract primary key from object
                var objDict = obj as IDictionary<string, object>;
                if (objDict == null) continue;
                
                var primaryKeyField = table.PrimaryKeyField.TrimStart('$', '.');
                if (!objDict.TryGetValue(primaryKeyField, out var rawPrimaryKeyValue)) continue;
                
                // CRITICAL FIX: Handle type conversion for primary keys (same fix as UPDATE)
                object primaryKeyValue = rawPrimaryKeyValue;
                if (rawPrimaryKeyValue is long longValue && longValue >= int.MinValue && longValue <= int.MaxValue)
                {
                    primaryKeyValue = (int)longValue;
                    Console.WriteLine($"DELETE DEBUG: Converted primary key from Int64({longValue}) to Int32({primaryKeyValue})");
                    _logger.LogDebug("DELETE: Converted primary key from Int64({LongValue}) to Int32({PrimaryKeyValue})", longValue, primaryKeyValue);
                }
                
                // Delete the object from the table
                Console.WriteLine($"DELETE DEBUG: Attempting to delete object with primary key {primaryKeyValue} (Type: {primaryKeyValue?.GetType().FullName})");
                bool deleted = await table.DeleteAsync(transaction, primaryKeyValue, cancellationToken);
                Console.WriteLine($"DELETE DEBUG: Delete result for primary key {primaryKeyValue}: {deleted}");
                if (deleted)
                {
                    affectedRows++;
                }
            }
            
            _logger.LogDebug("DELETE: Deleted {AffectedRows} rows from table '{TableName}'", affectedRows, statement.TableName);
            
            return new SqlResult
            {
                StatementType = SqlStatementType.Delete,
                AffectedRows = affectedRows,
                Columns = Array.Empty<SqlColumnInfo>(),
                Rows = Array.Empty<object[]>()
            };
        }
        catch (Exception ex) when (!(ex is SqlExecutionException))
        {
            throw new SqlExecutionException(
                $"Failed to delete from table '{statement.TableName}': {ex.Message}", 
                $"DELETE FROM {statement.TableName}");
        }
    }
    
    /// <summary>
    /// Extracts the actual field name from SqlParserCS key formats.
    /// SqlParserCS returns column keys in format "ColumnName { Name = fieldname }" instead of just "fieldname".
    /// This method handles various formats and extracts the actual field name.
    /// </summary>
    /// <param name="key">The key object from SqlParserCS</param>
    /// <returns>The extracted field name</returns>
    private static string ExtractFieldNameFromKey(object? key)
    {
        if (key == null) return "unknown";
        
        var keyString = key.ToString() ?? "";
        
        // Handle SqlParserCS format: "ColumnName { Name = fieldname }"
        if (keyString.Contains("Name = ") && keyString.Contains(" }"))
        {
            // Try to extract using regex with word boundaries
            var patterns = new[]
            {
                @"Name = (\w+) \}",         // "Name = age }"
                @"Name = ([^}]+) \}",       // "Name = field_name }" (handles underscores)
                @"Name = ([^,}]+)",         // "Name = fieldname" (handles cases without closing brace)
            };
            
            foreach (var pattern in patterns)
            {
                var match = System.Text.RegularExpressions.Regex.Match(keyString, pattern);
                if (match.Success)
                {
                    return match.Groups[1].Value.Trim();
                }
            }
        }
        
        // If it's already a simple string, return as-is
        if (key is string simpleString && !simpleString.Contains("{"))
        {
            return simpleString;
        }
        
        // Fallback: return the string representation
        return keyString;
    }
    
    /// <summary>
    /// Simple query filter that matches all objects.
    /// Used for SELECT statements without WHERE clauses.
    /// </summary>
    private class MatchAllQueryFilter : IQueryFilter
    {
        public bool Matches(dynamic obj)
        {
            return true; // Match all objects
        }
    }
}