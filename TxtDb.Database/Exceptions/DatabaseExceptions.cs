namespace TxtDb.Database.Exceptions;

/// <summary>
/// Base exception for all database layer exceptions.
/// </summary>
public abstract class DatabaseException : Exception
{
    protected DatabaseException(string message) : base(message) { }
    protected DatabaseException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Thrown when attempting to create a database that already exists.
/// </summary>
public class DatabaseAlreadyExistsException : DatabaseException
{
    public string DatabaseName { get; }
    
    public DatabaseAlreadyExistsException(string databaseName) 
        : base($"Database '{databaseName}' already exists")
    {
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Thrown when database name violates naming conventions.
/// </summary>
public class InvalidDatabaseNameException : DatabaseException
{
    public string DatabaseName { get; }
    
    public InvalidDatabaseNameException(string databaseName, string reason) 
        : base($"Invalid database name '{databaseName}': {reason}")
    {
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Thrown when attempting to use a database that is currently in use.
/// </summary>
public class DatabaseInUseException : DatabaseException
{
    public string DatabaseName { get; }
    public int ActiveTransactions { get; }
    
    public DatabaseInUseException(string databaseName, int activeTransactions) 
        : base($"Database '{databaseName}' is in use with {activeTransactions} active transactions")
    {
        DatabaseName = databaseName;
        ActiveTransactions = activeTransactions;
    }
}

/// <summary>
/// Thrown when database is not found.
/// </summary>
public class DatabaseNotFoundException : DatabaseException
{
    public string DatabaseName { get; }
    
    public DatabaseNotFoundException(string databaseName) 
        : base($"Database '{databaseName}' not found")
    {
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Thrown when attempting to create a table that already exists.
/// </summary>
public class TableAlreadyExistsException : DatabaseException
{
    public string TableName { get; }
    public string DatabaseName { get; }
    
    public TableAlreadyExistsException(string databaseName, string tableName) 
        : base($"Table '{tableName}' already exists in database '{databaseName}'")
    {
        TableName = tableName;
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Thrown when table name violates naming conventions.
/// </summary>
public class InvalidTableNameException : DatabaseException
{
    public string TableName { get; }
    
    public InvalidTableNameException(string tableName, string reason) 
        : base($"Invalid table name '{tableName}': {reason}")
    {
        TableName = tableName;
    }
}

/// <summary>
/// Thrown when primary key path is malformed.
/// </summary>
public class InvalidPrimaryKeyPathException : DatabaseException
{
    public string PrimaryKeyPath { get; }
    
    public InvalidPrimaryKeyPathException(string primaryKeyPath, string reason) 
        : base($"Invalid primary key path '{primaryKeyPath}': {reason}")
    {
        PrimaryKeyPath = primaryKeyPath;
    }
}

/// <summary>
/// Thrown when attempting to use a table that is currently in use.
/// </summary>
public class TableInUseException : DatabaseException
{
    public string TableName { get; }
    public int ActiveOperations { get; }
    
    public TableInUseException(string tableName, int activeOperations) 
        : base($"Table '{tableName}' is in use with {activeOperations} active operations")
    {
        TableName = tableName;
        ActiveOperations = activeOperations;
    }
}

/// <summary>
/// Thrown when an object is missing its required primary key.
/// </summary>
public class MissingPrimaryKeyException : DatabaseException
{
    public string PrimaryKeyField { get; }
    
    public MissingPrimaryKeyException(string primaryKeyField) 
        : base($"Object is missing required primary key field '{primaryKeyField}'")
    {
        PrimaryKeyField = primaryKeyField;
    }
}

/// <summary>
/// Thrown when attempting to insert an object with a duplicate primary key.
/// </summary>
public class DuplicatePrimaryKeyException : DatabaseException
{
    public object PrimaryKey { get; }
    public string TableName { get; }
    
    public DuplicatePrimaryKeyException(string tableName, object primaryKey) 
        : base($"Duplicate primary key '{primaryKey}' in table '{tableName}'")
    {
        PrimaryKey = primaryKey;
        TableName = tableName;
    }
}

/// <summary>
/// Thrown when an object cannot be serialized or is invalid.
/// </summary>
public class InvalidObjectException : DatabaseException
{
    public InvalidObjectException(string reason) 
        : base($"Invalid object: {reason}") { }
        
    public InvalidObjectException(string reason, Exception innerException) 
        : base($"Invalid object: {reason}", innerException) { }
}

/// <summary>
/// Thrown when an object is not found by its primary key.
/// </summary>
public class ObjectNotFoundException : DatabaseException
{
    public object PrimaryKey { get; }
    public string TableName { get; }
    
    public ObjectNotFoundException(string tableName, object primaryKey) 
        : base($"Object with primary key '{primaryKey}' not found in table '{tableName}'")
    {
        PrimaryKey = primaryKey;
        TableName = tableName;
    }
}

/// <summary>
/// Thrown when attempting to update an object with a mismatched primary key.
/// </summary>
public class PrimaryKeyMismatchException : DatabaseException
{
    public object ExpectedKey { get; }
    public object ActualKey { get; }
    
    public PrimaryKeyMismatchException(object expectedKey, object actualKey) 
        : base($"Primary key mismatch: expected '{expectedKey}', got '{actualKey}'")
    {
        ExpectedKey = expectedKey;
        ActualKey = actualKey;
    }
}

/// <summary>
/// Thrown when a transaction has already been completed.
/// </summary>
public class TransactionAlreadyCompletedException : DatabaseException
{
    public long TransactionId { get; }
    public string State { get; }
    
    public TransactionAlreadyCompletedException(long transactionId, string state) 
        : base($"Transaction {transactionId} has already been completed with state: {state}")
    {
        TransactionId = transactionId;
        State = state;
    }
}

/// <summary>
/// Thrown when a transaction is aborted due to conflicts or other issues.
/// </summary>
public class TransactionAbortedException : DatabaseException
{
    public long TransactionId { get; }
    
    public TransactionAbortedException(long transactionId, string reason) 
        : base($"Transaction {transactionId} was aborted: {reason}")
    {
        TransactionId = transactionId;
    }
    
    public TransactionAbortedException(long transactionId, string reason, Exception innerException) 
        : base($"Transaction {transactionId} was aborted: {reason}", innerException)
    {
        TransactionId = transactionId;
    }
}