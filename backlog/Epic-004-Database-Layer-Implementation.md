# Epic 004: Database Layer Implementation

## Epic Overview

Following the successful implementation of the high-performance MVCC storage subsystem (Epic 001), async optimization (Epic 002), and multi-process coordination (Epic 003), Epic 004 introduces a comprehensive database abstraction layer that transforms the generic object storage into a structured database system with primary key management, indexing, and multi-process cache coordination.

### Goals
- Implement database and table abstractions with primary key enforcement
- Build page-aware indexing system using SortedDictionary structures
- Enable multi-process cache coordination through page event system
- Maintain backward compatibility with existing storage subsystem
- Achieve performance targets: < 10ms P95 latency for database operations

### Current System Gaps
- **No Primary Key Management:** Objects lack designated identity fields
- **No Database Abstraction:** Direct namespace manipulation only
- **No Page-Aware Indexing:** No indexes that map objects to their containing pages
- **No Query Optimization:** No ability to leverage indexes for page-efficient access
- **No Event System:** No cache invalidation mechanism for multi-process scenarios

## Architecture & Design

### Layered Architecture
```
┌─────────────────────────────────────────┐
│         Future SQL Engine               │
├─────────────────────────────────────────┤
│        Database Layer (Epic 004)        │
│  - Database/Table Management            │
│  - Primary Key Management               │ 
│  - Page-Aware SortedDictionary Indexes  │
│  - Expando Objects                      │
│  - Page Event System                    │
│  - Transaction Coordination             │
├─────────────────────────────────────────┤
│    Storage Layer (Epic 001-003)         │
│  - MVCC Transactions                    │
│  - Page-based Storage                   │
│  - Multi-Process Coordination           │
└─────────────────────────────────────────┘
```

### Key Architectural Decisions

#### 1. Page Management - Keep It Simple
- NO automatic page splitting in the database layer
- Storage subsystem handles all page size management
- Database layer works with whatever page structure storage provides
- Empty pages are garbage collected by storage subsystem

#### 2. Concurrency Model - Optimistic
- Storage subsystem implements version control for pages
- Version conflicts raise exceptions that database layer handles
- Database layer can retry operations or propagate exceptions to users
- No pessimistic locking at the database layer

#### 3. Index Updates - Safety First
- When a page is modified, rebuild indexes for ALL objects in that page
- No optimization attempts for partial page updates
- This ensures index consistency even if it's less efficient
- Performance optimization can come later if needed

### Namespace Mapping
```
Storage Namespace → Database Structure
_system.databases → Database metadata
_system.databases.{dbname} → Database-specific metadata
{dbname}.{tablename} → Table data pages (multiple objects per page)
{dbname}.{tablename}._metadata → Table metadata (includes primary key definition)
{dbname}.{tablename}._indexes.{indexname} → Index B-tree storage (maps keys to page IDs)
{dbname}.{tablename}._indexes._primary → Primary key index (maps primary keys to page IDs)
```

## Interface Specifications

### Database Management Interfaces

```csharp
/// <summary>
/// Main entry point for database layer operations.
/// Provides database lifecycle management and transaction coordination.
/// </summary>
public interface IDatabaseLayer
{
    /// <summary>
    /// Creates a new database with the specified name.
    /// </summary>
    /// <param name="name">Database name (must follow namespace conventions)</param>
    /// <returns>Database instance</returns>
    /// <exception cref="DatabaseAlreadyExistsException">If database exists</exception>
    /// <exception cref="InvalidDatabaseNameException">If name violates conventions</exception>
    Task<IDatabase> CreateDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets an existing database by name.
    /// </summary>
    /// <param name="name">Database name</param>
    /// <returns>Database instance or null if not found</returns>
    Task<IDatabase?> GetDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a database and all its tables.
    /// </summary>
    /// <param name="name">Database name</param>
    /// <returns>True if deleted, false if not found</returns>
    /// <exception cref="DatabaseInUseException">If active transactions exist</exception>
    Task<bool> DeleteDatabaseAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all databases in the system.
    /// </summary>
    /// <returns>Array of database names</returns>
    Task<string[]> ListDatabasesAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Begins a database transaction that wraps a storage transaction.
    /// Maintains 1:1 mapping with underlying storage transactions.
    /// </summary>
    /// <param name="database">Target database</param>
    /// <param name="isolationLevel">Transaction isolation level</param>
    /// <returns>Database transaction handle</returns>
    Task<IDatabaseTransaction> BeginTransactionAsync(
        string database, 
        TransactionIsolationLevel isolationLevel = TransactionIsolationLevel.Snapshot,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Subscribes to page modification events for cache invalidation.
    /// </summary>
    /// <param name="subscriber">Event subscriber implementation</param>
    void SubscribeToPageEvents(IPageEventSubscriber subscriber);
    
    /// <summary>
    /// Unsubscribes from page modification events.
    /// </summary>
    /// <param name="subscriber">Event subscriber to remove</param>
    void UnsubscribeFromPageEvents(IPageEventSubscriber subscriber);
}

/// <summary>
/// Represents a database containing tables and indexes.
/// </summary>
public interface IDatabase
{
    /// <summary>
    /// Database name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// Database creation timestamp.
    /// </summary>
    DateTime CreatedAt { get; }
    
    /// <summary>
    /// Database metadata for extensibility.
    /// </summary>
    IDictionary<string, object> Metadata { get; }
    
    /// <summary>
    /// Creates a new table with primary key specification.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <param name="primaryKeyField">JSON path to primary key field (e.g., "$.id")</param>
    /// <returns>Table instance</returns>
    /// <exception cref="TableAlreadyExistsException">If table exists</exception>
    /// <exception cref="InvalidTableNameException">If name violates conventions</exception>
    /// <exception cref="InvalidPrimaryKeyPathException">If path is malformed</exception>
    Task<ITable> CreateTableAsync(
        string name, 
        string primaryKeyField,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets an existing table by name.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>Table instance or null if not found</returns>
    Task<ITable?> GetTableAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a table and all its data and indexes.
    /// </summary>
    /// <param name="name">Table name</param>
    /// <returns>True if deleted, false if not found</returns>
    /// <exception cref="TableInUseException">If active operations exist</exception>
    Task<bool> DeleteTableAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all tables in the database.
    /// </summary>
    /// <returns>Array of table names</returns>
    Task<string[]> ListTablesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a table with primary key management and indexing.
/// Objects are stored in pages with multiple objects per page.
/// </summary>
public interface ITable
{
    /// <summary>
    /// Table name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// Primary key field JSON path (e.g., "$.id").
    /// </summary>
    string PrimaryKeyField { get; }
    
    /// <summary>
    /// Table creation timestamp.
    /// </summary>
    DateTime CreatedAt { get; }
    
    /// <summary>
    /// Inserts a new object into the table.
    /// The object will be added to an existing page or a new page will be created.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="obj">Object to insert (must contain primary key field)</param>
    /// <returns>Primary key value of inserted object</returns>
    /// <exception cref="MissingPrimaryKeyException">If object lacks primary key</exception>
    /// <exception cref="DuplicatePrimaryKeyException">If primary key already exists</exception>
    /// <exception cref="InvalidObjectException">If object cannot be serialized</exception>
    Task<object> InsertAsync(
        IDatabaseTransaction txn, 
        dynamic obj,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates an existing object by primary key (full replacement).
    /// This operation requires reading the containing page, finding the object,
    /// replacing it, and rewriting the entire page.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <param name="obj">New object (must contain same primary key)</param>
    /// <exception cref="ObjectNotFoundException">If primary key not found</exception>
    /// <exception cref="PrimaryKeyMismatchException">If object has different primary key</exception>
    Task UpdateAsync(
        IDatabaseTransaction txn, 
        object primaryKey, 
        dynamic obj,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Retrieves an object by primary key.
    /// Uses the primary key index to locate the page, then searches within the page.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <returns>Expando object or null if not found</returns>
    Task<dynamic?> GetAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes an object by primary key.
    /// This operation requires reading the containing page, removing the object,
    /// and rewriting the page (or deleting it if empty).
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="primaryKey">Primary key value</param>
    /// <returns>True if deleted, false if not found</returns>
    Task<bool> DeleteAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Queries objects using index-optimized filtering.
    /// Indexes are used to identify relevant pages, which are then scanned.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="filter">Query filter specification</param>
    /// <returns>Async enumerable of matching objects</returns>
    IAsyncEnumerable<dynamic> QueryAsync(
        IDatabaseTransaction txn, 
        IQueryFilter filter,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Creates a secondary index on the specified field.
    /// The index maps field values to page IDs containing matching objects.
    /// </summary>
    /// <param name="name">Index name</param>
    /// <param name="jsonPath">JSON path to indexed field</param>
    /// <param name="options">Index creation options</param>
    /// <returns>Index instance</returns>
    /// <exception cref="IndexAlreadyExistsException">If index name exists</exception>
    /// <exception cref="InvalidIndexPathException">If path is malformed</exception>
    Task<IIndex> CreateIndexAsync(
        string name, 
        string jsonPath,
        IndexOptions? options = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Drops a secondary index (primary key index cannot be dropped).
    /// </summary>
    /// <param name="name">Index name</param>
    /// <returns>True if dropped, false if not found</returns>
    /// <exception cref="CannotDropPrimaryIndexException">If attempting to drop primary index</exception>
    Task<bool> DropIndexAsync(string name, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all indexes on the table.
    /// </summary>
    /// <returns>Array of index instances</returns>
    Task<IIndex[]> ListIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Database transaction that wraps a storage transaction with 1:1 mapping.
/// Provides database-level consistency and isolation.
/// </summary>
public interface IDatabaseTransaction : IAsyncDisposable
{
    /// <summary>
    /// Unique transaction identifier.
    /// </summary>
    long TransactionId { get; }
    
    /// <summary>
    /// Database this transaction operates on.
    /// </summary>
    string DatabaseName { get; }
    
    /// <summary>
    /// Transaction isolation level.
    /// </summary>
    TransactionIsolationLevel IsolationLevel { get; }
    
    /// <summary>
    /// Transaction start timestamp.
    /// </summary>
    DateTime StartedAt { get; }
    
    /// <summary>
    /// Current transaction state.
    /// </summary>
    TransactionState State { get; }
    
    /// <summary>
    /// Commits all changes made in this transaction.
    /// </summary>
    /// <exception cref="TransactionAlreadyCompletedException">If already committed/rolled back</exception>
    /// <exception cref="TransactionAbortedException">If transaction cannot be committed</exception>
    Task CommitAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Commits with specified flush priority for performance tuning.
    /// </summary>
    /// <param name="priority">Flush priority for batch operations</param>
    Task CommitAsync(FlushPriority priority, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Rolls back all changes made in this transaction.
    /// </summary>
    /// <exception cref="TransactionAlreadyCompletedException">If already committed/rolled back</exception>
    Task RollbackAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the underlying storage transaction ID for internal operations.
    /// </summary>
    long GetStorageTransactionId();
}

/// <summary>
/// Represents an index on a table field using SortedDictionary implementation.
/// Indexes map field values to page IDs containing objects with those values.
/// </summary>
public interface IIndex
{
    /// <summary>
    /// Index name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// JSON path to indexed field.
    /// </summary>
    string JsonPath { get; }
    
    /// <summary>
    /// True if this is the primary key index.
    /// </summary>
    bool IsPrimaryKey { get; }
    
    /// <summary>
    /// Index creation timestamp.
    /// </summary>
    DateTime CreatedAt { get; }
    
    /// <summary>
    /// Index configuration options.
    /// </summary>
    IndexOptions Options { get; }
    
    /// <summary>
    /// Number of entries in the index.
    /// </summary>
    long EntryCount { get; }
    
    /// <summary>
    /// Looks up exact key matches in the index.
    /// Returns page IDs containing objects with the specified key value.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="key">Key value to search</param>
    /// <returns>Page IDs containing matching objects</returns>
    IAsyncEnumerable<string> LookupPagesAsync(
        IDatabaseTransaction txn, 
        object key,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Performs range queries on the index.
    /// Returns page IDs containing objects within the specified range.
    /// </summary>
    /// <param name="txn">Active database transaction</param>
    /// <param name="start">Range start (inclusive, null for unbounded)</param>
    /// <param name="end">Range end (inclusive, null for unbounded)</param>
    /// <param name="descending">True for descending order</param>
    /// <returns>Page IDs containing matching objects</returns>
    IAsyncEnumerable<string> RangePagesAsync(
        IDatabaseTransaction txn, 
        object? start, 
        object? end,
        bool descending = false,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets index statistics for query optimization.
    /// </summary>
    /// <returns>Index statistics</returns>
    Task<IndexStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Query filter specification for table queries.
/// </summary>
public interface IQueryFilter
{
    /// <summary>
    /// Evaluates whether an object matches the filter.
    /// </summary>
    /// <param name="obj">Object to evaluate</param>
    /// <returns>True if matches</returns>
    bool Matches(dynamic obj);
    
    /// <summary>
    /// Gets index hints for query optimization.
    /// </summary>
    /// <returns>Index usage hints</returns>
    IEnumerable<IndexHint> GetIndexHints();
}

/// <summary>
/// Subscriber interface for page modification events.
/// Used for cache invalidation and cross-process coordination.
/// </summary>
public interface IPageEventSubscriber
{
    /// <summary>
    /// Called when a page is modified after transaction commit.
    /// </summary>
    /// <param name="evt">Page modification event details</param>
    /// <returns>Task for async processing</returns>
    Task OnPageModifiedAsync(PageModifiedEvent evt);
    
    /// <summary>
    /// Gets the namespaces this subscriber is interested in.
    /// Null means subscribe to all namespaces.
    /// </summary>
    string[]? InterestedNamespaces { get; }
}

/// <summary>
/// Extensions to IAsyncStorageSubsystem for page event support.
/// These modifications maintain 100% backward compatibility.
/// </summary>
public interface IAsyncStorageSubsystemWithEvents : IAsyncStorageSubsystem
{
    /// <summary>
    /// Registers a page event handler.
    /// </summary>
    /// <param name="handler">Event handler delegate</param>
    /// <returns>Registration token for unsubscription</returns>
    Guid RegisterPageEventHandler(Func<PageModifiedEvent, Task> handler);
    
    /// <summary>
    /// Unregisters a page event handler.
    /// </summary>
    /// <param name="token">Registration token</param>
    void UnregisterPageEventHandler(Guid token);
    
    /// <summary>
    /// Gets whether page events are enabled.
    /// </summary>
    bool PageEventsEnabled { get; }
    
    /// <summary>
    /// Enables or disables page event generation.
    /// </summary>
    /// <param name="enabled">True to enable events</param>
    void SetPageEventsEnabled(bool enabled);
}
```

### Supporting Types

```csharp
/// <summary>
/// Transaction isolation levels supported by the database layer.
/// </summary>
public enum TransactionIsolationLevel
{
    /// <summary>
    /// Snapshot isolation (default) - reads see consistent snapshot.
    /// </summary>
    Snapshot,
    
    /// <summary>
    /// Serializable - highest isolation level.
    /// </summary>
    Serializable
}

/// <summary>
/// Transaction state enumeration.
/// </summary>
public enum TransactionState
{
    Active,
    Committed,
    RolledBack,
    Aborted
}

/// <summary>
/// Index creation options.
/// </summary>
public class IndexOptions
{
    /// <summary>
    /// How to handle null values in the index.
    /// </summary>
    public NullHandling NullHandling { get; set; } = NullHandling.Include;
    
    /// <summary>
    /// Custom comparer for index ordering (optional).
    /// </summary>
    public IComparer<object>? CustomComparer { get; set; }
    
    /// <summary>
    /// Whether to allow duplicate key values.
    /// </summary>
    public bool AllowDuplicates { get; set; } = true;
}

/// <summary>
/// Null value handling in indexes.
/// </summary>
public enum NullHandling
{
    /// <summary>
    /// Include null values in the index.
    /// </summary>
    Include,
    
    /// <summary>
    /// Exclude null values from the index.
    /// </summary>
    Exclude
}

/// <summary>
/// Index statistics for query optimization.
/// </summary>
public class IndexStatistics
{
    public long TotalEntries { get; set; }
    public long UniqueKeys { get; set; }
    public double AverageEntriesPerKey { get; set; }
    public object? MinKey { get; set; }
    public object? MaxKey { get; set; }
    public DateTime LastUpdated { get; set; }
    public long TotalPages { get; set; }
    public double AverageObjectsPerPage { get; set; }
}

/// <summary>
/// Index hint for query optimization.
/// </summary>
public class IndexHint
{
    /// <summary>
    /// JSON path that could use an index.
    /// </summary>
    public string JsonPath { get; set; } = "";
    
    /// <summary>
    /// Operation type for the path.
    /// </summary>
    public IndexOperation Operation { get; set; }
    
    /// <summary>
    /// Values for the operation (if applicable).
    /// </summary>
    public object[]? Values { get; set; }
}

/// <summary>
/// Index operation types.
/// </summary>
public enum IndexOperation
{
    Equals,
    Range,
    In,
    StartsWith
}

/// <summary>
/// Standard query filter implementations.
/// </summary>
public static class QueryFilters
{
    /// <summary>
    /// Creates an equality filter.
    /// </summary>
    public static IQueryFilter Equals(string jsonPath, object value)
    {
        return new EqualityFilter(jsonPath, value);
    }
    
    /// <summary>
    /// Creates a range filter.
    /// </summary>
    public static IQueryFilter Range(string jsonPath, object? start, object? end)
    {
        return new RangeFilter(jsonPath, start, end);
    }
    
    /// <summary>
    /// Combines multiple filters with AND logic.
    /// </summary>
    public static IQueryFilter And(params IQueryFilter[] filters)
    {
        return new CompositeFilter(LogicalOperator.And, filters);
    }
    
    /// <summary>
    /// Combines multiple filters with OR logic.
    /// </summary>
    public static IQueryFilter Or(params IQueryFilter[] filters)
    {
        return new CompositeFilter(LogicalOperator.Or, filters);
    }
}

/// <summary>
/// Page modification event details.
/// </summary>
public class PageModifiedEvent
{
    /// <summary>
    /// Storage namespace where change occurred.
    /// </summary>
    public string Namespace { get; set; } = "";
    
    /// <summary>
    /// Page identifier that was modified.
    /// </summary>
    public string PageId { get; set; } = "";
    
    /// <summary>
    /// Type of change that occurred.
    /// </summary>
    public PageChangeType ChangeType { get; set; }
    
    /// <summary>
    /// Transaction ID that made the change.
    /// </summary>
    public long TransactionId { get; set; }
    
    /// <summary>
    /// Timestamp when the change was committed.
    /// </summary>
    public DateTime CommittedAt { get; set; }
    
    /// <summary>
    /// Process ID that made the change.
    /// </summary>
    public int ProcessId { get; set; }
    
    /// <summary>
    /// Machine name for distributed scenarios.
    /// </summary>
    public string MachineName { get; set; } = "";
    
    /// <summary>
    /// Objects before modification (for updates/deletes).
    /// </summary>
    public object[]? BeforeState { get; set; }
    
    /// <summary>
    /// Objects after modification (for inserts/updates).
    /// </summary>
    public object[]? AfterState { get; set; }
    
    /// <summary>
    /// Database name extracted from namespace.
    /// </summary>
    public string? DatabaseName { get; set; }
    
    /// <summary>
    /// Table name extracted from namespace.
    /// </summary>
    public string? TableName { get; set; }
}

/// <summary>
/// Type of page change.
/// </summary>
public enum PageChangeType
{
    /// <summary>
    /// New page created with objects.
    /// </summary>
    Insert,
    
    /// <summary>
    /// Existing page modified.
    /// </summary>
    Update,
    
    /// <summary>
    /// Page deleted.
    /// </summary>
    Delete,
    
    /// <summary>
    /// Namespace created.
    /// </summary>
    NamespaceCreated,
    
    /// <summary>
    /// Namespace deleted.
    /// </summary>
    NamespaceDeleted,
    
    /// <summary>
    /// Namespace renamed.
    /// </summary>
    NamespaceRenamed
}

/// <summary>
/// Database metadata stored in _system.databases namespace.
/// </summary>
public class DatabaseMetadata
{
    /// <summary>
    /// Database name (unique identifier).
    /// </summary>
    public string Name { get; set; } = "";
    
    /// <summary>
    /// Database creation timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }
    
    /// <summary>
    /// Last modification timestamp.
    /// </summary>
    public DateTime ModifiedAt { get; set; }
    
    /// <summary>
    /// Database version for schema evolution.
    /// </summary>
    public int Version { get; set; } = 1;
    
    /// <summary>
    /// Default serialization format for tables.
    /// </summary>
    public SerializationFormat DefaultFormat { get; set; } = SerializationFormat.Json;
    
    /// <summary>
    /// Custom database properties.
    /// </summary>
    public Dictionary<string, object> Properties { get; set; } = new();
    
    /// <summary>
    /// List of tables in this database.
    /// </summary>
    public List<string> Tables { get; set; } = new();
    
    /// <summary>
    /// Average objects per page for optimization.
    /// </summary>
    public int AverageObjectsPerPage { get; set; } = 100;
}

/// <summary>
/// Table metadata stored in {database}.{table}._metadata namespace.
/// </summary>
public class TableMetadata
{
    /// <summary>
    /// Table name.
    /// </summary>
    public string Name { get; set; } = "";
    
    /// <summary>
    /// Database name this table belongs to.
    /// </summary>
    public string DatabaseName { get; set; } = "";
    
    /// <summary>
    /// Primary key field JSON path (required).
    /// </summary>
    public string PrimaryKeyField { get; set; } = "";
    
    /// <summary>
    /// Table creation timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }
    
    /// <summary>
    /// Last modification timestamp.
    /// </summary>
    public DateTime ModifiedAt { get; set; }
    
    /// <summary>
    /// List of indexes on this table.
    /// </summary>
    public List<IndexMetadata> Indexes { get; set; } = new();
    
    /// <summary>
    /// Serialization format for objects in this table.
    /// </summary>
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;
    
    /// <summary>
    /// Custom table properties.
    /// </summary>
    public Dictionary<string, object> Properties { get; set; } = new();
    
    /// <summary>
    /// Target objects per page - used by storage subsystem only.
    /// Database layer does not perform page splitting.
    /// </summary>
    public int TargetObjectsPerPage { get; set; } = 100;
}

/// <summary>
/// Index metadata stored with table metadata.
/// </summary>
public class IndexMetadata
{
    /// <summary>
    /// Index name (unique within table).
    /// </summary>
    public string Name { get; set; } = "";
    
    /// <summary>
    /// JSON path to indexed field.
    /// </summary>
    public string JsonPath { get; set; } = "";
    
    /// <summary>
    /// True if this is the primary key index.
    /// </summary>
    public bool IsPrimaryKey { get; set; }
    
    /// <summary>
    /// Index creation timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }
    
    /// <summary>
    /// Index options.
    /// </summary>
    public IndexOptions Options { get; set; } = new();
    
    /// <summary>
    /// Storage namespace for this index.
    /// </summary>
    public string StorageNamespace => IsPrimaryKey 
        ? $"{DatabaseName}.{TableName}._indexes._primary"
        : $"{DatabaseName}.{TableName}._indexes.{Name}";
}

/// <summary>
/// Serialized index data structure using SortedDictionary for page-based storage.
/// </summary>
public class SerializedPageAwareIndexData
{
    /// <summary>
    /// Index metadata.
    /// </summary>
    public IndexMetadata Metadata { get; set; } = new();
    
    /// <summary>
    /// Sorted entries for efficient range queries.
    /// Key: Indexed field value
    /// Value: Set of page IDs containing objects with this value
    /// </summary>
    public SortedDictionary<object, HashSet<string>> KeyToPageIds { get; set; } = new();
    
    /// <summary>
    /// Reverse lookup from page ID to indexed values.
    /// Used for efficient index updates when pages are modified.
    /// </summary>
    public Dictionary<string, HashSet<object>> PageToKeys { get; set; } = new();
    
    /// <summary>
    /// Index statistics for query optimization.
    /// </summary>
    public IndexStatistics Statistics { get; set; } = new();
    
    /// <summary>
    /// Last update timestamp.
    /// </summary>
    public DateTime LastUpdated { get; set; }
}
```

## Implementation Algorithms

### Table CRUD Operations

```
ALGORITHM: InsertObject
INPUT: table, dbTxn, obj
OUTPUT: primaryKeyValue

1. Extract primary key from object
   primaryKeyValue = ExtractValueByJsonPath(obj, table.PrimaryKeyField)
   IF primaryKeyValue == NULL:
      THROW MissingPrimaryKeyException
      
2. Check primary key uniqueness via index
   primaryIndex = table.GetIndex("_primary")
   existingPageIds = primaryIndex.LookupPagesAsync(dbTxn, primaryKeyValue)
   IF existingPageIds.Any():
      THROW DuplicatePrimaryKeyException
      
3. Serialize object
   serializedData = Serialize(obj, table.Database.SerializationFormat)
   
4. Insert into storage (storage layer handles page allocation)
   pageId = storage.InsertObjectAsync(
      dbTxn.TransactionId,
      table.GetStorageNamespace(),
      serializedData
   )
   
5. Update primary key index to map primary key to page ID
   primaryIndex.AddEntry(dbTxn, primaryKeyValue, pageId)
   
6. Update all secondary indexes
   FOR EACH index IN table.GetSecondaryIndexes():
      indexKeyValue = ExtractValueByJsonPath(obj, index.JsonPath)
      IF ShouldIncludeInIndex(indexKeyValue, index.Options):
         index.AddEntry(dbTxn, indexKeyValue, pageId)
         
7. Track change for page events
   dbTxn.TrackedChanges.Add(new ChangeInfo {
      Namespace = table.GetStorageNamespace(),
      PageId = pageId,
      ChangeType = PageChangeType.Insert,
      AfterState = [obj]
   })
   
8. RETURN primaryKeyValue


ALGORITHM: UpdateObject
INPUT: table, dbTxn, primaryKey, newObj
OUTPUT: void

1. Validate primary key in new object
   newPrimaryKey = ExtractValueByJsonPath(newObj, table.PrimaryKeyField)
   IF newPrimaryKey != primaryKey:
      THROW PrimaryKeyMismatchException
      
2. Look up page containing the object via primary index
   primaryIndex = table.GetIndex("_primary")
   pageIds = primaryIndex.LookupPagesAsync(dbTxn, primaryKey).ToList()
   IF pageIds.Empty():
      THROW ObjectNotFoundException
      
3. Read the page containing multiple objects
   pageId = pageIds[0]
   pageObjects = storage.ReadPageAsync(dbTxn.TransactionId, table.GetStorageNamespace(), pageId)
   
4. Find the specific object within the page by sequential search
   oldObj = NULL
   objectIndex = -1
   FOR i = 0 TO pageObjects.Length - 1:
      obj = Deserialize(pageObjects[i])
      objPrimaryKey = ExtractValueByJsonPath(obj, table.PrimaryKeyField)
      IF objPrimaryKey == primaryKey:
         oldObj = obj
         objectIndex = i
         BREAK
         
   IF oldObj == NULL:
      THROW ObjectNotFoundException("Object not found in page")
      
5. Replace object in page and serialize entire page
   serializedNewObj = Serialize(newObj, table.Database.SerializationFormat)
   pageObjects[objectIndex] = serializedNewObj
   
   // Update entire page with modified object array using optimistic concurrency
   TRY:
      storage.UpdatePageAsync(
         dbTxn.TransactionId,
         table.GetStorageNamespace(),
         pageId,
         pageObjects
      )
   CATCH VersionConflictException:
      // Another writer updated the page - storage subsystem raises exception
      // Let exception propagate - caller can retry if appropriate
      THROW
   
6. Rebuild ALL indexes for ALL objects in the modified page
   // ARCHITECTURAL DECISION: Rebuild indexes for ALL objects in the page
   // This is the safe approach - even if only one object changed
   
   // First, remove all old index entries for this page
   FOR EACH index IN table.GetAllIndexes(): // Including primary
      RemoveAllEntriesForPage(index, pageId)
   
   // Then, re-add all index entries for all objects in the page
   FOR EACH serializedObj IN pageObjects:
      obj = Deserialize(serializedObj)
      
      // Re-add to primary index
      primaryKeyValue = ExtractValueByJsonPath(obj, table.PrimaryKeyField)
      primaryIndex.AddEntry(dbTxn, primaryKeyValue, pageId)
      
      // Re-add to secondary indexes
      FOR EACH index IN table.GetSecondaryIndexes():
         indexValue = ExtractValueByJsonPath(obj, index.JsonPath)
         IF ShouldIncludeInIndex(indexValue, index.Options):
            index.AddEntry(dbTxn, indexValue, pageId)
            
7. Track change for page events
   dbTxn.TrackedChanges.Add(new ChangeInfo {
      Namespace = table.GetStorageNamespace(),
      PageId = pageId,
      ChangeType = PageChangeType.Update,
      BeforeState = [oldObj],
      AfterState = [newObj]
   })


ALGORITHM: GetObject
INPUT: table, dbTxn, primaryKey
OUTPUT: object or null

1. Look up page containing the object via primary index
   primaryIndex = table.GetIndex("_primary")
   pageIds = primaryIndex.LookupPagesAsync(dbTxn, primaryKey).ToList()
   IF pageIds.Empty():
      RETURN null
      
2. Read the page containing multiple objects
   pageId = pageIds[0]
   pageObjects = storage.ReadPageAsync(dbTxn.TransactionId, table.GetStorageNamespace(), pageId)
   
3. Find the specific object within the page by sequential search
   FOR EACH serializedObj IN pageObjects:
      obj = Deserialize(serializedObj)
      objPrimaryKey = ExtractValueByJsonPath(obj, table.PrimaryKeyField)
      IF objPrimaryKey == primaryKey:
         RETURN ConvertToExpando(obj)
         
4. RETURN null // Object not found in page


ALGORITHM: DeleteObject
INPUT: table, dbTxn, primaryKey
OUTPUT: bool (true if deleted, false if not found)

1. Look up page containing the object via primary index
   primaryIndex = table.GetIndex("_primary")
   pageIds = primaryIndex.LookupPagesAsync(dbTxn, primaryKey).ToList()
   IF pageIds.Empty():
      RETURN false
      
2. Read the page containing multiple objects
   pageId = pageIds[0]
   pageObjects = storage.ReadPageAsync(dbTxn.TransactionId, table.GetStorageNamespace(), pageId)
   
3. Find and remove the specific object within the page
   oldObj = NULL
   newPageObjects = new List<object>()
   found = false
   
   FOR EACH serializedObj IN pageObjects:
      obj = Deserialize(serializedObj)
      objPrimaryKey = ExtractValueByJsonPath(obj, table.PrimaryKeyField)
      IF objPrimaryKey == primaryKey:
         oldObj = obj
         found = true
         // Skip adding this object to newPageObjects
      ELSE:
         newPageObjects.Add(serializedObj)
         
   IF NOT found:
      RETURN false
      
4. Remove from all indexes
   // Remove primary key -> page mapping
   primaryIndex.RemoveEntry(dbTxn, primaryKey, pageId)
   
   // Remove from secondary indexes
   FOR EACH index IN table.GetSecondaryIndexes():
      indexValue = ExtractValueByJsonPath(oldObj, index.JsonPath)
      IF ShouldIncludeInIndex(indexValue, index.Options):
         index.RemoveEntry(dbTxn, indexValue, pageId)
         
5. Update or delete the page
   IF newPageObjects.Count > 0:
      // Update page with remaining objects using optimistic concurrency
      TRY:
         storage.UpdatePageAsync(
            dbTxn.TransactionId,
            table.GetStorageNamespace(),
            pageId,
            newPageObjects.ToArray()
         )
      CATCH VersionConflictException:
         // Another writer updated the page - let exception propagate
         THROW
   ELSE:
      // Delete empty page - storage subsystem handles garbage collection
      TRY:
         storage.UpdatePageAsync(
            dbTxn.TransactionId,
            table.GetStorageNamespace(),
            pageId,
            [] // Empty array signals page deletion to storage subsystem
         )
      CATCH VersionConflictException:
         // Another writer updated the page - let exception propagate
         THROW
      
6. Track change for page events
   dbTxn.TrackedChanges.Add(new ChangeInfo {
      Namespace = table.GetStorageNamespace(),
      PageId = pageId,
      ChangeType = PageChangeType.Delete,
      BeforeState = [oldObj]
   })
   
7. RETURN true
```

### Transaction Management

```
ALGORITHM: BeginDatabaseTransaction
INPUT: databaseName, isolationLevel
OUTPUT: IDatabaseTransaction

1. Validate database exists in metadata
   IF NOT EXISTS:
      THROW DatabaseNotFoundException
      
2. Begin storage transaction
   storageTransactionId = storage.BeginTransactionAsync()
   
3. Create database transaction wrapper
   dbTxn = new DatabaseTransaction {
      TransactionId = storageTransactionId,
      DatabaseName = databaseName,
      IsolationLevel = isolationLevel,
      StartedAt = DateTime.UtcNow,
      State = TransactionState.Active
   }
   
4. Register transaction in active set
   activeTransactions[storageTransactionId] = dbTxn
   
5. RETURN dbTxn


ALGORITHM: CommitDatabaseTransaction
INPUT: dbTxn, flushPriority
OUTPUT: void

1. Validate transaction state
   IF dbTxn.State != Active:
      THROW TransactionAlreadyCompletedException
      
2. Flush all pending index updates
   FOR EACH pendingIndexUpdate IN dbTxn.PendingIndexUpdates:
      FlushIndexToStorage(pendingIndexUpdate)
      
3. Commit storage transaction
   TRY:
      storage.CommitTransactionAsync(dbTxn.TransactionId, flushPriority)
   CATCH VersionConflictException e:
      // Optimistic concurrency failure at commit time
      dbTxn.State = TransactionState.Aborted
      THROW OptimisticConcurrencyException(
         "Transaction failed due to concurrent modifications", e)
   CATCH Exception e:
      dbTxn.State = TransactionState.Aborted
      THROW TransactionAbortedException(e)
      
4. Update transaction state
   dbTxn.State = TransactionState.Committed
   
5. Remove from active set
   activeTransactions.Remove(dbTxn.TransactionId)
   
6. Trigger page events for committed changes
   FOR EACH change IN dbTxn.TrackedChanges:
      FirePageEvent(change)
```

### Page-Aware Index Maintenance

```
ALGORITHM: MaintainPageAwareSortedDictionaryIndex
INPUT: index, operation, key, pageId
OUTPUT: void

NOTE: When a page is updated, ALL objects in that page must be reprocessed
for index consistency. This is the safe approach - no optimization attempts.

STRUCTURE: PageAwareIndexData
   SortedDictionary<object, HashSet<string>> keyToPageIds  // Maps keys to page IDs
   Dictionary<string, HashSet<object>> pageToKeys          // Maps page IDs to keys
   IndexOptions options
   DateTime lastModified

1. Load index from cache or storage
   indexData = LoadIndex(index.Name)
   
2. Apply operation
   SWITCH operation:
      CASE AddEntry:
         // Add key -> page mapping
         IF NOT indexData.keyToPageIds.ContainsKey(key):
            indexData.keyToPageIds[key] = new HashSet<string>()
         indexData.keyToPageIds[key].Add(pageId)
         
         // Add page -> key mapping for efficient updates
         IF NOT indexData.pageToKeys.ContainsKey(pageId):
            indexData.pageToKeys[pageId] = new HashSet<object>()
         indexData.pageToKeys[pageId].Add(key)
         
      CASE RemoveEntry:
         // Remove key -> page mapping
         IF indexData.keyToPageIds.ContainsKey(key):
            indexData.keyToPageIds[key].Remove(pageId)
            IF indexData.keyToPageIds[key].Count == 0:
               indexData.keyToPageIds.Remove(key)
               
         // Remove page -> key mapping
         IF indexData.pageToKeys.ContainsKey(pageId):
            indexData.pageToKeys[pageId].Remove(key)
            IF indexData.pageToKeys[pageId].Count == 0:
               indexData.pageToKeys.Remove(pageId)
         
      CASE RebuildIndex:
         indexData = new PageAwareIndexData()
         
         // Scan all pages in the table
         allPageIds = storage.ListPagesAsync(table.GetStorageNamespace())
         FOR EACH pageId IN allPageIds:
            pageObjects = storage.ReadPageAsync(dbTxn.TransactionId, table.GetStorageNamespace(), pageId)
            
            FOR EACH serializedObj IN pageObjects:
               obj = Deserialize(serializedObj)
               keyValue = ExtractValueByJsonPath(obj, index.JsonPath)
               IF ShouldIncludeInIndex(keyValue, index.Options):
                  AddEntry(index, keyValue, pageId)
               
3. Update metadata
   indexData.lastModified = DateTime.UtcNow
   
4. Mark index as dirty for transaction
   dbTxn.PendingIndexUpdates[index.Name] = indexData
```

## Testing Strategy and TDD Recommendations

### Overall Test Architecture

The database layer requires comprehensive testing that validates both correctness and performance under real-world conditions. Following TDD principles, all tests must be written before implementation, driving the design through failing tests that define expected behavior.

#### Core Testing Philosophy
- **Zero Mocking Policy**: Every test uses the real storage subsystem with actual file I/O
- **Process Isolation**: Multi-process tests use `Process.Start()` for true process separation
- **Page-Based Validation**: All tests must validate page-level operations and consistency
- **Performance Benchmarks**: Every operation must meet defined latency targets
- **Error Injection**: Comprehensive edge case testing with controlled failure scenarios

### Test Coverage Targets

| Test Category | Focus | Real-World Validation |
|--------------|-------|----------------------|
| E2E Tests | Complete user workflows | Real database operations from creation to query |
| Multi-Process Tests | Process coordination | Actual processes with Process.Start() |
| Performance Tests | Sustained load | Real operations for hours/days |
| Stress Tests | Breaking points | Push system to actual limits |
| Chaos Tests | Failure handling | Real crashes and recovery |

### Story-Specific TDD Test Plans

#### Story 1: Core Database Infrastructure

**Test Complexity**: Medium  
**Risk Areas**: Metadata corruption, transaction leakage, namespace conflicts

**Critical Test Scenarios**:

```
TEST: DatabaseCreation_WithValidation
BEFORE:
  - No database exists
  - Storage subsystem initialized
ACTION:
  - Create database with name "test_db"
  - Verify metadata stored in _system.databases
  - Create second database with same name
ASSERT:
  - First creation succeeds
  - Metadata contains correct properties
  - Second creation throws DatabaseAlreadyExistsException
  - Storage namespace created correctly

TEST: TransactionWrapper_MaintainsOneToOneMapping
BEFORE:
  - Database exists
  - No active transactions
ACTION:
  - Begin database transaction
  - Get underlying storage transaction ID
  - Perform operations
  - Commit transaction
ASSERT:
  - Storage transaction ID matches throughout lifecycle
  - Transaction state transitions correctly
  - No orphaned storage transactions
  - Cleanup on dispose

TEST: MetadataCaching_Performance
BEFORE:
  - Database with 1000 tables created
ACTION:
  - Measure first metadata access (cold cache)
  - Measure subsequent accesses (warm cache)
  - Modify metadata and verify cache invalidation
ASSERT:
  - Cold access < 50ms
  - Warm access < 1ms
  - Cache invalidation works correctly
```

**Performance Benchmarks**:
- Database creation: < 10ms
- Transaction begin/commit: < 5ms overhead
- Metadata access (cached): < 1ms

#### Story 2: Page-Based Object Management

**Test Complexity**: High  
**Risk Areas**: Page corruption, object loss, concurrent modifications, primary key violations

**Critical Test Scenarios**:

```
TEST: Insert_EnforcesPrimaryKeyUniqueness_AcrossPages
BEFORE:
  - Table with primary key on "$.id"
  - Objects spread across multiple pages
ACTION:
  - Insert 1000 objects with unique IDs
  - Attempt to insert duplicate ID
  - Verify objects distributed across pages
ASSERT:
  - All unique inserts succeed
  - Duplicate throws DuplicatePrimaryKeyException
  - Primary index correctly maps to all pages
  - Objects retrievable by primary key

TEST: Update_SequentialSearchAndPageRewrite
BEFORE:
  - Page with 100 objects
  - Target object at position 75
ACTION:
  - Update object by primary key
  - Measure sequential search time
  - Verify entire page rewritten
  - Check other objects unchanged
ASSERT:
  - Update completes < 5ms
  - All 100 objects remain in page
  - Only target object modified
  - Page version incremented

TEST: OptimisticConcurrency_PageConflicts
BEFORE:
  - Page with multiple objects
  - Two concurrent transactions
ACTION:
  - Txn1: Read and update object A
  - Txn2: Read and update object B (same page)
  - Commit both transactions
ASSERT:
  - First commit succeeds
  - Second commit throws VersionConflictException
  - Retry logic handles conflict
  - Both updates eventually succeed

TEST: Delete_HandlesEmptyPages
BEFORE:
  - Page with single object
ACTION:
  - Delete the object
  - Verify page cleanup
  - Check storage garbage collection
ASSERT:
  - Object removed from indexes
  - Empty page deleted
  - No orphaned references
```

**Performance Benchmarks**:
- Insert (with PK check): < 5ms
- Update (sequential search): < 5ms for 100 objects/page
- Get by PK: < 2ms
- Delete: < 5ms

#### Story 3: Page-Aware Indexing System

**Test Complexity**: High  
**Risk Areas**: Index corruption, inconsistent mappings, memory bloat

**Critical Test Scenarios**:

```
TEST: PrimaryKeyIndex_AutomaticMaintenance
BEFORE:
  - New table created
  - No data inserted
ACTION:
  - Verify primary index created
  - Insert objects across multiple pages
  - Update objects causing page rewrites
  - Delete objects
ASSERT:
  - Primary index exists at creation
  - All operations maintain index consistency
  - Key-to-page mappings accurate
  - Reverse mappings (page-to-keys) correct

TEST: SecondaryIndex_RangeQueries
BEFORE:
  - Table with 10K objects
  - Index on "$.age" field
ACTION:
  - Query range [25-35]
  - Retrieve page IDs from index
  - Scan pages for matching objects
ASSERT:
  - Index returns correct page set
  - No false positive pages
  - Range query < 10ms
  - Results properly ordered

TEST: FullPageRebuild_OnModification
BEFORE:
  - Page with 50 objects
  - Multiple indexes on different fields
ACTION:
  - Update single object in page
  - Track index operations
  - Verify all indexes rebuilt
ASSERT:
  - ALL 50 objects re-indexed
  - Not just the modified object
  - Index consistency maintained
  - Performance < 20ms for rebuild

TEST: SortedDictionary_MemoryEfficiency
BEFORE:
  - Empty index
ACTION:
  - Add 1M unique keys
  - Measure memory usage
  - Perform range operations
ASSERT:
  - Memory usage < 100MB
  - Range queries < 1ms
  - Serialization < 1 second
```

**Performance Benchmarks**:
- Index lookup to pages: < 1ms for 1M entries
- Range query: < 5ms for 1000 pages
- Full page rebuild: < 20ms for 100 objects
- Index serialization: < 1s for 1M entries

#### Story 4: Query Execution Engine

**Test Complexity**: Medium  
**Risk Areas**: Inefficient page scanning, missing results, index selection

**Critical Test Scenarios**:

```
TEST: IndexOptimizedQuery_MinimizesPageReads
BEFORE:
  - Table with 100K objects across 1000 pages
  - Index on query field
ACTION:
  - Query for specific value
  - Track pages accessed
  - Measure performance
ASSERT:
  - Only relevant pages read
  - No full table scan
  - Query completes < 10ms
  - All matching objects returned

TEST: CompositeFilters_AndLogic
BEFORE:
  - Objects with multiple indexed fields
ACTION:
  - Query with AND conditions
  - Use index hints for optimization
  - Verify page intersection
ASSERT:
  - Minimal page set identified
  - Correct result filtering
  - Performance scales with selectivity

TEST: AsyncEnumeration_Streaming
BEFORE:
  - Large result set (10K objects)
ACTION:
  - Execute query
  - Enumerate results with cancellation
  - Measure memory usage
ASSERT:
  - Results stream without loading all
  - Cancellation stops immediately
  - Memory usage constant
  - First result < 5ms
```

**Performance Benchmarks**:
- Index-optimized query: < 10ms for 1% selectivity
- Page scanning: < 5ms per page (100 objects)
- First result latency: < 5ms
- Memory usage: O(1) for streaming

#### Story 5: Page Event System

**Test Complexity**: Very High  
**Risk Areas**: Event loss, cross-process race conditions, performance impact

**Critical Test Scenarios**:

```
TEST: CrossProcess_EventDelivery
BEFORE:
  - Two separate processes running
  - Shared database file
ACTION:
  - Process1: Modify page
  - Process2: Subscribe to events
  - Measure delivery latency
ASSERT:
  - Event delivered < 100ms
  - No events lost
  - Correct event details
  - Process isolation maintained

TEST: CacheInvalidation_Correctness
BEFORE:
  - Process1 and Process2 with cached data
ACTION:
  - Process1: Update object
  - Track cache invalidation in Process2
  - Verify subsequent read
ASSERT:
  - Cache invalidated for modified page only
  - Other cache entries retained
  - Fresh data on next read
  - No stale reads

TEST: EventSystem_PerformanceImpact
BEFORE:
  - Baseline performance measured
  - Events disabled
ACTION:
  - Enable events
  - Run same workload
  - Compare performance
ASSERT:
  - Performance degradation < 5%
  - No impact when no subscribers
  - Linear scaling with subscribers

TEST: FileSystemMonitoring_Reliability
BEFORE:
  - Event file directory setup
ACTION:
  - Rapid event generation (1000/sec)
  - Simulate file system delays
  - Add/remove subscribers
ASSERT:
  - All events eventually delivered
  - No file handle exhaustion
  - Graceful degradation under load
```

**Performance Benchmarks**:
- Event delivery latency: < 100ms P95
- Performance overhead: < 5% when enabled
- Event throughput: > 10K events/second
- File system monitoring: < 10ms detection

#### Story 6: Production Hardening

**Test Complexity**: High  
**Risk Areas**: Performance degradation, resource leaks, error cascades

**Critical Test Scenarios**:

```
TEST: SustainedLoad_NoResourceLeaks
BEFORE:
  - Fresh database instance
ACTION:
  - Run 24-hour workload
  - 1000 operations/second
  - Monitor resources
ASSERT:
  - No memory leaks
  - No handle leaks
  - Consistent performance
  - All operations < 10ms P95

TEST: ErrorRecovery_GracefulDegradation
BEFORE:
  - Normal operation
ACTION:
  - Inject storage failures
  - Corrupt index data
  - Simulate process crashes
ASSERT:
  - Errors contained
  - Automatic recovery attempted
  - Clear error messages
  - No data loss

TEST: Monitoring_ComprehensiveMetrics
BEFORE:
  - Metrics collection enabled
ACTION:
  - Run varied workload
  - Collect all metrics
  - Verify dashboards
ASSERT:
  - All operations instrumented
  - Latency percentiles accurate
  - Error rates tracked
  - Resource usage visible
```

**Performance Benchmarks**:
- Sustained throughput: > 1000 ops/sec
- P95 latency: < 10ms under load
- Recovery time: < 1 second
- Metric overhead: < 1%

### Test Implementation Guidelines

#### Directory Structure
```
tests/
├── e2e/
│   ├── real-workflows/
│   │   ├── ECommerceWorkflowTests.cs
│   │   ├── BankingSystemTests.cs
│   │   ├── InventoryManagementTests.cs
│   │   └── DocumentManagementTests.cs
│   ├── multi-process/
│   │   ├── MicroservicesDeploymentTests.cs
│   │   ├── HighAvailabilityTests.cs
│   │   ├── DeveloperCollaborationTests.cs
│   │   └── ProcessCoordinationTests.cs
│   ├── performance/
│   │   ├── SustainedLoadTests.cs
│   │   ├── PeakLoadScenarios.cs
│   │   ├── LongRunningOperations.cs
│   │   └── ResourceUtilizationTests.cs
│   ├── stress/
│   │   ├── BreakingPointTests.cs
│   │   ├── ConcurrencyLimitsTests.cs
│   │   └── MemoryExhaustionTests.cs
│   ├── chaos/
│   │   ├── DisasterRecoveryTests.cs
│   │   ├── NetworkFailureTests.cs
│   │   ├── CorruptionRecoveryTests.cs
│   │   └── ProcessCrashTests.cs
│   └── production-simulation/
│       ├── YearLongSimulation.cs
│       ├── BlackFridaySimulation.cs
│       └── RealWorldPatterns.cs
└── test-data/
    ├── production-datasets/
    ├── real-world-objects/
    └── performance-baselines/
```

#### Test Data Management

```
CLASS: TestDataGenerator
METHODS:
  - GenerateObjects(count, distribution) -> List<dynamic>
  - GeneratePageSizedBatches(objectsPerPage) -> List<List<dynamic>>
  - GenerateIndexableData(cardinality) -> List<dynamic>
  - GenerateConflictingUpdates() -> List<UpdateOperation>

CLASS: TestHarness
METHODS:
  - SetupMultiProcessEnvironment() -> TestEnvironment
  - InjectStorageFailure(type, probability) -> void
  - MeasureOperationLatency(operation) -> LatencyMetrics
  - SimulateConcurrentLoad(ops/sec) -> LoadTestResults
```

#### Critical Testing Patterns

**Pattern 1: Page Consistency Validation**
```
AFTER EACH page operation:
  - Verify all objects in page accessible
  - Confirm index entries for all objects
  - Check page version incremented
  - Validate no orphaned data
```

**Pattern 2: Multi-Process Coordination**
```
FOR EACH multi-process test:
  - Use Process.Start() for true isolation
  - Implement inter-process synchronization
  - Verify no shared memory assumptions
  - Test with 3+ processes for edge cases
```

**Pattern 3: Performance Regression Prevention**
```
FOR EACH performance test:
  - Establish baseline measurements
  - Set strict latency budgets
  - Fail tests on regression > 10%
  - Profile hot paths
```

### Quality Metrics and Success Criteria

#### Code Coverage Requirements
- Line Coverage: 90% minimum
- Branch Coverage: 85% minimum
- Critical Path Coverage: 100%
- Error Path Coverage: 95%

#### Performance Validation
- All operations meet latency targets
- No memory leaks under sustained load
- Linear scaling up to 10 processes
- Graceful degradation under overload

#### Test Execution Standards
- All tests run in < 5 minutes (excluding E2E)
- E2E tests complete in < 30 minutes
- Zero flaky tests tolerated
- Parallel execution supported

## Story Breakdown

### Story 1: Core Database Infrastructure
**As a** developer  
**I want** database and table management with primary keys  
**So that** I can organize data into logical structures

**Acceptance Criteria:**
- Can create/delete databases with metadata storage
- Can create tables with primary key specification
- Database names follow namespace conventions
- Metadata is cached for performance
- Transaction wrapper maintains 1:1 mapping with storage
- E2E tests validate complete database lifecycle workflows
- Multi-process tests confirm metadata consistency
- Performance tests confirm < 10ms database operations

### Story 2: Page-Based Object Management
**As a** developer  
**I want** to perform CRUD operations on objects stored in pages  
**So that** I can manage data with primary key enforcement

**Acceptance Criteria:**
- Insert enforces primary key uniqueness
- Update finds object in page and rewrites entire page
- Get performs efficient page lookup then sequential search
- Delete removes object and handles empty page cleanup
- All operations handle optimistic concurrency conflicts
- Page operation tests cover 85% of sequential search logic
- Concurrent modification tests validate optimistic concurrency
- Performance benchmarks: < 5ms for page operations

### Story 3: Page-Aware Indexing System
**As a** developer  
**I want** indexes that map field values to page IDs  
**So that** I can efficiently query data stored in pages

**Acceptance Criteria:**
- Primary key index automatically created and maintained
- Secondary indexes can be created on any JSON path
- Indexes use SortedDictionary for efficient range queries
- Page modifications trigger full index rebuild for that page
- Indexes persist across restarts
- E2E tests validate real query patterns from production
- Multi-day tests confirm index consistency under load
- Performance tests verify < 1ms index lookups for 1M entries

### Story 4: Query Execution Engine
**As a** developer  
**I want** to query data using index-optimized filters  
**So that** I can retrieve objects efficiently

**Acceptance Criteria:**
- Query filters support equality and range operations
- Indexes used to identify relevant pages
- Pages scanned sequentially for matching objects
- Results returned as async enumerable
- Query hints guide index selection
- Query optimization tests validate index usage patterns
- Performance tests confirm < 10ms for indexed queries
- Memory tests verify O(1) streaming behavior

### Story 5: Page Event System
**As a** developer  
**I want** page modification events for cache coordination  
**So that** multiple processes can share the database

**Acceptance Criteria:**
- Storage subsystem fires events after commit
- Events include page ID and change type
- Cross-process delivery via file system monitoring
- Cache invalidation based on page changes
- No performance impact when events disabled
- Multi-process tests use Process.Start() for true isolation
- Event delivery tests confirm < 100ms cross-process latency
- Performance tests validate < 5% overhead when enabled

### Story 6: Production Hardening
**As a** developer  
**I want** the database layer to be production-ready  
**So that** it can handle real-world workloads

**Acceptance Criteria:**
- Performance meets targets (< 10ms P95 latency)
- Monitoring and metrics collection
- Graceful error handling with retries
- Comprehensive logging for debugging
- Documentation with examples
- 24-hour sustained load tests show no resource leaks
- Error injection tests validate recovery mechanisms
- All critical paths instrumented with metrics

## Testing Summary

This epic follows a **"No Mocking, No Falsehoods, No Bullshit"** testing philosophy with end-to-end tests that validate real-world usage:

- **Test Philosophy**: Every test uses real storage, real processes, real scenarios
- **E2E Focus**: Complete user workflows, not isolated components
- **Multi-Process**: True process isolation with Process.Start()
- **Production Validation**: Extended runs, peak loads, failure scenarios
- **Real Metrics**: Actual performance under genuine conditions

### Critical E2E Test Examples

```csharp
[TestClass]
public class RealWorldE2ETests
{
    [TestMethod]
    public async Task ECommerce_CompleteOrderWorkflow_WithRealProcesses()
    {
        // Setup - Launch multiple processes for microservices
        var orderService = Process.Start("OrderService.exe", "--db ./ecommerce.db");
        var inventoryService = Process.Start("InventoryService.exe", "--db ./ecommerce.db");
        var paymentService = Process.Start("PaymentService.exe", "--db ./ecommerce.db");
        
        // Real customer workflow
        var customerId = await CreateCustomerAccount();
        var cart = await BrowseAndAddProducts(customerId, productCount: 5);
        var orderId = await PlaceOrder(customerId, cart);
        
        // Validate across all services
        await ValidateInventoryDeducted(inventoryService, cart.Items);
        await ValidatePaymentProcessed(paymentService, orderId);
        await ValidateOrderFulfilled(orderService, orderId);
        
        // Run for 24 hours with continuous orders
        await RunContinuousOrderFlow(hours: 24);
        
        // Verify no resource leaks, data integrity maintained
        await ValidateSystemHealth();
    }
    
    [TestMethod]
    public async Task Banking_MultiProcessTransfers_MaintainsExactBalance()
    {
        // Setup - Real multi-process deployment
        var processes = new List<Process>();
        for (int i = 0; i < 10; i++)
        {
            processes.Add(Process.Start("BankingTeller.exe", $"--id {i} --db ./bank.db"));
        }
        
        // Initial state: $10M across 10,000 accounts
        var initialTotal = await GetTotalBalance();
        Assert.AreEqual(10_000_000m, initialTotal);
        
        // Run 100,000 concurrent transfers for 48 hours
        var transferTask = RunConcurrentTransfers(processes, transfersPerHour: 100_000);
        await Task.Delay(TimeSpan.FromHours(48));
        
        // Stop transfers and validate
        await StopAllProcesses(processes);
        var finalTotal = await GetTotalBalance();
        
        // Must maintain exact balance despite millions of transfers
        Assert.AreEqual(10_000_000m, finalTotal, "Money was created or destroyed!");
    }
    
    [TestMethod]
    public async Task DisasterRecovery_DataCenterFailure_ZeroDataLoss()
    {
        // Setup production-like deployment
        var primaryDC = await DeployDataCenter("primary", processCount: 20);
        var backupDC = await DeployDataCenter("backup", processCount: 20);
        
        // Run production workload
        await RunProductionWorkload(primaryDC, hours: 24);
        var preFailureData = await CaptureCompleteDataSnapshot(primaryDC);
        
        // Simulate catastrophic failure
        await SimulatePowerLoss(primaryDC);
        await KillAllProcesses(primaryDC, force: true);
        await CorruptRandomFiles(primaryDC.DataPath, corruptionPercent: 10);
        
        // Failover to backup
        await PromoteToActive(backupDC);
        await RecoverFromBackup(backupDC, primaryDC.LastBackup);
        
        // Validate zero data loss
        var postRecoveryData = await CaptureCompleteDataSnapshot(backupDC);
        await ValidateDataIntegrity(preFailureData, postRecoveryData);
        
        // Resume operations and verify stability
        await RunProductionWorkload(backupDC, hours: 24);
    }
}
```

### Real Performance Validation

```csharp
[TestMethod]
public async Task Performance_SustainedProductionLoad_MeetsAllSLAs()
{
    // Deploy full production topology
    var deployment = await DeployProductionTopology(
        webServers: 5,
        appServers: 10,
        databaseProcesses: 3
    );
    
    // Ramp up to production load
    await RampUpLoad(deployment, targetOpsPerSec: 10_000, rampMinutes: 30);
    
    // Sustain production load for 7 days
    var metrics = await RunProductionWorkload(deployment, days: 7);
    
    // Validate all SLAs met throughout
    Assert.IsTrue(metrics.P50Latency < TimeSpan.FromMilliseconds(5));
    Assert.IsTrue(metrics.P95Latency < TimeSpan.FromMilliseconds(10));
    Assert.IsTrue(metrics.P99Latency < TimeSpan.FromMilliseconds(50));
    Assert.IsTrue(metrics.ErrorRate < 0.001); // < 0.1% errors
    Assert.IsTrue(metrics.AvailabilityPercent > 99.99);
    
    // Verify no resource leaks
    Assert.IsTrue(metrics.MemoryLeakDetected == false);
    Assert.IsTrue(metrics.HandleLeakDetected == false);
    Assert.IsTrue(metrics.DiskSpaceExhausted == false);
}

[TestMethod]
public async Task Performance_BlackFridaySimulation_HandlesExtremeLoad()
{
    // Setup e-commerce platform
    var platform = await DeployECommercePlatform(production: true);
    
    // Normal load baseline
    await RunNormalLoad(platform, hours: 2);
    var baselineMetrics = await CaptureMetrics(platform);
    
    // Black Friday: 50x normal load spike
    var spikeLoad = baselineMetrics.OpsPerSecond * 50;
    await SpikeLoa d(platform, targetOps: spikeLoad);
    
    // Sustain extreme load for 12 hours
    var blackFridayMetrics = await RunBlackFridaySimulation(platform, hours: 12);
    
    // System must remain operational
    Assert.IsTrue(blackFridayMetrics.SystemOperational);
    Assert.IsTrue(blackFridayMetrics.OrdersProcessed > 1_000_000);
    Assert.IsTrue(blackFridayMetrics.DataIntegrityMaintained);
    
    // Graceful degradation acceptable, crashes not
    Assert.IsTrue(blackFridayMetrics.CrashCount == 0);
}
```

## Implementation Plan

### Phase 1: Core Database Infrastructure (Week 1)
- Database and table management with primary keys
- Metadata storage and caching
- Transaction wrapper implementation
- Page-aware data structures

### Phase 2: Page-Based Object Management (Week 2)
- Expando object conversion
- Page-aware CRUD operations with sequential search
- Streaming result iterators
- Optimistic concurrency handling

### Phase 3: Page-Aware SortedDictionary Indexing (Week 3)
- Primary key index mapping to pages
- Secondary index creation and maintenance
- Index serialization with page references
- Page-to-key reverse mapping

### Phase 4: Query Execution (Week 4)
- Page-based query optimization
- Query filter evaluation with page scanning
- Transaction isolation for queries
- Performance optimization for large pages

### Phase 5: Page Event System (Week 5)
- Storage subsystem modifications
- File system monitoring
- Cross-process event delivery
- Cache invalidation based on page changes

### Phase 6: Production Hardening (Week 6)
- Performance optimization
- Monitoring and diagnostics
- Comprehensive error handling
- Documentation and examples

## Success Criteria

### Epic Complete When:
- [ ] All interfaces implemented with full test coverage per category targets
- [ ] Primary key management enforced at table level with 95% test coverage
- [ ] Page-aware SortedDictionary indexes fully operational with < 1ms lookups
- [ ] Page-based object updates with sequential search working at < 5ms latency
- [ ] Page event system reliably propagating changes with < 100ms delivery
- [ ] Multi-process cache coordination working with Process.Start() validation
- [ ] Performance targets achieved: < 10ms P95 for all database operations
- [ ] 100% backward compatibility proven through migration tests
- [ ] Production monitoring and alerting configured with < 1% overhead
- [ ] Comprehensive documentation with examples and test scenarios
- [ ] Real disaster recovery scenarios successfully tested
- [ ] 24-hour sustained load test passed with no resource leaks
- [ ] All E2E test scenarios passing with real data and real load

### Performance Requirements
- Database operations: < 10ms P95 latency
- Index lookups to pages: < 1ms for up to 1M entries
- Page scanning: < 5ms for pages with up to 200 objects
- Cache hit rate: > 90% for metadata operations
- Page event delivery: < 100ms cross-process
- Transaction overhead: < 5% vs raw storage

---

**Epic Owner:** Database Architecture Team  
**Priority:** High  
**Status:** Ready for Implementation  
**Dependencies:** Epic-003 completion  
**Next Action:** Begin Phase 1 - Core Database Infrastructure