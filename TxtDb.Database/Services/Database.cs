using TxtDb.Database.Interfaces;
using TxtDb.Database.Models;
using TxtDb.Database.Exceptions;
using TxtDb.Storage.Interfaces.Async;

namespace TxtDb.Database.Services;

/// <summary>
/// Represents a database containing tables and indexes.
/// 
/// CRITICAL FIX: Removed table caching infrastructure to eliminate stale index issues.
/// Table instances are now created fresh on each access, ensuring indexes reflect
/// current persistent state. This fixes Epic 004 Story 1 data visibility problems.
/// 
/// THREAD SAFETY: Uses proper metadata synchronization to prevent collection
/// modification during enumeration exceptions. All metadata operations are
/// protected with defensive copying and atomic updates.
/// </summary>
public class Database : IDatabase
{
    private readonly DatabaseMetadata _metadata;
    private readonly IAsyncStorageSubsystem _storageSubsystem;
    private readonly string _storageRootPath; // PHASE 2 FIX: Store the storage root path
    private readonly object _metadataLock = new(); // CRITICAL: Separate lock for metadata operations

    public string Name => _metadata.Name;
    public DateTime CreatedAt => _metadata.CreatedAt;
    public IDictionary<string, object> Metadata => _metadata.Properties;

    public Database(string name, DatabaseMetadata metadata, IAsyncStorageSubsystem storageSubsystem, string storageRootPath)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _storageSubsystem = storageSubsystem ?? throw new ArgumentNullException(nameof(storageSubsystem));
        _storageRootPath = storageRootPath ?? throw new ArgumentNullException(nameof(storageRootPath));
        // CRITICAL FIX: Removed table cache initialization - no more caching
    }

    public async Task<ITable> CreateTableAsync(
        string name, 
        string primaryKeyField,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new InvalidTableNameException(name, "Table name cannot be null or empty");
        
        if (name.Contains(" "))
            throw new InvalidTableNameException(name, "Table name cannot contain spaces");
        
        if (string.IsNullOrWhiteSpace(primaryKeyField))
            throw new InvalidPrimaryKeyPathException(primaryKeyField, "Primary key path cannot be null or empty");
        
        if (!primaryKeyField.StartsWith("$."))
            throw new InvalidPrimaryKeyPathException(primaryKeyField, "Primary key path must start with '$.'");

        // CRITICAL FIX: Check table existence through storage layer instead of cache
        // This ensures we always check the current persistent state
        var existingTable = await GetTableAsync(name, cancellationToken);
        if (existingTable != null)
        {
            throw new TableAlreadyExistsException(_metadata.Name, name);
        }

        // CRITICAL FIX: Create table metadata with thread-safe access to database metadata
        TableMetadata tableMetadata;
        lock (_metadataLock)
        {
            tableMetadata = new TableMetadata
            {
                Name = name,
                DatabaseName = _metadata.Name,
                PrimaryKeyField = primaryKeyField,
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
                SerializationFormat = _metadata.DefaultFormat,
                Properties = new Dictionary<string, object>(),
                TargetObjectsPerPage = _metadata.AverageObjectsPerPage
            };
        }

        // Store table metadata
        var txnId = await _storageSubsystem.BeginTransactionAsync(cancellationToken);
        try
        {
            var tableNamespace = $"{_metadata.Name}.{name}";
            var metadataNamespace = $"{tableNamespace}._metadata";
            
            // Create namespaces - it's ok if they already exist
            try { await _storageSubsystem.CreateNamespaceAsync(txnId, tableNamespace, cancellationToken); } catch { }
            try { await _storageSubsystem.CreateNamespaceAsync(txnId, metadataNamespace, cancellationToken); } catch { }
            
            // CRITICAL FIX: Create a completely isolated copy to avoid serialization issues
            // This prevents concurrent access during JSON serialization
            var tableMetadataCopy = new TableMetadata
            {
                Name = tableMetadata.Name,
                DatabaseName = tableMetadata.DatabaseName,
                PrimaryKeyField = tableMetadata.PrimaryKeyField,
                CreatedAt = tableMetadata.CreatedAt,
                ModifiedAt = tableMetadata.ModifiedAt,
                SerializationFormat = tableMetadata.SerializationFormat,
                Properties = new Dictionary<string, object>(tableMetadata.Properties),
                TargetObjectsPerPage = tableMetadata.TargetObjectsPerPage
            };
            
            await _storageSubsystem.InsertObjectAsync(txnId, metadataNamespace, tableMetadataCopy, cancellationToken);
            
            // Create primary key index namespace
            var primaryIndexNamespace = $"{tableNamespace}._indexes._primary";
            try { await _storageSubsystem.CreateNamespaceAsync(txnId, primaryIndexNamespace, cancellationToken); } catch { }
            
            await _storageSubsystem.CommitTransactionAsync(txnId, cancellationToken);
        }
        catch (Exception ex)
        {
            try
            {
                await _storageSubsystem.RollbackTransactionAsync(txnId, cancellationToken);
            }
            catch
            {
                // Ignore rollback errors - transaction might already be completed
            }
            
            // Check if this is a duplicate table error and throw appropriate exception
            if (ex.Message.Contains("already exists") || ex.Message.Contains("duplicate"))
            {
                throw new TableAlreadyExistsException(_metadata.Name, name);
            }
            
            throw;
        }

        // CRITICAL FIX: Create table instance without caching
        // Fresh instances ensure indexes reflect current persistent state
        var table = new Table(name, tableMetadata, _storageSubsystem, _storageRootPath);
        
        // CRITICAL FIX: Initialize indexes even for newly created tables
        // This ensures the table can find any existing data (edge case: table recreation)
        // and prepares the index structures for immediate use
        await ((Table)table).InitializeIndexesFromStorageAsync(cancellationToken);

        // CRITICAL FIX: Update database metadata with proper synchronization
        // Use separate metadata lock to prevent collection modification during enumeration
        lock (_metadataLock)
        {
            // Create a completely new Tables list to avoid any concurrent access issues
            var newTables = new List<string>(_metadata.Tables) { name };
            _metadata.Tables = newTables;
            _metadata.ModifiedAt = DateTime.UtcNow;
        }

        return table;
    }

    public async Task<ITable?> GetTableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            return null;

        // PHASE 3 FIX: Use proper MVCC Storage layer instead of bypassing it
        // The Storage layer properly handles version visibility for fresh instances
        
        try
        {
            var txnId = await _storageSubsystem.BeginTransactionAsync(cancellationToken);
            try
            {
                var metadataNamespace = $"{_metadata.Name}.{name}._metadata";
                var objects = await _storageSubsystem.GetMatchingObjectsAsync(txnId, metadataNamespace, "*", cancellationToken);

                foreach (var pageObjects in objects.Values)
                {
                    foreach (var obj in pageObjects)
                    {
                        TableMetadata? tableMetadata = null;
                        
                        // Handle different object types from storage  
                        // CRITICAL FIX: Add support for ExpandoObject deserialization
                        // The Storage layer returns data as ExpandoObject when using JSON format adapter
                        if (obj is TableMetadata tableMeta)
                        {
                            tableMetadata = tableMeta;
                        }
                        else if (obj is Newtonsoft.Json.Linq.JObject jObj)
                        {
                            try
                            {
                                tableMetadata = jObj.ToObject<TableMetadata>();
                            }
                            catch
                            {
                                continue; // Skip objects that can't be deserialized
                            }
                        }
                        else if (obj is System.Dynamic.ExpandoObject expandoObj)
                        {
                            try
                            {
                                // Convert ExpandoObject to JSON string then deserialize
                                var json = Newtonsoft.Json.JsonConvert.SerializeObject(expandoObj);
                                tableMetadata = Newtonsoft.Json.JsonConvert.DeserializeObject<TableMetadata>(json);
                            }
                            catch
                            {
                                continue; // Skip objects that can't be deserialized
                            }
                        }
                        
                        if (tableMetadata != null && tableMetadata.Name == name)
                        {
                            // CRITICAL FIX: Create fresh table instance without caching
                            // Each access gets a new instance with fresh index state
                            var table = new Table(name, tableMetadata, _storageSubsystem, _storageRootPath);
                            
                            // CRITICAL FIX: Initialize indexes from existing storage data
                            // Fresh table instances need to rebuild indexes from persisted data
                            await ((Table)table).InitializeIndexesFromStorageAsync(cancellationToken);
                            
                            await _storageSubsystem.CommitTransactionAsync(txnId, cancellationToken);
                            Console.WriteLine($"[PHASE3] Found table '{name}' using proper MVCC Storage layer");
                            return table;
                        }
                    }
                }
                
                await _storageSubsystem.CommitTransactionAsync(txnId, cancellationToken);
            }
            catch
            {
                await _storageSubsystem.RollbackTransactionAsync(txnId, cancellationToken);
                throw;
            }
        }
        catch
        {
            throw;
        }

        return null;
    }

    public async Task<bool> DeleteTableAsync(string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
            return false;

        var table = await GetTableAsync(name, cancellationToken);
        if (table == null)
            return false;

        // TODO: Check for active operations

        var txnId = await _storageSubsystem.BeginTransactionAsync(cancellationToken);
        try
        {
            var tableNamespace = $"{_metadata.Name}.{name}";
            await _storageSubsystem.DeleteNamespaceAsync(txnId, tableNamespace, cancellationToken);
            await _storageSubsystem.CommitTransactionAsync(txnId, cancellationToken);
        }
        catch
        {
            await _storageSubsystem.RollbackTransactionAsync(txnId, cancellationToken);
            throw;
        }

        // CRITICAL FIX: No cache to remove from - update metadata with proper synchronization
        
        // Use separate metadata lock and defensive copying
        lock (_metadataLock)
        {
            // Create a completely new Tables list without the removed table
            var newTables = new List<string>(_metadata.Tables);
            newTables.Remove(name);
            _metadata.Tables = newTables;
            _metadata.ModifiedAt = DateTime.UtcNow;
        }

        return true;
    }

    public async Task<string[]> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        // CRITICAL FIX: Use defensive copying to prevent enumeration modification exceptions
        lock (_metadataLock)
        {
            return _metadata.Tables.ToArray(); // Creates a snapshot, safe for enumeration
        }
    }
    
}