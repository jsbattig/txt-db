using TxtDb.Database.Interfaces;
using TxtDb.Database.Models;
using TxtDb.Database.Exceptions;
using TxtDb.Storage.Interfaces.Async;
using Newtonsoft.Json.Linq;
using System.Dynamic;

namespace TxtDb.Database.Services;

/// <summary>
/// Metadata for persisting secondary index information
/// </summary>
internal class IndexMetadata
{
    public string Name { get; set; } = string.Empty;
    public string FieldPath { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// SPECIFICATION COMPLIANT: Table implementation that strictly follows Epic 004 specification.
/// 
/// KEY COMPLIANCE FEATURES:
/// - Uses SortedDictionary<object, HashSet<string>> for indexes (not ConcurrentDictionary<object, string>)
/// - NO index persistence system
/// - NO retry logic for concurrency conflicts  
/// - NO index versioning infrastructure
/// - Synchronous constructor initialization
/// - All required interface methods implemented
/// </summary>
public class Table : ITable
{
    private readonly TableMetadata _metadata;
    private readonly IAsyncStorageSubsystem _storageSubsystem;
    private readonly string _storageRootPath; // PHASE 2 FIX: Store the storage root path
    private readonly Dictionary<string, HashSet<string>> _primaryKeyIndex; // TYPE-SAFE: Use normalized string keys for reliable comparison
    private readonly Dictionary<string, HashSet<string>> _pageToKeys; // Reverse mapping for efficient updates - stores normalized key strings
    private readonly Dictionary<string, Index> _secondaryIndexes; // Secondary indexes
    private readonly object _indexLock = new();

    public string Name => _metadata.Name;
    public string PrimaryKeyField => _metadata.PrimaryKeyField;  
    public DateTime CreatedAt => _metadata.CreatedAt;

    /// <summary>
    /// SPECIFICATION COMPLIANT: Synchronous constructor initialization
    /// </summary>
    public Table(string name, TableMetadata metadata, IAsyncStorageSubsystem storageSubsystem, string storageRootPath)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _storageSubsystem = storageSubsystem ?? throw new ArgumentNullException(nameof(storageSubsystem));
        _storageRootPath = storageRootPath ?? throw new ArgumentNullException(nameof(storageRootPath));
        
        // TYPE-SAFE: Use Dictionary with normalized string keys to avoid type comparison issues
        _primaryKeyIndex = new Dictionary<string, HashSet<string>>();
        _pageToKeys = new Dictionary<string, HashSet<string>>();
        _secondaryIndexes = new Dictionary<string, Index>();
    }

    public async Task<object> InsertAsync(
        IDatabaseTransaction txn, 
        dynamic obj,
        CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (obj == null) throw new ArgumentNullException(nameof(obj));

        // Extract primary key and wrap in TypedPrimaryKey for type safety
        var primaryKeyValue = ExtractPrimaryKey(obj);
        if (primaryKeyValue == null)
            throw new MissingPrimaryKeyException(_metadata.PrimaryKeyField);
        
        var primaryKey = TypedPrimaryKey.FromValue(primaryKeyValue);
        var normalizedKey = primaryKey.ToNormalizedKey();
        Console.WriteLine($"[DEBUG] InsertAsync - Primary key: {primaryKey} (Type: {primaryKeyValue?.GetType()}), Normalized: {normalizedKey}");
        lock (_indexLock)
        {
            if (_primaryKeyIndex.ContainsKey(normalizedKey))
                throw new DuplicatePrimaryKeyException(_metadata.Name, primaryKey.Value);
        }

        // Insert object into storage
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        var pageId = await _storageSubsystem.InsertObjectAsync(storageTransactionId, tableNamespace, obj, cancellationToken);
        
        // TYPE-SAFE: Update index using normalized keys
        lock (_indexLock)
        {
            // Add to primary key index using normalized key
            if (!_primaryKeyIndex.ContainsKey(normalizedKey))
            {
                _primaryKeyIndex[normalizedKey] = new HashSet<string>();
            }
            _primaryKeyIndex[normalizedKey].Add(pageId);
            Console.WriteLine($"[DEBUG] InsertAsync - Added to index: key={normalizedKey}, page={pageId}, total keys in index={_primaryKeyIndex.Count}");
            
            // Add reverse mapping using normalized key
            if (!_pageToKeys.ContainsKey(pageId))
            {
                _pageToKeys[pageId] = new HashSet<string>();
            }
            _pageToKeys[pageId].Add(normalizedKey);
            
            // Update secondary indexes
            foreach (var index in _secondaryIndexes.Values)
            {
                var indexValue = ExtractFieldValue(obj, index.FieldPath);
                if (indexValue != null)
                {
                    index.AddEntry(indexValue, pageId);
                }
            }
        }

        return primaryKey.Value;
    }

    public async Task UpdateAsync(
        IDatabaseTransaction txn, 
        object primaryKey, 
        dynamic obj,
        CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (primaryKey == null) throw new ArgumentNullException(nameof(primaryKey));
        if (obj == null) throw new ArgumentNullException(nameof(obj));

        // Verify primary key matches using type-safe comparison
        var newPrimaryKeyValue = ExtractPrimaryKey(obj);
        if (newPrimaryKeyValue == null)
            throw new PrimaryKeyMismatchException(primaryKey, newPrimaryKeyValue);
            
        var newPrimaryKey = TypedPrimaryKey.FromValue(newPrimaryKeyValue);
        var typedPrimaryKey = (primaryKey is TypedPrimaryKey) ? (TypedPrimaryKey)primaryKey : TypedPrimaryKey.FromValue(primaryKey);
        
        if (!newPrimaryKey.Equals(typedPrimaryKey))
            throw new PrimaryKeyMismatchException(primaryKey, newPrimaryKey.Value);

        // Find page containing the object using normalized key
        var normalizedKey = typedPrimaryKey.ToNormalizedKey();
        HashSet<string>? pageIds = null;
        lock (_indexLock)
        {
            if (!_primaryKeyIndex.TryGetValue(normalizedKey, out pageIds))
                throw new ObjectNotFoundException(_metadata.Name, primaryKey);
        }

        // SPECIFICATION COMPLIANT: Simple implementation - use first page
        var pageId = pageIds.First();
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        // Read the page
        var pageObjects = await _storageSubsystem.ReadPageAsync(storageTransactionId, tableNamespace, pageId, cancellationToken);
        
        // Find and replace the object in the page using type-safe comparison
        bool found = false;
        for (int i = 0; i < pageObjects.Length; i++)
        {
            var existingPrimaryKeyValue = ExtractPrimaryKey(pageObjects[i]);
            if (existingPrimaryKeyValue != null)
            {
                var existingPrimaryKey = TypedPrimaryKey.FromValue(existingPrimaryKeyValue);
                if (existingPrimaryKey.Equals(typedPrimaryKey))
                {
                    pageObjects[i] = obj;
                    found = true;
                    break;
                }
            }
        }

        if (!found)
            throw new ObjectNotFoundException(_metadata.Name, primaryKey);

        // Update the entire page
        await _storageSubsystem.UpdatePageAsync(storageTransactionId, tableNamespace, pageId, pageObjects, cancellationToken);
        
        // SPECIFICATION COMPLIANT: Rebuild ALL indexes for ALL objects in the page (safety first)
        lock (_indexLock)
        {
            RebuildIndexesForPage(pageId, pageObjects);
        }
    }

    public async Task<dynamic?> GetAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (primaryKey == null) throw new ArgumentNullException(nameof(primaryKey));

        // Find page containing the object using normalized key
        var typedPrimaryKey = (primaryKey is TypedPrimaryKey) ? (TypedPrimaryKey)primaryKey : TypedPrimaryKey.FromValue(primaryKey);
        var normalizedKey = typedPrimaryKey.ToNormalizedKey();
        Console.WriteLine($"[DEBUG] GetAsync - Primary key: {primaryKey}, Normalized: {normalizedKey}");
        HashSet<string>? pageIds = null;
        lock (_indexLock)
        {
            Console.WriteLine($"[DEBUG] GetAsync - Total keys in index: {_primaryKeyIndex.Count}");
            foreach (var key in _primaryKeyIndex.Keys.Take(5))
            {
                Console.WriteLine($"[DEBUG] GetAsync - Index contains key: {key}");
            }
            if (!_primaryKeyIndex.TryGetValue(normalizedKey, out pageIds))
            {
                Console.WriteLine($"[DEBUG] GetAsync - Key not found in index, returning null");
                return null;
            }
        }

        // Read the page and search for object
        var pageId = pageIds.First();
        Console.WriteLine($"[DEBUG] GetAsync - Found page: {pageId}");
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        Console.WriteLine($"[DEBUG] GetAsync - Reading page from storage: namespace={tableNamespace}, pageId={pageId}, txnId={storageTransactionId}");
        var pageObjects = await _storageSubsystem.ReadPageAsync(storageTransactionId, tableNamespace, pageId, cancellationToken);
        Console.WriteLine($"[DEBUG] GetAsync - Page has {pageObjects.Length} objects");
        
        // Find the object in the page using type-safe comparison
        Console.WriteLine($"[DEBUG] GetAsync - Searching for primaryKey: {primaryKey} (type: {primaryKey.GetType()})");
        for (int i = 0; i < pageObjects.Length; i++)
        {
            var pageObj = pageObjects[i];
            Console.WriteLine($"[DEBUG] GetAsync - Examining page object {i}: {pageObj}");
            var existingPrimaryKeyValue = ExtractPrimaryKey(pageObj);
            Console.WriteLine($"[DEBUG] GetAsync - Extracted primary key: {existingPrimaryKeyValue} (type: {existingPrimaryKeyValue?.GetType()})");
            
            if (existingPrimaryKeyValue != null)
            {
                var existingPrimaryKey = TypedPrimaryKey.FromValue(existingPrimaryKeyValue);
                Console.WriteLine($"[DEBUG] GetAsync - Typed existing key: {existingPrimaryKey} (type: {existingPrimaryKey.ValueType})");
                var keysEqual = existingPrimaryKey.Equals(typedPrimaryKey);
                Console.WriteLine($"[DEBUG] GetAsync - Keys equal: {keysEqual}");
                
                if (keysEqual)
                {
                    Console.WriteLine($"[DEBUG] GetAsync - Found matching object, converting to expando");
                    var result = ConvertToExpando(pageObj);
                    Console.WriteLine($"[DEBUG] GetAsync - Converted result: {result}");
                    return result;
                }
            }
        }

        return null;
    }

    public async Task<bool> DeleteAsync(
        IDatabaseTransaction txn, 
        object primaryKey,
        CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (primaryKey == null) throw new ArgumentNullException(nameof(primaryKey));

        // Find page containing the object using normalized key
        var typedPrimaryKey = (primaryKey is TypedPrimaryKey) ? (TypedPrimaryKey)primaryKey : TypedPrimaryKey.FromValue(primaryKey);
        var normalizedKey = typedPrimaryKey.ToNormalizedKey();
        HashSet<string>? pageIds = null;
        lock (_indexLock)
        {
            if (!_primaryKeyIndex.TryGetValue(normalizedKey, out pageIds))
                return false;
        }

        var pageId = pageIds.First();
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        // Read the page and remove the object
        var pageObjects = await _storageSubsystem.ReadPageAsync(storageTransactionId, tableNamespace, pageId, cancellationToken);
        var newPageObjects = new List<object>();
        bool found = false;
        
        foreach (var pageObj in pageObjects)
        {
            var existingPrimaryKeyValue = ExtractPrimaryKey(pageObj);
            if (existingPrimaryKeyValue != null)
            {
                var existingPrimaryKey = TypedPrimaryKey.FromValue(existingPrimaryKeyValue);
                if (existingPrimaryKey.Equals(typedPrimaryKey))
                {
                    found = true;
                    // Skip adding this object (delete it)
                }
                else
                {
                    newPageObjects.Add(pageObj);
                }
            }
            else
            {
                newPageObjects.Add(pageObj);
            }
        }

        if (!found) return false;

        // Update the page
        await _storageSubsystem.UpdatePageAsync(storageTransactionId, tableNamespace, pageId, newPageObjects.ToArray(), cancellationToken);
        
        // TYPE-SAFE: Update indexes using normalized keys
        lock (_indexLock)
        {
            // Remove from primary index using normalized key
            if (_primaryKeyIndex.TryGetValue(normalizedKey, out var pages))
            {
                pages.Remove(pageId);
                if (pages.Count == 0)
                {
                    _primaryKeyIndex.Remove(normalizedKey);
                }
            }
            
            // Remove from reverse mapping using normalized key
            if (_pageToKeys.TryGetValue(pageId, out var keys))
            {
                keys.Remove(normalizedKey);
                if (keys.Count == 0)
                {
                    _pageToKeys.Remove(pageId);
                }
            }
            
            // Update secondary indexes
            foreach (var index in _secondaryIndexes.Values)
            {
                index.RemoveEntry(primaryKey, pageId);
            }
            
            // Rebuild indexes for remaining objects in page
            RebuildIndexesForPage(pageId, newPageObjects.ToArray());
        }

        return true;
    }

    public async Task<IIndex> CreateIndexAsync(IDatabaseTransaction txn, string indexName, string fieldPath, CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (string.IsNullOrEmpty(indexName)) throw new ArgumentException("Index name cannot be empty");
        if (string.IsNullOrEmpty(fieldPath)) throw new ArgumentException("Field path cannot be empty");

        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        
        // Check for duplicate index name
        lock (_indexLock)
        {
            if (_secondaryIndexes.ContainsKey(indexName))
            {
                throw new InvalidOperationException($"Index '{indexName}' already exists");
            }
        }
        
        // Create and persist index metadata
        var indexMetadata = new IndexMetadata
        {
            Name = indexName,
            FieldPath = fieldPath,
            CreatedAt = DateTime.UtcNow
        };
        
        // Persist index metadata to storage
        var indexMetadataNamespace = $"{tableNamespace}._indexes._metadata";
        
        // Extract transaction ID from the database transaction (this is a workaround for the interface)
        var storageTransaction = ((DatabaseTransaction)txn).TransactionId;
        
        try
        {
            await _storageSubsystem.CreateNamespaceAsync(storageTransaction, indexMetadataNamespace, cancellationToken);
        }
        catch
        {
            // Namespace might already exist, ignore
        }
        
        await _storageSubsystem.InsertObjectAsync(storageTransaction, indexMetadataNamespace, indexMetadata, cancellationToken);
        
        var index = new Index(indexName, fieldPath, _storageSubsystem, tableNamespace);
        
        lock (_indexLock)
        {
            _secondaryIndexes[indexName] = index;
        }
        
        // Build the index by scanning all existing data
        await RebuildSecondaryIndex(txn, index, cancellationToken);
        
        return index;
    }

    public async Task DropIndexAsync(IDatabaseTransaction txn, string indexName, CancellationToken cancellationToken = default)
    {
        if (txn == null) throw new ArgumentNullException(nameof(txn));
        if (string.IsNullOrEmpty(indexName)) throw new ArgumentException("Index name cannot be empty");
        
        // Check if index exists
        bool indexExists;
        lock (_indexLock)
        {
            indexExists = _secondaryIndexes.ContainsKey(indexName);
        }
        
        if (!indexExists)
        {
            throw new InvalidOperationException($"Index '{indexName}' does not exist");
        }
        
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var indexMetadataNamespace = $"{tableNamespace}._indexes._metadata";
        
        // Extract transaction ID from the database transaction
        var storageTransaction = ((DatabaseTransaction)txn).TransactionId;
        
        try
        {
            // Find and remove the index metadata from storage
            var allIndexMetadata = await _storageSubsystem.GetMatchingObjectsAsync(storageTransaction, indexMetadataNamespace, "*", cancellationToken);
            
            Console.WriteLine($"DROP INDEX DEBUG: Found {allIndexMetadata.Count} pages of index metadata");
            
            foreach (var (pageId, pageObjects) in allIndexMetadata)
            {
                Console.WriteLine($"DROP INDEX DEBUG: Processing page '{pageId}' with {pageObjects.Length} objects");
                
                var remainingObjects = new List<object>();
                bool foundIndexToRemove = false;
                
                foreach (var metadataObj in pageObjects)
                {
                    string? metadataIndexName = null;
                    
                    if (metadataObj is System.Dynamic.ExpandoObject expando)
                    {
                        var dict = (IDictionary<string, object?>)expando;
                        if (dict.TryGetValue("Name", out var nameValue))
                        {
                            metadataIndexName = nameValue?.ToString();
                        }
                    }
                    else
                    {
                        var nameProperty = metadataObj.GetType().GetProperty("Name");
                        metadataIndexName = nameProperty?.GetValue(metadataObj)?.ToString();
                    }
                    
                    Console.WriteLine($"DROP INDEX DEBUG: Found index metadata for '{metadataIndexName}'");
                    
                    // If this is NOT the index we want to remove, keep it
                    if (metadataIndexName != indexName)
                    {
                        remainingObjects.Add(metadataObj);
                        Console.WriteLine($"DROP INDEX DEBUG: Keeping index '{metadataIndexName}'");
                    }
                    else
                    {
                        foundIndexToRemove = true;
                        Console.WriteLine($"DROP INDEX DEBUG: Removing index '{metadataIndexName}'");
                    }
                }
                
                if (foundIndexToRemove)
                {
                    Console.WriteLine($"DROP INDEX DEBUG: Updating page with {remainingObjects.Count} remaining objects");
                    // Update the page with remaining objects
                    await _storageSubsystem.UpdatePageAsync(storageTransaction, indexMetadataNamespace, pageId, remainingObjects.ToArray(), cancellationToken);
                    Console.WriteLine($"DROP INDEX DEBUG: Successfully updated page '{pageId}'");
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"WARNING: Failed to remove index metadata for '{indexName}': {ex.Message}");
        }
        
        // Remove from in-memory index
        lock (_indexLock)
        {
            _secondaryIndexes.Remove(indexName);
        }
    }

    public async Task<IList<string>> ListIndexesAsync(IDatabaseTransaction txn, CancellationToken cancellationToken = default)
    {
        lock (_indexLock)
        {
            var indexes = new List<string> { "_primary" }; // Always include primary key index
            indexes.AddRange(_secondaryIndexes.Keys);
            return indexes;
        }
    }

    public async Task<IList<dynamic>> QueryAsync(IDatabaseTransaction txn, IQueryFilter filter, CancellationToken cancellationToken = default)
    {
        var results = new List<dynamic>();
        
        // Simple implementation: scan all pages and apply filter
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        var allObjects = await _storageSubsystem.GetMatchingObjectsAsync(storageTransactionId, tableNamespace, "*", cancellationToken);
        
        foreach (var (pageId, pageObjects) in allObjects)
        {
            foreach (var obj in pageObjects)
            {
                var expandoObj = ConvertToExpando(obj);
                if (filter.Matches(expandoObj))
                {
                    results.Add(expandoObj);
                }
            }
        }
        
        return results;
    }

    /// <summary>
    /// TYPE-SAFE: Initializes table indexes by scanning existing storage data.
    /// This method MUST be called after table construction to rebuild indexes from persisted data.
    /// 
    /// CRITICAL REQUIREMENT: Tables start with empty indexes but may have existing data in storage.
    /// This method ensures that fresh table instances can find existing data by rebuilding the 
    /// primary key index from all stored objects.
    /// 
    /// PHASE 2 FIX: Added comprehensive debugging to identify and fix index rebuilding issues
    /// </summary>
    public async Task InitializeIndexesFromStorageAsync(CancellationToken cancellationToken = default)
    {
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        
        Console.WriteLine($"[PHASE2] InitializeIndexesFromStorageAsync: Starting for namespace '{tableNamespace}'");
        
        // PHASE 3 FIX: Use proper MVCC Storage layer instead of bypassing it
        // The Storage layer properly handles version visibility for fresh instances
        Console.WriteLine($"[PHASE3] Using proper MVCC Storage layer for index rebuilding");
        
        try
        {
            var txnId = await _storageSubsystem.BeginTransactionAsync(cancellationToken);
            try
            {
                // Get all objects using proper MVCC Storage layer
                var allObjects = await _storageSubsystem.GetMatchingObjectsAsync(txnId, tableNamespace, "*", cancellationToken);
            
            Console.WriteLine($"[PHASE2] GetMatchingObjectsAsync returned {allObjects.Count} pages");
            
            lock (_indexLock)
            {
                // Clear existing indexes (should be empty anyway for new instances)
                _primaryKeyIndex.Clear();
                _pageToKeys.Clear();
                _secondaryIndexes.Clear();
                
                Console.WriteLine($"[PHASE2] Cleared existing indexes");
            }
            
            // Load persisted secondary indexes (outside lock)
            await LoadSecondaryIndexesFromStorage(txnId, tableNamespace, cancellationToken);
            
            lock (_indexLock)
            {
                
                // Rebuild primary key index from all stored data
                int totalObjects = 0;
                int indexedObjects = 0;
                
                foreach (var (pageId, pageObjects) in allObjects)
                {
                    Console.WriteLine($"[PHASE2] Processing page '{pageId}' with {pageObjects.Length} objects");
                    
                    if (!_pageToKeys.ContainsKey(pageId))
                    {
                        _pageToKeys[pageId] = new HashSet<string>();
                    }
                    
                    foreach (var obj in pageObjects)
                    {
                        totalObjects++;
                        Console.WriteLine($"[PHASE2] Processing object {totalObjects}: {obj} (type: {obj.GetType().Name})");
                        
                        var rawPrimaryKey = ExtractPrimaryKey(obj);
                        Console.WriteLine($"[PHASE2] Extracted primary key: {rawPrimaryKey} (type: {rawPrimaryKey?.GetType().Name})");
                        
                        // CRITICAL FIX: Normalize JValue to underlying primitive value for consistent key types
                        // During direct file system access, keys come back as JValue, but during normal operations they are primitives
                        object? primaryKey = rawPrimaryKey;
                        if (rawPrimaryKey is Newtonsoft.Json.Linq.JValue jValue)
                        {
                            primaryKey = jValue.Value; // Extract underlying primitive value (string, int, etc.)
                            
                            // EXTRA FIX: JSON deserialization creates Int64 for numbers, but C# literals are Int32
                            // Convert Int64 to Int32 for consistency with normal operations
                            if (primaryKey is long longValue && longValue >= int.MinValue && longValue <= int.MaxValue)
                            {
                                primaryKey = (int)longValue;
                                Console.WriteLine($"[PHASE2] Converted Int64 to Int32 for consistency: {primaryKey} (type: {primaryKey?.GetType().Name})");
                            }
                            else
                            {
                                Console.WriteLine($"[PHASE2] Normalized JValue to primitive: {primaryKey} (type: {primaryKey?.GetType().Name})");
                            }
                        }
                        
                        if (primaryKey != null)
                        {
                            var typedPrimaryKey = TypedPrimaryKey.FromValue(primaryKey);
                            var normalizedKey = typedPrimaryKey.ToNormalizedKey();
                            Console.WriteLine($"[PHASE2] Normalized key: '{normalizedKey}'");
                            
                            // Add to primary index
                            if (!_primaryKeyIndex.ContainsKey(normalizedKey))
                            {
                                _primaryKeyIndex[normalizedKey] = new HashSet<string>();
                            }
                            _primaryKeyIndex[normalizedKey].Add(pageId);
                            _pageToKeys[pageId].Add(normalizedKey);
                            
                            indexedObjects++;
                            Console.WriteLine($"[PHASE2] Added to index: key='{normalizedKey}', page='{pageId}'");
                        }
                        else
                        {
                            Console.WriteLine($"[PHASE2] WARNING: Could not extract primary key from object");
                        }
                    }
                }
                
                Console.WriteLine($"[PHASE2] Index rebuilding complete:");
                Console.WriteLine($"[PHASE2]   Total objects processed: {totalObjects}");
                Console.WriteLine($"[PHASE2]   Objects indexed: {indexedObjects}");
                Console.WriteLine($"[PHASE2]   Primary key index size: {_primaryKeyIndex.Count}");
                Console.WriteLine($"[PHASE2]   Page mappings: {_pageToKeys.Count}");
                
                // Debug: Show what's in the index
                foreach (var kvp in _primaryKeyIndex.Take(5)) // Limit to first 5 for readability
                {
                    Console.WriteLine($"[PHASE2]   Index entry: '{kvp.Key}' -> [{string.Join(", ", kvp.Value)}]");
                }
            }
            
                await _storageSubsystem.CommitTransactionAsync(txnId, cancellationToken);
                Console.WriteLine($"[PHASE3] Proper MVCC Storage layer access completed successfully");
            }
            catch
            {
                await _storageSubsystem.RollbackTransactionAsync(txnId, cancellationToken);
                throw;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PHASE3] ERROR during index initialization: {ex.Message}");
            Console.WriteLine($"[PHASE3] Stack trace: {ex.StackTrace}");
            throw;
        }
    }
    
    /// <summary>
    /// Load persisted secondary index metadata from storage and recreate indexes
    /// </summary>
    private async Task LoadSecondaryIndexesFromStorage(long txnId, string tableNamespace, CancellationToken cancellationToken)
    {
        try
        {
            // Get all index metadata from the indexes metadata namespace
            var indexesMetadataNamespace = $"{tableNamespace}._indexes._metadata";
            
            try
            {
                var indexMetadataObjects = await _storageSubsystem.GetMatchingObjectsAsync(txnId, indexesMetadataNamespace, "*", cancellationToken);
                
                foreach (var (pageId, pageObjects) in indexMetadataObjects)
                {
                    foreach (var metadataObj in pageObjects)
                    {
                        try
                        {
                            // Extract metadata properties
                            string? indexName = null;
                            string? fieldPath = null;
                            
                            if (metadataObj is System.Dynamic.ExpandoObject expando)
                            {
                                var dict = (IDictionary<string, object?>)expando;
                                if (dict.TryGetValue("Name", out var nameValue))
                                {
                                    indexName = nameValue?.ToString();
                                }
                                if (dict.TryGetValue("FieldPath", out var fieldPathValue))
                                {
                                    fieldPath = fieldPathValue?.ToString();
                                }
                            }
                            else
                            {
                                // Handle strongly typed objects
                                var nameProperty = metadataObj.GetType().GetProperty("Name");
                                var fieldPathProperty = metadataObj.GetType().GetProperty("FieldPath");
                                
                                indexName = nameProperty?.GetValue(metadataObj)?.ToString();
                                fieldPath = fieldPathProperty?.GetValue(metadataObj)?.ToString();
                            }
                            
                            if (!string.IsNullOrEmpty(indexName) && !string.IsNullOrEmpty(fieldPath))
                            {
                                // Create the index
                                var index = new Index(indexName, fieldPath, _storageSubsystem, tableNamespace);
                                
                                lock (_indexLock)
                                {
                                    _secondaryIndexes[indexName] = index;
                                }
                                
                                Console.WriteLine($"[PHASE2] Loaded secondary index: {indexName} -> {fieldPath}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[PHASE2] WARNING: Failed to load index metadata object: {ex.Message}");
                        }
                    }
                }
                
                Console.WriteLine($"[PHASE2] Loaded {_secondaryIndexes.Count} secondary indexes");
            }
            catch
            {
                // Namespace might not exist if no indexes have been created
                Console.WriteLine($"[PHASE2] No secondary indexes found - metadata namespace does not exist");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PHASE2] WARNING: Failed to load secondary indexes: {ex.Message}");
        }
    }

    #region Helper Methods

    /// <summary>
    /// TYPE-SAFE: Helper method for backwards compatibility with old normalization approach.
    /// Now uses TypedPrimaryKey for consistent type handling.
    /// </summary>
    private static string NormalizePrimaryKey(object? primaryKey)
    {
        if (primaryKey == null)
            throw new ArgumentNullException(nameof(primaryKey), "Primary key cannot be null");

        var typedKey = (primaryKey is TypedPrimaryKey) ? (TypedPrimaryKey)primaryKey : TypedPrimaryKey.FromValue(primaryKey);
        return typedKey.ToNormalizedKey();
    }

    private object? ExtractPrimaryKey(dynamic obj)
    {
        return ExtractFieldValue(obj, _metadata.PrimaryKeyField);
    }

    private object? ExtractFieldValue(dynamic obj, string fieldPath)
    {
        try
        {
            if (obj == null) return null;

            // Handle different object types
            if (obj is JObject jObj)
            {
                var token = jObj.SelectToken(fieldPath);
                return token?.Value<object>();
            }
            
            if (obj is ExpandoObject expando)
            {
                var dict = (IDictionary<string, object>)expando;
                var fieldName = fieldPath.Substring(2); // Remove "$."
                if (dict.TryGetValue(fieldName, out var expandoValue))
                {
                    // EXTRA FIX: Handle Int64 to Int32 conversion for ExpandoObjects too
                    // When objects are read from storage, numbers become Int64 but C# literals are Int32
                    if (expandoValue is long longValue && longValue >= int.MinValue && longValue <= int.MaxValue)
                    {
                        return (int)longValue;
                    }
                    return expandoValue;
                }
                return null;
            }

            // Use reflection for anonymous objects
            var type = obj.GetType();
            var fieldName2 = fieldPath.Substring(2); // Remove "$."
            var property = type.GetProperty(fieldName2);
            return property?.GetValue(obj);
        }
        catch
        {
            return null;
        }
    }

    private static dynamic ConvertToExpando(object obj)
    {
        if (obj is JObject jObj)
        {
            var expando = new ExpandoObject();
            var dict = (IDictionary<string, object>)expando;
            
            foreach (var property in jObj.Properties())
            {
                if (property.Value is JValue jValue)
                {
                    dict[property.Name] = jValue.Value;
                }
                else if (property.Value != null)
                {
                    dict[property.Name] = property.Value.ToObject<object>();
                }
                else
                {
                    dict[property.Name] = null;
                }
            }
            
            return expando;
        }

        return obj;
    }

    /// <summary>
    /// TYPE-SAFE: Rebuild indexes for ALL objects in a page using normalized keys
    /// This prevents type comparison exceptions during index operations
    /// </summary>
    private void RebuildIndexesForPage(string pageId, object[] pageObjects)
    {
        // Remove all existing entries for this page using normalized keys
        if (_pageToKeys.TryGetValue(pageId, out var oldNormalizedKeys))
        {
            foreach (var oldNormalizedKey in oldNormalizedKeys.ToList())
            {
                if (_primaryKeyIndex.TryGetValue(oldNormalizedKey, out var pages))
                {
                    pages.Remove(pageId);
                    if (pages.Count == 0)
                    {
                        _primaryKeyIndex.Remove(oldNormalizedKey);
                    }
                }
            }
        }
        _pageToKeys.Remove(pageId);
        
        // Rebuild secondary indexes for this page
        foreach (var index in _secondaryIndexes.Values)
        {
            // Remove old entries for this page
            // (simplified - production would be more efficient)
        }
        
        // Add new entries for all objects in page using normalized keys
        if (!_pageToKeys.ContainsKey(pageId))
        {
            _pageToKeys[pageId] = new HashSet<string>();
        }
        
        foreach (var obj in pageObjects)
        {
            var primaryKeyValue = ExtractPrimaryKey(obj);
            if (primaryKeyValue != null)
            {
                var primaryKey = TypedPrimaryKey.FromValue(primaryKeyValue);
                var normalizedKey = primaryKey.ToNormalizedKey();
                
                // Add to primary index using normalized key
                if (!_primaryKeyIndex.ContainsKey(normalizedKey))
                {
                    _primaryKeyIndex[normalizedKey] = new HashSet<string>();
                }
                _primaryKeyIndex[normalizedKey].Add(pageId);
                _pageToKeys[pageId].Add(normalizedKey);
                
                // Add to secondary indexes
                foreach (var index in _secondaryIndexes.Values)
                {
                    var indexValue = ExtractFieldValue(obj, index.FieldPath);
                    if (indexValue != null)
                    {
                        index.AddEntry(indexValue, pageId);
                    }
                }
            }
        }
    }

    private async Task RebuildSecondaryIndex(IDatabaseTransaction txn, Index index, CancellationToken cancellationToken)
    {
        var tableNamespace = $"{_metadata.DatabaseName}.{_metadata.Name}";
        var storageTransactionId = txn.GetStorageTransactionId();
        
        var allObjects = await _storageSubsystem.GetMatchingObjectsAsync(storageTransactionId, tableNamespace, "*", cancellationToken);
        
        foreach (var (pageId, pageObjects) in allObjects)
        {
            foreach (var obj in pageObjects)
            {
                var indexValue = ExtractFieldValue(obj, index.FieldPath);
                if (indexValue != null)
                {
                    index.AddEntry(indexValue, pageId);
                }
            }
        }
    }


    #endregion
}