using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services;

public class StorageSubsystem : IStorageSubsystem, IDisposable
{
    protected readonly object _metadataLock = new object();
    protected readonly ConcurrentDictionary<long, MVCCTransaction> _activeTransactions = new();
    protected readonly ConcurrentDictionary<string, int> _nextPageNumbers = new(); // Track next page number per namespace
    protected Timer? _cleanupTimer;
    protected Timer? _metadataPersistTimer;
    protected volatile bool _metadataDirty = false;
    protected readonly object _dirtyFlagLock = new object();
    protected EnhancedDeadlockAwareLockManager? _lockManager;
    
    protected string _rootPath = string.Empty;
    protected StorageConfig _config = new();
    protected IFormatAdapter _formatAdapter = new JsonFormatAdapter();
    protected VersionMetadata _metadata = new();
    protected long _nextTransactionId = 1;

    public StorageSubsystem()
    {
        // Constructor - cleanup timer will be initialized in StartVersionCleanup
    }

    public void Initialize(string rootPath, StorageConfig? config = null)
    {
        _rootPath = Path.GetFullPath(rootPath);
        _config = config ?? new StorageConfig();
        
        // Create root directory if it doesn't exist
        Directory.CreateDirectory(_rootPath);
        
        // Initialize format adapter
        _formatAdapter = _config.Format switch
        {
            SerializationFormat.Json => new JsonFormatAdapter(),
            SerializationFormat.Xml => new XmlFormatAdapter(),
            SerializationFormat.Yaml => new YamlFormatAdapter(),
            _ => throw new ArgumentException($"Unsupported format: {_config.Format}")
        };

        // Load or create configuration file
        LoadOrCreateConfig();
        
        // Load or create version metadata
        LoadOrCreateMetadata();
        
        // Initialize metadata persistence timer (every 5 seconds)
        _metadataPersistTimer = new Timer(PersistMetadataIfDirty, null,
            TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
            
        // Initialize enhanced deadlock-aware lock manager
        _lockManager = new EnhancedDeadlockAwareLockManager(_config.DeadlockTimeoutMs, _config.EnableWaitDieDeadlockPrevention);
    }

    public long BeginTransaction()
    {
        lock (_metadataLock)
        {
            // CRITICAL MVCC FIX: Prevent TSN assignment race conditions
            // The TSN assignment must be atomic to prevent concurrent transactions
            // from having inconsistent snapshot views
            var transactionId = _nextTransactionId++;
            
            // CRITICAL FIX: Snapshot TSN should be set to the current committed TSN
            // at the time this transaction begins (BEFORE updating CurrentTSN)
            // This ensures proper read-committed isolation level
            var snapshotTSN = _metadata.CurrentTSN;
            
            // Update CurrentTSN to include this transaction for future transactions
            // This allows the transaction to see its own writes (version = transactionId)
            _metadata.CurrentTSN = Math.Max(_metadata.CurrentTSN, transactionId);
            
            var transaction = new MVCCTransaction
            {
                TransactionId = transactionId,
                // CRITICAL MVCC FIX: Use snapshot TSN captured BEFORE CurrentTSN update
                // This ensures this transaction sees a consistent snapshot of committed data
                // but does not see uncommitted changes from other concurrent transactions
                SnapshotTSN = snapshotTSN,
                StartTime = DateTime.UtcNow
            };

            _activeTransactions.TryAdd(transactionId, transaction);
            _metadata.AddActiveTransaction(transactionId);
            
            MarkMetadataDirty(); // Use dirty flag optimization instead of immediate persist
            return transactionId;
        }
    }

    public void CommitTransaction(long transactionId)
    {
        if (!_activeTransactions.TryGetValue(transactionId, out var transaction))
            throw new ArgumentException($"Transaction {transactionId} not found or already completed");

        lock (_metadataLock)
        {
            // CRITICAL: Only check for conflicts if transaction wrote something
            // Read-only transactions should not fail due to concurrent modifications
            if (transaction.WrittenPages.Count > 0)
            {
                // Check for optimistic concurrency conflicts on READ pages
                foreach (var (pageId, readVersion) in transaction.ReadVersions)
                {
                    if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                    {
                        if (pageInfo.CurrentVersion > readVersion)
                        {
                            // Conflict detected - another transaction modified pages we read
                            throw new InvalidOperationException(
                                $"Optimistic concurrency conflict: Page {pageId} was modified by another transaction. " +
                                $"Read version: {readVersion}, Current version: {pageInfo.CurrentVersion}");
                        }
                    }
                }
            }
            // Read-only transactions (WrittenPages.Count == 0) always succeed

            // Update current versions for written pages
            foreach (var (pageId, version) in transaction.WrittenPages)
            {
                if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                {
                    pageInfo.CurrentVersion = version;
                }
            }

            // Mark transaction as committed
            transaction.IsCommitted = true;
            _metadata.RemoveActiveTransaction(transactionId);
            _activeTransactions.TryRemove(transactionId, out _);
            
            PersistMetadata();
        }
        
        // CRITICAL: Release all locks held by this transaction to prevent deadlocks
        _lockManager?.ReleaseLocks(transactionId);
    }

    public void RollbackTransaction(long transactionId)
    {
        if (!_activeTransactions.TryGetValue(transactionId, out var transaction))
            throw new ArgumentException($"Transaction {transactionId} not found or already completed");

        lock (_metadataLock)
        {
            // Mark all written versions as invalid
            foreach (var (pageId, version) in transaction.WrittenPages)
            {
                if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                {
                    pageInfo.RemoveVersion(version);
                }
                
                // Delete version files created by this transaction
                var namespacePath = GetNamespacePathFromPageId(pageId);
                var versionFile = Path.Combine(namespacePath, $"{GetPageFileNameFromPageId(pageId)}.v{transactionId}");
                
                if (File.Exists(versionFile))
                {
                    File.Delete(versionFile);
                }
            }

            transaction.IsRolledBack = true;
            _metadata.RemoveActiveTransaction(transactionId);
            _activeTransactions.TryRemove(transactionId, out _);
            
            PersistMetadata();
        }
        
        // CRITICAL: Release all locks held by this transaction to prevent deadlocks
        _lockManager?.ReleaseLocks(transactionId);
    }

    public string InsertObject(long transactionId, string @namespace, object data)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        var transaction = _activeTransactions[transactionId];
        var namespacePath = GetNamespacePath(@namespace);
        
        // Increment operation count for namespace
        IncrementNamespaceOperations(@namespace);
        
        try
        {
            // Ensure namespace directory exists
            Directory.CreateDirectory(namespacePath);
            
            // Find or create page for insertion
            var pageId = FindOrCreatePageForInsertion(@namespace, data);
            var fullPageId = $"{@namespace}:{pageId}";
            
            // Read current page content - check if we've already written to this page in this transaction
            object[] currentContent;
            if (transaction.WrittenPages.ContainsKey(fullPageId))
            {
                // Read from our own transaction's version file
                var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{transactionId}");
                if (File.Exists(versionFile))
                {
                    var content = File.ReadAllText(versionFile);
                    currentContent = _formatAdapter.DeserializeArray(content, typeof(object));
                }
                else
                {
                    currentContent = Array.Empty<object>();
                }
            }
            else
            {
                // Read committed content using normal snapshot isolation
                currentContent = ReadPageInternal(transactionId, @namespace, pageId);
            }
            var newContent = currentContent.Concat(new[] { data }).ToArray();
            
            // Write new version
            WritePageVersion(transactionId, @namespace, pageId, newContent);
            
            // Track in transaction
            transaction.WrittenPages[fullPageId] = transactionId;
            
            return pageId;
        }
        finally
        {
            DecrementNamespaceOperations(@namespace);
        }
    }

    public void UpdatePage(long transactionId, string @namespace, string pageId, object[] pageContent)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{@namespace}:{pageId}";
        
        // CRITICAL MVCC FIX: Enforce read-before-write for ACID isolation compliance
        // This is a fundamental MVCC requirement to prevent lost updates and ensure proper transaction isolation.
        // The test MVCCIsolationTests.UpdatePage_WithoutPriorRead_ShouldThrowReadBeforeWriteViolation expects this.
        // 
        // However, we need to allow legitimate cases where a page was read in the same transaction.
        // If the transaction has already recorded reading this page, allow the update.
        // This supports both:
        // 1. MVCC isolation requirements (direct UpdatePage calls must have prior reads)  
        // 2. Table layer operations (which read pages internally before updates)
        
        if (!transaction.ReadVersions.ContainsKey(fullPageId))
        {
            throw new InvalidOperationException(
                $"MVCC ACID isolation violation: Cannot update page '{pageId}' in namespace '{@namespace}' " +
                $"without a prior read-before-write in the same transaction. " +
                $"This violates ACID isolation guarantees and could lead to lost updates. " +
                $"Read the page first using ReadPage() or GetMatchingObjects() before updating.");
        }
        
        IncrementNamespaceOperations(@namespace);
        
        try
        {
            // Write new version (read version already recorded by ReadPage)
            WritePageVersion(transactionId, @namespace, pageId, pageContent);
            
            // Track in transaction
            transaction.WrittenPages[fullPageId] = transactionId;
        }
        finally
        {
            DecrementNamespaceOperations(@namespace);
        }
    }

    public object[] ReadPage(long transactionId, string @namespace, string pageId)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{@namespace}:{pageId}";
        
        IncrementNamespaceOperations(@namespace);
        
        try
        {
            var result = ReadPageInternal(transactionId, @namespace, pageId);
            
            // CRITICAL: Record the actual version we read for conflict detection
            lock (_metadataLock)
            {
                if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
                {
                    // Find the exact version this transaction can see
                    var readableVersion = pageInfo.GetVersionsCopy()
                        .Where(v => v <= transaction.SnapshotTSN && !IsVersionRolledBack(v))
                        .DefaultIfEmpty(0)
                        .Max();
                    
                    // Record this version for conflict detection during commit
                    transaction.ReadVersions[fullPageId] = readableVersion;
                }
                else
                {
                    // Page doesn't exist - record as version 0
                    transaction.ReadVersions[fullPageId] = 0;
                }
            }
            
            return result;
        }
        finally
        {
            DecrementNamespaceOperations(@namespace);
        }
    }

    public Dictionary<string, object[]> GetMatchingObjects(long transactionId, string @namespace, string pattern)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        var transaction = _activeTransactions[transactionId];
        
        IncrementNamespaceOperations(@namespace);
        
        try
        {
            var result = new Dictionary<string, object[]>();
            var namespacePath = GetNamespacePath(@namespace);
            
            if (!Directory.Exists(namespacePath))
                return result;
            
            var regex = CreateRegexFromPattern(pattern);
            var files = Directory.GetFiles(namespacePath, $"*{_formatAdapter.FileExtension}.v*");
            
            var pageGroups = files
                .Select(f => Path.GetFileName(f))
                .Where(f => f.Contains(".v"))
                .Select(f => {
                    var nameWithoutVersion = f.Substring(0, f.LastIndexOf(".v"));
                    // Remove the file extension to get just the pageId (e.g., "page001.json" -> "page001")
                    var extensionIndex = nameWithoutVersion.LastIndexOf(_formatAdapter.FileExtension);
                    return extensionIndex > 0 ? nameWithoutVersion.Substring(0, extensionIndex) : nameWithoutVersion;
                })
                .Distinct()
                .Where(pageId => regex.IsMatch(pageId))
                .ToList();
            
            // CRITICAL DEADLOCK FIX: Acquire locks on all matching pages using resource ordering
            // This prevents deadlocks when multiple transactions query overlapping page sets
            var fullPageIds = pageGroups.Select(pageId => $"{@namespace}:{pageId}").ToList();
            if (fullPageIds.Count > 0)
            {
                _lockManager?.AcquireMultipleLocks(transactionId, fullPageIds);
            }
            
            // CRITICAL: Apply snapshot isolation and record read versions for ALL pages
            lock (_metadataLock)
            {
                foreach (var pageId in pageGroups)
                {
                    var fullPageId = $"{@namespace}:{pageId}";
                    
                    // Record read version for conflict detection - BEFORE reading data
                    if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
                    {
                        var readableVersion = pageInfo.GetVersionsCopy()
                            .Where(v => v <= transaction.SnapshotTSN && !IsVersionRolledBack(v))
                            .DefaultIfEmpty(0)
                            .Max();
                        
                        transaction.ReadVersions[fullPageId] = readableVersion;
                    }
                    else
                    {
                        transaction.ReadVersions[fullPageId] = 0;
                    }
                    
                    // Now read the data using proper snapshot isolation (already locked above)
                    // Skip the locking since we already have the lock from AcquireMultipleLocks
                    var pageContent = ReadPageInternalWithoutLock(transactionId, @namespace, pageId);
                    if (pageContent.Length > 0)
                    {
                        result[pageId] = pageContent;
                    }
                }
            }
            
            return result;
        }
        finally
        {
            DecrementNamespaceOperations(@namespace);
        }
    }

    public void CreateNamespace(long transactionId, string @namespace)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        var namespacePath = GetNamespacePath(@namespace);
        Directory.CreateDirectory(namespacePath);
        
        // Create namespace marker file
        var markerFile = Path.Combine(namespacePath, $".namespace{_formatAdapter.FileExtension}");
        var markerContent = _formatAdapter.Serialize(new { 
            Namespace = @namespace, 
            Created = DateTime.UtcNow,
            CreatedByTransaction = transactionId 
        });
        
        File.WriteAllText(markerFile, markerContent);
        FlushToDisk(markerFile);
    }

    public void DeleteNamespace(long transactionId, string @namespace)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(@namespace);
        
        // Wait for all operations to complete
        WaitForNamespaceOperationsToComplete(@namespace);
        
        var namespacePath = GetNamespacePath(@namespace);
        if (Directory.Exists(namespacePath))
        {
            Directory.Delete(namespacePath, recursive: true);
        }
        
        // Clean up metadata
        lock (_metadataLock)
        {
            var keysToRemove = _metadata.PageVersions.Keys
                .Where(k => k.StartsWith($"{@namespace}:"))
                .ToList();
                
            foreach (var key in keysToRemove)
            {
                _metadata.PageVersions.TryRemove(key, out _);
            }
            
            _metadata.NamespaceOperations.TryRemove(@namespace, out _);
            PersistMetadata();
        }
    }

    public void RenameNamespace(long transactionId, string oldName, string newName)
    {
        ValidateTransaction(transactionId);
        ValidateNamespace(oldName);
        ValidateNamespace(newName);
        
        WaitForNamespaceOperationsToComplete(oldName);
        
        var oldPath = GetNamespacePath(oldName);
        var newPath = GetNamespacePath(newName);
        
        if (!Directory.Exists(oldPath))
            throw new ArgumentException($"Namespace '{oldName}' does not exist");
            
        if (Directory.Exists(newPath))
            throw new ArgumentException($"Namespace '{newName}' already exists");
        
        // Ensure parent directory exists
        Directory.CreateDirectory(Path.GetDirectoryName(newPath)!);
        Directory.Move(oldPath, newPath);
        
        // Update metadata
        lock (_metadataLock)
        {
            var keysToUpdate = _metadata.PageVersions.Keys
                .Where(k => k.StartsWith($"{oldName}:"))
                .ToList();
                
            foreach (var oldKey in keysToUpdate)
            {
                var newKey = oldKey.Replace($"{oldName}:", $"{newName}:");
                _metadata.PageVersions[newKey] = _metadata.PageVersions[oldKey];
                _metadata.PageVersions.TryRemove(oldKey, out _);
            }
            
            if (_metadata.NamespaceOperations.TryGetValue(oldName, out var operations))
            {
                _metadata.NamespaceOperations[newName] = operations;
                _metadata.NamespaceOperations.TryRemove(oldName, out _);
            }
            
            PersistMetadata();
        }
    }

    public void StartVersionCleanup(int intervalMinutes = 15)
    {
        // Handle immediate cleanup request
        if (intervalMinutes == 0)
        {
            // Run cleanup once immediately
            Task.Run(() => RunVersionCleanup(null));
            return;
        }
        
        // Create cleanup timer that runs periodically
        _cleanupTimer = new Timer(RunVersionCleanup, null, 
            TimeSpan.FromMinutes(1), // Start after 1 minute
            TimeSpan.FromMinutes(intervalMinutes));
    }

    protected void ValidateTransaction(long transactionId)
    {
        if (transactionId <= 0)
            throw new ArgumentException("Transaction ID must be positive");
            
        if (!_activeTransactions.ContainsKey(transactionId))
            throw new ArgumentException($"Transaction {transactionId} is not active");
    }

    protected void ValidateNamespace(string @namespace)
    {
        if (string.IsNullOrWhiteSpace(@namespace))
            throw new ArgumentException("Namespace cannot be null or empty");
            
        if (@namespace.Contains('/') || @namespace.Contains('\\') || 
            @namespace.Contains(':') || @namespace.Contains('*') ||
            @namespace.Contains('?') || @namespace.Contains('<') ||
            @namespace.Contains('>') || @namespace.Contains('|'))
        {
            throw new ArgumentException($"Namespace contains invalid characters: {@namespace}");
        }
    }

    protected string GetNamespacePath(string @namespace)
    {
        var parts = @namespace.Split(_config.NamespaceDelimiter);
        return Path.Combine(_rootPath, Path.Combine(parts));
    }

    protected void LoadOrCreateConfig()
    {
        var configFiles = new[]
        {
            Path.Combine(_rootPath, "storage.json"),
            Path.Combine(_rootPath, "storage.yaml"),
            Path.Combine(_rootPath, "storage.xml")
        };

        var existingConfig = configFiles.FirstOrDefault(File.Exists);
        
        if (existingConfig != null)
        {
            var content = File.ReadAllText(existingConfig);
            var adapter = GetAdapterFromExtension(Path.GetExtension(existingConfig));
            _config = adapter.Deserialize<StorageConfig>(content);
        }
        else
        {
            // Create default config
            var configPath = Path.Combine(_rootPath, $"storage{_formatAdapter.FileExtension}");
            var configContent = _formatAdapter.Serialize(_config);
            File.WriteAllText(configPath, configContent);
            FlushToDisk(configPath);
        }
    }

    protected void LoadOrCreateMetadata()
    {
        var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
        
        if (File.Exists(metadataFile))
        {
            var content = File.ReadAllText(metadataFile);
            
            // Try to load as snapshot first (new format), fallback to direct metadata (old format)
            try
            {
                var snapshot = _formatAdapter.Deserialize<VersionMetadataSnapshot>(content);
                _metadata = VersionMetadata.FromSnapshot(snapshot);
            }
            catch
            {
                // Fallback to old format for backward compatibility
                _metadata = _formatAdapter.Deserialize<VersionMetadata>(content);
            }
            
            _nextTransactionId = Math.Max(_nextTransactionId, _metadata.CurrentTSN + 1);
            
            // Clean up stale active transactions (crash recovery)
            _metadata.ClearActiveTransactions();
        }
        else
        {
            _metadata = new VersionMetadata { CurrentTSN = 0 };
            PersistMetadata();
        }
    }

    protected void PersistMetadata()
    {
        // CRITICAL FIX: Create a snapshot to prevent concurrent modification exceptions during serialization
        // This prevents "Collection was modified; enumeration operation may not execute" errors
        _metadata.LastUpdated = DateTime.UtcNow;
        var snapshot = _metadata.CreateSnapshot();
        
        var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
        var content = _formatAdapter.Serialize(snapshot);
        File.WriteAllText(metadataFile, content);
        FlushToDisk(metadataFile);
    }

    protected object[] ReadPageInternal(long transactionId, string @namespace, string pageId)
    {
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{@namespace}:{pageId}";
        var namespacePath = GetNamespacePath(@namespace);
        
        // CRITICAL DEADLOCK FIX: Use Two-Phase Locking with deterministic resource ordering
        // Track this resource and ensure we acquire locks in alphabetical order to prevent deadlocks
        if (!transaction.RequiredResources.Contains(fullPageId))
        {
            transaction.RequiredResources.Add(fullPageId);
            
            // CRITICAL: When acquiring a new resource, we must acquire ALL current and new resources
            // in deterministic order to prevent deadlock. This implements true Two-Phase Locking.
            var allRequiredResources = transaction.RequiredResources.ToList();
            _lockManager?.AcquireMultipleLocks(transactionId, allRequiredResources);
        }
        
        try
        {
            // CRITICAL FIX: Implement proper SNAPSHOT isolation to prevent phantom reads
            // Only read versions that were committed at or before this transaction's snapshot TSN
            if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
            {
                var allVersions = pageInfo.GetVersionsCopy()
                    .Where(v => v <= transaction.SnapshotTSN) // CRITICAL: Only see data from snapshot time or before
                    .OrderByDescending(v => v)
                    .ToList();
                
                foreach (var version in allVersions)
                {
                    // CRITICAL FIX: Allow current transaction to see its own writes
                    // Skip versions from OTHER active transactions, but allow current transaction's writes
                    if (_metadata.ContainsActiveTransaction(version) && version != transactionId)
                        continue;
                        
                    // Skip rolled back versions
                    if (IsVersionRolledBack(version))
                        continue;
                        
                    var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{version}");
                    if (File.Exists(versionFile))
                    {
                        var content = File.ReadAllText(versionFile);
                        return _formatAdapter.DeserializeArray(content, typeof(object));
                    }
                }
            }
            
            return Array.Empty<object>();
        }
        catch (TimeoutException)
        {
            // Re-throw deadlock timeout as-is for proper error handling
            throw;
        }
    }

    /// <summary>
    /// Internal page reading method without lock acquisition - used when locks are already held
    /// This method is used by GetMatchingObjects after it has already acquired all necessary locks
    /// </summary>
    protected object[] ReadPageInternalWithoutLock(long transactionId, string @namespace, string pageId)
    {
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{@namespace}:{pageId}";
        var namespacePath = GetNamespacePath(@namespace);
        
        try
        {
            // CRITICAL FIX: Implement proper SNAPSHOT isolation to prevent phantom reads
            // Only read versions that were committed at or before this transaction's snapshot TSN
            if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
            {
                var allVersions = pageInfo.GetVersionsCopy()
                    .Where(v => v <= transaction.SnapshotTSN) // CRITICAL: Only see data from snapshot time or before
                    .OrderByDescending(v => v)
                    .ToList();
                
                foreach (var version in allVersions)
                {
                    // CRITICAL FIX: Allow current transaction to see its own writes
                    // Skip versions from OTHER active transactions, but allow current transaction's writes
                    if (_metadata.ContainsActiveTransaction(version) && version != transactionId)
                        continue;
                        
                    // Skip rolled back versions
                    if (IsVersionRolledBack(version))
                        continue;
                        
                    var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{version}");
                    if (File.Exists(versionFile))
                    {
                        var content = File.ReadAllText(versionFile);
                        return _formatAdapter.DeserializeArray(content, typeof(object));
                    }
                }
            }
            
            return Array.Empty<object>();
        }
        catch (TimeoutException)
        {
            // Re-throw deadlock timeout as-is for proper error handling
            throw;
        }
    }

    protected void WritePageVersion(long transactionId, string @namespace, string pageId, object[] content)
    {
        var namespacePath = GetNamespacePath(@namespace);
        var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{transactionId}");
        var fullPageId = $"{@namespace}:{pageId}";
        
        var serializedContent = _formatAdapter.SerializeArray(content);
        File.WriteAllText(versionFile, serializedContent);
        FlushToDisk(versionFile);
        
        // Update metadata
        lock (_metadataLock)
        {
            if (!_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
            {
                pageInfo = new PageVersionInfo();
                _metadata.PageVersions[fullPageId] = pageInfo;
            }
            
            pageInfo.AddVersion(transactionId);
            
            MarkMetadataDirty();
        }
    }

    protected string FindOrCreatePageForInsertion(string @namespace, object data)
    {
        var namespacePath = GetNamespacePath(@namespace);
        var serializedSize = _formatAdapter.Serialize(data).Length;
        var maxSizeBytes = _config.MaxPageSizeKB * 1024;
        
        // CRITICAL FIX: Use metadata lock to ensure thread-safe page creation
        lock (_metadataLock)
        {
            // Initialize the next page number for this namespace if not already done
            if (!_nextPageNumbers.ContainsKey(@namespace))
            {
                // Find existing pages to determine the starting point
                var existingPages = Directory.GetFiles(namespacePath, $"page*{_formatAdapter.FileExtension}.v*")
                    .Select(f => Path.GetFileName(f))
                    .Where(f => f.StartsWith("page") && f.Contains(".v"))
                    .Select(f => {
                        var nameWithoutVersion = f.Substring(0, f.LastIndexOf(".v"));
                        // Remove the file extension to get just the pageId (e.g., "page001.json" -> "page001")
                        var extensionIndex = nameWithoutVersion.LastIndexOf(_formatAdapter.FileExtension);
                        return extensionIndex > 0 ? nameWithoutVersion.Substring(0, extensionIndex) : nameWithoutVersion;
                    })
                    .Distinct()
                    .ToList();
                
                // Find the highest existing page number
                var highestPageNumber = 0;
                foreach (var pageId in existingPages)
                {
                    if (pageId.StartsWith("page") && pageId.Length >= 7) // "pageXXX" format
                    {
                        var numberPart = pageId.Substring(4); // Skip "page"
                        if (int.TryParse(numberPart, out var pageNumber))
                        {
                            highestPageNumber = Math.Max(highestPageNumber, pageNumber);
                        }
                    }
                }
                
                _nextPageNumbers[@namespace] = highestPageNumber + 1;
            }
            
            // CRITICAL ATOMIC CONCURRENCY FIX: Ensure transaction count check and page allocation
            // are performed atomically within the same metadata lock scope.
            //
            // This eliminates the TOCTOU (Time-of-Check-Time-of-Use) race condition where:
            // 1. Multiple threads check ActiveTransactions.Count <= 1 simultaneously
            // 2. All threads pass the check and proceed to page space checking
            // 3. Multiple threads select the same page, causing MVCC conflicts
            //
            // ATOMIC SOLUTION: Perform both the decision and page selection within the same lock,
            // making the entire operation atomic and preventing race condition windows.
            
            bool shouldReusePages = !_config.ForceOneObjectPerPage && _metadata.ActiveTransactions.Count <= 1;
            
            if (shouldReusePages)
            {
                // CRITICAL PERFORMANCE FIX: Single directory scan with cached file information
                // This eliminates multiple Directory.GetFiles() calls per insertion (was causing 200ms+ delays)
                var allVersionFiles = Directory.GetFiles(namespacePath, $"*{_formatAdapter.FileExtension}.v*");
                
                // Pre-compute page sizes from the single directory scan
                var pageSizes = new Dictionary<string, long>();
                var existingPages = new HashSet<string>();
                
                foreach (var filePath in allVersionFiles)
                {
                    var fileName = Path.GetFileName(filePath);
                    if (fileName.StartsWith("page") && fileName.Contains(".v"))
                    {
                        var nameWithoutVersion = fileName.Substring(0, fileName.LastIndexOf(".v"));
                        var extensionIndex = nameWithoutVersion.LastIndexOf(_formatAdapter.FileExtension);
                        var pageId = extensionIndex > 0 ? nameWithoutVersion.Substring(0, extensionIndex) : nameWithoutVersion;
                        
                        existingPages.Add(pageId);
                        
                        // Track the largest file size for this page (latest version)
                        var fileSize = new FileInfo(filePath).Length;
                        if (!pageSizes.ContainsKey(pageId) || pageSizes[pageId] < fileSize)
                        {
                            pageSizes[pageId] = fileSize;
                        }
                    }
                }
                
                // OPTIMIZED: Page size checking without additional directory scans
                foreach (var pageId in existingPages.OrderBy(p => p))
                {
                    var currentSize = pageSizes.GetValueOrDefault(pageId, 0);
                    if (currentSize + serializedSize <= maxSizeBytes)
                    {
                        // ATOMIC: Page selection decision made within lock scope
                        return pageId;
                    }
                }
            }
            
            // CRITICAL ATOMIC FIX: Create new page with atomic page number allocation
            // This ensures each concurrent transaction gets a unique page number within the metadata lock.
            // The increment operation is atomic because it's protected by the same metadata lock.
            var nextPageNumber = _nextPageNumbers[@namespace];
            _nextPageNumbers[@namespace] = nextPageNumber + 1;
            
            // ATOMIC: Page number allocation completed atomically within metadata lock scope
            return $"page{nextPageNumber:D3}";
        }
    }

    protected long GetCurrentPageSize(string @namespace, string pageId)
    {
        var namespacePath = GetNamespacePath(@namespace);
        var files = Directory.GetFiles(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v*");
        
        if (files.Length == 0)
            return 0;
        
        // Get the latest version file
        var latestFile = files.OrderByDescending(f => f).First();
        return new FileInfo(latestFile).Length;
    }

    protected void IncrementNamespaceOperations(string @namespace)
    {
        lock (_metadataLock)
        {
            _metadata.NamespaceOperations[@namespace] = 
                _metadata.NamespaceOperations.GetValueOrDefault(@namespace, 0) + 1;
            MarkMetadataDirty();
        }
    }

    protected void DecrementNamespaceOperations(string @namespace)
    {
        lock (_metadataLock)
        {
            var currentCount = _metadata.NamespaceOperations.GetValueOrDefault(@namespace, 0);
            if (currentCount > 0)
            {
                _metadata.NamespaceOperations[@namespace] = currentCount - 1;
            }
            else
            {
                _metadata.NamespaceOperations.TryRemove(@namespace, out _);
            }
            MarkMetadataDirty();
        }
    }

    protected void WaitForNamespaceOperationsToComplete(string @namespace)
    {
        var timeout = TimeSpan.FromMinutes(5);
        var start = DateTime.UtcNow;
        
        while (DateTime.UtcNow - start < timeout)
        {
            lock (_metadataLock)
            {
                if (!_metadata.NamespaceOperations.ContainsKey(@namespace) || 
                    _metadata.NamespaceOperations[@namespace] == 0)
                {
                    return;
                }
            }
            
            Thread.Sleep(100);
        }
        
        throw new TimeoutException($"Timeout waiting for operations to complete on namespace '{@namespace}'");
    }

    protected void FlushToDisk(string filePath)
    {
        // Force immediate write to disk
        using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        fs.Flush(flushToDisk: true);
    }

    protected IFormatAdapter GetAdapterFromExtension(string extension)
    {
        return extension.ToLowerInvariant() switch
        {
            ".json" => new JsonFormatAdapter(),
            ".xml" => new XmlFormatAdapter(),
            ".yaml" => new YamlFormatAdapter(),
            _ => throw new ArgumentException($"Unsupported file extension: {extension}")
        };
    }

    protected Regex CreateRegexFromPattern(string pattern)
    {
        // Convert wildcard pattern to regex
        var regexPattern = pattern
            .Replace(".", "\\.")
            .Replace("*", ".*")
            .Replace("?", ".");
        
        return new Regex($"^{regexPattern}$", RegexOptions.IgnoreCase);
    }

    protected string GetNamespacePathFromPageId(string fullPageId)
    {
        var parts = fullPageId.Split(':');
        return GetNamespacePath(parts[0]);
    }

    protected string GetPageFileNameFromPageId(string fullPageId)
    {
        var parts = fullPageId.Split(':');
        return parts[1];
    }

    protected bool IsVersionRolledBack(long version)
    {
        // Check if this version belongs to a rolled back transaction
        // For now, we'll implement this by checking if any active transaction matches
        // A more sophisticated implementation would track rolled back transactions
        return false; // TODO: Implement proper rollback tracking
    }

    protected void RunVersionCleanup(object? state)
    {
        try
        {
            HashSet<long> activeTransactionIds;
            Dictionary<string, PageVersionInfo> pageVersions;
            
            // Get snapshot of current state
            lock (_metadataLock)
            {
                activeTransactionIds = new HashSet<long>(_metadata.ActiveTransactions.Keys);
                pageVersions = new Dictionary<string, PageVersionInfo>(_metadata.PageVersions);
            }
            
            if (activeTransactionIds.Count == 0)
                return; // No cleanup needed
            
            var oldestActiveTSN = activeTransactionIds.Min();
            var versionsToDelete = new List<(string fullPageId, long version)>();
            
            // Find versions that can be cleaned up
            foreach (var (fullPageId, pageInfo) in pageVersions)
            {
                var obsoleteVersions = pageInfo.GetVersionsCopy()
                    .Where(v => v < oldestActiveTSN)
                    .OrderByDescending(v => v)
                    .Skip(1) // Keep at least one old version
                    .ToList();
                
                foreach (var version in obsoleteVersions)
                {
                    versionsToDelete.Add((fullPageId, version));
                }
            }
            
            // Delete obsolete version files
            foreach (var (fullPageId, version) in versionsToDelete)
            {
                try
                {
                    var parts = fullPageId.Split(':');
                    var namespacePath = GetNamespacePath(parts[0]);
                    var pageId = parts[1];
                    var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{version}");
                    
                    if (File.Exists(versionFile))
                    {
                        File.Delete(versionFile);
                    }
                }
                catch
                {
                    // Continue cleanup even if individual files fail
                }
            }
            
            // Update metadata
            if (versionsToDelete.Count > 0)
            {
                lock (_metadataLock)
                {
                    foreach (var (fullPageId, version) in versionsToDelete)
                    {
                        if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
                        {
                            pageInfo.RemoveVersion(version);
                        }
                    }
                    
                    PersistMetadata();
                }
            }
        }
        catch
        {
            // Cleanup failures should not crash the system
        }
    }

    // Performance optimization methods
        protected void MarkMetadataDirty()
        {
            lock (_dirtyFlagLock)
            {
                _metadataDirty = true;
            }
        }
    
        protected void PersistMetadataIfDirty(object? state)
        {
            // Skip if not initialized yet
            if (string.IsNullOrEmpty(_rootPath))
                return;
                
            bool shouldPersist = false;
            lock (_dirtyFlagLock)
            {
                if (_metadataDirty)
                {
                    _metadataDirty = false;
                    shouldPersist = true;
                }
            }
            
            if (shouldPersist)
            {
                try
                {
                    PersistMetadata();
                }
                catch
                {
                    // Don't crash on persistence errors
                }
            }
        }

    public void Dispose()
    {
        // Dispose timers
        _cleanupTimer?.Dispose();
        _metadataPersistTimer?.Dispose();
        
        // Release all locks for remaining active transactions
        foreach (var transactionId in _activeTransactions.Keys)
        {
            _lockManager?.ReleaseLocks(transactionId);
        }
        
        // Persist any pending metadata changes before disposing
        PersistMetadataIfDirty(null);
    }
}
