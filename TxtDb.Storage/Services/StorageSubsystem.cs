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
    }

    public long BeginTransaction()
    {
        lock (_metadataLock)
        {
            var transactionId = _nextTransactionId++;
            var transaction = new MVCCTransaction
            {
                TransactionId = transactionId,
                // CRITICAL FIX: For proper MVCC, snapshot TSN should be set to the highest
                // committed transaction at the time this transaction begins
                // This allows read-committed isolation level
                SnapshotTSN = _metadata.CurrentTSN,
                StartTime = DateTime.UtcNow
            };

            _activeTransactions.TryAdd(transactionId, transaction);
            _metadata.ActiveTransactions.Add(transactionId);
            _metadata.CurrentTSN = Math.Max(_metadata.CurrentTSN, transactionId);
            
            PersistMetadata();
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
            _metadata.ActiveTransactions.Remove(transactionId);
            _activeTransactions.TryRemove(transactionId, out _);
            
            PersistMetadata();
        }
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
            _metadata.ActiveTransactions.Remove(transactionId);
            _activeTransactions.TryRemove(transactionId, out _);
            
            PersistMetadata();
        }
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
        
        // CRITICAL: Enforce read-before-write rule for ACID isolation
        if (!transaction.ReadVersions.ContainsKey(fullPageId))
        {
            throw new InvalidOperationException(
                $"Cannot update page '{pageId}' in namespace '{@namespace}' without reading it first " +
                $"in transaction {transactionId}. ACID isolation requires read-before-write for updates.");
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
                .Where(pageId => regex.IsMatch(pageId));
            
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
                    
                    // Now read the data using proper snapshot isolation
                    var pageContent = ReadPageInternal(transactionId, @namespace, pageId);
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
                _metadata.PageVersions.Remove(key);
            }
            
            _metadata.NamespaceOperations.Remove(@namespace);
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
                _metadata.PageVersions.Remove(oldKey);
            }
            
            if (_metadata.NamespaceOperations.TryGetValue(oldName, out var operations))
            {
                _metadata.NamespaceOperations[newName] = operations;
                _metadata.NamespaceOperations.Remove(oldName);
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
            _metadata = _formatAdapter.Deserialize<VersionMetadata>(content);
            _nextTransactionId = Math.Max(_nextTransactionId, _metadata.CurrentTSN + 1);
            
            // Clean up stale active transactions (crash recovery)
            _metadata.ActiveTransactions.Clear();
        }
        else
        {
            _metadata = new VersionMetadata { CurrentTSN = 0 };
            PersistMetadata();
        }
    }

    protected void PersistMetadata()
    {
        _metadata.LastUpdated = DateTime.UtcNow;
        var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
        var content = _formatAdapter.Serialize(_metadata);
        File.WriteAllText(metadataFile, content);
        FlushToDisk(metadataFile);
    }

    protected object[] ReadPageInternal(long transactionId, string @namespace, string pageId)
    {
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{@namespace}:{pageId}";
        var namespacePath = GetNamespacePath(@namespace);
        
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
                // Skip versions from transactions that are still active (uncommitted)
                if (_metadata.ActiveTransactions.Contains(version))
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
            
            // Try to find a page with space (only if not forcing one object per page)
            if (!_config.ForceOneObjectPerPage)
            {
                // Check existing pages for available space
                var existingPages = Directory.GetFiles(namespacePath, $"page*{_formatAdapter.FileExtension}.v*")
                    .Select(f => Path.GetFileName(f))
                    .Where(f => f.StartsWith("page") && f.Contains(".v"))
                    .Select(f => {
                        var nameWithoutVersion = f.Substring(0, f.LastIndexOf(".v"));
                        var extensionIndex = nameWithoutVersion.LastIndexOf(_formatAdapter.FileExtension);
                        return extensionIndex > 0 ? nameWithoutVersion.Substring(0, extensionIndex) : nameWithoutVersion;
                    })
                    .Distinct()
                    .OrderBy(p => p)
                    .ToList();
                
                foreach (var pageId in existingPages)
                {
                    var currentSize = GetCurrentPageSize(@namespace, pageId);
                    if (currentSize + serializedSize <= maxSizeBytes)
                    {
                        return pageId;
                    }
                }
            }
            
            // CRITICAL FIX: Create new page with atomic page number allocation
            // This ensures each concurrent transaction gets a unique page number
            var nextPageNumber = _nextPageNumbers[@namespace];
            _nextPageNumbers[@namespace] = nextPageNumber + 1;
            
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
                _metadata.NamespaceOperations.Remove(@namespace);
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
                activeTransactionIds = new HashSet<long>(_metadata.ActiveTransactions);
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
        
        // Persist any pending metadata changes before disposing
        PersistMetadataIfDirty(null);
    }
}
