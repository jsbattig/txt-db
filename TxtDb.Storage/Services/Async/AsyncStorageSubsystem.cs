using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Interfaces.Async;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async storage subsystem providing MVCC-based object persistence with async/await patterns.
/// CRITICAL: Maintains all ACID guarantees while releasing threads during I/O waits.
/// Phase 2: Core Async Storage - Target 200+ ops/sec throughput improvement
/// </summary>
public class AsyncStorageSubsystem : StorageSubsystem, IAsyncStorageSubsystem
{
    private readonly object _flushCountLock = new object();
    private long _flushOperationCount = 0;
    private BatchFlushCoordinator? _batchFlushCoordinator;

    public long FlushOperationCount
    {
        get
        {
            lock (_flushCountLock)
            {
                return _flushOperationCount;
            }
        }
    }

    public async Task<long> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        // CRITICAL: Check cancellation before any CPU-bound work
        cancellationToken.ThrowIfCancellationRequested();

        // Use Task.Run to offload CPU-bound lock acquisition to thread pool
        // This allows the calling thread to continue processing other requests
        return await Task.Run(() =>
        {
            // Check cancellation again before acquiring lock
            cancellationToken.ThrowIfCancellationRequested();
            
            lock (_metadataLock)
            {
                // Check cancellation while holding lock (keep lock time minimal)
                cancellationToken.ThrowIfCancellationRequested();
                
                var transactionId = _nextTransactionId++;
                var transaction = new MVCCTransaction
                {
                    TransactionId = transactionId,
                    SnapshotTSN = _metadata.CurrentTSN,
                    StartTime = DateTime.UtcNow
                };

                _activeTransactions.TryAdd(transactionId, transaction);
                _metadata.ActiveTransactions.Add(transactionId);
                _metadata.CurrentTSN = Math.Max(_metadata.CurrentTSN, transactionId);
                
                return transactionId;
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task CommitTransactionAsync(long transactionId, CancellationToken cancellationToken = default)
    {
        await CommitTransactionAsync(transactionId, FlushPriority.Normal, cancellationToken).ConfigureAwait(false);
    }

    public async Task CommitTransactionAsync(long transactionId, FlushPriority flushPriority, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        if (!_activeTransactions.TryGetValue(transactionId, out var transaction))
            throw new ArgumentException($"Transaction {transactionId} not found or already completed");

        // Perform commit logic in thread pool to avoid blocking calling thread
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            lock (_metadataLock)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                // CRITICAL: Only check for conflicts if transaction wrote something
                if (transaction.WrittenPages.Count > 0)
                {
                    foreach (var (pageId, readVersion) in transaction.ReadVersions)
                    {
                        if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                        {
                            if (pageInfo.CurrentVersion > readVersion)
                            {
                                throw new InvalidOperationException(
                                    $"Optimistic concurrency conflict: Page {pageId} was modified by another transaction. " +
                                    $"Read version: {readVersion}, Current version: {pageInfo.CurrentVersion}");
                            }
                        }
                    }
                }

                // Update current versions for written pages
                foreach (var (pageId, version) in transaction.WrittenPages)
                {
                    if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                    {
                        pageInfo.CurrentVersion = version;
                    }
                }

                transaction.IsCommitted = true;
                _metadata.ActiveTransactions.Remove(transactionId);
                _activeTransactions.TryRemove(transactionId, out _);
            }
            
            // Async metadata persistence with appropriate flush priority
            if (_config.EnableBatchFlushing && _batchFlushCoordinator != null)
            {
                var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
                await _batchFlushCoordinator.QueueFlushAsync(metadataFile, flushPriority).ConfigureAwait(false);
            }
            else
            {
                await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task RollbackTransactionAsync(long transactionId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        if (!_activeTransactions.TryGetValue(transactionId, out var transaction))
            throw new ArgumentException($"Transaction {transactionId} not found or already completed");

        // Perform rollback in thread pool
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var filesToDelete = new List<string>();
            
            lock (_metadataLock)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                // Mark all written versions as invalid
                foreach (var (pageId, version) in transaction.WrittenPages)
                {
                    if (_metadata.PageVersions.TryGetValue(pageId, out var pageInfo))
                    {
                        pageInfo.RemoveVersion(version);
                    }
                    
                    // Collect version files for deletion
                    var namespacePath = GetNamespacePathFromPageId(pageId);
                    var versionFile = Path.Combine(namespacePath, $"{GetPageFileNameFromPageId(pageId)}.v{transactionId}");
                    filesToDelete.Add(versionFile);
                }

                transaction.IsRolledBack = true;
                _metadata.ActiveTransactions.Remove(transactionId);
                _activeTransactions.TryRemove(transactionId, out _);
            }
            
            // Delete version files asynchronously
            var deleteTasks = filesToDelete.Where(File.Exists).Select(async file =>
            {
                await Task.Run(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    File.Delete(file);
                }, cancellationToken).ConfigureAwait(false);
            });
            
            await Task.WhenAll(deleteTasks).ConfigureAwait(false);
            
            // Persist metadata changes
            await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<object[]> ReadPageAsync(long transactionId, string namespaceName, string pageId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{namespaceName}:{pageId}";
        
        // Increment operation count
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementNamespaceOperations(namespaceName);
        }, cancellationToken).ConfigureAwait(false);
        
        try
        {
            // Perform I/O-bound read operations asynchronously
            var result = await ReadPageInternalAsync(transactionId, namespaceName, pageId, cancellationToken).ConfigureAwait(false);
            
            // Record read version for conflict detection
            await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                lock (_metadataLock)
                {
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
                }
            }, cancellationToken).ConfigureAwait(false);
            
            return result;
        }
        finally
        {
            DecrementNamespaceOperations(namespaceName);
        }
    }

    public async Task<Dictionary<string, object[]>> GetMatchingObjectsAsync(long transactionId, string namespaceName, string pattern, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        var transaction = _activeTransactions[transactionId];
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementNamespaceOperations(namespaceName);
        }, cancellationToken).ConfigureAwait(false);
        
        try
        {
            var result = new Dictionary<string, object[]>();
            var namespacePath = GetNamespacePath(namespaceName);
            
            if (!Directory.Exists(namespacePath))
                return result;
            
            // Perform file system operations asynchronously
            var files = await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Directory.GetFiles(namespacePath, $"*{_formatAdapter.FileExtension}.v*");
            }, cancellationToken).ConfigureAwait(false);
            
            var regex = CreateRegexFromPattern(pattern);
            var pageGroups = files
                .Select(f => Path.GetFileName(f))
                .Where(f => f.Contains(".v"))
                .Select(f => {
                    var nameWithoutVersion = f.Substring(0, f.LastIndexOf(".v"));
                    var extensionIndex = nameWithoutVersion.LastIndexOf(_formatAdapter.FileExtension);
                    return extensionIndex > 0 ? nameWithoutVersion.Substring(0, extensionIndex) : nameWithoutVersion;
                })
                .Distinct()
                .Where(pageId => regex.IsMatch(pageId));
            
            // Process pages with proper snapshot isolation
            await Task.Run(() =>
            {
                lock (_metadataLock)
                {
                    foreach (var pageId in pageGroups)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        
                        var fullPageId = $"{namespaceName}:{pageId}";
                        
                        // Record read version for conflict detection
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
                    }
                }
            }, cancellationToken).ConfigureAwait(false);
            
            // Read page content asynchronously
            var readTasks = pageGroups.Select(async pageId =>
            {
                var pageContent = await ReadPageInternalAsync(transactionId, namespaceName, pageId, cancellationToken).ConfigureAwait(false);
                if (pageContent.Length > 0)
                {
                    return new KeyValuePair<string, object[]>(pageId, pageContent);
                }
                return default(KeyValuePair<string, object[]>?);
            });
            
            var pageResults = await Task.WhenAll(readTasks).ConfigureAwait(false);
            
            foreach (var pageResult in pageResults.Where(r => r.HasValue))
            {
                var kvp = pageResult!.Value;
                result[kvp.Key] = kvp.Value;
            }
            
            return result;
        }
        finally
        {
            DecrementNamespaceOperations(namespaceName);
        }
    }

    public async Task<string> InsertObjectAsync(long transactionId, string namespaceName, object obj, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        var transaction = _activeTransactions[transactionId];
        var namespacePath = GetNamespacePath(namespaceName);
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementNamespaceOperations(namespaceName);
        }, cancellationToken).ConfigureAwait(false);
        
        try
        {
            return await Task.Run(async () =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                // Ensure namespace directory exists
                Directory.CreateDirectory(namespacePath);
                
                // Find or create page for insertion
                var pageId = FindOrCreatePageForInsertion(namespaceName, obj);
                var fullPageId = $"{namespaceName}:{pageId}";
                
                // Read current page content
                object[] currentContent;
                if (transaction.WrittenPages.ContainsKey(fullPageId))
                {
                    var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{transactionId}");
                    if (File.Exists(versionFile))
                    {
                        var content = await File.ReadAllTextAsync(versionFile, cancellationToken).ConfigureAwait(false);
                        currentContent = _formatAdapter.DeserializeArray(content, typeof(object));
                    }
                    else
                    {
                        currentContent = Array.Empty<object>();
                    }
                }
                else
                {
                    currentContent = await ReadPageInternalAsync(transactionId, namespaceName, pageId, cancellationToken).ConfigureAwait(false);
                }
                
                var newContent = currentContent.Concat(new[] { obj }).ToArray();
                
                // Write new version asynchronously
                await WritePageVersionAsync(transactionId, namespaceName, pageId, newContent, cancellationToken).ConfigureAwait(false);
                
                // Track in transaction
                transaction.WrittenPages[fullPageId] = transactionId;
                
                return pageId;
            }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DecrementNamespaceOperations(namespaceName);
        }
    }

    public async Task UpdatePageAsync(long transactionId, string namespaceName, string pageId, object[] objects, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        var transaction = _activeTransactions[transactionId];
        var fullPageId = $"{namespaceName}:{pageId}";
        
        // CRITICAL: Enforce read-before-write rule for ACID isolation
        if (!transaction.ReadVersions.ContainsKey(fullPageId))
        {
            throw new InvalidOperationException(
                $"Cannot update page '{pageId}' in namespace '{namespaceName}' without reading it first " +
                $"in transaction {transactionId}. ACID isolation requires read-before-write for updates.");
        }
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementNamespaceOperations(namespaceName);
        }, cancellationToken).ConfigureAwait(false);
        
        try
        {
            // Write new version asynchronously
            await WritePageVersionAsync(transactionId, namespaceName, pageId, objects, cancellationToken).ConfigureAwait(false);
            
            // Track in transaction
            transaction.WrittenPages[fullPageId] = transactionId;
        }
        finally
        {
            DecrementNamespaceOperations(namespaceName);
        }
    }

    public async Task CreateNamespaceAsync(long transactionId, string namespaceName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var namespacePath = GetNamespacePath(namespaceName);
            Directory.CreateDirectory(namespacePath);
            
            // Create namespace marker file
            var markerFile = Path.Combine(namespacePath, $".namespace{_formatAdapter.FileExtension}");
            var markerContent = _formatAdapter.Serialize(new { 
                Namespace = namespaceName, 
                Created = DateTime.UtcNow,
                CreatedByTransaction = transactionId 
            });
            
            await File.WriteAllTextAsync(markerFile, markerContent, cancellationToken).ConfigureAwait(false);
            
            if (_config.EnableBatchFlushing && _batchFlushCoordinator != null)
            {
                await _batchFlushCoordinator.QueueFlushAsync(markerFile, FlushPriority.Normal).ConfigureAwait(false);
            }
            else
            {
                await FlushToDiskAsync(markerFile, cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteNamespaceAsync(long transactionId, string namespaceName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(namespaceName);
        
        // Wait for all operations to complete
        await WaitForNamespaceOperationsToCompleteAsync(namespaceName, cancellationToken).ConfigureAwait(false);
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var namespacePath = GetNamespacePath(namespaceName);
            if (Directory.Exists(namespacePath))
            {
                Directory.Delete(namespacePath, recursive: true);
            }
            
            // Clean up metadata
            lock (_metadataLock)
            {
                var keysToRemove = _metadata.PageVersions.Keys
                    .Where(k => k.StartsWith($"{namespaceName}:"))
                    .ToList();
                    
                foreach (var key in keysToRemove)
                {
                    _metadata.PageVersions.Remove(key);
                }
                
                _metadata.NamespaceOperations.Remove(namespaceName);
            }
        }, cancellationToken).ConfigureAwait(false);
        
        await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task RenameNamespaceAsync(long transactionId, string oldName, string newName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        ValidateTransaction(transactionId);
        ValidateNamespace(oldName);
        ValidateNamespace(newName);
        
        await WaitForNamespaceOperationsToCompleteAsync(oldName, cancellationToken).ConfigureAwait(false);
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
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
            }
        }, cancellationToken).ConfigureAwait(false);
        
        await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task InitializeAsync(string rootPath, StorageConfig? config = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
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
            await LoadOrCreateConfigAsync(cancellationToken).ConfigureAwait(false);
            
            // Load or create version metadata
            await LoadOrCreateMetadataAsync(cancellationToken).ConfigureAwait(false);
            
            // Initialize batch flush coordinator if enabled
            if (_config.EnableBatchFlushing)
            {
                var batchConfig = _config.BatchFlushConfig ?? new BatchFlushConfig();
                _batchFlushCoordinator = new BatchFlushCoordinator(batchConfig);
                await _batchFlushCoordinator.StartAsync().ConfigureAwait(false);
            }
            
            // Initialize metadata persistence timer (every 5 seconds)
            _metadataPersistTimer = new Timer(async state => await PersistMetadataIfDirtyAsync(state).ConfigureAwait(false), 
                null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task StartVersionCleanupAsync(int intervalMinutes = 15, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            // Handle immediate cleanup request
            if (intervalMinutes == 0)
            {
                // Run cleanup once immediately
                _ = Task.Run(async () => await RunVersionCleanupAsync(null, cancellationToken).ConfigureAwait(false));
                return;
            }
            
            // Create cleanup timer that runs periodically
            _cleanupTimer = new Timer(async state => await RunVersionCleanupAsync(state, CancellationToken.None).ConfigureAwait(false), 
                null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(intervalMinutes));
        }, cancellationToken).ConfigureAwait(false);
    }

    // Private async helper methods
    
    private async Task<object[]> ReadPageInternalAsync(long transactionId, string namespaceName, string pageId, CancellationToken cancellationToken)
    {
        return await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var transaction = _activeTransactions[transactionId];
            var fullPageId = $"{namespaceName}:{pageId}";
            var namespacePath = GetNamespacePath(namespaceName);
            
            if (_metadata.PageVersions.TryGetValue(fullPageId, out var pageInfo))
            {
                var allVersions = pageInfo.GetVersionsCopy()
                    .Where(v => v <= transaction.SnapshotTSN)
                    .OrderByDescending(v => v)
                    .ToList();
                
                foreach (var version in allVersions)
                {
                    if (_metadata.ActiveTransactions.Contains(version))
                        continue;
                        
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
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task WritePageVersionAsync(long transactionId, string namespaceName, string pageId, object[] content, CancellationToken cancellationToken)
    {
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var namespacePath = GetNamespacePath(namespaceName);
            var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{transactionId}");
            var fullPageId = $"{namespaceName}:{pageId}";
            
            var serializedContent = _formatAdapter.SerializeArray(content);
            await File.WriteAllTextAsync(versionFile, serializedContent, cancellationToken).ConfigureAwait(false);
            
            if (_config.EnableBatchFlushing && _batchFlushCoordinator != null)
            {
                await _batchFlushCoordinator.QueueFlushAsync(versionFile, FlushPriority.Normal).ConfigureAwait(false);
            }
            else
            {
                await FlushToDiskAsync(versionFile, cancellationToken).ConfigureAwait(false);
            }
            
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
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task FlushToDiskAsync(string filePath, CancellationToken cancellationToken)
    {
        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            // Force immediate write to disk
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            fs.Flush(flushToDisk: true);
            
            // Track flush operation count
            lock (_flushCountLock)
            {
                _flushOperationCount++;
            }
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task PersistMetadataAsync(CancellationToken cancellationToken)
    {
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            _metadata.LastUpdated = DateTime.UtcNow;
            var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
            var content = _formatAdapter.Serialize(_metadata);
            await File.WriteAllTextAsync(metadataFile, content, cancellationToken).ConfigureAwait(false);
            
            if (_config.EnableBatchFlushing && _batchFlushCoordinator != null)
            {
                await _batchFlushCoordinator.QueueFlushAsync(metadataFile, FlushPriority.Critical).ConfigureAwait(false);
            }
            else
            {
                await FlushToDiskAsync(metadataFile, cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task LoadOrCreateConfigAsync(CancellationToken cancellationToken)
    {
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var configFiles = new[]
            {
                Path.Combine(_rootPath, "storage.json"),
                Path.Combine(_rootPath, "storage.yaml"),
                Path.Combine(_rootPath, "storage.xml")
            };

            var existingConfig = configFiles.FirstOrDefault(File.Exists);
            
            if (existingConfig != null)
            {
                var content = await File.ReadAllTextAsync(existingConfig, cancellationToken).ConfigureAwait(false);
                var adapter = GetAdapterFromExtension(Path.GetExtension(existingConfig));
                _config = adapter.Deserialize<StorageConfig>(content);
            }
            else
            {
                // Create default config
                var configPath = Path.Combine(_rootPath, $"storage{_formatAdapter.FileExtension}");
                var configContent = _formatAdapter.Serialize(_config);
                await File.WriteAllTextAsync(configPath, configContent, cancellationToken).ConfigureAwait(false);
                await FlushToDiskAsync(configPath, cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task LoadOrCreateMetadataAsync(CancellationToken cancellationToken)
    {
        await Task.Run(async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var metadataFile = Path.Combine(_rootPath, $".versions{_formatAdapter.FileExtension}");
            
            if (File.Exists(metadataFile))
            {
                var content = await File.ReadAllTextAsync(metadataFile, cancellationToken).ConfigureAwait(false);
                _metadata = _formatAdapter.Deserialize<VersionMetadata>(content);
                _nextTransactionId = Math.Max(_nextTransactionId, _metadata.CurrentTSN + 1);
                
                // Clean up stale active transactions (crash recovery)
                _metadata.ActiveTransactions.Clear();
            }
            else
            {
                _metadata = new VersionMetadata { CurrentTSN = 0 };
                await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }
    
    private async Task WaitForNamespaceOperationsToCompleteAsync(string namespaceName, CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromMinutes(5);
        var start = DateTime.UtcNow;
        
        while (DateTime.UtcNow - start < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            bool hasOperations = false;
            lock (_metadataLock)
            {
                hasOperations = _metadata.NamespaceOperations.ContainsKey(namespaceName) && 
                               _metadata.NamespaceOperations[namespaceName] > 0;
            }
            
            if (!hasOperations)
            {
                return;
            }
            
            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }
        
        throw new TimeoutException($"Timeout waiting for operations to complete on namespace '{namespaceName}'");
    }
    
    private async Task RunVersionCleanupAsync(object? state, CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            
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
            
            // Delete obsolete version files asynchronously
            var deleteTasks = versionsToDelete.Select(async item =>
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    var (fullPageId, version) = item;
                    var parts = fullPageId.Split(':');
                    var namespacePath = GetNamespacePath(parts[0]);
                    var pageId = parts[1];
                    var versionFile = Path.Combine(namespacePath, $"{pageId}{_formatAdapter.FileExtension}.v{version}");
                    
                    if (File.Exists(versionFile))
                    {
                        await Task.Run(() =>
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            File.Delete(versionFile);
                        }, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch
                {
                    // Continue cleanup even if individual files fail
                }
            });
            
            await Task.WhenAll(deleteTasks).ConfigureAwait(false);
            
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
                }
                
                await PersistMetadataAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            // Cleanup failures should not crash the system
        }
    }
    
    private async Task PersistMetadataIfDirtyAsync(object? state)
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
                await PersistMetadataAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Don't crash on persistence errors
            }
        }
    }
}