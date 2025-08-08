using System.Collections.Concurrent;

namespace TxtDb.Storage.Models;

/// <summary>
/// Version metadata with thread-safe collections to prevent concurrent modification exceptions during serialization.
/// CRITICAL FIX: All collections must be thread-safe to prevent "Collection was modified; enumeration operation may not execute"
/// exceptions when serializing metadata while other threads are modifying the collections.
/// </summary>
public class VersionMetadata
{
    public long CurrentTSN { get; set; }
    
    // Thread-safe collections to prevent concurrent modification exceptions during JSON serialization
    public ConcurrentDictionary<long, byte> ActiveTransactions { get; set; } = new();
    public ConcurrentDictionary<string, PageVersionInfo> PageVersions { get; set; } = new();
    public ConcurrentDictionary<string, int> NamespaceOperations { get; set; } = new();
    
    /// <summary>
    /// Track deleted namespaces to prevent operations on them
    /// CRITICAL: Operations on deleted namespaces must throw ArgumentException
    /// </summary>
    public ConcurrentDictionary<string, DateTime> DeletedNamespaces { get; set; } = new();
    
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Creates a serialization-safe snapshot of this metadata.
    /// Used to prevent concurrent modification exceptions during JSON serialization.
    /// </summary>
    public VersionMetadataSnapshot CreateSnapshot()
    {
        return new VersionMetadataSnapshot
        {
            CurrentTSN = this.CurrentTSN,
            ActiveTransactions = new HashSet<long>(this.ActiveTransactions.Keys),
            PageVersions = new Dictionary<string, PageVersionInfo>(this.PageVersions.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Clone())),
            NamespaceOperations = new Dictionary<string, int>(this.NamespaceOperations),
            DeletedNamespaces = new Dictionary<string, DateTime>(this.DeletedNamespaces),
            LastUpdated = this.LastUpdated
        };
    }
    
    /// <summary>
    /// Helper methods for HashSet-like operations on ActiveTransactions
    /// </summary>
    public bool AddActiveTransaction(long transactionId) => ActiveTransactions.TryAdd(transactionId, 0);
    public bool RemoveActiveTransaction(long transactionId) => ActiveTransactions.TryRemove(transactionId, out _);
    public bool ContainsActiveTransaction(long transactionId) => ActiveTransactions.ContainsKey(transactionId);
    public void ClearActiveTransactions() => ActiveTransactions.Clear();
    
    /// <summary>
    /// Restores metadata from a snapshot (used during loading from disk)
    /// </summary>
    public static VersionMetadata FromSnapshot(VersionMetadataSnapshot snapshot)
    {
        var metadata = new VersionMetadata
        {
            CurrentTSN = snapshot.CurrentTSN,
            LastUpdated = snapshot.LastUpdated
        };
        
        // Convert snapshot collections back to thread-safe collections
        foreach (var transactionId in snapshot.ActiveTransactions)
        {
            metadata.AddActiveTransaction(transactionId);
        }
        
        foreach (var kvp in snapshot.PageVersions)
        {
            metadata.PageVersions[kvp.Key] = kvp.Value;
        }
        
        foreach (var kvp in snapshot.NamespaceOperations)
        {
            metadata.NamespaceOperations[kvp.Key] = kvp.Value;
        }
        
        return metadata;
    }
}

public class PageVersionInfo
{
    private readonly object _lock = new object();
    
    public string PageId { get; set; } = string.Empty;
    public List<long> Versions { get; set; } = new();
    public long CurrentVersion { get; set; }
    public long OldestActiveVersion { get; set; }
    public DateTime CreatedTime { get; set; } = DateTime.UtcNow;
    public DateTime LastModified { get; set; } = DateTime.UtcNow;
    
    // Thread-safe access methods
    public List<long> GetVersionsCopy()
    {
        lock (_lock)
        {
            return new List<long>(Versions);
        }
    }
    
    public void AddVersion(long version)
    {
        lock (_lock)
        {
            if (!Versions.Contains(version))
            {
                Versions.Add(version);
                Versions.Sort(); // Keep sorted for performance
            }
        }
    }
    
    public void RemoveVersion(long version)
    {
        lock (_lock)
        {
            Versions.Remove(version);
        }
    }
    
    public bool HasVersion(long version)
    {
        lock (_lock)
        {
            return Versions.Contains(version);
        }
    }
    
    /// <summary>
    /// Creates a deep copy of this PageVersionInfo instance
    /// Used by GlobalState for atomic state management
    /// </summary>
    public PageVersionInfo Clone()
    {
        lock (_lock)
        {
            return new PageVersionInfo
            {
                PageId = this.PageId,
                Versions = new List<long>(this.Versions),
                CurrentVersion = this.CurrentVersion,
                OldestActiveVersion = this.OldestActiveVersion,
                CreatedTime = this.CreatedTime,
                LastModified = this.LastModified
            };
        }
    }
}

public class MVCCTransaction
{
    public long TransactionId { get; set; }
    public long SnapshotTSN { get; set; }
    public DateTime StartTime { get; set; }
    public Dictionary<string, long> ReadVersions { get; set; } = new();
    public Dictionary<string, long> WrittenPages { get; set; } = new();
    
    /// <summary>
    /// CRITICAL SECURITY FIX: Track actual file paths written by this transaction
    /// for transaction-specific flushing during critical operations.
    /// This prevents critical operations from flushing other transactions' data,
    /// maintaining MVCC isolation and ACID properties.
    /// </summary>
    public HashSet<string> WrittenFilePaths { get; set; } = new();
    
    /// <summary>
    /// Tracks all resource IDs that this transaction intends to access
    /// Used for deadlock prevention by pre-acquiring locks in deterministic order
    /// </summary>
    public HashSet<string> RequiredResources { get; set; } = new();
    
    /// <summary>
    /// Indicates if this transaction has already acquired its full resource set
    /// </summary>
    public bool ResourcesAcquired { get; set; } = false;
    
    /// <summary>
    /// PERFORMANCE OPTIMIZATION: Track if this transaction requires critical priority operations.
    /// When true, operations should use synchronous path to achieve <10ms performance targets.
    /// Set when CommitTransactionAsync is called with FlushPriority.Critical.
    /// </summary>
    public bool IsCriticalPriority { get; set; } = false;
    
    public bool IsCommitted { get; set; }
    public bool IsRolledBack { get; set; }
}

/// <summary>
/// Immutable snapshot of VersionMetadata used for safe serialization.
/// This prevents concurrent modification exceptions during JSON serialization.
/// </summary>
public class VersionMetadataSnapshot
{
    public long CurrentTSN { get; set; }
    public HashSet<long> ActiveTransactions { get; set; } = new();
    public Dictionary<string, PageVersionInfo> PageVersions { get; set; } = new();
    public Dictionary<string, int> NamespaceOperations { get; set; } = new();
    public Dictionary<string, DateTime> DeletedNamespaces { get; set; } = new();
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}