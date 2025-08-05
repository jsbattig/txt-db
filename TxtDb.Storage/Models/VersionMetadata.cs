using System.Collections.Concurrent;

namespace TxtDb.Storage.Models;

public class VersionMetadata
{
    public long CurrentTSN { get; set; }
    public HashSet<long> ActiveTransactions { get; set; } = new();
    public Dictionary<string, PageVersionInfo> PageVersions { get; set; } = new();
    public Dictionary<string, int> NamespaceOperations { get; set; } = new();
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
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
    public bool IsCommitted { get; set; }
    public bool IsRolledBack { get; set; }
}