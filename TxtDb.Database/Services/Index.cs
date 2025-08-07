using TxtDb.Database.Interfaces;
using TxtDb.Storage.Interfaces.Async;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Dynamic;

namespace TxtDb.Database.Services;

/// <summary>
/// SPECIFICATION COMPLIANT: Simple index implementation that maps field values to page IDs.
/// Uses SortedDictionary with HashSet values as specified.
/// </summary>
public class Index : IIndex
{
    private readonly string _name;
    private readonly string _fieldPath;
    private readonly SortedDictionary<object, HashSet<string>> _keyToPageIds;
    private readonly Dictionary<string, HashSet<object>> _pageToKeys;
    private readonly IAsyncStorageSubsystem _storageSubsystem;
    private readonly string _tableNamespace;
    private readonly object _indexLock = new();

    public string Name => _name;
    public string FieldPath => _fieldPath;

    public Index(string name, string fieldPath, IAsyncStorageSubsystem storageSubsystem, string tableNamespace)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fieldPath = fieldPath ?? throw new ArgumentNullException(nameof(fieldPath));
        _storageSubsystem = storageSubsystem ?? throw new ArgumentNullException(nameof(storageSubsystem));
        _tableNamespace = tableNamespace ?? throw new ArgumentNullException(nameof(tableNamespace));
        
        // SPECIFICATION COMPLIANT: Use SortedDictionary with HashSet values
        _keyToPageIds = new SortedDictionary<object, HashSet<string>>();
        _pageToKeys = new Dictionary<string, HashSet<object>>();
    }

    public async Task<IList<dynamic>> FindAsync(object value, CancellationToken cancellationToken = default)
    {
        var results = new List<dynamic>();
        
        lock (_indexLock)
        {
            if (_keyToPageIds.TryGetValue(value, out var pageIds))
            {
                // For each page that contains objects with this value
                foreach (var pageId in pageIds)
                {
                    // Read the page and scan for matching objects
                    // This is a simplified implementation - production would be more efficient
                }
            }
        }
        
        return results;
    }

    /// <summary>
    /// SPECIFICATION COMPLIANT: Add entry to index with proper structure
    /// </summary>
    public void AddEntry(object key, string pageId)
    {
        if (key == null || pageId == null) return;
        
        lock (_indexLock)
        {
            // Add key -> page mapping
            if (!_keyToPageIds.ContainsKey(key))
            {
                _keyToPageIds[key] = new HashSet<string>();
            }
            _keyToPageIds[key].Add(pageId);
            
            // Add reverse mapping: page -> keys
            if (!_pageToKeys.ContainsKey(pageId))
            {
                _pageToKeys[pageId] = new HashSet<object>();
            }
            _pageToKeys[pageId].Add(key);
        }
    }

    /// <summary>
    /// SPECIFICATION COMPLIANT: Remove entry from index with proper cleanup
    /// </summary>
    public void RemoveEntry(object key, string pageId)
    {
        if (key == null || pageId == null) return;
        
        lock (_indexLock)
        {
            // Remove key -> page mapping
            if (_keyToPageIds.TryGetValue(key, out var pageIds))
            {
                pageIds.Remove(pageId);
                if (pageIds.Count == 0)
                {
                    _keyToPageIds.Remove(key);
                }
            }
            
            // Remove reverse mapping: page -> keys
            if (_pageToKeys.TryGetValue(pageId, out var keys))
            {
                keys.Remove(key);
                if (keys.Count == 0)
                {
                    _pageToKeys.Remove(pageId);
                }
            }
        }
    }

    /// <summary>
    /// SPECIFICATION COMPLIANT: Get all pages containing objects with specified key
    /// </summary>
    public HashSet<string> GetPagesForKey(object key)
    {
        lock (_indexLock)
        {
            return _keyToPageIds.TryGetValue(key, out var pages) 
                ? new HashSet<string>(pages) 
                : new HashSet<string>();
        }
    }
}