using Newtonsoft.Json.Linq;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// Helper methods for stress tests to properly handle page updates
/// </summary>
public static class StressTestHelpers
{
    /// <summary>
    /// Properly updates specific objects on a page while preserving all others
    /// </summary>
    public static object[] UpdateObjectsOnPage(
        object[] currentPageContent,
        Func<dynamic, bool> selector,
        Func<dynamic, object> updater)
    {
        var result = new List<object>();
        
        foreach (var obj in currentPageContent)
        {
            dynamic item = obj;
            if (selector(item))
            {
                // Update this object
                result.Add(updater(item));
            }
            else
            {
                // Preserve unchanged object
                result.Add(obj);
            }
        }
        
        return result.ToArray();
    }
    
    /// <summary>
    /// Merges updates into existing page content
    /// </summary>
    public static object[] MergeUpdatesIntoPage(
        object[] currentPageContent,
        Dictionary<int, object> updates) // Key = object ID, Value = new object
    {
        var result = new List<object>();
        
        foreach (var obj in currentPageContent)
        {
            dynamic item = obj;
            int id = (int)item.Id;
            
            if (updates.ContainsKey(id))
            {
                // Use updated version
                result.Add(updates[id]);
            }
            else
            {
                // Preserve original
                result.Add(obj);
            }
        }
        
        // Add any new objects not in original content
        var existingIds = currentPageContent.Select(o => {
            dynamic item = o;
            return (int)item.Id;
        }).ToHashSet();
        
        foreach (var kvp in updates)
        {
            if (!existingIds.Contains(kvp.Key))
            {
                result.Add(kvp.Value);
            }
        }
        
        return result.ToArray();
    }
    
    /// <summary>
    /// Safely gets property value from dynamic object
    /// </summary>
    public static T GetPropertyValue<T>(dynamic obj, string propertyName, T defaultValue = default!)
    {
        try
        {
            if (obj is JObject jObj && jObj.ContainsKey(propertyName))
            {
                return jObj[propertyName]!.ToObject<T>()!;
            }
            
            // Try direct property access
            return (T)obj.GetType().GetProperty(propertyName)?.GetValue(obj) ?? defaultValue;
        }
        catch
        {
            return defaultValue;
        }
    }
}
