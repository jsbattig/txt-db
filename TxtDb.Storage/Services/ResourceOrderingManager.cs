using System;
using System.Collections.Generic;
using System.Linq;

namespace TxtDb.Storage.Services;

/// <summary>
/// ResourceOrderingManager - Provides deterministic resource ordering to prevent deadlocks
/// Implements lock ordering protocol to eliminate circular dependencies
/// </summary>
public static class ResourceOrderingManager
{
    /// <summary>
    /// Compares two resources using deterministic ordering
    /// Returns negative if resource1 < resource2, positive if resource1 > resource2, zero if equal
    /// </summary>
    /// <param name="resource1">First resource identifier</param>
    /// <param name="resource2">Second resource identifier</param>
    /// <returns>Comparison result for ordering</returns>
    public static int CompareResources(string resource1, string resource2)
    {
        if (resource1 == null && resource2 == null) return 0;
        if (resource1 == null) return -1;
        if (resource2 == null) return 1;
        
        return string.Compare(resource1, resource2, StringComparison.Ordinal);
    }

    /// <summary>
    /// Orders a collection of resources deterministically
    /// All transactions will acquire locks in this same order, preventing deadlocks
    /// </summary>
    /// <param name="resources">Collection of resource identifiers</param>
    /// <returns>Deterministically ordered list of resources</returns>
    public static List<string> OrderResources(IEnumerable<string> resources)
    {
        if (resources == null)
            return new List<string>();

        return resources
            .Where(r => r != null)
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Validates that a set of resources are in correct order
    /// Used for debugging and validation purposes
    /// </summary>
    /// <param name="resources">Resources to validate</param>
    /// <returns>True if resources are in deterministic order</returns>
    public static bool AreResourcesOrdered(IEnumerable<string> resources)
    {
        if (resources == null) return true;
        
        var resourceList = resources.ToList();
        if (resourceList.Count <= 1) return true;

        for (int i = 1; i < resourceList.Count; i++)
        {
            if (CompareResources(resourceList[i - 1], resourceList[i]) >= 0)
            {
                return false; // Not in proper order
            }
        }

        return true;
    }
}