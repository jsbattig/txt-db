using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace TxtDb.Storage.Services;

/// <summary>
/// WaitForGraphDetector - Tracks transaction dependencies and detects deadlock cycles
/// Implements wait-for graph analysis for immediate deadlock detection
/// Thread-safe implementation for concurrent access
/// </summary>
public class WaitForGraphDetector
{
    /// <summary>
    /// Maps transaction ID to the set of resources it's waiting for and their holders
    /// Key: Transaction ID, Value: Dictionary of Resource -> Holder Transaction ID
    /// </summary>
    private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, long>> _waitingFor = new();

    /// <summary>
    /// Maps transaction ID to the set of transactions waiting for it
    /// Key: Transaction ID, Value: Set of waiting transaction IDs
    /// </summary>
    private readonly ConcurrentDictionary<long, HashSet<long>> _waitedForBy = new();

    /// <summary>
    /// Lock for consistent graph updates
    /// </summary>
    private readonly object _graphLock = new object();

    /// <summary>
    /// Adds a wait relation to the graph and detects if it creates a cycle
    /// </summary>
    /// <param name="transactionId">Transaction that is waiting</param>
    /// <param name="resourceId">Resource being waited for</param>
    /// <param name="holderTransactionId">Transaction currently holding the resource</param>
    /// <returns>True if adding this relation creates a deadlock cycle</returns>
    public bool AddWaitRelation(long transactionId, string resourceId, long holderTransactionId)
    {
        if (transactionId == holderTransactionId)
        {
            return false; // Transaction can't wait for itself
        }

        lock (_graphLock)
        {
            // Add the wait relation
            var waitingResources = _waitingFor.GetOrAdd(transactionId, _ => new ConcurrentDictionary<string, long>());
            waitingResources[resourceId] = holderTransactionId;

            var waiters = _waitedForBy.GetOrAdd(holderTransactionId, _ => new HashSet<long>());
            waiters.Add(transactionId);

            // Check for cycles using depth-first search
            return HasCycleFromTransaction(transactionId, new HashSet<long>());
        }
    }

    /// <summary>
    /// Removes a wait relation from the graph
    /// </summary>
    /// <param name="transactionId">Transaction that was waiting</param>
    /// <param name="resourceId">Resource that was being waited for</param>
    public void RemoveWaitRelation(long transactionId, string resourceId)
    {
        lock (_graphLock)
        {
            if (_waitingFor.TryGetValue(transactionId, out var waitingResources))
            {
                if (waitingResources.TryRemove(resourceId, out var holderTransactionId))
                {
                    // Remove from the reverse mapping
                    if (_waitedForBy.TryGetValue(holderTransactionId, out var waiters))
                    {
                        waiters.Remove(transactionId);
                        if (waiters.Count == 0)
                        {
                            _waitedForBy.TryRemove(holderTransactionId, out _);
                        }
                    }
                }

                // Clean up empty transaction entry
                if (waitingResources.IsEmpty)
                {
                    _waitingFor.TryRemove(transactionId, out _);
                }
            }
        }
    }

    /// <summary>
    /// Removes all wait relations for a specific transaction (only removes where it's waiting, not where it's holding)
    /// </summary>
    /// <param name="transactionId">Transaction to remove all waits for</param>
    public void RemoveAllWaitsForTransaction(long transactionId)
    {
        lock (_graphLock)
        {
            // Remove all resources this transaction is waiting for
            if (_waitingFor.TryRemove(transactionId, out var waitingResources))
            {
                foreach (var holderTransactionId in waitingResources.Values)
                {
                    if (_waitedForBy.TryGetValue(holderTransactionId, out var waiters))
                    {
                        waiters.Remove(transactionId);
                        if (waiters.Count == 0)
                        {
                            _waitedForBy.TryRemove(holderTransactionId, out _);
                        }
                    }
                }
            }

            // NOTE: We do NOT remove this transaction as a holder that others might be waiting for
            // That would be handled when the transaction completes/commits and releases its locks
        }
    }

    /// <summary>
    /// Checks if a wait relation exists
    /// </summary>
    /// <param name="transactionId">Waiting transaction</param>
    /// <param name="resourceId">Resource being waited for</param>
    /// <param name="holderTransactionId">Expected holder transaction</param>
    /// <returns>True if the relation exists</returns>
    public bool HasWaitRelation(long transactionId, string resourceId, long holderTransactionId)
    {
        lock (_graphLock)
        {
            return _waitingFor.TryGetValue(transactionId, out var resources) &&
                   resources.TryGetValue(resourceId, out var holder) &&
                   holder == holderTransactionId;
        }
    }

    /// <summary>
    /// Detects deadlock cycles in the wait-for graph
    /// </summary>
    /// <returns>List of transaction IDs involved in the deadlock cycle, or empty if no deadlock</returns>
    public List<long> DetectDeadlock()
    {
        lock (_graphLock)
        {
            var visited = new HashSet<long>();

            foreach (var transactionId in _waitingFor.Keys)
            {
                if (!visited.Contains(transactionId))
                {
                    var cycle = FindCycleFromTransaction(transactionId, new HashSet<long>(), new List<long>());
                    if (cycle.Count > 0)
                    {
                        return cycle;
                    }
                }
            }

            return new List<long>();
        }
    }

    /// <summary>
    /// Gets diagnostic information about the current wait-for graph
    /// </summary>
    /// <returns>Diagnostic information</returns>
    public WaitForGraphDiagnostics GetDiagnostics()
    {
        lock (_graphLock)
        {
            var allTransactions = new HashSet<long>();
            var totalWaitRelations = 0;

            foreach (var kvp in _waitingFor)
            {
                allTransactions.Add(kvp.Key);
                totalWaitRelations += kvp.Value.Count;

                foreach (var holderTxnId in kvp.Value.Values)
                {
                    allTransactions.Add(holderTxnId);
                }
            }

            var deadlockCycle = DetectDeadlock();

            return new WaitForGraphDiagnostics
            {
                ActiveWaitRelations = totalWaitRelations,
                InvolvedTransactions = allTransactions.Count,
                HasDeadlock = deadlockCycle.Count > 0,
                DeadlockCycle = deadlockCycle
            };
        }
    }

    /// <summary>
    /// Checks if adding a dependency would create a cycle using DFS
    /// </summary>
    private bool HasCycleFromTransaction(long transactionId, HashSet<long> visited)
    {
        if (visited.Contains(transactionId))
        {
            return true; // Found a cycle
        }

        visited.Add(transactionId);

        if (_waitingFor.TryGetValue(transactionId, out var waitingResources))
        {
            foreach (var holderTransactionId in waitingResources.Values)
            {
                if (HasCycleFromTransaction(holderTransactionId, visited))
                {
                    return true;
                }
            }
        }

        visited.Remove(transactionId);
        return false;
    }

    /// <summary>
    /// Finds a cycle starting from a specific transaction
    /// </summary>
    private List<long> FindCycleFromTransaction(long transactionId, HashSet<long> visiting, List<long> path)
    {
        if (visiting.Contains(transactionId))
        {
            // Found a cycle - return the cycle portion
            var cycleStart = path.IndexOf(transactionId);
            return path.Skip(cycleStart).ToList();
        }

        visiting.Add(transactionId);
        path.Add(transactionId);

        if (_waitingFor.TryGetValue(transactionId, out var waitingResources))
        {
            foreach (var holderTransactionId in waitingResources.Values)
            {
                var cycle = FindCycleFromTransaction(holderTransactionId, visiting, path);
                if (cycle.Count > 0)
                {
                    return cycle;
                }
            }
        }

        visiting.Remove(transactionId);
        path.RemoveAt(path.Count - 1);
        return new List<long>();
    }
}

/// <summary>
/// Diagnostic information about the wait-for graph
/// </summary>
public class WaitForGraphDiagnostics
{
    /// <summary>
    /// Number of active wait relations in the graph
    /// </summary>
    public int ActiveWaitRelations { get; set; }

    /// <summary>
    /// Number of unique transactions involved in the graph
    /// </summary>
    public int InvolvedTransactions { get; set; }

    /// <summary>
    /// Whether a deadlock cycle exists
    /// </summary>
    public bool HasDeadlock { get; set; }

    /// <summary>
    /// List of transaction IDs in the deadlock cycle (empty if no deadlock)
    /// </summary>
    public List<long> DeadlockCycle { get; set; } = new();
}