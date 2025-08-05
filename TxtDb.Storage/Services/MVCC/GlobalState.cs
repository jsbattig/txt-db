using System;
using System.Collections.Generic;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Global State for Epic 003 Story 002
    /// 
    /// Represents the global database state shared across all processes:
    /// - Global Transaction Sequence Number (TSN) allocation
    /// - Active transaction tracking
    /// - Page version metadata coordination
    /// - Last updated timestamp for consistency checks
    /// 
    /// CRITICAL: This state must be atomically updated across processes
    /// </summary>
    public class GlobalState
    {
        /// <summary>
        /// Current highest allocated Transaction Sequence Number
        /// </summary>
        public long CurrentTSN { get; set; }

        /// <summary>
        /// Next transaction ID to be allocated
        /// </summary>
        public long NextTransactionId { get; set; }

        /// <summary>
        /// Set of currently active transaction IDs across all processes
        /// </summary>
        public HashSet<long> ActiveTransactions { get; set; } = new HashSet<long>();

        /// <summary>
        /// Page version information for all pages across all namespaces
        /// Key format: "namespace:pageId"
        /// </summary>
        public Dictionary<string, PageVersionInfo> PageVersions { get; set; } = new Dictionary<string, PageVersionInfo>();

        /// <summary>
        /// Timestamp when this state was last updated
        /// Used for consistency checks and debugging
        /// </summary>
        public DateTime LastUpdated { get; set; }

        /// <summary>
        /// Creates a new GlobalState with default values
        /// </summary>
        public GlobalState()
        {
            CurrentTSN = 0;
            NextTransactionId = 1;
            LastUpdated = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates a deep copy of this GlobalState instance
        /// </summary>
        public GlobalState Clone()
        {
            var cloned = new GlobalState
            {
                CurrentTSN = this.CurrentTSN,
                NextTransactionId = this.NextTransactionId,
                ActiveTransactions = new HashSet<long>(this.ActiveTransactions),
                PageVersions = new Dictionary<string, PageVersionInfo>(),
                LastUpdated = this.LastUpdated
            };

            // Deep copy PageVersionInfo objects
            foreach (var kvp in this.PageVersions)
            {
                cloned.PageVersions[kvp.Key] = kvp.Value.Clone();
            }

            return cloned;
        }
    }
}