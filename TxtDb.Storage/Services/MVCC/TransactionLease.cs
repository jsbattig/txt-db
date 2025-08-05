using System;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Transaction state enumeration for Epic 003 Story 003
    /// </summary>
    public enum TransactionState
    {
        /// <summary>
        /// Transaction is currently active with regular heartbeats
        /// </summary>
        Active,

        /// <summary>
        /// Transaction has been completed successfully
        /// </summary>
        Completed,

        /// <summary>
        /// Transaction has been rolled back
        /// </summary>
        RolledBack,

        /// <summary>
        /// Transaction is suspected to be abandoned (stale heartbeat or dead process)
        /// </summary>
        Abandoned
    }

    /// <summary>
    /// Transaction Lease for Epic 003 Story 003
    /// 
    /// Represents a transaction lease file that tracks:
    /// - Transaction identification and state
    /// - Process ownership information
    /// - Heartbeat timestamps for liveness detection
    /// - Snapshot isolation information
    /// 
    /// CRITICAL: This data is persisted to disk for cross-process coordination
    /// </summary>
    public class TransactionLease
    {
        /// <summary>
        /// Unique transaction identifier
        /// </summary>
        public long TransactionId { get; set; }

        /// <summary>
        /// Process ID that owns this transaction
        /// </summary>
        public int ProcessId { get; set; }

        /// <summary>
        /// Machine name where the process is running
        /// </summary>
        public string MachineName { get; set; } = "";

        /// <summary>
        /// Transaction Sequence Number snapshot for MVCC isolation
        /// </summary>
        public long SnapshotTSN { get; set; }

        /// <summary>
        /// When the transaction was started
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Last heartbeat timestamp - updated every 5 seconds by owning process
        /// </summary>
        public DateTime Heartbeat { get; set; }

        /// <summary>
        /// When the transaction was completed (if applicable)
        /// </summary>
        public DateTime? CompletedTime { get; set; }

        /// <summary>
        /// Current state of the transaction
        /// </summary>
        public TransactionState State { get; set; }

        /// <summary>
        /// Creates a new TransactionLease with current timestamp
        /// </summary>
        public TransactionLease()
        {
            StartTime = DateTime.UtcNow;
            Heartbeat = DateTime.UtcNow;
            State = TransactionState.Active;
            MachineName = Environment.MachineName;
        }

        /// <summary>
        /// Updates the heartbeat timestamp to current time
        /// </summary>
        public void UpdateHeartbeat()
        {
            Heartbeat = DateTime.UtcNow;
        }

        /// <summary>
        /// Marks the transaction as completed
        /// </summary>
        public void MarkCompleted()
        {
            State = TransactionState.Completed;
            CompletedTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Marks the transaction as rolled back
        /// </summary>
        public void MarkRolledBack()
        {
            State = TransactionState.RolledBack;
            CompletedTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Marks the transaction as abandoned
        /// </summary>
        public void MarkAbandoned()
        {
            State = TransactionState.Abandoned;
        }

        /// <summary>
        /// Checks if the transaction lease is still valid based on heartbeat age
        /// </summary>
        /// <param name="maxAge">Maximum allowed age for heartbeat</param>
        /// <returns>True if lease is still valid</returns>
        public bool IsValid(TimeSpan maxAge)
        {
            if (State != TransactionState.Active)
                return false;

            return DateTime.UtcNow - Heartbeat <= maxAge;
        }
    }
}