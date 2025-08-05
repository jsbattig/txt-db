using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Transaction Lease Manager for Epic 003 Story 003
    /// 
    /// Implements heartbeat-based transaction monitoring with:
    /// - Automatic detection of crashed processes (30s timeout)
    /// - Heartbeat mechanism to prevent false positives
    /// - Orphaned transaction cleanup within 60s
    /// - Active transactions visible across all processes
    /// - Lease validity checking and renewal
    /// 
    /// CRITICAL: Each transaction gets its own lease file for cross-process visibility
    /// </summary>
    public class TransactionLeaseManager : IDisposable
    {
        private readonly string _leasePath;
        private readonly Dictionary<long, Timer> _heartbeatTimers;
        private readonly Dictionary<long, TransactionLease> _activeLeases;
        private readonly object _leaseLock = new object();
        private volatile bool _disposed = false;

        // Configuration constants
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan CleanupAge = TimeSpan.FromHours(1);

        public TransactionLeaseManager(string leasePath)
        {
            _leasePath = leasePath ?? throw new ArgumentNullException(nameof(leasePath));
            _heartbeatTimers = new Dictionary<long, Timer>();
            _activeLeases = new Dictionary<long, TransactionLease>();

            // Ensure lease directory exists
            Directory.CreateDirectory(_leasePath);
        }

        /// <summary>
        /// Creates a new transaction lease with automatic heartbeat mechanism
        /// </summary>
        public async Task CreateLeaseAsync(long transactionId, long snapshotTSN)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TransactionLeaseManager));

            var lease = new TransactionLease
            {
                TransactionId = transactionId,
                ProcessId = Process.GetCurrentProcess().Id,
                SnapshotTSN = snapshotTSN,
                MachineName = Environment.MachineName
            };

            var leasePath = GetLeasePath(transactionId);

            lock (_leaseLock)
            {
                _activeLeases[transactionId] = lease;

                // Start heartbeat timer (immediate first run, then every 5 seconds)
                _heartbeatTimers[transactionId] = new Timer(
                    _ => Task.Run(async () => await UpdateHeartbeatAsync(transactionId)),
                    null,
                    TimeSpan.Zero, // Start immediately
                    HeartbeatInterval);
            }

            // Persist initial lease
            await PersistLeaseAsync(lease);
        }

        /// <summary>
        /// Gets all currently active transactions across all processes
        /// </summary>
        public async Task<List<TransactionLease>> GetActiveTransactionsAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TransactionLeaseManager));

            var activeLeases = new List<TransactionLease>();

            // Scan all lease files in the directory
            var leaseFiles = Directory.GetFiles(_leasePath, "txn_*.json");

            foreach (var leaseFile in leaseFiles)
            {
                try
                {
                    var leaseContent = await File.ReadAllTextAsync(leaseFile);
                    var lease = JsonSerializer.Deserialize<TransactionLease>(leaseContent);

                    if (lease != null && await IsLeaseValidAsync(lease))
                    {
                        activeLeases.Add(lease);
                    }
                    else if (lease != null && lease.State == TransactionState.Active)
                    {
                        // Only mark active leases as abandoned - don't touch completed/rolledback leases
                        lease.MarkAbandoned();
                        await PersistLeaseAsync(lease);
                        
                        // Clean up very old leases
                        if (ShouldCleanupLease(lease))
                        {
                            try
                            {
                                File.Delete(leaseFile);
                            }
                            catch
                            {
                                // Ignore cleanup errors
                            }
                        }
                    }
                    else if (lease != null && ShouldCleanupLease(lease))
                    {
                        // Clean up old completed/abandoned leases without modifying them
                        try
                        {
                            File.Delete(leaseFile);
                        }
                        catch
                        {
                            // Ignore cleanup errors
                        }
                    }
                }
                catch
                {
                    // Ignore corrupted lease files
                    try
                    {
                        File.Delete(leaseFile);
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
            }

            return activeLeases;
        }

        /// <summary>
        /// Marks a transaction lease as completed and stops its heartbeat
        /// </summary>
        public async Task CompleteLeaseAsync(long transactionId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TransactionLeaseManager));

            TransactionLease? lease = null;
            Timer? timer = null;

            lock (_leaseLock)
            {
                if (_activeLeases.TryGetValue(transactionId, out lease))
                {
                    lease.MarkCompleted();
                    _activeLeases.Remove(transactionId);
                }

                if (_heartbeatTimers.TryGetValue(transactionId, out timer))
                {
                    _heartbeatTimers.Remove(transactionId);
                }
            }

            // Stop heartbeat timer
            timer?.Dispose();

            // Persist completed state
            if (lease != null)
            {
                await PersistLeaseAsync(lease);
            }
        }

        /// <summary>
        /// Updates the heartbeat for a specific transaction
        /// </summary>
        private async Task UpdateHeartbeatAsync(long transactionId)
        {
            if (_disposed)
                return;

            TransactionLease? lease = null;
            lock (_leaseLock)
            {
                _activeLeases.TryGetValue(transactionId, out lease);
            }

            if (lease != null)
            {
                lease.UpdateHeartbeat();
                try
                {
                    await PersistLeaseAsync(lease);
                }
                catch
                {
                    // Ignore heartbeat persistence errors to avoid stopping the timer
                }
            }
        }

        /// <summary>
        /// Checks if a lease is still valid (process alive and heartbeat recent)
        /// </summary>
        private async Task<bool> IsLeaseValidAsync(TransactionLease lease)
        {
            // Skip non-active leases
            if (lease.State != TransactionState.Active)
                return false;

            // Check if heartbeat is recent enough
            if (!lease.IsValid(HeartbeatTimeout))
                return false;

            // Check if the process is still running
            if (!IsProcessRunning(lease.ProcessId))
                return false;

            return true;
        }

        /// <summary>
        /// Checks if a process with the given ID is currently running
        /// </summary>
        private bool IsProcessRunning(int processId)
        {
            try
            {
                var process = Process.GetProcessById(processId);
                return !process.HasExited;
            }
            catch
            {
                return false; // Process not found
            }
        }

        /// <summary>
        /// Determines if a lease should be cleaned up (deleted)
        /// </summary>
        private bool ShouldCleanupLease(TransactionLease lease)
        {
            // Clean up completed/rolled back leases older than 1 hour
            if (lease.State == TransactionState.Completed || lease.State == TransactionState.RolledBack)
            {
                var age = DateTime.UtcNow - (lease.CompletedTime ?? lease.StartTime);
                return age > CleanupAge;
            }

            // Clean up abandoned leases older than 1 hour
            if (lease.State == TransactionState.Abandoned)
            {
                var age = DateTime.UtcNow - lease.Heartbeat;
                return age > CleanupAge;
            }

            return false;
        }

        /// <summary>
        /// Persists a transaction lease to disk
        /// </summary>
        private async Task PersistLeaseAsync(TransactionLease lease)
        {
            var leasePath = GetLeasePath(lease.TransactionId);
            var leaseJson = JsonSerializer.Serialize(lease, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });

            await File.WriteAllTextAsync(leasePath, leaseJson);
        }

        /// <summary>
        /// Gets the file path for a transaction lease
        /// </summary>
        private string GetLeasePath(long transactionId)
        {
            return Path.Combine(_leasePath, $"txn_{transactionId}.json");
        }

        /// <summary>
        /// Releases resources used by the TransactionLeaseManager
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            lock (_leaseLock)
            {
                // Stop all heartbeat timers
                foreach (var timer in _heartbeatTimers.Values)
                {
                    try
                    {
                        timer.Dispose();
                    }
                    catch
                    {
                        // Ignore disposal errors
                    }
                }

                _heartbeatTimers.Clear();
                _activeLeases.Clear();
            }
        }
    }
}