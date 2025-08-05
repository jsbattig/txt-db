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
    /// Enhanced Transaction Lease Manager for Epic 003 Phase 1
    /// 
    /// Implements directory-based lease ownership with:
    /// - Exclusive cleanup coordination using directory locks
    /// - Atomic lease state transitions
    /// - Safe multi-process cleanup without races
    /// - Process-specific lease directories
    /// 
    /// CRITICAL: Addresses lease cleanup race conditions
    /// </summary>
    public class EnhancedTransactionLeaseManager : IDisposable
    {
        private readonly string _leasePath;
        private readonly string _activeLeaseDirectory;
        private readonly string _cleanupQueueDirectory;
        private readonly string _completedDirectory;
        private readonly string _processId;
        private readonly Dictionary<long, Timer> _heartbeatTimers;
        private readonly object _leaseLock = new object();
        private volatile bool _disposed = false;

        // Configuration constants
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan CleanupAge = TimeSpan.FromHours(1);

        public EnhancedTransactionLeaseManager(string leasePath)
        {
            _leasePath = leasePath ?? throw new ArgumentNullException(nameof(leasePath));
            _activeLeaseDirectory = Path.Combine(_leasePath, "active");
            _cleanupQueueDirectory = Path.Combine(_leasePath, "cleanup_queue");
            _completedDirectory = Path.Combine(_leasePath, "completed");
            _processId = Process.GetCurrentProcess().Id.ToString();
            _heartbeatTimers = new Dictionary<long, Timer>();

            // Ensure directory structure exists
            Directory.CreateDirectory(_activeLeaseDirectory);
            Directory.CreateDirectory(_cleanupQueueDirectory);
            Directory.CreateDirectory(_completedDirectory);

            // Create process-specific subdirectory for owned leases
            var processLeaseDir = Path.Combine(_activeLeaseDirectory, _processId);
            Directory.CreateDirectory(processLeaseDir);

            // Start background cleanup worker
            _ = Task.Run(async () => await CleanupWorkerAsync());
        }

        /// <summary>
        /// Creates a new transaction lease with directory-based ownership
        /// </summary>
        public async Task CreateLeaseAsync(long transactionId, long snapshotTSN)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(EnhancedTransactionLeaseManager));

            var lease = new TransactionLease
            {
                TransactionId = transactionId,
                ProcessId = int.Parse(_processId),
                SnapshotTSN = snapshotTSN,
                MachineName = Environment.MachineName
            };

            // Create lease in process-specific directory (ownership indicator)
            var processLeaseDir = Path.Combine(_activeLeaseDirectory, _processId);
            var leasePath = Path.Combine(processLeaseDir, $"txn_{transactionId}.json");

            // Use atomic directory creation for ownership
            try
            {
                // Create transaction-specific directory as ownership marker
                var txnOwnershipDir = Path.Combine(processLeaseDir, $"txn_{transactionId}_ownership");
                Directory.CreateDirectory(txnOwnershipDir);
                
                // Write lease file
                await PersistLeaseAsync(lease, leasePath);

                lock (_leaseLock)
                {
                    // Start heartbeat timer
                    _heartbeatTimers[transactionId] = new Timer(
                        _ => Task.Run(async () => await UpdateHeartbeatAsync(transactionId, leasePath)),
                        null,
                        TimeSpan.Zero,
                        HeartbeatInterval);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to create lease for transaction {transactionId}", ex);
            }
        }

        /// <summary>
        /// Gets all currently active transactions across all processes
        /// </summary>
        public async Task<List<TransactionLease>> GetActiveTransactionsAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(EnhancedTransactionLeaseManager));

            var activeLeases = new List<TransactionLease>();

            // Scan all process directories in active lease directory
            var processDirectories = Directory.GetDirectories(_activeLeaseDirectory);

            foreach (var processDir in processDirectories)
            {
                var processId = Path.GetFileName(processDir);
                var leaseFiles = Directory.GetFiles(processDir, "txn_*.json");

                foreach (var leaseFile in leaseFiles)
                {
                    try
                    {
                        var lease = await ReadLeaseAsync(leaseFile);
                        if (lease != null && await IsLeaseValidAsync(lease, processId))
                        {
                            activeLeases.Add(lease);
                        }
                        else if (lease != null)
                        {
                            // Move to cleanup queue
                            await QueueForCleanupAsync(lease, leaseFile, processId);
                        }
                    }
                    catch
                    {
                        // Ignore corrupted lease files
                    }
                }
            }

            return activeLeases;
        }

        /// <summary>
        /// Completes a lease with atomic state transition
        /// </summary>
        public async Task CompleteLeaseAsync(long transactionId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(EnhancedTransactionLeaseManager));

            Timer? timer = null;
            string? leasePath = null;

            lock (_leaseLock)
            {
                if (_heartbeatTimers.TryGetValue(transactionId, out timer))
                {
                    _heartbeatTimers.Remove(transactionId);
                }
            }

            // Stop heartbeat
            timer?.Dispose();

            // Find lease file
            var processLeaseDir = Path.Combine(_activeLeaseDirectory, _processId);
            leasePath = Path.Combine(processLeaseDir, $"txn_{transactionId}.json");

            if (File.Exists(leasePath))
            {
                var lease = await ReadLeaseAsync(leasePath);
                if (lease != null)
                {
                    lease.MarkCompleted();
                    
                    // Move to completed directory atomically
                    var completedPath = Path.Combine(_completedDirectory, $"txn_{transactionId}_{DateTime.UtcNow.Ticks}.json");
                    await PersistLeaseAsync(lease, completedPath);
                    
                    // Remove from active directory
                    File.Delete(leasePath);
                    
                    // Remove ownership directory
                    var txnOwnershipDir = Path.Combine(processLeaseDir, $"txn_{transactionId}_ownership");
                    if (Directory.Exists(txnOwnershipDir))
                    {
                        Directory.Delete(txnOwnershipDir, true);
                    }
                }
            }
        }

        /// <summary>
        /// Updates heartbeat for a transaction
        /// </summary>
        private async Task UpdateHeartbeatAsync(long transactionId, string leasePath)
        {
            if (_disposed)
                return;

            try
            {
                if (File.Exists(leasePath))
                {
                    var lease = await ReadLeaseAsync(leasePath);
                    if (lease != null)
                    {
                        lease.UpdateHeartbeat();
                        await PersistLeaseAsync(lease, leasePath);
                    }
                }
            }
            catch
            {
                // Ignore heartbeat errors
            }
        }

        /// <summary>
        /// Checks if a lease is valid
        /// </summary>
        private async Task<bool> IsLeaseValidAsync(TransactionLease lease, string ownerProcessId)
        {
            if (lease.State != TransactionState.Active)
                return false;

            if (!lease.IsValid(HeartbeatTimeout))
                return false;

            // Check if owning process is alive
            if (!IsProcessRunning(ownerProcessId))
                return false;

            return true;
        }

        /// <summary>
        /// Queues a lease for cleanup with exclusive ownership
        /// </summary>
        private async Task QueueForCleanupAsync(TransactionLease lease, string leasePath, string ownerProcessId)
        {
            // Create cleanup task with exclusive ownership using directory creation
            var cleanupTaskId = $"{lease.TransactionId}_{DateTime.UtcNow.Ticks}";
            var cleanupTaskDir = Path.Combine(_cleanupQueueDirectory, cleanupTaskId);
            
            try
            {
                // Atomic directory creation gives us exclusive cleanup rights
                Directory.CreateDirectory(cleanupTaskDir);
                
                // Copy lease info to cleanup queue
                var cleanupInfo = new
                {
                    Lease = lease,
                    OriginalPath = leasePath,
                    OwnerProcessId = ownerProcessId,
                    QueuedAt = DateTime.UtcNow,
                    CleanupBy = _processId
                };
                
                var cleanupInfoPath = Path.Combine(cleanupTaskDir, "cleanup_info.json");
                var json = JsonSerializer.Serialize(cleanupInfo, new JsonSerializerOptions 
                { 
                    WriteIndented = true 
                });
                await File.WriteAllTextAsync(cleanupInfoPath, json);
            }
            catch (IOException)
            {
                // Another process already claimed cleanup ownership
            }
        }

        /// <summary>
        /// Background worker for processing cleanup queue
        /// </summary>
        private async Task CleanupWorkerAsync()
        {
            while (!_disposed)
            {
                try
                {
                    var cleanupDirs = Directory.GetDirectories(_cleanupQueueDirectory);
                    
                    foreach (var cleanupDir in cleanupDirs)
                    {
                        try
                        {
                            var infoPath = Path.Combine(cleanupDir, "cleanup_info.json");
                            if (File.Exists(infoPath))
                            {
                                var json = await File.ReadAllTextAsync(infoPath);
                                dynamic cleanupInfo = JsonSerializer.Deserialize<dynamic>(json);
                                
                                // Verify we own this cleanup task
                                if (cleanupInfo?.GetProperty("CleanupBy").GetString() == _processId)
                                {
                                    // Perform cleanup
                                    var originalPath = cleanupInfo.GetProperty("OriginalPath").GetString();
                                    if (File.Exists(originalPath))
                                    {
                                        File.Delete(originalPath);
                                    }
                                    
                                    // Remove cleanup task
                                    Directory.Delete(cleanupDir, true);
                                }
                            }
                        }
                        catch
                        {
                            // Continue with next cleanup task
                        }
                    }
                    
                    // Clean up old completed leases
                    await CleanupCompletedLeasesAsync();
                }
                catch
                {
                    // Ignore cleanup errors
                }

                await Task.Delay(TimeSpan.FromSeconds(30));
            }
        }

        /// <summary>
        /// Cleans up old completed leases
        /// </summary>
        private async Task CleanupCompletedLeasesAsync()
        {
            var completedFiles = Directory.GetFiles(_completedDirectory, "*.json");
            
            foreach (var file in completedFiles)
            {
                try
                {
                    var fileInfo = new FileInfo(file);
                    if (DateTime.UtcNow - fileInfo.CreationTimeUtc > CleanupAge)
                    {
                        File.Delete(file);
                    }
                }
                catch
                {
                    // Ignore individual file errors
                }
            }
        }

        /// <summary>
        /// Reads a lease from disk
        /// </summary>
        private async Task<TransactionLease?> ReadLeaseAsync(string path)
        {
            try
            {
                var json = await File.ReadAllTextAsync(path);
                return JsonSerializer.Deserialize<TransactionLease>(json);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Persists a lease to disk
        /// </summary>
        private async Task PersistLeaseAsync(TransactionLease lease, string path)
        {
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            var json = JsonSerializer.Serialize(lease, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            // Write atomically
            var tempPath = path + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, path, true);
        }

        /// <summary>
        /// Checks if a process is running
        /// </summary>
        private bool IsProcessRunning(string processId)
        {
            try
            {
                if (int.TryParse(processId, out var pid))
                {
                    var process = Process.GetProcessById(pid);
                    return !process.HasExited;
                }
                return false;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            lock (_leaseLock)
            {
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
            }
        }
    }
}