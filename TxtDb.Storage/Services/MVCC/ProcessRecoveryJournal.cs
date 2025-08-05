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
    /// Process Recovery Journal for Epic 003 Phase 1
    /// 
    /// Implements process-specific journaling with:
    /// - Checkpoint-based recovery
    /// - Dead process detection
    /// - State reconstruction capabilities
    /// - Resource cleanup coordination
    /// 
    /// CRITICAL: Provides error recovery mechanisms for multi-process scenarios
    /// </summary>
    public class ProcessRecoveryJournal : IDisposable
    {
        private readonly string _journalPath;
        private readonly string _journalsDirectory;
        private readonly string _checkpointsDirectory;
        private readonly string _deadProcessesDirectory;
        private readonly string _processId;
        private readonly Timer _checkpointTimer;
        private readonly Timer _deadProcessTimer;
        private volatile bool _disposed = false;

        // Configuration
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan DeadProcessCheckInterval = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Journal entry for process operations
        /// </summary>
        public class JournalEntry
        {
            public string EntryId { get; set; } = Guid.NewGuid().ToString();
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
            public JournalEntryType Type { get; set; }
            public string OperationId { get; set; } = "";
            public string Data { get; set; } = "";
            public bool Completed { get; set; } = false;
            public string ErrorMessage { get; set; } = "";
        }

        /// <summary>
        /// Process checkpoint for recovery
        /// </summary>
        public class ProcessCheckpoint
        {
            public string CheckpointId { get; set; } = Guid.NewGuid().ToString();
            public string ProcessId { get; set; } = "";
            public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
            public Dictionary<string, object> State { get; set; } = new();
            public List<string> ActiveTransactions { get; set; } = new();
            public List<string> HeldLocks { get; set; } = new();
            public List<string> OpenFiles { get; set; } = new();
            public long LastProcessedJournalEntry { get; set; }
        }

        /// <summary>
        /// Dead process information
        /// </summary>
        public class DeadProcessInfo
        {
            public string ProcessId { get; set; } = "";
            public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
            public DateTime LastActivity { get; set; }
            public ProcessCheckpoint? LastCheckpoint { get; set; }
            public List<string> OrphanedResources { get; set; } = new();
            public bool CleanupCompleted { get; set; } = false;
        }

        public enum JournalEntryType
        {
            ProcessStart,
            ProcessHeartbeat,
            TransactionBegin,
            TransactionCommit,
            TransactionRollback,
            LockAcquired,
            LockReleased,
            FileOpened,
            FileClosed,
            StateUpdate,
            Error,
            ProcessShutdown
        }

        public ProcessRecoveryJournal(string coordinationPath)
        {
            if (string.IsNullOrEmpty(coordinationPath))
                throw new ArgumentNullException(nameof(coordinationPath));

            _journalPath = Path.Combine(coordinationPath, "recovery");
            _journalsDirectory = Path.Combine(_journalPath, "journals");
            _checkpointsDirectory = Path.Combine(_journalPath, "checkpoints");
            _deadProcessesDirectory = Path.Combine(_journalPath, "dead_processes");
            _processId = Process.GetCurrentProcess().Id.ToString();

            // Ensure directories exist
            Directory.CreateDirectory(_journalsDirectory);
            Directory.CreateDirectory(_checkpointsDirectory);
            Directory.CreateDirectory(_deadProcessesDirectory);

            // Create process-specific journal directory
            var processJournalDir = Path.Combine(_journalsDirectory, _processId);
            Directory.CreateDirectory(processJournalDir);

            // Log process start
            _ = Task.Run(async () => await LogProcessStartAsync());

            // Start checkpoint timer
            _checkpointTimer = new Timer(
                _ => Task.Run(async () => await CreateCheckpointAsync()),
                null,
                CheckpointInterval,
                CheckpointInterval
            );

            // Start dead process detection timer
            _deadProcessTimer = new Timer(
                _ => Task.Run(async () => await DetectAndCleanupDeadProcessesAsync()),
                null,
                DeadProcessCheckInterval,
                DeadProcessCheckInterval
            );
        }

        /// <summary>
        /// Logs a journal entry for the current process
        /// </summary>
        public async Task<JournalEntry> LogOperationAsync(JournalEntryType type, string operationId, object data)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ProcessRecoveryJournal));

            var entry = new JournalEntry
            {
                Type = type,
                OperationId = operationId,
                Data = JsonSerializer.Serialize(data)
            };

            await PersistJournalEntryAsync(entry);
            return entry;
        }

        /// <summary>
        /// Marks a journal entry as completed
        /// </summary>
        public async Task CompleteOperationAsync(string entryId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ProcessRecoveryJournal));

            var entry = await LoadJournalEntryAsync(entryId);
            if (entry != null)
            {
                entry.Completed = true;
                await PersistJournalEntryAsync(entry);
            }
        }

        /// <summary>
        /// Logs an error for a journal entry
        /// </summary>
        public async Task LogOperationErrorAsync(string entryId, string errorMessage)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ProcessRecoveryJournal));

            var entry = await LoadJournalEntryAsync(entryId);
            if (entry != null)
            {
                entry.ErrorMessage = errorMessage;
                await PersistJournalEntryAsync(entry);
            }

            // Also log as separate error entry
            await LogOperationAsync(JournalEntryType.Error, entryId, new { Error = errorMessage });
        }

        /// <summary>
        /// Creates a checkpoint of current process state
        /// </summary>
        public async Task<ProcessCheckpoint> CreateCheckpointAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ProcessRecoveryJournal));

            var checkpoint = new ProcessCheckpoint
            {
                ProcessId = _processId,
                LastProcessedJournalEntry = await GetLastProcessedJournalEntryIdAsync()
            };

            // Gather current state information
            // This would integrate with other components to get:
            // - Active transactions from TransactionLeaseManager
            // - Held locks from CrossProcessLock
            // - Open files from StorageSubsystem
            // For now, using placeholder data
            checkpoint.State["ProcessStartTime"] = Process.GetCurrentProcess().StartTime;
            checkpoint.State["WorkingSet"] = Process.GetCurrentProcess().WorkingSet64;
            checkpoint.State["ThreadCount"] = Process.GetCurrentProcess().Threads.Count;

            await PersistCheckpointAsync(checkpoint);
            
            // Log checkpoint creation
            await LogOperationAsync(JournalEntryType.StateUpdate, checkpoint.CheckpointId, 
                new { CheckpointCreated = checkpoint.CheckpointId });

            return checkpoint;
        }

        /// <summary>
        /// Recovers process state from checkpoint and journal
        /// </summary>
        public async Task<ProcessRecoveryInfo> RecoverProcessStateAsync(string processId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ProcessRecoveryJournal));

            var recoveryInfo = new ProcessRecoveryInfo
            {
                ProcessId = processId,
                RecoveryStarted = DateTime.UtcNow
            };

            // Load latest checkpoint
            var checkpoint = await LoadLatestCheckpointAsync(processId);
            if (checkpoint != null)
            {
                recoveryInfo.LastCheckpoint = checkpoint;
                recoveryInfo.CheckpointFound = true;
            }

            // Load ALL journal entries regardless of checkpoint - we need to check completion status
            // The checkpoint is for optimization, not for filtering out entries
            var journalEntries = await LoadJournalEntriesAfterAsync(processId, 0);
            
            recoveryInfo.JournalEntries = journalEntries;

            // Identify incomplete operations
            recoveryInfo.IncompleteOperations = journalEntries
                .Where(e => !e.Completed && string.IsNullOrEmpty(e.ErrorMessage))
                .ToList();

            // Identify resources needing cleanup
            var heldLocks = journalEntries
                .Where(e => e.Type == JournalEntryType.LockAcquired && !e.Completed)
                .Select(e => e.OperationId)
                .ToList();
            
            var openTransactions = journalEntries
                .Where(e => e.Type == JournalEntryType.TransactionBegin && !e.Completed)
                .Select(e => e.OperationId)
                .ToList();

            recoveryInfo.ResourcesNeedingCleanup["HeldLocks"] = heldLocks;
            recoveryInfo.ResourcesNeedingCleanup["OpenTransactions"] = openTransactions;

            recoveryInfo.RecoveryCompleted = DateTime.UtcNow;
            return recoveryInfo;
        }

        /// <summary>
        /// Detects and cleans up dead processes
        /// </summary>
        private async Task DetectAndCleanupDeadProcessesAsync()
        {
            if (_disposed)
                return;

            try
            {
                var processDirectories = Directory.GetDirectories(_journalsDirectory);
                
                foreach (var processDir in processDirectories)
                {
                    var processId = Path.GetFileName(processDir);
                    if (processId == _processId)
                        continue; // Skip self

                    // Check if process is alive
                    if (!IsProcessRunning(processId))
                    {
                        // Check last activity
                        var lastActivity = await GetLastActivityTimeAsync(processId);
                        if (DateTime.UtcNow - lastActivity > ProcessTimeout)
                        {
                            await HandleDeadProcessAsync(processId, lastActivity);
                        }
                    }
                }

                // Clean up old dead process records
                await CleanupOldDeadProcessRecordsAsync();
            }
            catch
            {
                // Ignore errors in background task
            }
        }

        /// <summary>
        /// Handles cleanup for a dead process
        /// </summary>
        private async Task HandleDeadProcessAsync(string deadProcessId, DateTime lastActivity)
        {
            // Check if already being handled
            var deadProcessFile = Path.Combine(_deadProcessesDirectory, $"{deadProcessId}.json");
            if (File.Exists(deadProcessFile))
                return;

            var deadProcessInfo = new DeadProcessInfo
            {
                ProcessId = deadProcessId,
                LastActivity = lastActivity
            };

            // Recover process state
            var recoveryInfo = await RecoverProcessStateAsync(deadProcessId);
            deadProcessInfo.LastCheckpoint = recoveryInfo.LastCheckpoint;
            
            // Identify orphaned resources
            if (recoveryInfo.ResourcesNeedingCleanup.TryGetValue("HeldLocks", out var locks))
            {
                deadProcessInfo.OrphanedResources.AddRange(locks.Select(l => $"Lock:{l}"));
            }
            
            if (recoveryInfo.ResourcesNeedingCleanup.TryGetValue("OpenTransactions", out var transactions))
            {
                deadProcessInfo.OrphanedResources.AddRange(transactions.Select(t => $"Transaction:{t}"));
            }

            // Persist dead process info for coordinated cleanup
            await PersistDeadProcessInfoAsync(deadProcessInfo);

            // Log detection
            await LogOperationAsync(JournalEntryType.Error, deadProcessId, 
                new { DeadProcessDetected = deadProcessId, OrphanedResources = deadProcessInfo.OrphanedResources });
        }

        /// <summary>
        /// Gets the last activity time for a process
        /// </summary>
        private async Task<DateTime> GetLastActivityTimeAsync(string processId)
        {
            var processJournalDir = Path.Combine(_journalsDirectory, processId);
            if (!Directory.Exists(processJournalDir))
                return DateTime.MinValue;

            var files = Directory.GetFiles(processJournalDir, "*.json")
                .OrderByDescending(f => new FileInfo(f).LastWriteTimeUtc)
                .FirstOrDefault();

            if (files != null)
            {
                return new FileInfo(files).LastWriteTimeUtc;
            }

            return DateTime.MinValue;
        }

        /// <summary>
        /// Cleans up old dead process records
        /// </summary>
        private async Task CleanupOldDeadProcessRecordsAsync()
        {
            var deadProcessFiles = Directory.GetFiles(_deadProcessesDirectory, "*.json");
            
            foreach (var file in deadProcessFiles)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var info = JsonSerializer.Deserialize<DeadProcessInfo>(json);
                    
                    if (info != null && info.CleanupCompleted && 
                        DateTime.UtcNow - info.DetectedAt > TimeSpan.FromHours(24))
                    {
                        File.Delete(file);
                        
                        // Also clean up journal directory if empty
                        var processJournalDir = Path.Combine(_journalsDirectory, info.ProcessId);
                        if (Directory.Exists(processJournalDir) && 
                            !Directory.EnumerateFileSystemEntries(processJournalDir).Any())
                        {
                            Directory.Delete(processJournalDir);
                        }
                    }
                }
                catch
                {
                    // Continue with next file
                }
            }
        }

        /// <summary>
        /// Logs process start
        /// </summary>
        private async Task LogProcessStartAsync()
        {
            await LogOperationAsync(JournalEntryType.ProcessStart, _processId, 
                new { 
                    ProcessName = Process.GetCurrentProcess().ProcessName,
                    StartTime = Process.GetCurrentProcess().StartTime,
                    MachineName = Environment.MachineName
                });
        }

        /// <summary>
        /// Persists a journal entry
        /// </summary>
        private async Task PersistJournalEntryAsync(JournalEntry entry)
        {
            var processJournalDir = Path.Combine(_journalsDirectory, _processId);
            var entryPath = Path.Combine(processJournalDir, $"{entry.Timestamp.Ticks}_{entry.EntryId}.json");
            
            var json = JsonSerializer.Serialize(entry, new JsonSerializerOptions { WriteIndented = true });
            
            // Write atomically
            var tempPath = entryPath + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, entryPath, true);
        }

        /// <summary>
        /// Loads a journal entry
        /// </summary>
        private async Task<JournalEntry?> LoadJournalEntryAsync(string entryId)
        {
            var processJournalDir = Path.Combine(_journalsDirectory, _processId);
            var files = Directory.GetFiles(processJournalDir, $"*_{entryId}.json");
            
            if (files.Length > 0)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(files[0]);
                    return JsonSerializer.Deserialize<JournalEntry>(json);
                }
                catch
                {
                    return null;
                }
            }
            
            return null;
        }

        /// <summary>
        /// Loads journal entries after a certain point
        /// </summary>
        private async Task<List<JournalEntry>> LoadJournalEntriesAfterAsync(string processId, long afterTimestamp)
        {
            var entries = new List<JournalEntry>();
            var processJournalDir = Path.Combine(_journalsDirectory, processId);
            
            if (!Directory.Exists(processJournalDir))
                return entries;

            var files = Directory.GetFiles(processJournalDir, "*.json")
                .Where(f => GetTimestampFromFileName(f) > afterTimestamp)
                .OrderBy(f => f);

            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var entry = JsonSerializer.Deserialize<JournalEntry>(json);
                    if (entry != null)
                        entries.Add(entry);
                }
                catch
                {
                    // Skip corrupted entries
                }
            }

            return entries;
        }

        /// <summary>
        /// Gets the last processed journal entry ID
        /// </summary>
        private async Task<long> GetLastProcessedJournalEntryIdAsync()
        {
            var processJournalDir = Path.Combine(_journalsDirectory, _processId);
            var files = Directory.GetFiles(processJournalDir, "*.json");
            
            if (files.Length == 0)
                return 0;

            return files.Select(f => GetTimestampFromFileName(f)).Max();
        }

        /// <summary>
        /// Extracts timestamp from journal file name
        /// </summary>
        private long GetTimestampFromFileName(string fileName)
        {
            var name = Path.GetFileNameWithoutExtension(fileName);
            var parts = name.Split('_');
            if (parts.Length > 0 && long.TryParse(parts[0], out var timestamp))
                return timestamp;
            return 0;
        }

        /// <summary>
        /// Persists a checkpoint
        /// </summary>
        private async Task PersistCheckpointAsync(ProcessCheckpoint checkpoint)
        {
            var checkpointPath = Path.Combine(_checkpointsDirectory, _processId, 
                $"{checkpoint.CreatedAt.Ticks}_{checkpoint.CheckpointId}.json");
            
            var dir = Path.GetDirectoryName(checkpointPath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);
            
            var json = JsonSerializer.Serialize(checkpoint, new JsonSerializerOptions { WriteIndented = true });
            
            // Write atomically
            var tempPath = checkpointPath + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, checkpointPath, true);
        }

        /// <summary>
        /// Loads the latest checkpoint for a process
        /// </summary>
        private async Task<ProcessCheckpoint?> LoadLatestCheckpointAsync(string processId)
        {
            var checkpointDir = Path.Combine(_checkpointsDirectory, processId);
            if (!Directory.Exists(checkpointDir))
                return null;

            var latestFile = Directory.GetFiles(checkpointDir, "*.json")
                .OrderByDescending(f => f)
                .FirstOrDefault();

            if (latestFile != null)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(latestFile);
                    return JsonSerializer.Deserialize<ProcessCheckpoint>(json);
                }
                catch
                {
                    return null;
                }
            }

            return null;
        }

        /// <summary>
        /// Persists dead process information
        /// </summary>
        private async Task PersistDeadProcessInfoAsync(DeadProcessInfo info)
        {
            var deadProcessPath = Path.Combine(_deadProcessesDirectory, $"{info.ProcessId}.json");
            var json = JsonSerializer.Serialize(info, new JsonSerializerOptions { WriteIndented = true });
            
            // Write atomically
            var tempPath = deadProcessPath + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, deadProcessPath, true);
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

        /// <summary>
        /// Process recovery information
        /// </summary>
        public class ProcessRecoveryInfo
        {
            public string ProcessId { get; set; } = "";
            public DateTime RecoveryStarted { get; set; }
            public DateTime RecoveryCompleted { get; set; }
            public bool CheckpointFound { get; set; }
            public ProcessCheckpoint? LastCheckpoint { get; set; }
            public List<JournalEntry> JournalEntries { get; set; } = new();
            public List<JournalEntry> IncompleteOperations { get; set; } = new();
            public Dictionary<string, List<string>> ResourcesNeedingCleanup { get; set; } = new();
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            // Stop timers first to prevent background operations
            _checkpointTimer?.Dispose();
            _deadProcessTimer?.Dispose();

            // Mark as disposed before attempting final log
            _disposed = true;

            // Don't try to log shutdown - it causes issues with disposal ordering
        }
    }
}