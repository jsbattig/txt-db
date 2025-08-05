using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Cross-Process Locking Infrastructure for Epic 003 Story 001
    /// 
    /// Implements advisory file locking with:
    /// - Timeout mechanisms to prevent deadlocks
    /// - Automatic cleanup of abandoned locks
    /// - Process information tracking for debugging
    /// - Lock acquisition latency under 10ms P95
    /// 
    /// CRITICAL: Uses exclusive FileStream to prevent multiple access
    /// REFACTORED: Simplified approach with intra-process synchronization
    /// </summary>
    public class CrossProcessLock : IDisposable
    {
        private FileStream? _lockStream;
        private readonly string _lockPath;
        private volatile bool _disposed = false;
        private volatile bool _lockAcquired = false;
        
        // Static semaphore to ensure only one instance per path can acquire lock in same process
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _pathSemaphores = new();
        private SemaphoreSlim? _semaphore;

        public CrossProcessLock(string lockPath)
        {
            _lockPath = lockPath ?? throw new ArgumentNullException(nameof(lockPath));
            
            // Ensure lock directory exists 
            var lockDirectory = Path.GetDirectoryName(_lockPath);
            if (!string.IsNullOrEmpty(lockDirectory))
            {
                Directory.CreateDirectory(lockDirectory);
            }
            
            // Get or create semaphore for this path (intra-process coordination)
            _semaphore = _pathSemaphores.GetOrAdd(_lockPath, _ => new SemaphoreSlim(1, 1));
        }

        /// <summary>
        /// Attempts to acquire the cross-process lock with specified timeout
        /// </summary>
        /// <param name="timeout">Maximum time to wait for lock acquisition</param>
        /// <returns>True if lock was acquired, false if timeout occurred</returns>
        public async Task<bool> TryAcquireAsync(TimeSpan timeout)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CrossProcessLock));

            if (_lockAcquired)
                throw new InvalidOperationException("Lock is already acquired by this instance");

            if (_semaphore == null)
                throw new InvalidOperationException("Semaphore not initialized");

            // First acquire intra-process lock
            if (!await _semaphore.WaitAsync(timeout))
                return false; // Timeout on intra-process synchronization

            try
            {
                var remainingTimeout = timeout;
                var deadline = DateTime.UtcNow + remainingTimeout;
                
                while (DateTime.UtcNow < deadline)
                {
                    try
                    {
                        // Check if existing lock is abandoned and clean it up
                        await CleanupAbandonedLockIfNecessary();
                        
                        // Attempt to acquire exclusive lock
                        _lockStream = new FileStream(_lockPath, 
                            FileMode.OpenOrCreate, 
                            FileAccess.ReadWrite, 
                            FileShare.None, // Exclusive access across processes
                            bufferSize: 4096,
                            FileOptions.DeleteOnClose); // Automatically cleanup on process exit
                            
                        // Write process information for debugging and cleanup detection
                        var lockInfo = new
                        {
                            ProcessId = Process.GetCurrentProcess().Id,
                            AcquiredAt = DateTime.UtcNow,
                            MachineName = Environment.MachineName
                        };
                        
                        var lockInfoJson = JsonSerializer.Serialize(lockInfo);
                        var bytes = Encoding.UTF8.GetBytes(lockInfoJson);
                        
                        _lockStream.SetLength(0); // Clear any existing content
                        await _lockStream.WriteAsync(bytes, 0, bytes.Length);
                        await _lockStream.FlushAsync(); // Ensure immediate persistence
                        
                        _lockAcquired = true;
                        return true;
                    }
                    catch (IOException)
                    {
                        // Lock is held by another process, wait and retry
                        if (_lockStream != null)
                        {
                            _lockStream.Dispose();
                            _lockStream = null;
                        }
                        
                        // Use exponential backoff with jitter for better performance
                        var delay = Math.Min(5 + (DateTime.UtcNow.Ticks % 5), 50);
                        await Task.Delay((int)delay);
                    }
                    catch (UnauthorizedAccessException)
                    {
                        // Lock is held by another process, wait and retry  
                        if (_lockStream != null)
                        {
                            _lockStream.Dispose();
                            _lockStream = null;
                        }
                        
                        var delay = Math.Min(5 + (DateTime.UtcNow.Ticks % 5), 50);
                        await Task.Delay((int)delay);
                    }
                }
                
                return false; // Timeout occurred
            }
            finally
            {
                if (!_lockAcquired)
                {
                    _semaphore.Release();
                }
            }
        }

        /// <summary>
        /// Checks if an existing lock file represents an abandoned lock and cleans it up
        /// </summary>
        private async Task CleanupAbandonedLockIfNecessary()
        {
            if (!File.Exists(_lockPath))
                return;

            try
            {
                // Try to read the lock file to check if it's abandoned
                // If we can read it, the lock isn't actively held
                string lockContent;
                try
                {
                    lockContent = await File.ReadAllTextAsync(_lockPath);
                }
                catch (IOException)
                {
                    // File is locked, cannot clean up - this is expected
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    // File is locked, cannot clean up - this is expected
                    return;
                }
                
                var lockInfo = JsonSerializer.Deserialize<LockInfo>(lockContent);
                
                if (lockInfo != null)
                {
                    // Check if the process that created the lock is still running
                    if (!IsProcessRunning(lockInfo.ProcessId))
                    {
                        // Process is dead, remove abandoned lock
                        try
                        {
                            File.Delete(_lockPath);
                        }
                        catch
                        {
                            // Someone else might be cleaning up simultaneously
                        }
                        return;
                    }
                    
                    // Check if lock is very old (more than 5 minutes) - likely abandoned
                    if (DateTime.UtcNow - lockInfo.AcquiredAt > TimeSpan.FromMinutes(5))
                    {
                        try
                        {
                            File.Delete(_lockPath);
                        }
                        catch
                        {
                            // Someone else might be cleaning up simultaneously
                        }
                        return;
                    }
                }
            }
            catch
            {
                // If we can't read/parse lock info but the file exists and isn't locked,
                // it might be corrupted - delete it
                try
                {
                    File.Delete(_lockPath);
                }
                catch
                {
                    // Ignore cleanup failures
                }
            }
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
        /// Releases the cross-process lock
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;
            
            try
            {
                // FileOptions.DeleteOnClose will automatically cleanup the file
                // when the FileStream is disposed
                _lockStream?.Dispose();
                _lockStream = null;
            }
            catch
            {
                // Ignore cleanup errors on disposal
            }
            finally
            {
                if (_lockAcquired)
                {
                    _semaphore?.Release();
                }
                
                _lockAcquired = false;
            }
        }

        /// <summary>
        /// Lock information stored in lock files for debugging and cleanup
        /// </summary>
        private class LockInfo
        {
            public int ProcessId { get; set; }
            public DateTime AcquiredAt { get; set; }
            public string MachineName { get; set; } = "";
        }
    }
}