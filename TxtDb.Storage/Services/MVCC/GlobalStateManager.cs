using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Global State Management for Epic 003 Story 002
    /// 
    /// Implements atomic global state updates across processes using:
    /// - CrossProcessLock for exclusive access during updates
    /// - Temp-file + rename pattern for atomic writes
    /// - Global TSN allocation coordination
    /// - Transaction lifecycle management
    /// - Crash recovery with consistent state restoration
    /// 
    /// CRITICAL: Uses temp-file + rename for atomic operations
    /// </summary>
    public class GlobalStateManager : IDisposable
    {
        private readonly string _statePath;
        private readonly string _lockPath;
        private GlobalState _currentState;
        private volatile bool _disposed = false;

        public GlobalStateManager(string statePath)
        {
            _statePath = statePath ?? throw new ArgumentNullException(nameof(statePath));
            _lockPath = _statePath + ".lock";
            _currentState = new GlobalState();
            
            // Ensure state directory exists
            var stateDirectory = Path.GetDirectoryName(_statePath);
            if (!string.IsNullOrEmpty(stateDirectory))
            {
                Directory.CreateDirectory(stateDirectory);
            }
        }

        /// <summary>
        /// Initializes the GlobalStateManager and loads/creates initial state
        /// </summary>
        public async Task InitializeAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(GlobalStateManager));

            if (File.Exists(_statePath))
            {
                // Load existing state
                var stateJson = await File.ReadAllTextAsync(_statePath);
                _currentState = JsonSerializer.Deserialize<GlobalState>(stateJson) ?? new GlobalState();
            }
            else
            {
                // Create initial state file
                _currentState = new GlobalState();
                await PersistStateAsync(_currentState);
            }
        }

        /// <summary>
        /// Gets a copy of the current global state
        /// </summary>
        public async Task<GlobalState> GetCurrentStateAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(GlobalStateManager));

            // Return a clone to prevent external modifications
            return await Task.FromResult(_currentState.Clone());
        }

        /// <summary>
        /// Atomically updates the global state using the provided updater function
        /// </summary>
        public async Task UpdateStateAsync(Func<GlobalState, GlobalState> updater)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(GlobalStateManager));

            // Create a new lock for each update operation to avoid reuse issues
            using (var operationLock = new CrossProcessLock(_lockPath))
            {
                // Acquire exclusive lock for atomic update
                if (!await operationLock.TryAcquireAsync(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Failed to acquire global state lock within 10 seconds");

                // Read current state from file (might have been updated by another process)
                if (File.Exists(_statePath))
                {
                    var stateJson = await File.ReadAllTextAsync(_statePath);
                    _currentState = JsonSerializer.Deserialize<GlobalState>(stateJson) ?? new GlobalState();
                }

                // Apply the update
                var newState = updater(_currentState);
                newState.LastUpdated = DateTime.UtcNow;

                // Persist atomically using temp-file + rename pattern
                await PersistStateAsync(newState);
                
                // Update in-memory state
                _currentState = newState;
            }
        }

        /// <summary>
        /// Allocates the next global Transaction Sequence Number atomically
        /// </summary>
        public async Task<long> AllocateNextTSNAsync()
        {
            long allocatedTSN = 0;
            
            await UpdateStateAsync(state =>
            {
                state.CurrentTSN++;
                state.NextTransactionId = Math.Max(state.NextTransactionId, state.CurrentTSN + 1);
                allocatedTSN = state.CurrentTSN;
                return state;
            });
            
            return allocatedTSN;
        }

        /// <summary>
        /// Begins a new transaction and returns its ID
        /// </summary>
        public async Task<long> BeginTransactionAsync()
        {
            long transactionId = 0;
            
            await UpdateStateAsync(state =>
            {
                transactionId = state.NextTransactionId++;
                state.ActiveTransactions.Add(transactionId);
                
                // Update CurrentTSN to ensure global ordering
                state.CurrentTSN = Math.Max(state.CurrentTSN, transactionId);
                
                return state;
            });
            
            return transactionId;
        }

        /// <summary>
        /// Marks a transaction as completed and removes it from active transactions
        /// </summary>
        public async Task CompleteTransactionAsync(long transactionId)
        {
            await UpdateStateAsync(state =>
            {
                state.ActiveTransactions.Remove(transactionId);
                return state;
            });
        }

        /// <summary>
        /// Persists the global state atomically using temp-file + rename pattern
        /// </summary>
        private async Task PersistStateAsync(GlobalState state)
        {
            var tempPath = _statePath + ".tmp";
            
            try
            {
                // Write to temporary file first
                var stateJson = JsonSerializer.Serialize(state, new JsonSerializerOptions 
                { 
                    WriteIndented = true 
                });
                
                await File.WriteAllTextAsync(tempPath, stateJson);
                
                // Ensure the write is flushed to disk
                using (var fs = new FileStream(tempPath, FileMode.Open, FileAccess.Read))
                {
                    fs.Flush(flushToDisk: true);
                }
                
                // Atomic rename - this is the commit point
                File.Move(tempPath, _statePath, overwrite: true);
            }
            finally
            {
                // Clean up temp file if it still exists
                try
                {
                    if (File.Exists(tempPath))
                        File.Delete(tempPath);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        /// <summary>
        /// Releases resources used by the GlobalStateManager
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;
            
            // No resources to dispose since we create locks per operation
        }
    }
}