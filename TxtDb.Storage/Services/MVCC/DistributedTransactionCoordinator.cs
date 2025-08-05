using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Distributed Transaction Coordinator for Epic 003 Phase 1
    /// 
    /// Implements file-based distributed transaction coordination with:
    /// - Write-ahead logging for durability
    /// - Participant coordination through file system
    /// - Recovery mechanisms for partial failures
    /// - Transaction state machine management
    /// 
    /// CRITICAL: Provides proper distributed transaction coordination
    /// </summary>
    public class DistributedTransactionCoordinator : IDisposable
    {
        private readonly string _dtcPath;
        private readonly string _transactionsDirectory;
        private readonly string _walDirectory;
        private readonly string _checkpointsDirectory;
        private readonly string _processId;
        private readonly Timer _recoveryTimer;
        private volatile bool _disposed = false;

        /// <summary>
        /// Distributed transaction state
        /// </summary>
        public class DistributedTransaction
        {
            public string TransactionId { get; set; } = "";
            public string CoordinatorProcessId { get; set; } = "";
            public DateTime StartTime { get; set; } = DateTime.UtcNow;
            public TransactionPhase Phase { get; set; } = TransactionPhase.Initializing;
            public List<Participant> Participants { get; set; } = new();
            public Dictionary<string, string> Metadata { get; set; } = new();
            public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
            public string FailureReason { get; set; } = "";
        }

        /// <summary>
        /// Transaction participant information
        /// </summary>
        public class Participant
        {
            public string ParticipantId { get; set; } = "";
            public string ProcessId { get; set; } = "";
            public ParticipantState State { get; set; } = ParticipantState.Pending;
            public DateTime LastUpdate { get; set; } = DateTime.UtcNow;
            public string VoteReason { get; set; } = "";
        }

        /// <summary>
        /// Write-ahead log entry
        /// </summary>
        public class WALEntry
        {
            public string EntryId { get; set; } = Guid.NewGuid().ToString();
            public string TransactionId { get; set; } = "";
            public WALEntryType Type { get; set; }
            public string Data { get; set; } = "";
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
            public bool Checkpointed { get; set; } = false;
        }

        public enum TransactionPhase
        {
            Initializing,
            Preparing,
            Prepared,
            Committing,
            Committed,
            Aborting,
            Aborted
        }

        public enum ParticipantState
        {
            Pending,
            Prepared,
            Committed,
            Aborted,
            Failed
        }

        public enum WALEntryType
        {
            TransactionStart,
            ParticipantAdd,
            PrepareRequest,
            PrepareResponse,
            CommitDecision,
            AbortDecision,
            TransactionComplete
        }

        public DistributedTransactionCoordinator(string coordinationPath)
        {
            if (string.IsNullOrEmpty(coordinationPath))
                throw new ArgumentNullException(nameof(coordinationPath));

            _dtcPath = Path.Combine(coordinationPath, "dtc");
            _transactionsDirectory = Path.Combine(_dtcPath, "transactions");
            _walDirectory = Path.Combine(_dtcPath, "wal");
            _checkpointsDirectory = Path.Combine(_dtcPath, "checkpoints");
            _processId = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            // Ensure directories exist
            Directory.CreateDirectory(_transactionsDirectory);
            Directory.CreateDirectory(_walDirectory);
            Directory.CreateDirectory(_checkpointsDirectory);

            // Start recovery timer
            _recoveryTimer = new Timer(
                _ => Task.Run(async () => await RecoverIncompleteTransactionsAsync()),
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1)
            );
        }

        /// <summary>
        /// Begins a new distributed transaction
        /// </summary>
        public async Task<DistributedTransaction> BeginTransactionAsync(Dictionary<string, string> metadata)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = new DistributedTransaction
            {
                TransactionId = $"dtx_{_processId}_{DateTime.UtcNow.Ticks}",
                CoordinatorProcessId = _processId,
                Metadata = metadata
            };

            // Write to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.TransactionStart,
                Data = JsonSerializer.Serialize(transaction)
            });

            // Persist transaction state
            await PersistTransactionAsync(transaction);

            return transaction;
        }

        /// <summary>
        /// Adds a participant to the distributed transaction
        /// </summary>
        public async Task AddParticipantAsync(string transactionId, string participantId, string processId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = await LoadTransactionAsync(transactionId);
            if (transaction == null)
                throw new InvalidOperationException($"Transaction {transactionId} not found");

            if (transaction.Phase != TransactionPhase.Initializing)
                throw new InvalidOperationException($"Cannot add participant to transaction in phase {transaction.Phase}");

            var participant = new Participant
            {
                ParticipantId = participantId,
                ProcessId = processId
            };

            transaction.Participants.Add(participant);
            transaction.LastUpdated = DateTime.UtcNow;

            // Write to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.ParticipantAdd,
                Data = JsonSerializer.Serialize(participant)
            });

            await PersistTransactionAsync(transaction);
        }

        /// <summary>
        /// Prepares the distributed transaction (Phase 1 of 2PC)
        /// </summary>
        public async Task<bool> PrepareTransactionAsync(string transactionId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = await LoadTransactionAsync(transactionId);
            if (transaction == null)
                throw new InvalidOperationException($"Transaction {transactionId} not found");

            if (transaction.Phase != TransactionPhase.Initializing)
                throw new InvalidOperationException($"Cannot prepare transaction in phase {transaction.Phase}");

            transaction.Phase = TransactionPhase.Preparing;
            transaction.LastUpdated = DateTime.UtcNow;
            await PersistTransactionAsync(transaction);

            // Write prepare request to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.PrepareRequest,
                Data = "Prepare phase started"
            });

            // Request prepare from all participants
            foreach (var participant in transaction.Participants)
            {
                await RequestPrepareFromParticipantAsync(transaction, participant);
            }

            // Wait for all participants to prepare
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                transaction = await LoadTransactionAsync(transactionId);
                if (transaction == null)
                    return false;

                var allPrepared = transaction.Participants.All(p => 
                    p.State == ParticipantState.Prepared || 
                    p.State == ParticipantState.Failed);

                if (allPrepared)
                {
                    var anyFailed = transaction.Participants.Any(p => p.State == ParticipantState.Failed);
                    if (!anyFailed)
                    {
                        transaction.Phase = TransactionPhase.Prepared;
                        await PersistTransactionAsync(transaction);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }

                await Task.Delay(100);
            }

            // Timeout - abort
            return false;
        }

        /// <summary>
        /// Commits the distributed transaction (Phase 2 of 2PC)
        /// </summary>
        public async Task CommitTransactionAsync(string transactionId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = await LoadTransactionAsync(transactionId);
            if (transaction == null)
                throw new InvalidOperationException($"Transaction {transactionId} not found");

            if (transaction.Phase != TransactionPhase.Prepared)
                throw new InvalidOperationException($"Cannot commit transaction in phase {transaction.Phase}");

            transaction.Phase = TransactionPhase.Committing;
            transaction.LastUpdated = DateTime.UtcNow;
            await PersistTransactionAsync(transaction);

            // Write commit decision to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.CommitDecision,
                Data = "Commit decision made"
            });

            // Send commit to all participants
            foreach (var participant in transaction.Participants)
            {
                await SendCommitToParticipantAsync(transaction, participant);
            }

            // Mark as committed
            transaction.Phase = TransactionPhase.Committed;
            transaction.LastUpdated = DateTime.UtcNow;
            await PersistTransactionAsync(transaction);

            // Write completion to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.TransactionComplete,
                Data = "Transaction committed successfully"
            });
        }

        /// <summary>
        /// Aborts the distributed transaction
        /// </summary>
        public async Task AbortTransactionAsync(string transactionId, string reason)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = await LoadTransactionAsync(transactionId);
            if (transaction == null)
                throw new InvalidOperationException($"Transaction {transactionId} not found");

            if (transaction.Phase == TransactionPhase.Committed || 
                transaction.Phase == TransactionPhase.Aborted)
                return; // Already in final state

            transaction.Phase = TransactionPhase.Aborting;
            transaction.FailureReason = reason;
            transaction.LastUpdated = DateTime.UtcNow;
            await PersistTransactionAsync(transaction);

            // Write abort decision to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.AbortDecision,
                Data = reason
            });

            // Send abort to all participants
            foreach (var participant in transaction.Participants)
            {
                await SendAbortToParticipantAsync(transaction, participant);
            }

            // Mark as aborted
            transaction.Phase = TransactionPhase.Aborted;
            transaction.LastUpdated = DateTime.UtcNow;
            await PersistTransactionAsync(transaction);
        }

        /// <summary>
        /// Participant responds to prepare request
        /// </summary>
        public async Task ParticipantVoteAsync(string transactionId, string participantId, bool prepared, string reason = "")
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DistributedTransactionCoordinator));

            var transaction = await LoadTransactionAsync(transactionId);
            if (transaction == null)
                throw new InvalidOperationException($"Transaction {transactionId} not found");

            var participant = transaction.Participants.FirstOrDefault(p => p.ParticipantId == participantId);
            if (participant == null)
                throw new InvalidOperationException($"Participant {participantId} not found in transaction");

            participant.State = prepared ? ParticipantState.Prepared : ParticipantState.Failed;
            participant.VoteReason = reason;
            participant.LastUpdate = DateTime.UtcNow;

            // Write vote to WAL
            await WriteWALAsync(new WALEntry
            {
                TransactionId = transaction.TransactionId,
                Type = WALEntryType.PrepareResponse,
                Data = JsonSerializer.Serialize(new { ParticipantId = participantId, Vote = prepared, Reason = reason })
            });

            await PersistTransactionAsync(transaction);
        }

        /// <summary>
        /// Requests prepare from a participant
        /// </summary>
        private async Task RequestPrepareFromParticipantAsync(DistributedTransaction transaction, Participant participant)
        {
            // Create prepare request file in participant's inbox
            var participantInbox = Path.Combine(_dtcPath, "participant_inbox", participant.ProcessId);
            Directory.CreateDirectory(participantInbox);

            var prepareRequest = new
            {
                TransactionId = transaction.TransactionId,
                ParticipantId = participant.ParticipantId,
                RequestType = "Prepare",
                Timestamp = DateTime.UtcNow
            };

            var requestPath = Path.Combine(participantInbox, $"prepare_{transaction.TransactionId}_{participant.ParticipantId}.json");
            var json = JsonSerializer.Serialize(prepareRequest, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(requestPath, json);
        }

        /// <summary>
        /// Sends commit decision to a participant
        /// </summary>
        private async Task SendCommitToParticipantAsync(DistributedTransaction transaction, Participant participant)
        {
            var participantInbox = Path.Combine(_dtcPath, "participant_inbox", participant.ProcessId);
            Directory.CreateDirectory(participantInbox);

            var commitRequest = new
            {
                TransactionId = transaction.TransactionId,
                ParticipantId = participant.ParticipantId,
                RequestType = "Commit",
                Timestamp = DateTime.UtcNow
            };

            var requestPath = Path.Combine(participantInbox, $"commit_{transaction.TransactionId}_{participant.ParticipantId}.json");
            var json = JsonSerializer.Serialize(commitRequest, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(requestPath, json);

            participant.State = ParticipantState.Committed;
            participant.LastUpdate = DateTime.UtcNow;
        }

        /// <summary>
        /// Sends abort decision to a participant
        /// </summary>
        private async Task SendAbortToParticipantAsync(DistributedTransaction transaction, Participant participant)
        {
            var participantInbox = Path.Combine(_dtcPath, "participant_inbox", participant.ProcessId);
            Directory.CreateDirectory(participantInbox);

            var abortRequest = new
            {
                TransactionId = transaction.TransactionId,
                ParticipantId = participant.ParticipantId,
                RequestType = "Abort",
                Reason = transaction.FailureReason,
                Timestamp = DateTime.UtcNow
            };

            var requestPath = Path.Combine(participantInbox, $"abort_{transaction.TransactionId}_{participant.ParticipantId}.json");
            var json = JsonSerializer.Serialize(abortRequest, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(requestPath, json);

            participant.State = ParticipantState.Aborted;
            participant.LastUpdate = DateTime.UtcNow;
        }

        /// <summary>
        /// Writes an entry to the write-ahead log
        /// </summary>
        private async Task WriteWALAsync(WALEntry entry)
        {
            var walPath = Path.Combine(_walDirectory, $"{entry.Timestamp.Ticks}_{entry.EntryId}.json");
            var json = JsonSerializer.Serialize(entry, new JsonSerializerOptions { WriteIndented = true });
            
            // Write atomically
            var tempPath = walPath + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, walPath, true);
        }

        /// <summary>
        /// Persists transaction state to disk
        /// </summary>
        private async Task PersistTransactionAsync(DistributedTransaction transaction)
        {
            var txnPath = Path.Combine(_transactionsDirectory, $"{transaction.TransactionId}.json");
            var json = JsonSerializer.Serialize(transaction, new JsonSerializerOptions { WriteIndented = true });
            
            // Write atomically
            var tempPath = txnPath + ".tmp";
            await File.WriteAllTextAsync(tempPath, json);
            File.Move(tempPath, txnPath, true);
        }

        /// <summary>
        /// Loads transaction state from disk
        /// </summary>
        private async Task<DistributedTransaction?> LoadTransactionAsync(string transactionId)
        {
            var txnPath = Path.Combine(_transactionsDirectory, $"{transactionId}.json");
            if (!File.Exists(txnPath))
                return null;

            try
            {
                var json = await File.ReadAllTextAsync(txnPath);
                return JsonSerializer.Deserialize<DistributedTransaction>(json);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Recovers incomplete transactions after crash
        /// </summary>
        private async Task RecoverIncompleteTransactionsAsync()
        {
            if (_disposed)
                return;

            try
            {
                var txnFiles = Directory.GetFiles(_transactionsDirectory, "*.json");
                
                foreach (var file in txnFiles)
                {
                    try
                    {
                        var json = await File.ReadAllTextAsync(file);
                        var transaction = JsonSerializer.Deserialize<DistributedTransaction>(json);
                        
                        if (transaction != null && 
                            transaction.CoordinatorProcessId == _processId)
                        {
                            // Check if transaction is stuck
                            var age = DateTime.UtcNow - transaction.LastUpdated;
                            
                            if (age > TimeSpan.FromMinutes(5))
                            {
                                switch (transaction.Phase)
                                {
                                    case TransactionPhase.Preparing:
                                    case TransactionPhase.Prepared:
                                        // Abort stuck transactions
                                        await AbortTransactionAsync(transaction.TransactionId, "Transaction timed out");
                                        break;
                                        
                                    case TransactionPhase.Committing:
                                        // Retry commit
                                        foreach (var participant in transaction.Participants)
                                        {
                                            if (participant.State != ParticipantState.Committed)
                                            {
                                                await SendCommitToParticipantAsync(transaction, participant);
                                            }
                                        }
                                        break;
                                        
                                    case TransactionPhase.Aborting:
                                        // Retry abort
                                        foreach (var participant in transaction.Participants)
                                        {
                                            if (participant.State != ParticipantState.Aborted)
                                            {
                                                await SendAbortToParticipantAsync(transaction, participant);
                                            }
                                        }
                                        break;
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Continue with next transaction
                    }
                }
            }
            catch
            {
                // Ignore recovery errors
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;
            
            _recoveryTimer?.Dispose();
        }
    }
}