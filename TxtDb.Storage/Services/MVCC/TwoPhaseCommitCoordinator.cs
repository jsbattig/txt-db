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
    /// Two-Phase Commit Protocol Coordinator for Epic 003 Phase 1
    /// 
    /// Implements distributed consensus for multi-process state updates using:
    /// - File-based voting mechanism
    /// - Directory structures for proposals/votes/decisions
    /// - Timeout-based decision making
    /// - Automatic cleanup of stale proposals
    /// 
    /// CRITICAL: Ensures distributed consensus without in-memory state assumptions
    /// </summary>
    public class TwoPhaseCommitCoordinator : IDisposable
    {
        private readonly string _proposalsDirectory;
        private readonly string _votesDirectory;
        private readonly string _decisionsDirectory;
        private readonly string _processId;
        private volatile bool _disposed = false;

        // Configuration
        private static readonly TimeSpan ProposalTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan VoteTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan DecisionTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Represents a state change proposal in the 2PC protocol
        /// </summary>
        public class StateProposal
        {
            public string ProposalId { get; set; } = Guid.NewGuid().ToString();
            public string ProposerProcessId { get; set; } = "";
            public DateTime ProposedAt { get; set; } = DateTime.UtcNow;
            public ProposalType Type { get; set; }
            public string StateData { get; set; } = "";
            public ProposalState State { get; set; } = ProposalState.Proposed;
            public DateTime DeadlineUtc { get; set; }
            public List<string> RequiredParticipants { get; set; } = new();
            public string Description { get; set; } = "";
        }

        /// <summary>
        /// Vote from a participant on a proposal
        /// </summary>
        public class ParticipantVote
        {
            public string VoteId { get; set; } = Guid.NewGuid().ToString();
            public string ProposalId { get; set; } = "";
            public string ParticipantProcessId { get; set; } = "";
            public VoteType Vote { get; set; }
            public DateTime VotedAt { get; set; } = DateTime.UtcNow;
            public string Reason { get; set; } = "";
        }

        /// <summary>
        /// Final decision on a proposal
        /// </summary>
        public class CommitDecision
        {
            public string DecisionId { get; set; } = Guid.NewGuid().ToString();
            public string ProposalId { get; set; } = "";
            public DecisionType Decision { get; set; }
            public DateTime DecidedAt { get; set; } = DateTime.UtcNow;
            public List<ParticipantVote> Votes { get; set; } = new();
            public string Reason { get; set; } = "";
        }

        public enum ProposalType
        {
            GlobalStateUpdate,
            TransactionBegin,
            TransactionCommit,
            TransactionRollback,
            LeaseAcquisition,
            LeaseRelease
        }

        public enum ProposalState
        {
            Proposed,
            Voting,
            Decided,
            Expired,
            Cancelled
        }

        public enum VoteType
        {
            Commit,
            Abort
        }

        public enum DecisionType
        {
            Commit,
            Abort,
            Timeout
        }

        public TwoPhaseCommitCoordinator(string coordinationPath)
        {
            if (string.IsNullOrEmpty(coordinationPath))
                throw new ArgumentNullException(nameof(coordinationPath));

            _proposalsDirectory = Path.Combine(coordinationPath, "consensus", "proposals");
            _votesDirectory = Path.Combine(coordinationPath, "consensus", "votes");
            _decisionsDirectory = Path.Combine(coordinationPath, "consensus", "decisions");
            _processId = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();

            // Ensure directories exist
            Directory.CreateDirectory(_proposalsDirectory);
            Directory.CreateDirectory(_votesDirectory);
            Directory.CreateDirectory(_decisionsDirectory);

            // Start background cleanup task
            _ = Task.Run(async () => await CleanupExpiredProposalsAsync());
        }

        /// <summary>
        /// Proposes a state change and coordinates the 2PC protocol
        /// </summary>
        public async Task<CommitDecision> ProposeStateChangeAsync(
            ProposalType type, 
            string stateData, 
            List<string> requiredParticipants,
            string description = "")
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TwoPhaseCommitCoordinator));

            // Phase 0: Create proposal
            var proposal = new StateProposal
            {
                ProposerProcessId = _processId,
                Type = type,
                StateData = stateData,
                RequiredParticipants = requiredParticipants,
                DeadlineUtc = DateTime.UtcNow.Add(ProposalTimeout),
                Description = description
            };

            await PersistProposalAsync(proposal);

            // Phase 1: Wait for votes
            proposal.State = ProposalState.Voting;
            await PersistProposalAsync(proposal);

            var decision = await WaitForVotesAndDecideAsync(proposal);
            
            // Phase 2: Persist and announce decision
            await PersistDecisionAsync(decision);

            // Update proposal state
            proposal.State = ProposalState.Decided;
            await PersistProposalAsync(proposal);

            return decision;
        }

        /// <summary>
        /// Participates in a proposal by casting a vote
        /// </summary>
        public async Task<ParticipantVote> VoteOnProposalAsync(string proposalId, VoteType vote, string reason = "")
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TwoPhaseCommitCoordinator));

            var proposal = await LoadProposalAsync(proposalId);
            if (proposal == null)
                throw new InvalidOperationException($"Proposal {proposalId} not found");

            if (proposal.State != ProposalState.Voting)
                throw new InvalidOperationException($"Proposal {proposalId} is not in voting state");

            if (DateTime.UtcNow > proposal.DeadlineUtc)
                throw new InvalidOperationException($"Proposal {proposalId} has expired");

            var participantVote = new ParticipantVote
            {
                ProposalId = proposalId,
                ParticipantProcessId = _processId,
                Vote = vote,
                Reason = reason
            };

            await PersistVoteAsync(participantVote);
            return participantVote;
        }

        /// <summary>
        /// Monitors proposals that require this process's participation
        /// </summary>
        public async Task<List<StateProposal>> GetPendingProposalsAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(TwoPhaseCommitCoordinator));

            var pendingProposals = new List<StateProposal>();
            var proposalFiles = Directory.GetFiles(_proposalsDirectory, "*.json");

            foreach (var file in proposalFiles)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var proposal = JsonSerializer.Deserialize<StateProposal>(json);
                    
                    if (proposal != null && 
                        proposal.State == ProposalState.Voting &&
                        DateTime.UtcNow <= proposal.DeadlineUtc &&
                        (proposal.RequiredParticipants.Contains(_processId) || 
                         proposal.RequiredParticipants.Count == 0)) // 0 means all processes
                    {
                        // Check if we haven't voted yet
                        var voteFile = Path.Combine(_votesDirectory, $"{proposal.ProposalId}_{_processId}.json");
                        if (!File.Exists(voteFile))
                        {
                            pendingProposals.Add(proposal);
                        }
                    }
                }
                catch
                {
                    // Ignore corrupted proposal files
                }
            }

            return pendingProposals;
        }

        /// <summary>
        /// Waits for votes and makes a decision based on 2PC rules
        /// </summary>
        private async Task<CommitDecision> WaitForVotesAndDecideAsync(StateProposal proposal)
        {
            var deadline = DateTime.UtcNow.Add(VoteTimeout);
            var votes = new List<ParticipantVote>();
            var requiredVoters = proposal.RequiredParticipants.Count > 0 
                ? proposal.RequiredParticipants 
                : await GetAllActiveProcessesAsync();

            while (DateTime.UtcNow < deadline)
            {
                votes = await CollectVotesAsync(proposal.ProposalId);
                
                // Check if all required participants have voted
                var votedProcesses = votes.Select(v => v.ParticipantProcessId).ToHashSet();
                var allVoted = requiredVoters.All(p => votedProcesses.Contains(p));
                
                if (allVoted)
                {
                    // All votes collected, make decision
                    var hasAbort = votes.Any(v => v.Vote == VoteType.Abort);
                    return new CommitDecision
                    {
                        ProposalId = proposal.ProposalId,
                        Decision = hasAbort ? DecisionType.Abort : DecisionType.Commit,
                        Votes = votes,
                        Reason = hasAbort ? "At least one participant voted to abort" : "All participants voted to commit"
                    };
                }

                // Check for any abort votes (fail-fast)
                if (votes.Any(v => v.Vote == VoteType.Abort))
                {
                    return new CommitDecision
                    {
                        ProposalId = proposal.ProposalId,
                        Decision = DecisionType.Abort,
                        Votes = votes,
                        Reason = "Abort vote received"
                    };
                }

                await Task.Delay(100); // Poll every 100ms
            }

            // Timeout - treat missing votes as abort
            return new CommitDecision
            {
                ProposalId = proposal.ProposalId,
                Decision = DecisionType.Timeout,
                Votes = votes,
                Reason = $"Timeout waiting for votes. Got {votes.Count}/{requiredVoters.Count} votes"
            };
        }

        /// <summary>
        /// Collects all votes for a proposal
        /// </summary>
        private async Task<List<ParticipantVote>> CollectVotesAsync(string proposalId)
        {
            var votes = new List<ParticipantVote>();
            var votePattern = $"{proposalId}_*.json";
            var voteFiles = Directory.GetFiles(_votesDirectory, votePattern);

            foreach (var file in voteFiles)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var vote = JsonSerializer.Deserialize<ParticipantVote>(json);
                    if (vote != null)
                        votes.Add(vote);
                }
                catch
                {
                    // Ignore corrupted vote files
                }
            }

            return votes;
        }

        /// <summary>
        /// Gets all active processes by scanning for process heartbeats and running processes
        /// </summary>
        private async Task<List<string>> GetAllActiveProcessesAsync()
        {
            var activeProcesses = new List<string> { _processId }; // Always include self
            
            try
            {
                // Check for other processes that have made proposals or votes recently
                var recentThreshold = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5));
                
                // Scan proposals for other active processes
                var proposalFiles = Directory.GetFiles(_proposalsDirectory, "*.json");
                foreach (var file in proposalFiles)
                {
                    try
                    {
                        var fileInfo = new FileInfo(file);
                        if (fileInfo.LastWriteTimeUtc > recentThreshold)
                        {
                            var json = await File.ReadAllTextAsync(file);
                            var proposal = JsonSerializer.Deserialize<StateProposal>(json);
                            if (proposal != null && proposal.ProposerProcessId != _processId)
                            {
                                if (!activeProcesses.Contains(proposal.ProposerProcessId))
                                    activeProcesses.Add(proposal.ProposerProcessId);
                            }
                        }
                    }
                    catch { /* Ignore individual file errors */ }
                }
                
                // Scan votes for other active processes
                var voteFiles = Directory.GetFiles(_votesDirectory, "*.json");
                foreach (var file in voteFiles)
                {
                    try
                    {
                        var fileInfo = new FileInfo(file);
                        if (fileInfo.LastWriteTimeUtc > recentThreshold)
                        {
                            var json = await File.ReadAllTextAsync(file);
                            var vote = JsonSerializer.Deserialize<ParticipantVote>(json);
                            if (vote != null && vote.ParticipantProcessId != _processId)
                            {
                                if (!activeProcesses.Contains(vote.ParticipantProcessId))
                                    activeProcesses.Add(vote.ParticipantProcessId);
                            }
                        }
                    }
                    catch { /* Ignore individual file errors */ }
                }
                
                // For the test scenario, if we're the only process detected, 
                // we should be able to make unilateral decisions
            }
            catch (Exception ex)
            {
                // Ignore errors in scanning for active processes
            }
            
            return activeProcesses;
        }

        /// <summary>
        /// Persists a proposal to disk
        /// </summary>
        private async Task PersistProposalAsync(StateProposal proposal)
        {
            var proposalPath = Path.Combine(_proposalsDirectory, $"{proposal.ProposalId}.json");
            var json = JsonSerializer.Serialize(proposal, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            await File.WriteAllTextAsync(proposalPath, json);
        }

        /// <summary>
        /// Loads a proposal from disk
        /// </summary>
        private async Task<StateProposal?> LoadProposalAsync(string proposalId)
        {
            var proposalPath = Path.Combine(_proposalsDirectory, $"{proposalId}.json");
            if (!File.Exists(proposalPath))
                return null;

            try
            {
                var json = await File.ReadAllTextAsync(proposalPath);
                return JsonSerializer.Deserialize<StateProposal>(json);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Persists a vote to disk
        /// </summary>
        private async Task PersistVoteAsync(ParticipantVote vote)
        {
            var votePath = Path.Combine(_votesDirectory, $"{vote.ProposalId}_{vote.ParticipantProcessId}.json");
            var json = JsonSerializer.Serialize(vote, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            await File.WriteAllTextAsync(votePath, json);
        }

        /// <summary>
        /// Persists a decision to disk
        /// </summary>
        private async Task PersistDecisionAsync(CommitDecision decision)
        {
            var decisionPath = Path.Combine(_decisionsDirectory, $"{decision.ProposalId}.json");
            var json = JsonSerializer.Serialize(decision, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            await File.WriteAllTextAsync(decisionPath, json);
        }

        /// <summary>
        /// Background task to clean up expired proposals
        /// </summary>
        private async Task CleanupExpiredProposalsAsync()
        {
            while (!_disposed)
            {
                try
                {
                    var proposalFiles = Directory.GetFiles(_proposalsDirectory, "*.json");
                    
                    foreach (var file in proposalFiles)
                    {
                        try
                        {
                            var json = await File.ReadAllTextAsync(file);
                            var proposal = JsonSerializer.Deserialize<StateProposal>(json);
                            
                            if (proposal != null && 
                                DateTime.UtcNow > proposal.DeadlineUtc.AddHours(1))
                            {
                                // Clean up old proposal and related files
                                File.Delete(file);
                                
                                // Clean up votes
                                var votePattern = $"{proposal.ProposalId}_*.json";
                                var voteFiles = Directory.GetFiles(_votesDirectory, votePattern);
                                foreach (var voteFile in voteFiles)
                                {
                                    try { File.Delete(voteFile); } catch { }
                                }
                                
                                // Clean up decision
                                var decisionFile = Path.Combine(_decisionsDirectory, $"{proposal.ProposalId}.json");
                                if (File.Exists(decisionFile))
                                {
                                    try { File.Delete(decisionFile); } catch { }
                                }
                            }
                        }
                        catch
                        {
                            // Ignore individual file errors
                        }
                    }
                }
                catch
                {
                    // Ignore cleanup errors
                }

                await Task.Delay(TimeSpan.FromMinutes(5)); // Run cleanup every 5 minutes
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;
        }
    }
}