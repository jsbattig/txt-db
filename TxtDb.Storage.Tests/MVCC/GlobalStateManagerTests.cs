using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Storage.Services.MVCC;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// TDD tests for Global State Management (Story 003-002)
    /// 
    /// Test Scenarios:
    /// 1. Atomic global state updates with temp-file + rename pattern
    /// 2. Global TSN allocation coordinated across processes
    /// 3. Zero race conditions in state updates
    /// 4. Crash recovery restores consistent state
    /// 5. Concurrent state updates maintain consistency
    /// 6. State persistence and loading
    /// </summary>
    public class GlobalStateManagerTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _testStateDirectory;
        private readonly List<string> _createdFiles;

        public GlobalStateManagerTests(ITestOutputHelper output)
        {
            _output = output;
            _testStateDirectory = Path.Combine(Path.GetTempPath(), "TxtDb_GlobalState_Tests", Guid.NewGuid().ToString());
            Directory.CreateDirectory(_testStateDirectory);
            _createdFiles = new List<string>();
        }

        [Fact]
        public async Task GlobalStateManager_InitialState_ShouldCreateDefaultState()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "global_state.json");
            _createdFiles.Add(statePath);

            // ACT - This should fail initially (TDD Red phase)
            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();
                var state = await stateManager.GetCurrentStateAsync();

                // ASSERT
                Assert.NotNull(state);
                Assert.Equal(0, state.CurrentTSN);
                Assert.Equal(1, state.NextTransactionId);
                Assert.Empty(state.ActiveTransactions);
                Assert.Empty(state.PageVersions);
                Assert.True(File.Exists(statePath), "State file should be created");
            }
        }

        [Fact]
        public async Task GlobalStateManager_AtomicUpdate_ShouldUseTempFileRenamePattern()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "atomic_test.json");
            _createdFiles.Add(statePath);

            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();

                // ACT - Update state atomically
                await stateManager.UpdateStateAsync(state => 
                {
                    state.CurrentTSN = 100;
                    state.NextTransactionId = 101;
                    state.ActiveTransactions.Add(100);
                    return state;
                });

                // ASSERT
                var updatedState = await stateManager.GetCurrentStateAsync();
                Assert.Equal(100, updatedState.CurrentTSN);
                Assert.Equal(101, updatedState.NextTransactionId);
                Assert.Contains(100, updatedState.ActiveTransactions);

                // Verify temp file doesn't exist (should be cleaned up)
                var tempFiles = Directory.GetFiles(_testStateDirectory, "*.tmp");
                Assert.Empty(tempFiles);
            }
        }

        [Fact]
        public async Task GlobalStateManager_GlobalTSNAllocation_ShouldAllocateUniqueSequentialTSN()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "tsn_test.json");
            _createdFiles.Add(statePath);

            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();

                // ACT - Allocate multiple TSNs
                var tsn1 = await stateManager.AllocateNextTSNAsync();
                var tsn2 = await stateManager.AllocateNextTSNAsync();
                var tsn3 = await stateManager.AllocateNextTSNAsync();

                // ASSERT
                Assert.Equal(1, tsn1);
                Assert.Equal(2, tsn2);
                Assert.Equal(3, tsn3);

                var finalState = await stateManager.GetCurrentStateAsync();
                Assert.Equal(3, finalState.CurrentTSN);
                Assert.Equal(4, finalState.NextTransactionId);
            }
        }

        [Fact]
        public async Task GlobalStateManager_ConcurrentUpdates_ShouldMaintainConsistency()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "concurrent_test.json");
            _createdFiles.Add(statePath);

            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();

                // ACT - Multiple concurrent TSN allocations
                var tasks = new List<Task<long>>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(stateManager.AllocateNextTSNAsync());
                }

                var allocatedTSNs = await Task.WhenAll(tasks);

                // ASSERT - All TSNs should be unique and sequential
                var expectedTSNs = new HashSet<long> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
                var actualTSNs = new HashSet<long>(allocatedTSNs);
                
                Assert.Equal(expectedTSNs, actualTSNs);

                var finalState = await stateManager.GetCurrentStateAsync();
                Assert.Equal(10, finalState.CurrentTSN);
                Assert.Equal(11, finalState.NextTransactionId);
            }
        }

        [Fact]
        public async Task GlobalStateManager_TransactionLifecycle_ShouldTrackActiveTransactions()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "transaction_test.json");
            _createdFiles.Add(statePath);

            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();

                // ACT - Add and remove active transactions
                var txnId1 = await stateManager.BeginTransactionAsync();
                var txnId2 = await stateManager.BeginTransactionAsync();

                var stateWithActive = await stateManager.GetCurrentStateAsync();
                Assert.Contains(txnId1, stateWithActive.ActiveTransactions);
                Assert.Contains(txnId2, stateWithActive.ActiveTransactions);

                // Complete one transaction
                await stateManager.CompleteTransactionAsync(txnId1);

                var stateAfterComplete = await stateManager.GetCurrentStateAsync();
                Assert.DoesNotContain(txnId1, stateAfterComplete.ActiveTransactions);
                Assert.Contains(txnId2, stateAfterComplete.ActiveTransactions);
            }
        }

        [Fact]
        public async Task GlobalStateManager_CrashRecovery_ShouldRestoreConsistentState()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "recovery_test.json");
            _createdFiles.Add(statePath);

            // Create initial state
            GlobalState? originalState;
            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();
                
                await stateManager.UpdateStateAsync(state =>
                {
                    state.CurrentTSN = 50;
                    state.NextTransactionId = 51;
                    state.ActiveTransactions.Add(50);
                    state.PageVersions["test:page1"] = new PageVersionInfo();
                    return state;
                });

                originalState = await stateManager.GetCurrentStateAsync();
            }

            // ACT - Simulate crash by creating new manager instance
            using (var recoveredStateManager = new GlobalStateManager(statePath))
            {
                await recoveredStateManager.InitializeAsync();
                var recoveredState = await recoveredStateManager.GetCurrentStateAsync();

                // ASSERT - State should be identical to before crash
                Assert.Equal(originalState.CurrentTSN, recoveredState.CurrentTSN);
                Assert.Equal(originalState.NextTransactionId, recoveredState.NextTransactionId);
                Assert.Equal(originalState.ActiveTransactions, recoveredState.ActiveTransactions);
                Assert.Equal(originalState.PageVersions.Keys, recoveredState.PageVersions.Keys);
            }
        }

        [Fact]
        public async Task GlobalStateManager_RaceConditionPrevention_ShouldUseProperLocking()
        {
            // ARRANGE
            var statePath = Path.Combine(_testStateDirectory, "race_test.json");
            _createdFiles.Add(statePath);

            using (var stateManager = new GlobalStateManager(statePath))
            {
                await stateManager.InitializeAsync();

                // ACT - Multiple concurrent complex state updates
                var updateTasks = new List<Task>();
                var results = new ConcurrentBag<long>();

                for (int i = 0; i < 20; i++)
                {
                    updateTasks.Add(Task.Run(async () =>
                    {
                        for (int j = 0; j < 5; j++)
                        {
                            var tsn = await stateManager.AllocateNextTSNAsync();
                            results.Add(tsn);
                            await Task.Delay(1); // Small delay to increase contention
                        }
                    }));
                }

                await Task.WhenAll(updateTasks);

                // ASSERT - All allocated TSNs should be unique
                var allTSNs = results.ToList();
                var uniqueTSNs = new HashSet<long>(allTSNs);
                
                Assert.Equal(100, allTSNs.Count); // 20 tasks * 5 allocations each
                Assert.Equal(100, uniqueTSNs.Count); // All should be unique
                Assert.Equal(Enumerable.Range(1, 100).Select(x => (long)x).ToHashSet(), uniqueTSNs);
            }
        }

        public void Dispose()
        {
            // Cleanup test files
            foreach (var file in _createdFiles)
            {
                try
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }

            try
            {
                if (Directory.Exists(_testStateDirectory))
                    Directory.Delete(_testStateDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}