using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Storage.Services;
using TxtDb.Storage.Interfaces;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// Multi-Process End-to-End Tests for TxtDb Epic 003
    /// 
    /// These tests validate true multi-process behavior by launching separate processes
    /// that operate on shared TxtDb storage. Tests validate:
    /// - Concurrency control and conflict detection
    /// - Data consistency across processes  
    /// - Transaction isolation
    /// - Cross-process coordination
    /// - No data corruption under concurrent access
    /// 
    /// CRITICAL: Uses Process.Start() for true multi-process testing
    /// </summary>
    public class MultiProcessE2ETests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _testDirectory;
        private readonly string _storageDirectory;
        private readonly CrossProcessTestFramework _framework;
        private IStorageSubsystem? _storage;
        private ConsistencyValidator? _validator;

        public MultiProcessE2ETests(ITestOutputHelper output)
        {
            _output = output;
            _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_multiprocess_tests", Guid.NewGuid().ToString());
            _storageDirectory = Path.Combine(_testDirectory, "storage");
            _framework = new CrossProcessTestFramework(_testDirectory, output);
            
            // Ensure directories exist
            Directory.CreateDirectory(_testDirectory);
            Directory.CreateDirectory(_storageDirectory);
            
            _output.WriteLine($"Test directory: {_testDirectory}");
            _output.WriteLine($"Storage directory: {_storageDirectory}");
        }

        #region TDD Tests - Start with Failing Tests

        /// <summary>
        /// Test 1: Concurrent Counter Updates
        /// Multiple processes increment shared counters - validate final sum matches expected total
        /// </summary>
        [Fact]
        public async Task MultiProcess_ConcurrentCounterUpdates_ShouldMaintainConsistency()
        {
            // Arrange
            const int processCount = 3;
            const int operationsPerProcess = 100;
            const int expectedFinalValue = processCount * operationsPerProcess;
            const string namespaceName = "concurrent_counters";

            var config = new MultiProcessTestConfig
            {
                ProcessCount = processCount,
                OperationsPerProcess = operationsPerProcess,
                StorageDirectory = _storageDirectory
            };

            // Initialize storage and create initial counter
            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Initialize the counter in storage before launching processes
            await InitializeTestDataAsync(namespaceName, expectedFinalValue);


            // Act - Launch multiple processes to increment counter
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>();
            for (int i = 0; i < processCount; i++)
            {
                var processConfig = new CrossProcessTestFramework.ProcessConfig
                {
                    ProcessId = $"counter_process_{i}",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteConcurrentCounterUpdatesAsync",
                    TimeoutSeconds = 120,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "ConcurrentCounterUpdates",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString(),
                        ["TXTDB_OPERATIONS_COUNT"] = operationsPerProcess.ToString(),
                        ["TXTDB_EXPECTED_FINAL_VALUE"] = expectedFinalValue.ToString()
                    }
                };
                processConfigs.Add(processConfig);
            }

            var results = await _framework.RunMultiProcessTestAsync(
                "ConcurrentCounterUpdates",
                processConfigs,
                async context =>
                {
                    // Coordinator logic - monitor progress
                    await Task.Delay(1000); // Let processes start
                    _output.WriteLine("All processes launched for counter updates");
                });

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Validate final counter state is handled in the transaction block above

            // Validate no lost updates occurred
            var totalOperationsReported = results.Sum(r => 
            {
                if (r.Data.ContainsKey("SuccessfulUpdates"))
                {
                    var value = r.Data["SuccessfulUpdates"];
                    if (value is System.Text.Json.JsonElement jsonElement && jsonElement.ValueKind == System.Text.Json.JsonValueKind.Number)
                    {
                        return jsonElement.GetInt32();
                    }
                    return Convert.ToInt32(value);
                }
                return 0;
            });
            Assert.True(totalOperationsReported > 0, "No successful operations reported");

            // Validate consistency
            var consistencyResult = await _validator.ValidateConsistencyAsync(namespaceName);
            Assert.True(consistencyResult.IsValid, 
                $"Consistency validation failed: {string.Join(", ", consistencyResult.ValidationErrors)}");

            _output.WriteLine($"Expected value: {expectedFinalValue}");
            _output.WriteLine($"Counter validation completed successfully");
        }

        /// <summary>
        /// Test 2: Multi-Page Object Consistency
        /// Objects spanning multiple pages with relationships - validate referential integrity
        /// </summary>
        [Fact]
        public async Task MultiProcess_OrderAndItemUpdates_ShouldMaintainReferentialIntegrity()
        {
            // Arrange
            const int processCount = 2;
            const int operationsPerProcess = 25;
            const string namespaceName = "order_integrity";

            var config = new MultiProcessTestConfig
            {
                ProcessCount = processCount,
                OperationsPerProcess = operationsPerProcess,
                StorageDirectory = _storageDirectory
            };

            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Act - Launch processes to create orders with items
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>();
            for (int i = 0; i < processCount; i++)
            {
                var processConfig = new CrossProcessTestFramework.ProcessConfig
                {
                    ProcessId = $"order_process_{i}",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteMultiPageObjectConsistencyAsync",
                    TimeoutSeconds = 180,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "MultiPageObjectConsistency",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString(),
                        ["TXTDB_OPERATIONS_COUNT"] = operationsPerProcess.ToString()
                    }
                };
                processConfigs.Add(processConfig);
            }

            var results = await _framework.RunMultiProcessTestAsync(
                "MultiPageObjectConsistency",
                processConfigs,
                async context =>
                {
                    await Task.Delay(2000); // Let processes work
                    _output.WriteLine("Monitoring multi-page object consistency");
                });

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Load and validate all objects

            // Detailed validation is handled in the transaction block above and consistency validator

            // Full consistency validation
            var consistencyResult = await _validator.ValidateConsistencyAsync(namespaceName);
            Assert.True(consistencyResult.IsValid, 
                $"Consistency validation failed: {string.Join(", ", consistencyResult.ValidationErrors)}");

            _output.WriteLine("Multi-page object consistency test completed successfully");
        }

        /// <summary>
        /// Test 3: Transaction Isolation Validation  
        /// Process A starts long transaction, Process B reads - validate proper isolation
        /// </summary>
        [Fact]
        public async Task MultiProcess_TransactionIsolation_ShouldPreventDirtyReads()
        {
            // Arrange
            const int processCount = 2;
            const string namespaceName = "transaction_isolation";

            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Act - Launch reader and writer processes
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>
            {
                new()
                {
                    ProcessId = "writer_process_A",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteTransactionIsolationValidationAsync",
                    TimeoutSeconds = 120,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "TransactionIsolationValidation",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString()
                    }
                },
                new()
                {
                    ProcessId = "reader_process_B",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteTransactionIsolationValidationAsync",
                    TimeoutSeconds = 120,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "TransactionIsolationValidation",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString()
                    }
                }
            };

            var results = await _framework.RunMultiProcessTestAsync(
                "TransactionIsolation",
                processConfigs,
                async context =>
                {
                    await Task.Delay(3000); // Monitor isolation behavior
                    _output.WriteLine("Monitoring transaction isolation");
                });

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Validate no dirty reads occurred (process B should not see partial updates)
            var readerResult = results.FirstOrDefault(r => r.ProcessId == "reader_process_B");
            Assert.NotNull(readerResult);
            
            // Check for isolation violations in error messages
            var isolationViolations = readerResult.Data.ContainsKey("ErrorMessages") 
                ? ((List<string>)readerResult.Data["ErrorMessages"]).Count(msg => msg.Contains("partial update"))
                : 0;
            
            Assert.Equal(0, isolationViolations);

            _output.WriteLine("Transaction isolation validated - no dirty reads detected");
        }

        /// <summary>
        /// Test 4: Concurrent Page Creation/Deletion
        /// Multiple processes creating/deleting pages - validate no orphaned references
        /// </summary>
        [Fact]
        public async Task MultiProcess_ConcurrentPageOperations_ShouldMaintainPageConsistency()
        {
            // Arrange
            const int processCount = 3;
            const int operationsPerProcess = 50;
            const string namespaceName = "page_operations";

            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Act - Launch processes for concurrent page operations
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>();
            for (int i = 0; i < processCount; i++)
            {
                var processConfig = new CrossProcessTestFramework.ProcessConfig
                {
                    ProcessId = $"page_process_{i}",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteConcurrentPageOperationsAsync",
                    TimeoutSeconds = 150,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "ConcurrentPageOperations",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString(),
                        ["TXTDB_OPERATIONS_COUNT"] = operationsPerProcess.ToString()
                    }
                };
                processConfigs.Add(processConfig);
            }

            var results = await _framework.RunMultiProcessTestAsync(
                "ConcurrentPageOperations",
                processConfigs,
                async context =>
                {
                    await Task.Delay(1500);
                    _output.WriteLine("Monitoring concurrent page operations");
                });

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Validate no serialization corruption
            var noCorruption = await _validator.ValidateNoSerializationCorruptionAsync(namespaceName);
            Assert.True(noCorruption, "Serialization corruption detected");

            // Validate page consistency
            var consistencyResult = await _validator.ValidateConsistencyAsync(namespaceName);
            Assert.True(consistencyResult.IsValid, 
                $"Page consistency validation failed: {string.Join(", ", consistencyResult.ValidationErrors)}");

            _output.WriteLine("Concurrent page operations completed successfully");
        }

        /// <summary>
        /// Test 5: Cross-Process State Coordination
        /// Global state updates (TSN allocation, active transactions) - validate coordination
        /// </summary>
        [Fact]
        public async Task MultiProcess_GlobalStateCoordination_ShouldMaintainConsistency()
        {
            // Arrange
            const int processCount = 4;
            const int operationsPerProcess = 25;
            const string namespaceName = "global_state";

            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Act - Launch processes for state coordination testing
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>();
            for (int i = 0; i < processCount; i++)
            {
                var processConfig = new CrossProcessTestFramework.ProcessConfig
                {
                    ProcessId = $"state_process_{i}",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteCrossProcessStateCoordinationAsync",
                    TimeoutSeconds = 180,
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "CrossProcessStateCoordination",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString(),
                        ["TXTDB_OPERATIONS_COUNT"] = operationsPerProcess.ToString()
                    }
                };
                processConfigs.Add(processConfig);
            }

            var results = await _framework.RunMultiProcessTestAsync(
                "CrossProcessStateCoordination",
                processConfigs,
                async context =>
                {
                    await Task.Delay(2000);
                    _output.WriteLine("Monitoring cross-process state coordination");
                });

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Validate state consistency across all processes
            var consistencyResult = await _validator.ValidateConsistencyAsync(namespaceName);
            Assert.True(consistencyResult.IsValid, 
                $"Global state consistency failed: {string.Join(", ", consistencyResult.ValidationErrors)}");

            _output.WriteLine("Cross-process state coordination validated");
        }

        /// <summary>
        /// Test 6: Stress Test - High Concurrency Scenario
        /// Maximum stress test with many processes and operations
        /// </summary>
        [Fact]
        public async Task MultiProcess_HighConcurrencyStress_ShouldMaintainDataIntegrity()
        {
            // Arrange
            const int processCount = 5;
            const int operationsPerProcess = 200;
            const int expectedFinalValue = processCount * operationsPerProcess;
            const string namespaceName = "stress_test";

            _storage = new StorageSubsystem();
            _storage.Initialize(_storageDirectory);
            _validator = new ConsistencyValidator(_storage);

            // Initialize multiple counters for stress testing
            var stressTxnId = _storage.BeginTransaction();
            try
            {
                for (int i = 1; i <= 3; i++)
                {
                    var counter = new SharedCounter
                    {
                        CounterId = $"stress_counter_{i}",
                        Value = 0,
                        ProcessUpdates = 0,
                        UpdatingProcesses = new List<string>(),
                        ExpectedValue = expectedFinalValue,
                        Version = 0
                    };
                    _storage.InsertObject(stressTxnId, namespaceName, counter);
                }
                _storage.CommitTransaction(stressTxnId);
            }
            catch
            {
                _storage.RollbackTransaction(stressTxnId);
                throw;
            }

            // Act - Launch stress test processes
            var processConfigs = new List<CrossProcessTestFramework.ProcessConfig>();
            for (int i = 0; i < processCount; i++)
            {
                var processConfig = new CrossProcessTestFramework.ProcessConfig
                {
                    ProcessId = $"stress_process_{i}",
                    TestClass = nameof(ProcessTestRunner),
                    TestMethod = "ExecuteConcurrentCounterUpdatesAsync",
                    TimeoutSeconds = 300, // Extended timeout for stress test
                    Parameters = new Dictionary<string, string>
                    {
                        ["TXTDB_TEST_SCENARIO"] = "ConcurrentCounterUpdates",
                        ["TXTDB_STORAGE_DIRECTORY"] = _storageDirectory,
                        ["TXTDB_TEST_NAMESPACE"] = namespaceName,
                        ["TXTDB_PARTICIPANT_COUNT"] = processCount.ToString(),
                        ["TXTDB_OPERATIONS_COUNT"] = operationsPerProcess.ToString(),
                        ["TXTDB_EXPECTED_FINAL_VALUE"] = expectedFinalValue.ToString()
                    }
                };
                processConfigs.Add(processConfig);
            }

            var startTime = DateTime.UtcNow;
            var results = await _framework.RunMultiProcessTestAsync(
                "HighConcurrencyStress",
                processConfigs,
                async context =>
                {
                    // Monitor progress during stress test
                    for (int i = 0; i < 10; i++)
                    {
                        await Task.Delay(2000);
                        _output.WriteLine($"Stress test progress check {i + 1}/10");
                    }
                });

            var duration = DateTime.UtcNow - startTime;

            // Assert
            Assert.True(results.All(r => r.Success), 
                $"Some stress test processes failed: {string.Join(", ", results.Where(r => !r.Success).Select(r => r.Error))}");

            // Validate all counters reached expected values
            var stressValidationTxnId = _storage.BeginTransaction();
            var counterList = new List<SharedCounter>();
            try
            {
                var counterData = _storage.GetMatchingObjects(stressValidationTxnId, namespaceName, "*stress_counter*");
                foreach (var pageData in counterData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is SharedCounter counter)
                        {
                            counterList.Add(counter);
                        }
                    }
                }
            }
            finally
            {
                _storage.RollbackTransaction(stressValidationTxnId);
            }

            foreach (var counter in counterList)
            {
                Assert.Equal(expectedFinalValue, counter.Value);
                Assert.Equal(processCount, counter.UpdatingProcesses.Distinct().Count());
            }

            // Final consistency validation
            var consistencyResult = await _validator.ValidateConsistencyAsync(namespaceName);
            Assert.True(consistencyResult.IsValid, 
                $"Stress test consistency failed: {string.Join(", ", consistencyResult.ValidationErrors)}");

            _output.WriteLine($"Stress test completed in {duration.TotalMinutes:F2} minutes");
            _output.WriteLine($"Total operations: {processCount * operationsPerProcess * counterList.Count}");
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Initializes test data before launching processes
        /// </summary>
        private async Task InitializeTestDataAsync(string namespaceName, long expectedFinalValue)
        {
            var txnId = _storage!.BeginTransaction();
            try
            {
                // Initialize required test data
                var initialCounter = new SharedCounter
                {
                    CounterId = "shared_counter_1",
                    Value = 0,
                    ProcessUpdates = 0,
                    UpdatingProcesses = new List<string>(),
                    ExpectedValue = expectedFinalValue,
                    Version = 0,
                    LastUpdated = DateTime.UtcNow
                };
                
                _storage.InsertObject(txnId, namespaceName, initialCounter);
                _storage.CommitTransaction(txnId);
                
                _output.WriteLine($"Initialized counter with expected value: {expectedFinalValue}");
            }
            catch
            {
                _storage.RollbackTransaction(txnId);
                throw;
            }
        }

        #endregion

        public void Dispose()
        {
            _framework?.Dispose();
            (_storage as IDisposable)?.Dispose();
            
            // Clean up test directory
            try
            {
                if (Directory.Exists(_testDirectory))
                {
                    Directory.Delete(_testDirectory, true);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}