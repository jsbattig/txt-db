using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Services;
using TxtDb.Storage.Services.MVCC;
using TxtDb.Storage.Interfaces;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// Process Test Runner for Multi-Process E2E Tests
    /// 
    /// Executable entry point for child processes in multi-process testing.
    /// Implements specific test scenarios that can be coordinated across processes.
    /// </summary>
    public class ProcessTestRunner : CrossProcessTestParticipant
    {
        private IStorageSubsystem? _storage;
        private ConsistencyValidator? _validator;
        private string _testScenario = "";
        private MultiProcessTestConfig _config = new();


        /// <summary>
        /// Initializes the test runner with parameters
        /// </summary>
        protected override async Task ExecuteTestAsync()
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] ProcessTestRunner.ExecuteTestAsync starting...");
            
            try
            {
                // Parse test parameters from environment or arguments
                _testScenario = Environment.GetEnvironmentVariable("TXTDB_TEST_SCENARIO") ?? "ConcurrentCounterUpdates";
                var storageDirectory = Environment.GetEnvironmentVariable("TXTDB_STORAGE_DIRECTORY") ?? Path.GetTempPath();
                var namespaceName = Environment.GetEnvironmentVariable("TXTDB_TEST_NAMESPACE") ?? "test";

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Environment variables:");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Test Scenario: {_testScenario}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Storage Directory: {storageDirectory}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Namespace: {namespaceName}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Participant Count: {GetParticipantCount()}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Operations Count: {GetOperationsCount()}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] - Expected Final Value: {GetExpectedFinalValue()}");

                // Initialize storage subsystem
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Initializing storage subsystem...");
                _storage = new StorageSubsystem();
                _storage.Initialize(storageDirectory);
                _validator = new ConsistencyValidator(_storage);
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Storage subsystem initialized successfully");

                // Test storage connectivity
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Testing storage connectivity...");
                var testTxnId = _storage.BeginTransaction();
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Test transaction started: {testTxnId}");
                _storage.RollbackTransaction(testTxnId);
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Storage connectivity confirmed");

                // Execute the specified test scenario
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Executing test scenario: {_testScenario}");
                switch (_testScenario)
                {
                    case "ConcurrentCounterUpdates":
                        await ExecuteConcurrentCounterUpdatesAsync(namespaceName);
                        break;
                    case "MultiPageObjectConsistency":
                        await ExecuteMultiPageObjectConsistencyAsync(namespaceName);
                        break;
                    case "TransactionIsolationValidation":
                        await ExecuteTransactionIsolationValidationAsync(namespaceName);
                        break;
                    case "ConcurrentPageOperations":
                        await ExecuteConcurrentPageOperationsAsync(namespaceName);
                        break;
                    case "CrossProcessStateCoordination":
                        await ExecuteCrossProcessStateCoordinationAsync(namespaceName);
                        break;
                    default:
                        throw new ArgumentException($"Unknown test scenario: {_testScenario}");
                }
                
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Test scenario completed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] ExecuteTestAsync failed: {ex}");
                throw;
            }
        }

        /// <summary>
        /// Executes concurrent counter updates test
        /// </summary>
        private async Task ExecuteConcurrentCounterUpdatesAsync(string namespaceName)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] ExecuteConcurrentCounterUpdatesAsync starting...");
            
            var processData = new ProcessTestData
            {
                ProcessId = ProcessId,
                StartTime = DateTime.UtcNow
            };

            try
            {
                // Wait for all processes to be ready
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Waiting at start barrier for {GetParticipantCount()} participants...");
                await WaitAtBarrierAsync("start", GetParticipantCount(), TimeSpan.FromMinutes(1));
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Passed start barrier, beginning operations...");

                var operationsCount = GetOperationsCount();
                var counterId = "shared_counter_1";
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Will perform {operationsCount} operations on counter '{counterId}'");

                for (int i = 0; i < operationsCount; i++)
                {
                    try
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Starting operation {i+1}/{operationsCount}");
                        
                        var txnId = _storage!.BeginTransaction();
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Transaction {txnId} started for operation {i+1}");
                        
                        try
                        {
                            // Load current counter using consistent object ID pattern
                            var searchPattern = $"SharedCounter_{counterId}";
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Searching for pattern: '{searchPattern}'");
                            
                            var counterData = _storage.GetMatchingObjects(txnId, namespaceName, searchPattern);
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Search returned {counterData.Count} pages");
                            
                            SharedCounter? counter = null;
                            int totalObjectsFound = 0;
                            
                            foreach (var pageData in counterData.Values)
                            {
                                var pageList = pageData.ToList();
                                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Examining page with {pageList.Count} objects");
                                totalObjectsFound += pageList.Count;
                                
                                foreach (var obj in pageData)
                                {
                                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Found object of type: {obj.GetType().Name}");
                                    
                                    if (obj is SharedCounter c && c.CounterId == counterId)
                                    {
                                        counter = c;
                                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Found matching counter: Value={c.Value}, Version={c.Version}");
                                        break;
                                    }
                                }
                                if (counter != null) break;
                            }
                            
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Total objects examined: {totalObjectsFound}");

                            if (counter == null)
                            {
                                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Counter not found, creating new counter");
                                
                                // Initialize counter if it doesn't exist
                                counter = new SharedCounter
                                {
                                    CounterId = counterId,
                                    Value = 0,
                                    ProcessUpdates = 0,
                                    UpdatingProcesses = new List<string>(),
                                    LastUpdated = DateTime.UtcNow,
                                    ExpectedValue = GetExpectedFinalValue(),
                                    Version = 0
                                };
                                
                                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Created new counter: Value={counter.Value}, ExpectedValue={counter.ExpectedValue}");
                            }
                            else
                            {
                                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Using existing counter: Value={counter.Value}, ProcessUpdates={counter.ProcessUpdates}");
                            }

                            // Increment counter
                            var oldValue = counter.Value;
                            counter.Value++;
                            counter.ProcessUpdates++;
                            if (!counter.UpdatingProcesses.Contains(ProcessId))
                            {
                                counter.UpdatingProcesses.Add(ProcessId);
                            }
                            counter.LastUpdated = DateTime.UtcNow;
                            counter.Version++;
                            
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Updated counter: {oldValue} -> {counter.Value}, ProcessUpdates={counter.ProcessUpdates}, Version={counter.Version}");

                            // Save updated counter
                            var pageId = _storage.InsertObject(txnId, namespaceName, counter);
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Counter saved to page {pageId}");
                            
                            _storage.CommitTransaction(txnId);
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Transaction {txnId} committed successfully");
                            
                            processData.SuccessfulUpdates++;
                            processData.ModifiedObjectIds.Add(counter.Value); // Use value as ID for tracking
                            
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Operation {i+1} completed successfully. Total successful: {processData.SuccessfulUpdates}");
                        }
                        catch (Exception innerEx)
                        {
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Operation {i+1} failed, rolling back transaction {txnId}: {innerEx.Message}");
                            _storage.RollbackTransaction(txnId);
                            throw;
                        }
                    }
                    catch (Exception ex)
                    {
                        processData.ConflictsEncountered++;
                        processData.ErrorMessages.Add($"Operation {i}: {ex.Message}");
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Operation {i+1} encountered conflict: {ex.Message}");
                        
                        // Add delay before retry to reduce contention
                        await Task.Delay(10);
                    }

                    processData.OperationsCompleted++;
                    
                    // Small delay to allow interleaving
                    await Task.Delay(5);
                }

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] All operations completed. Successful: {processData.SuccessfulUpdates}, Conflicts: {processData.ConflictsEncountered}");

                // Wait for all processes to complete their operations
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Waiting at operations_complete barrier...");
                await WaitAtBarrierAsync("operations_complete", GetParticipantCount(), TimeSpan.FromMinutes(2));
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Passed operations_complete barrier");

                // Validate final state
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Performing final validation...");
                var finalValidation = await _validator!.ValidateConsistencyAsync(namespaceName);
                processData.ValidationResults["CounterConsistency"] = finalValidation.ValidationErrors.Count == 0;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Final validation completed. Errors: {finalValidation.ValidationErrors.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] ExecuteConcurrentCounterUpdatesAsync failed: {ex}");
                processData.ErrorMessages.Add($"Test execution failed: {ex.Message}");
            }
            finally
            {
                processData.EndTime = DateTime.UtcNow;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Saving process data...");
                await SaveProcessDataAsync(processData);
                
                // Populate test result data for framework
                TestResultData["SuccessfulUpdates"] = processData.SuccessfulUpdates;
                TestResultData["OperationsCompleted"] = processData.OperationsCompleted;
                TestResultData["ConflictsEncountered"] = processData.ConflictsEncountered;
                TestResultData["ValidationResults"] = processData.ValidationResults;
                
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Test result data populated: SuccessfulUpdates={processData.SuccessfulUpdates}");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] ExecuteConcurrentCounterUpdatesAsync completed");
            }
        }

        /// <summary>
        /// Executes multi-page object consistency test
        /// </summary>
        private async Task ExecuteMultiPageObjectConsistencyAsync(string namespaceName)
        {
            var processData = new ProcessTestData
            {
                ProcessId = ProcessId,
                StartTime = DateTime.UtcNow
            };

            try
            {
                await WaitAtBarrierAsync("start", GetParticipantCount(), TimeSpan.FromMinutes(1));

                var operationsCount = GetOperationsCount();
                var customerId = 1000 + int.Parse(ProcessId.Substring(ProcessId.Length - 1)); // Simple ID derivation

                for (int i = 0; i < operationsCount; i++)
                {
                    try
                    {
                        var txnId = _storage!.BeginTransaction();
                        try
                        {
                            // Create order
                            var order = new Order
                            {
                                OrderId = GenerateOrderId(),
                                CustomerId = customerId,
                                Created = DateTime.UtcNow,
                                LastModified = DateTime.UtcNow,
                                Status = "Created",
                                LastModifiedBy = ProcessId,
                                Version = 1
                            };

                            // Create order items
                            var orderItems = new List<OrderItem>();
                            var itemCount = 2 + (i % 3); // 2-4 items per order
                            decimal totalAmount = 0;

                            for (int j = 0; j < itemCount; j++)
                            {
                                var amount = 10.00m + (j * 5.00m);
                                var quantity = 1 + (j % 3);
                                
                                var orderItem = new OrderItem
                                {
                                    OrderItemId = GenerateOrderItemId(),
                                    OrderId = order.OrderId,
                                    Product = $"Product_{ProcessId}_{i}_{j}",
                                    Amount = amount,
                                    Quantity = quantity,
                                    Sequence = j + 1,
                                    Created = DateTime.UtcNow,
                                    Version = 1,
                                    ProcessId = ProcessId
                                };

                                orderItems.Add(orderItem);
                                totalAmount += amount * quantity;
                            }

                            order.TotalAmount = totalAmount;
                            order.ItemCount = itemCount;

                            // Save order and items in transaction
                            _storage.InsertObject(txnId, namespaceName, order);
                            foreach (var item in orderItems)
                            {
                                _storage.InsertObject(txnId, namespaceName, item);
                            }
                            
                            _storage.CommitTransaction(txnId);
                            
                            processData.SuccessfulUpdates++;
                            processData.ModifiedObjectIds.Add(order.OrderId);
                        }
                        catch
                        {
                            _storage.RollbackTransaction(txnId);
                            throw;
                        }
                    }
                    catch (Exception ex)
                    {
                        processData.ConflictsEncountered++;
                        processData.ErrorMessages.Add($"Operation {i}: {ex.Message}");
                    }

                    processData.OperationsCompleted++;
                    await Task.Delay(10);
                }

                await WaitAtBarrierAsync("operations_complete", GetParticipantCount(), TimeSpan.FromMinutes(2));

                // Validate referential integrity
                var validation = await _validator!.ValidateConsistencyAsync(namespaceName);
                processData.ValidationResults["ReferentialIntegrity"] = validation.IsValid;
            }
            catch (Exception ex)
            {
                processData.ErrorMessages.Add($"Test execution failed: {ex.Message}");
            }
            finally
            {
                processData.EndTime = DateTime.UtcNow;
                await SaveProcessDataAsync(processData);
            }
        }

        /// <summary>
        /// Executes transaction isolation validation test
        /// </summary>
        private async Task ExecuteTransactionIsolationValidationAsync(string namespaceName)
        {
            var processData = new ProcessTestData
            {
                ProcessId = ProcessId,
                StartTime = DateTime.UtcNow
            };

            try
            {
                await WaitAtBarrierAsync("start", GetParticipantCount(), TimeSpan.FromMinutes(1));

                // Process A performs long-running updates
                // Process B performs quick reads to validate isolation
                if (ProcessId.EndsWith("A"))
                {
                    await PerformLongRunningUpdatesAsync(namespaceName, processData);
                }
                else
                {
                    await PerformIsolatedReadsAsync(namespaceName, processData);
                }

                await WaitAtBarrierAsync("operations_complete", GetParticipantCount(), TimeSpan.FromMinutes(2));
            }
            catch (Exception ex)
            {
                processData.ErrorMessages.Add($"Test execution failed: {ex.Message}");
            }
            finally
            {
                processData.EndTime = DateTime.UtcNow;
                await SaveProcessDataAsync(processData);
            }
        }

        /// <summary>
        /// Executes concurrent page operations test
        /// </summary>
        private async Task ExecuteConcurrentPageOperationsAsync(string namespaceName)
        {
            var processData = new ProcessTestData
            {
                ProcessId = ProcessId,
                StartTime = DateTime.UtcNow
            };

            try
            {
                await WaitAtBarrierAsync("start", GetParticipantCount(), TimeSpan.FromMinutes(1));

                var operationsCount = GetOperationsCount();

                for (int i = 0; i < operationsCount; i++)
                {
                    try
                    {
                        var txnId = _storage!.BeginTransaction();
                        try
                        {
                            // Create and delete pages rapidly to test coordination
                            var tempOrder = new Order
                            {
                                OrderId = GenerateOrderId(),
                                CustomerId = 999,
                                Created = DateTime.UtcNow,
                                LastModified = DateTime.UtcNow,
                                Version = 1
                            };

                            var pageId = _storage.InsertObject(txnId, namespaceName, tempOrder);
                            // Note: Deletion would require specific page management - skip for now
                            _storage.CommitTransaction(txnId);
                        }
                        catch
                        {
                            _storage.RollbackTransaction(txnId);
                            throw;
                        }

                        processData.SuccessfulUpdates++;
                    }
                    catch (Exception ex)
                    {
                        processData.ConflictsEncountered++;
                        processData.ErrorMessages.Add($"Operation {i}: {ex.Message}");
                    }

                    processData.OperationsCompleted++;
                    await Task.Delay(5);
                }

                await WaitAtBarrierAsync("operations_complete", GetParticipantCount(), TimeSpan.FromMinutes(2));
            }
            catch (Exception ex)
            {
                processData.ErrorMessages.Add($"Test execution failed: {ex.Message}");
            }
            finally
            {
                processData.EndTime = DateTime.UtcNow;
                await SaveProcessDataAsync(processData);
            }
        }

        /// <summary>
        /// Executes cross-process state coordination test
        /// </summary>
        private async Task ExecuteCrossProcessStateCoordinationAsync(string namespaceName)
        {
            var processData = new ProcessTestData
            {
                ProcessId = ProcessId,
                StartTime = DateTime.UtcNow
            };

            try
            {
                await WaitAtBarrierAsync("start", GetParticipantCount(), TimeSpan.FromMinutes(1));

                // Test global state coordination through shared counters
                await ExecuteConcurrentCounterUpdatesAsync(namespaceName);
            }
            catch (Exception ex)
            {
                processData.ErrorMessages.Add($"Test execution failed: {ex.Message}");
            }
            finally
            {
                processData.EndTime = DateTime.UtcNow;
                await SaveProcessDataAsync(processData);
            }
        }

        #region Helper Methods

        private async Task PerformLongRunningUpdatesAsync(string namespaceName, ProcessTestData processData)
        {
            for (int i = 0; i < 10; i++)
            {
                var txnId = _storage!.BeginTransaction();
                try
                {
                    var order = new Order
                    {
                        OrderId = GenerateOrderId(),
                        CustomerId = 1001,
                        Created = DateTime.UtcNow,
                        Status = "Processing",
                        Version = 1
                    };

                    // Simulate long-running operation
                    await Task.Delay(100);
                    
                    order.Status = "Completed";
                    order.LastModified = DateTime.UtcNow;

                    _storage.InsertObject(txnId, namespaceName, order);
                    _storage.CommitTransaction(txnId);
                    processData.SuccessfulUpdates++;
                }
                catch (Exception ex)
                {
                    _storage.RollbackTransaction(txnId);
                    processData.ErrorMessages.Add($"Long update {i}: {ex.Message}");
                }
                processData.OperationsCompleted++;
            }
        }

        private async Task PerformIsolatedReadsAsync(string namespaceName, ProcessTestData processData)
        {
            for (int i = 0; i < 50; i++)
            {
                var txnId = _storage!.BeginTransaction();
                try
                {
                    var orders = new List<Order>();
                    var orderData = _storage.GetMatchingObjects(txnId, namespaceName, "Order_*");
                    foreach (var pageData in orderData.Values)
                    {
                        foreach (var obj in pageData)
                        {
                            if (obj is Order order)
                            {
                                orders.Add(order);
                            }
                        }
                    }
                    var orderList = orders;
                    
                    // Validate that we see consistent states (no partial updates)
                    var processingOrders = orderList.Where(o => o.Status == "Processing").ToList();
                    foreach (var order in processingOrders)
                    {
                        if (order.LastModified > order.Created)
                        {
                            processData.ErrorMessages.Add($"Saw partial update in order {order.OrderId}");
                        }
                    }

                    _storage.RollbackTransaction(txnId);
                    processData.SuccessfulUpdates++;
                }
                catch (Exception ex)
                {
                    _storage.RollbackTransaction(txnId);
                    processData.ErrorMessages.Add($"Isolated read {i}: {ex.Message}");
                }
                processData.OperationsCompleted++;
                await Task.Delay(20);
            }
        }

        private int GetParticipantCount()
        {
            return int.Parse(Environment.GetEnvironmentVariable("TXTDB_PARTICIPANT_COUNT") ?? "2");
        }

        private int GetOperationsCount()
        {
            return int.Parse(Environment.GetEnvironmentVariable("TXTDB_OPERATIONS_COUNT") ?? "50");
        }

        private long GetExpectedFinalValue()
        {
            return long.Parse(Environment.GetEnvironmentVariable("TXTDB_EXPECTED_FINAL_VALUE") ?? "100");
        }

        private long GenerateOrderId()
        {
            return DateTime.UtcNow.Ticks + Thread.CurrentThread.ManagedThreadId + new Random().Next(1000);
        }

        private long GenerateOrderItemId()
        {
            return DateTime.UtcNow.Ticks + Thread.CurrentThread.ManagedThreadId + new Random().Next(10000);
        }

        private async Task SaveProcessDataAsync(ProcessTestData processData)
        {
            try
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Saving process data to directory: {ResultsDirectory}");
                
                // Ensure directory exists
                Directory.CreateDirectory(ResultsDirectory);
                
                var dataPath = Path.Combine(ResultsDirectory, $"{ProcessId}_data.json");
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Process data file path: {dataPath}");
                
                var json = System.Text.Json.JsonSerializer.Serialize(processData, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Process data JSON ({json.Length} chars): {json.Substring(0, Math.Min(200, json.Length))}...");
                
                await File.WriteAllTextAsync(dataPath, json);
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Process data saved successfully");
                
                // Verify file was written
                if (File.Exists(dataPath))
                {
                    var fileInfo = new FileInfo(dataPath);
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Verified file exists: Size={fileInfo.Length} bytes");
                }
                else
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] WARNING: File was not created!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{ProcessId}] Error saving process data: {ex}");
            }
        }

        #endregion
    }
}