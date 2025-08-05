using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TxtDb.Storage.Interfaces;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// Consistency Validator for Multi-Process E2E Tests
    /// 
    /// Validates database-wide invariants and consistency requirements:
    /// - Referential integrity between objects
    /// - Sequential numbering consistency
    /// - Counter value correctness
    /// - Cross-process state consistency
    /// - No data corruption or serialization errors
    /// </summary>
    public class ConsistencyValidator
    {
        private readonly IStorageSubsystem _storage;

        public ConsistencyValidator(IStorageSubsystem storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        /// <summary>
        /// Performs comprehensive consistency validation
        /// </summary>
        public async Task<ConsistencyValidationResult> ValidateConsistencyAsync(string namespaceName)
        {
            var startTime = DateTime.UtcNow;
            var result = new ConsistencyValidationResult
            {
                ValidationTime = startTime,
                IsValid = true
            };

            try
            {
                // Validate orders and order items referential integrity
                await ValidateOrderConsistencyAsync(namespaceName, result);

                // Validate shared counters
                await ValidateCounterConsistencyAsync(namespaceName, result);

                // Validate customer aggregates
                await ValidateCustomerAggregatesAsync(namespaceName, result);

                // Validate sequential numbering
                await ValidateSequentialNumberingAsync(namespaceName, result);

                // Validate version consistency
                await ValidateVersionConsistencyAsync(namespaceName, result);

                // Final validation state
                result.IsValid = result.ValidationErrors.Count == 0;
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ValidationErrors.Add($"Validation exception: {ex.Message}");
            }
            finally
            {
                result.ValidationDuration = DateTime.UtcNow - startTime;
            }

            return result;
        }

        /// <summary>
        /// Validates order and order item referential integrity
        /// </summary>
        private async Task ValidateOrderConsistencyAsync(string namespaceName, ConsistencyValidationResult result)
        {
            var orders = new List<Order>();
            var orderItems = new List<OrderItem>();

            // Begin transaction for consistent read
            var txnId = _storage.BeginTransaction();
            try
            {
                // Load all orders using pattern matching
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
                result.ObjectCounts["Orders"] = orders.Count;

                // Load all order items using pattern matching
                var itemData = _storage.GetMatchingObjects(txnId, namespaceName, "OrderItem_*");
                foreach (var pageData in itemData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is OrderItem item)
                        {
                            orderItems.Add(item);
                        }
                    }
                }
                result.ObjectCounts["OrderItems"] = orderItems.Count;
            }
            catch (Exception ex)
            {
                result.ValidationErrors.Add($"Failed to load objects: {ex.Message}");
                return;
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }

            // Validate referential integrity
            var orderIds = orders.Select(o => o.OrderId).ToHashSet();
            var orphanedItems = orderItems.Where(item => !orderIds.Contains(item.OrderId)).ToList();
            
            if (orphanedItems.Any())
            {
                result.OrphanedReferences.AddRange(
                    orphanedItems.Select(item => $"OrderItem {item.OrderItemId} references non-existent Order {item.OrderId}")
                );
            }

            // Validate order item counts match
            foreach (var order in orders)
            {
                var actualItemCount = orderItems.Count(item => item.OrderId == order.OrderId);
                if (actualItemCount != order.ItemCount)
                {
                    result.ValidationErrors.Add(
                        $"Order {order.OrderId} ItemCount mismatch: expected {order.ItemCount}, actual {actualItemCount}"
                    );
                }

                // Validate total amount matches sum of items
                var actualTotal = orderItems
                    .Where(item => item.OrderId == order.OrderId)
                    .Sum(item => item.Amount * item.Quantity);
                
                if (Math.Abs(actualTotal - order.TotalAmount) > 0.01m)
                {
                    result.ValidationErrors.Add(
                        $"Order {order.OrderId} TotalAmount mismatch: expected {order.TotalAmount}, actual {actualTotal}"
                    );
                }
            }
        }

        /// <summary>
        /// Validates shared counter consistency
        /// </summary>
        private async Task ValidateCounterConsistencyAsync(string namespaceName, ConsistencyValidationResult result)
        {
            var txnId = _storage.BeginTransaction();
            try
            {
                var counters = new List<SharedCounter>();
                var counterData = _storage.GetMatchingObjects(txnId, namespaceName, "SharedCounter_*");
                foreach (var pageData in counterData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is SharedCounter counter)
                        {
                            counters.Add(counter);
                        }
                    }
                }
                var counterList = counters;
                result.ObjectCounts["SharedCounters"] = counterList.Count;

                foreach (var counter in counterList)
                {
                    result.CounterValues[counter.CounterId] = counter.Value;

                    // Validate expected value matches actual value
                    if (counter.ExpectedValue > 0 && counter.Value != counter.ExpectedValue)
                    {
                        result.ValidationErrors.Add(
                            $"Counter {counter.CounterId} value mismatch: expected {counter.ExpectedValue}, actual {counter.Value}"
                        );
                    }

                    // Validate process update count matches updating processes
                    if (counter.UpdatingProcesses.Count != counter.ProcessUpdates)
                    {
                        result.ValidationErrors.Add(
                            $"Counter {counter.CounterId} process count mismatch: ProcessUpdates={counter.ProcessUpdates}, UpdatingProcesses.Count={counter.UpdatingProcesses.Count}"
                        );
                    }

                    // Validate no duplicate process IDs
                    var uniqueProcesses = counter.UpdatingProcesses.Distinct().Count();
                    if (uniqueProcesses != counter.UpdatingProcesses.Count)
                    {
                        result.ValidationErrors.Add(
                            $"Counter {counter.CounterId} has duplicate process IDs in UpdatingProcesses"
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                result.ValidationErrors.Add($"Failed to validate counters: {ex.Message}");
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }
        }

        /// <summary>
        /// Validates customer aggregate consistency
        /// </summary>
        private async Task ValidateCustomerAggregatesAsync(string namespaceName, ConsistencyValidationResult result)
        {
            var txnId = _storage.BeginTransaction();
            try
            {
                var customers = new List<Customer>();
                var customerData = _storage.GetMatchingObjects(txnId, namespaceName, "Customer_*");
                foreach (var pageData in customerData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is Customer customer)
                        {
                            customers.Add(customer);
                        }
                    }
                }
                var customerList = customers;
                result.ObjectCounts["Customers"] = customerList.Count;

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

                foreach (var customer in customerList)
                {
                    var customerOrders = orderList.Where(o => o.CustomerId == customer.CustomerId).ToList();
                    
                    // Validate order count
                    if (customerOrders.Count != customer.OrderCount)
                    {
                        result.ValidationErrors.Add(
                            $"Customer {customer.CustomerId} OrderCount mismatch: expected {customer.OrderCount}, actual {customerOrders.Count}"
                        );
                    }

                    // Validate total spent
                    var actualTotalSpent = customerOrders.Sum(o => o.TotalAmount);
                    if (Math.Abs(actualTotalSpent - customer.TotalSpent) > 0.01m)
                    {
                        result.ValidationErrors.Add(
                            $"Customer {customer.CustomerId} TotalSpent mismatch: expected {customer.TotalSpent}, actual {actualTotalSpent}"
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                result.ValidationErrors.Add($"Failed to validate customer aggregates: {ex.Message}");
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }
        }

        /// <summary>
        /// Validates sequential numbering consistency
        /// </summary>
        private async Task ValidateSequentialNumberingAsync(string namespaceName, ConsistencyValidationResult result)
        {
            var txnId = _storage.BeginTransaction();
            try
            {
                var orderItems = new List<OrderItem>();
                var itemData = _storage.GetMatchingObjects(txnId, namespaceName, "OrderItem_*");
                foreach (var pageData in itemData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is OrderItem item)
                        {
                            orderItems.Add(item);
                        }
                    }
                }
                var itemList = orderItems;

                // Group by order and validate sequence numbers
                var orderGroups = itemList.GroupBy(item => item.OrderId);
                
                foreach (var group in orderGroups)
                {
                    var sequences = group.Select(item => item.Sequence).OrderBy(s => s).ToList();
                    
                    // Validate sequences start at 1 and are consecutive
                    for (int i = 0; i < sequences.Count; i++)
                    {
                        var expectedSequence = i + 1;
                        if (sequences[i] != expectedSequence)
                        {
                            result.SequenceErrors.Add(
                                $"Order {group.Key} has sequence gap: expected {expectedSequence}, found {sequences[i]}"
                            );
                        }
                    }

                    // Validate no duplicate sequences
                    var uniqueSequences = sequences.Distinct().Count();
                    if (uniqueSequences != sequences.Count)
                    {
                        result.SequenceErrors.Add(
                            $"Order {group.Key} has duplicate sequence numbers"
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                result.ValidationErrors.Add($"Failed to validate sequential numbering: {ex.Message}");
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }
        }

        /// <summary>
        /// Validates version consistency (no version conflicts)
        /// </summary>
        private async Task ValidateVersionConsistencyAsync(string namespaceName, ConsistencyValidationResult result)
        {
            var txnId = _storage.BeginTransaction();
            try
            {
                // Load all versioned entities and validate no negative versions
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
                
                var invalidVersionOrders = orderList.Where(o => o.Version < 0).ToList();
                if (invalidVersionOrders.Any())
                {
                    result.ValidationErrors.AddRange(
                        invalidVersionOrders.Select(o => $"Order {o.OrderId} has invalid version: {o.Version}")
                    );
                }

                var orderItems = new List<OrderItem>();
                var itemData = _storage.GetMatchingObjects(txnId, namespaceName, "OrderItem_*");
                foreach (var pageData in itemData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is OrderItem item)
                        {
                            orderItems.Add(item);
                        }
                    }
                }
                var itemList = orderItems;
                
                var invalidVersionItems = itemList.Where(i => i.Version < 0).ToList();
                if (invalidVersionItems.Any())
                {
                    result.ValidationErrors.AddRange(
                        invalidVersionItems.Select(i => $"OrderItem {i.OrderItemId} has invalid version: {i.Version}")
                    );
                }

                var counters = new List<SharedCounter>();
                var counterData = _storage.GetMatchingObjects(txnId, namespaceName, "SharedCounter_*");
                foreach (var pageData in counterData.Values)
                {
                    foreach (var obj in pageData)
                    {
                        if (obj is SharedCounter counter)
                        {
                            counters.Add(counter);
                        }
                    }
                }
                var counterList = counters;
                
                var invalidVersionCounters = counterList.Where(c => c.Version < 0).ToList();
                if (invalidVersionCounters.Any())
                {
                    result.ValidationErrors.AddRange(
                        invalidVersionCounters.Select(c => $"SharedCounter {c.CounterId} has invalid version: {c.Version}")
                    );
                }
            }
            catch (Exception ex)
            {
                result.ValidationErrors.Add($"Failed to validate version consistency: {ex.Message}");
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }
        }

        /// <summary>
        /// Validates that no serialization corruption has occurred
        /// </summary>
        public async Task<bool> ValidateNoSerializationCorruptionAsync(string namespaceName)
        {
            var txnId = _storage.BeginTransaction();
            try
            {
                // Attempt to load all entity types - any corruption should throw
                _storage.GetMatchingObjects(txnId, namespaceName, "Order_*");
                _storage.GetMatchingObjects(txnId, namespaceName, "OrderItem_*");
                _storage.GetMatchingObjects(txnId, namespaceName, "SharedCounter_*");
                _storage.GetMatchingObjects(txnId, namespaceName, "Customer_*");
                
                return true;
            }
            catch (Exception ex)
            {
                // Serialization corruption detected
                return false;
            }
            finally
            {
                _storage.RollbackTransaction(txnId);
            }
        }

        /// <summary>
        /// Quick validation for performance tests
        /// </summary>
        public async Task<bool> QuickValidationAsync(string namespaceName)
        {
            try
            {
                var result = await ValidateConsistencyAsync(namespaceName);
                return result.IsValid;
            }
            catch
            {
                return false;
            }
        }
    }
}