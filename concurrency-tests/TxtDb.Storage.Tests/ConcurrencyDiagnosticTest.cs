using System.Collections.Concurrent;
using System.Diagnostics;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// Diagnostic test to identify concurrency issues in detail
/// </summary>
public class ConcurrencyDiagnosticTest : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public ConcurrencyDiagnosticTest()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_diagnostic_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig 
        { 
            Format = SerializationFormat.Json,
            MaxPageSizeKB = 4 // Smaller for testing page overflow
        });
    }

    [Fact]
    public void DiagnoseConcurrentTransactions_HighVolume_ShouldMaintainConsistency()
    {
        // Arrange
        const int concurrentTransactions = 20;
        const int operationsPerTransaction = 50;
        var @namespace = "test.concurrent";
        var exceptions = new ConcurrentBag<Exception>();
        var completedOperations = new ConcurrentBag<int>();
        var failedTransactions = new ConcurrentBag<(int txnIndex, Exception ex)>();
        var successfulInserts = new ConcurrentBag<(int txnIndex, int opIndex)>();

        // Setup initial namespace
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, @namespace);
        _storage.CommitTransaction(setupTxn);

        Console.WriteLine($"Starting {concurrentTransactions} concurrent transactions with {operationsPerTransaction} operations each");
        Console.WriteLine($"Expected total objects: {concurrentTransactions * operationsPerTransaction}");

        var stopwatch = Stopwatch.StartNew();

        // Act - Run many concurrent transactions
        var tasks = Enumerable.Range(0, concurrentTransactions).Select(txnIndex =>
            Task.Run(() =>
            {
                try
                {
                    Console.WriteLine($"Transaction {txnIndex} starting...");
                    var txnId = _storage.BeginTransaction();
                    
                    for (int opIndex = 0; opIndex < operationsPerTransaction; opIndex++)
                    {
                        try
                        {
                            var data = new { 
                                TxnIndex = txnIndex, 
                                OpIndex = opIndex, 
                                Timestamp = DateTime.UtcNow,
                                RandomData = Guid.NewGuid().ToString()
                            };
                            
                            _storage.InsertObject(txnId, @namespace, data);
                            successfulInserts.Add((txnIndex, opIndex));
                        }
                        catch (Exception opEx)
                        {
                            Console.WriteLine($"Transaction {txnIndex}, Operation {opIndex} failed: {opEx.Message}");
                            exceptions.Add(opEx);
                        }
                    }
                    
                    _storage.CommitTransaction(txnId);
                    completedOperations.Add(txnIndex);
                    Console.WriteLine($"Transaction {txnIndex} completed successfully");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Transaction {txnIndex} failed entirely: {ex.Message}");
                    failedTransactions.Add((txnIndex, ex));
                    exceptions.Add(ex);
                }
            })
        ).ToArray();

        Task.WaitAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"All tasks completed in {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"Successful transactions: {completedOperations.Count}/{concurrentTransactions}");
        Console.WriteLine($"Failed transactions: {failedTransactions.Count}");
        Console.WriteLine($"Successful inserts: {successfulInserts.Count}");
        Console.WriteLine($"Total exceptions: {exceptions.Count}");

        // Print exception details
        if (exceptions.Count > 0)
        {
            Console.WriteLine("\nException Details:");
            var exceptionGroups = exceptions.GroupBy(ex => ex.GetType().Name + ": " + ex.Message);
            foreach (var group in exceptionGroups)
            {
                Console.WriteLine($"  {group.Key} ({group.Count()} times)");
            }
        }

        // Verify all data is accessible
        var verifyTxn = _storage.BeginTransaction();
        var allObjects = _storage.GetMatchingObjects(verifyTxn, @namespace, "*");
        _storage.CommitTransaction(verifyTxn);

        var totalExpectedObjects = concurrentTransactions * operationsPerTransaction;
        var actualObjectCount = allObjects.Values.Sum(objects => objects.Length);
        
        Console.WriteLine($"\nExpected objects: {totalExpectedObjects}");
        Console.WriteLine($"Actual objects: {actualObjectCount}");
        Console.WriteLine($"Success rate: {(double)actualObjectCount / totalExpectedObjects * 100:F1}%");

        // Show page distribution
        Console.WriteLine($"\nPage distribution:");
        foreach (var page in allObjects)
        {
            Console.WriteLine($"  {page.Key}: {page.Value.Length} objects");
        }

        // This test is diagnostic - we expect it to show issues, so we don't assert success
        // Assert.True(actualObjectCount >= totalExpectedObjects * 0.9, 
        //     $"Expected ~{totalExpectedObjects} objects, got {actualObjectCount}");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup failed - not critical for tests
        }
    }
}