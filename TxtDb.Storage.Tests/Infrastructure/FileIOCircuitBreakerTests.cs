using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Models;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Infrastructure;

/// <summary>
/// TDD tests for file I/O circuit breaker functionality
/// Tests circuit breaker behavior during high load conditions and I/O failures
/// 
/// Phase 4 Infrastructure Hardening requirement:
/// - Circuit breaker for file I/O operations during high load
/// - Prevents resource exhaustion and cascade failures
/// - Configurable failure thresholds and recovery mechanisms
/// </summary>
public class FileIOCircuitBreakerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly List<string> _testFiles;
    private readonly List<IDisposable> _disposables;

    public FileIOCircuitBreakerTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "TxtDbFileIOCircuitBreakerTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testFiles = new List<string>();
        _disposables = new List<IDisposable>();
    }

    public void Dispose()
    {
        foreach (var disposable in _disposables)
        {
            disposable?.Dispose();
        }
        
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    /// <summary>
    /// Test: File I/O circuit breaker should remain closed under normal conditions
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldRemainClosed_UnderNormalConditions()
    {
        // ARRANGE: Create file I/O circuit breaker with normal configuration
        var config = new FileIOCircuitBreakerConfig
        {
            FailureThreshold = 5,
            TimeoutMs = 2000,
            MaxConcurrentOperations = 10
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // ACT: Perform multiple successful file operations
        var testFile = CreateTestFile();
        for (int i = 0; i < 10; i++)
        {
            await circuitBreaker.ExecuteAsync(async () =>
            {
                var content = await File.ReadAllTextAsync(testFile);
                await File.WriteAllTextAsync(testFile, content + $"\nLine {i}");
            });
        }
        
        // ASSERT: Circuit breaker should remain closed
        Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
        Assert.Equal(10, circuitBreaker.SuccessfulOperations);
        Assert.Equal(0, circuitBreaker.FailedOperations);
    }

    /// <summary>
    /// Test: File I/O circuit breaker should open when failure threshold is exceeded
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldOpen_WhenFailureThresholdExceeded()
    {
        // ARRANGE: Create circuit breaker with low failure threshold
        var config = new FileIOCircuitBreakerConfig
        {
            FailureThreshold = 3,
            TimeoutMs = 1000
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // ACT: Cause failures by attempting to access non-existent files
        var exceptions = new List<Exception>();
        for (int i = 0; i < 5; i++)
        {
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    await File.ReadAllTextAsync(Path.Combine(_testDirectory, "nonexistent.txt"));
                });
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }
        
        // ASSERT: Circuit breaker should be open after threshold exceeded
        Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        Assert.True(circuitBreaker.ConsecutiveFailures >= config.FailureThreshold);
        Assert.True(exceptions.Count >= config.FailureThreshold);
    }

    /// <summary>
    /// Test: File I/O circuit breaker should reject operations when open
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldRejectOperations_WhenOpen()
    {
        // ARRANGE: Create circuit breaker and force it open
        var config = new FileIOCircuitBreakerConfig
        {
            FailureThreshold = 2,
            TimeoutMs = 10000 // Long timeout to keep breaker open
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // Force circuit breaker open by causing failures
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    await File.ReadAllTextAsync(Path.Combine(_testDirectory, "nonexistent.txt"));
                });
            }
            catch { /* Expected failures */ }
        }
        
        Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        
        // ACT & ASSERT: New operations should be rejected immediately
        var testFile = CreateTestFile();
        var ex = await Assert.ThrowsAsync<CircuitBreakerOpenException>(
            () => circuitBreaker.ExecuteAsync(async () =>
            {
                await File.ReadAllTextAsync(testFile);
            }));
        
        Assert.Contains("Circuit breaker is open", ex.Message);
    }

    /// <summary>
    /// Test: File I/O circuit breaker should transition to half-open after timeout
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldTransitionToHalfOpen_AfterTimeout()
    {
        // ARRANGE: Create circuit breaker with short timeout
        var config = new FileIOCircuitBreakerConfig
        {
            FailureThreshold = 2,
            TimeoutMs = 200, // Short timeout for testing
            HalfOpenMaxAttempts = 3
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // Force circuit breaker open
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    await File.ReadAllTextAsync(Path.Combine(_testDirectory, "nonexistent.txt"));
                });
            }
            catch { /* Expected failures */ }
        }
        
        Assert.Equal(CircuitBreakerState.Open, circuitBreaker.State);
        
        // ACT: Wait for timeout to elapse
        await Task.Delay(config.TimeoutMs + 100);
        
        // ASSERT: Circuit breaker should allow test operations in half-open state
        var testFile = CreateTestFile();
        await circuitBreaker.ExecuteAsync(async () =>
        {
            await File.ReadAllTextAsync(testFile);
        });
        
        // Should now be closed after successful half-open operation
        Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
    }

    /// <summary>
    /// Test: File I/O circuit breaker should handle concurrent operations safely
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldHandleConcurrentOperations_Safely()
    {
        // ARRANGE: Create circuit breaker with high concurrency limits
        var config = new FileIOCircuitBreakerConfig
        {
            MaxConcurrentOperations = 20,
            FailureThreshold = 10,
            TimeoutMs = 2000
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // Create multiple test files
        var testFiles = new List<string>();
        for (int i = 0; i < 30; i++)
        {
            testFiles.Add(CreateTestFile());
        }
        
        // ACT: Perform many concurrent file operations
        var tasks = new Task[30];
        for (int i = 0; i < tasks.Length; i++)
        {
            int fileIndex = i; // Capture for closure
            tasks[i] = circuitBreaker.ExecuteAsync(async () =>
            {
                var content = await File.ReadAllTextAsync(testFiles[fileIndex]);
                await File.WriteAllTextAsync(testFiles[fileIndex], content + "\nUpdated");
            });
        }
        
        await Task.WhenAll(tasks);
        
        // ASSERT: All operations should have completed successfully
        Assert.Equal(CircuitBreakerState.Closed, circuitBreaker.State);
        Assert.Equal(0, circuitBreaker.ConsecutiveFailures);
        Assert.Equal(30, circuitBreaker.SuccessfulOperations);
        Assert.Equal(0, circuitBreaker.FailedOperations);
    }

    /// <summary>
    /// Test: File I/O circuit breaker should limit concurrent operations
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldLimitConcurrentOperations()
    {
        // ARRANGE: Create circuit breaker with low concurrency limit
        var config = new FileIOCircuitBreakerConfig
        {
            MaxConcurrentOperations = 3,
            FailureThreshold = 10,
            OperationTimeoutMs = 1000
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        var testFile = CreateTestFile();
        var concurrentOperations = 0;
        var maxConcurrentOperations = 0;
        var lockObject = new object();
        
        // ACT: Start many operations that will block
        var tasks = new Task[10];
        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = circuitBreaker.ExecuteAsync(async () =>
            {
                lock (lockObject)
                {
                    concurrentOperations++;
                    if (concurrentOperations > maxConcurrentOperations)
                    {
                        maxConcurrentOperations = concurrentOperations;
                    }
                }
                
                // Simulate slow I/O operation
                await Task.Delay(200);
                
                lock (lockObject)
                {
                    concurrentOperations--;
                }
                
                await File.ReadAllTextAsync(testFile);
            });
        }
        
        await Task.WhenAll(tasks);
        
        // ASSERT: Concurrent operations should not exceed limit
        Assert.True(maxConcurrentOperations <= config.MaxConcurrentOperations,
            $"Max concurrent operations was {maxConcurrentOperations}, should be <= {config.MaxConcurrentOperations}");
    }

    /// <summary>
    /// Test: File I/O circuit breaker should timeout long-running operations
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldTimeout_LongRunningOperations()
    {
        // ARRANGE: Create circuit breaker with short operation timeout
        var config = new FileIOCircuitBreakerConfig
        {
            OperationTimeoutMs = 100,
            FailureThreshold = 5
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        // ACT & ASSERT: Long-running operation should timeout
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => circuitBreaker.ExecuteAsync(async () =>
            {
                await Task.Delay(500); // Longer than timeout
                var testFile = CreateTestFile();
                await File.ReadAllTextAsync(testFile);
            }));
        
        Assert.Contains("timed out", ex.Message.ToLower());
    }

    /// <summary>
    /// Test: File I/O circuit breaker should track failure rates accurately
    /// </summary>
    [Fact]
    public async Task FileIOCircuitBreaker_ShouldTrackFailureRates_Accurately()
    {
        // ARRANGE: Create circuit breaker with metrics tracking
        var config = new FileIOCircuitBreakerConfig
        {
            FailureThreshold = 10, // High threshold to avoid opening during test
            FailureRateWindowMs = 10000
        };
        
        var circuitBreaker = new FileIOCircuitBreaker(config);
        _disposables.Add(circuitBreaker);
        
        var testFile = CreateTestFile();
        var totalOperations = 10;
        var expectedSuccesses = 7;
        var expectedFailures = 3;
        
        // ACT: Mix successful and failed operations
        // Successful operations
        for (int i = 0; i < expectedSuccesses; i++)
        {
            await circuitBreaker.ExecuteAsync(async () =>
            {
                await File.ReadAllTextAsync(testFile);
            });
        }
        
        // Failed operations
        for (int i = 0; i < expectedFailures; i++)
        {
            try
            {
                await circuitBreaker.ExecuteAsync(async () =>
                {
                    await File.ReadAllTextAsync(Path.Combine(_testDirectory, $"nonexistent_{i}.txt"));
                });
            }
            catch { /* Expected failures */ }
        }
        
        // ASSERT: Metrics should be accurate
        Assert.Equal(expectedSuccesses, circuitBreaker.SuccessfulOperations);
        Assert.Equal(expectedFailures, circuitBreaker.FailedOperations);
        
        var expectedFailureRate = (double)expectedFailures / totalOperations;
        Assert.Equal(expectedFailureRate, circuitBreaker.FailureRate, 2); // 2 decimal places precision
    }

    private string CreateTestFile()
    {
        var fileName = Path.Combine(_testDirectory, $"test_{Guid.NewGuid()}.txt");
        File.WriteAllText(fileName, "test data");
        _testFiles.Add(fileName);
        return fileName;
    }
}