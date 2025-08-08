using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Infrastructure;

/// <summary>
/// TDD tests for enhanced circuit breaker pattern in BatchFlushCoordinator
/// Tests circuit breaker states, recovery mechanisms, and failure handling
/// 
/// These tests validate the infrastructure hardening requirements from Phase 4:
/// - Circuit breaker prevents cascade failures
/// - Half-open state enables gradual recovery
/// - Configurable failure thresholds and timeouts
/// - Automatic recovery mechanisms
/// </summary>
public class CircuitBreakerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly List<string> _testFiles;
    private readonly List<BatchFlushCoordinator> _coordinators;

    public CircuitBreakerTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "TxtDbCircuitBreakerTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testFiles = new List<string>();
        _coordinators = new List<BatchFlushCoordinator>();
    }

    public void Dispose()
    {
        foreach (var coordinator in _coordinators)
        {
            coordinator?.Dispose();
        }
        
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    /// <summary>
    /// Test: Circuit breaker should remain closed under normal operation
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldRemainClosed_WhenOperationsSucceed()
    {
        // ARRANGE: Create coordinator with circuit breaker
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 10,
            MaxDelayMs = 100,
            CircuitBreakerFailureThreshold = 3,
            CircuitBreakerTimeoutMs = 1000
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        var testFile = CreateTestFile();
        await coordinator.StartAsync();
        
        // ACT: Perform multiple successful operations
        for (int i = 0; i < 10; i++)
        {
            await coordinator.QueueFlushAsync(testFile);
        }
        
        // ASSERT: Circuit breaker should remain closed
        Assert.False(coordinator.IsCircuitBreakerOpen);
        Assert.Equal(0, coordinator.ConsecutiveFailures);
        Assert.Equal(CircuitBreakerState.Closed, coordinator.CircuitBreakerState);
    }

    /// <summary>
    /// Test: Circuit breaker should open after consecutive failures exceed threshold
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldOpen_WhenFailureThresholdExceeded()
    {
        // ARRANGE: Create coordinator with low failure threshold
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 1,
            MaxDelayMs = 50,
            CircuitBreakerFailureThreshold = 3,
            CircuitBreakerTimeoutMs = 2000
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // ACT: Cause failures by attempting to flush non-existent files
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        var exceptions = new List<Exception>();
        
        for (int i = 0; i < 5; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }
        
        // ASSERT: Circuit breaker should be open after threshold exceeded
        Assert.True(coordinator.IsCircuitBreakerOpen);
        Assert.True(coordinator.ConsecutiveFailures >= config.CircuitBreakerFailureThreshold);
        Assert.Equal(CircuitBreakerState.Open, coordinator.CircuitBreakerState);
        Assert.True(exceptions.Count >= config.CircuitBreakerFailureThreshold);
    }

    /// <summary>
    /// Test: Circuit breaker should reject operations when open
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldRejectOperations_WhenOpen()
    {
        // ARRANGE: Create coordinator and force circuit breaker open
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 2,
            CircuitBreakerTimeoutMs = 10000 // Long timeout to keep breaker open
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Force circuit breaker open by causing failures
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch
            {
                // Expected failures
            }
        }
        
        // ACT & ASSERT: New operations should be rejected immediately
        var testFile = CreateTestFile();
        
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => coordinator.QueueFlushAsync(testFile));
        
        Assert.Contains("Circuit breaker is Open", ex.Message);
        Assert.Contains("consecutive failures", ex.Message);
    }

    /// <summary>
    /// Test: Circuit breaker should transition to half-open state after timeout
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldTransitionToHalfOpen_AfterTimeout()
    {
        // ARRANGE: Create coordinator with short timeout
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 2,
            CircuitBreakerTimeoutMs = 200 // Short timeout for testing
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Force circuit breaker open
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch
            {
                // Expected failures
            }
        }
        
        Assert.Equal(CircuitBreakerState.Open, coordinator.CircuitBreakerState);
        
        // ACT: Wait for timeout to elapse
        await Task.Delay(config.CircuitBreakerTimeoutMs + 100);
        
        // ASSERT: Circuit breaker should be ready to transition to half-open
        // Next operation attempt should trigger half-open state
        var testFile = CreateTestFile();
        
        // This should trigger half-open state transition
        await coordinator.QueueFlushAsync(testFile);
        
        Assert.Equal(CircuitBreakerState.Closed, coordinator.CircuitBreakerState);
        Assert.Equal(0, coordinator.ConsecutiveFailures);
    }

    /// <summary>
    /// Test: Circuit breaker should recover to closed state after successful half-open operation
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldRecoverToClosed_AfterSuccessfulHalfOpenOperation()
    {
        // ARRANGE: Create coordinator with circuit breaker
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 2,
            CircuitBreakerTimeoutMs = 200,
            CircuitBreakerHalfOpenMaxAttempts = 3
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Force circuit breaker open
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch { }
        }
        
        // Wait for timeout
        await Task.Delay(config.CircuitBreakerTimeoutMs + 100);
        
        // ACT: Perform successful operation to test recovery
        var testFile = CreateTestFile();
        await coordinator.QueueFlushAsync(testFile);
        
        // ASSERT: Circuit breaker should be closed and reset
        Assert.Equal(CircuitBreakerState.Closed, coordinator.CircuitBreakerState);
        Assert.Equal(0, coordinator.ConsecutiveFailures);
        Assert.False(coordinator.IsCircuitBreakerOpen);
    }

    /// <summary>
    /// Test: Circuit breaker should return to open state if half-open operations fail
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldReturnToOpen_IfHalfOpenOperationsFail()
    {
        // ARRANGE: Create coordinator with circuit breaker
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 2,
            CircuitBreakerTimeoutMs = 200,
            CircuitBreakerHalfOpenMaxAttempts = 2
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Force circuit breaker open
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch { }
        }
        
        // Wait for timeout to trigger half-open state
        await Task.Delay(config.CircuitBreakerTimeoutMs + 100);
        
        // ACT: Attempt operations that will fail in half-open state
        for (int i = 0; i < config.CircuitBreakerHalfOpenMaxAttempts; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch { }
        }
        
        // ASSERT: Circuit breaker should be open again
        Assert.Equal(CircuitBreakerState.Open, coordinator.CircuitBreakerState);
        Assert.True(coordinator.IsCircuitBreakerOpen);
    }

    /// <summary>
    /// Test: Circuit breaker should track failure rates correctly
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldTrackFailureRates_Accurately()
    {
        // ARRANGE: Create coordinator with metrics tracking
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 5,
            CircuitBreakerTimeoutMs = 1000
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        var testFile = CreateTestFile();
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        
        // ACT: Mix successful and failed operations
        var totalOperations = 10;
        var expectedSuccesses = 6;
        var expectedFailures = 4;
        
        // 6 successful operations
        for (int i = 0; i < expectedSuccesses; i++)
        {
            await coordinator.QueueFlushAsync(testFile);
        }
        
        // 4 failed operations
        for (int i = 0; i < expectedFailures; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch { }
        }
        
        // ASSERT: Failure tracking should be accurate
        Assert.Equal(expectedFailures, coordinator.ConsecutiveFailures);
        Assert.Equal(expectedSuccesses, coordinator.SuccessfulOperations);
        Assert.Equal(expectedFailures, coordinator.FailedOperations);
        
        var expectedFailureRate = (double)expectedFailures / totalOperations;
        Assert.Equal(expectedFailureRate, coordinator.FailureRate, 2); // 2 decimal places precision
    }

    /// <summary>
    /// Test: Circuit breaker should handle concurrent operations safely
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldHandleConcurrentOperations_Safely()
    {
        // ARRANGE: Create coordinator with circuit breaker and smaller batches
        var config = new BatchFlushConfig
        {
            MaxBatchSize = 5, // Smaller batches to ensure individual operation tracking
            MaxDelayMs = 10,  // Shorter delay to force more frequent batching
            MaxConcurrentFlushes = 10,
            CircuitBreakerFailureThreshold = 5,
            CircuitBreakerTimeoutMs = 1000
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        // Create multiple test files to avoid deduplication
        var testFiles = new List<string>();
        for (int i = 0; i < 50; i++)
        {
            testFiles.Add(CreateTestFile());
        }
        
        // ACT: Perform many concurrent operations on different files
        var tasks = new Task[50];
        for (int i = 0; i < tasks.Length; i++)
        {
            int fileIndex = i; // Capture for closure
            tasks[i] = coordinator.QueueFlushAsync(testFiles[fileIndex]);
        }
        
        await Task.WhenAll(tasks);
        
        // Wait a bit to ensure all batches are processed
        await Task.Delay(100);
        
        
        // ASSERT: Circuit breaker state should be consistent
        Assert.Equal(CircuitBreakerState.Closed, coordinator.CircuitBreakerState);
        Assert.Equal(0, coordinator.ConsecutiveFailures);
        Assert.Equal(tasks.Length, coordinator.SuccessfulOperations);
    }

    /// <summary>
    /// Test: Circuit breaker should reset consecutive failures on success
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ShouldResetConsecutiveFailures_OnSuccess()
    {
        // ARRANGE: Create coordinator
        var config = new BatchFlushConfig
        {
            CircuitBreakerFailureThreshold = 5
        };
        
        var coordinator = new BatchFlushCoordinator(config);
        _coordinators.Add(coordinator);
        
        await coordinator.StartAsync();
        
        var testFile = CreateTestFile();
        var nonExistentFile = Path.Combine(_testDirectory, "nonexistent.txt");
        
        // ACT: Cause some failures, then succeed
        // 3 failures
        for (int i = 0; i < 3; i++)
        {
            try
            {
                await coordinator.QueueFlushAsync(nonExistentFile);
            }
            catch { }
        }
        
        Assert.Equal(3, coordinator.ConsecutiveFailures);
        
        // 1 success should reset counter
        await coordinator.QueueFlushAsync(testFile);
        
        // ASSERT: Consecutive failures should be reset
        Assert.Equal(0, coordinator.ConsecutiveFailures);
        Assert.Equal(CircuitBreakerState.Closed, coordinator.CircuitBreakerState);
    }

    private string CreateTestFile()
    {
        var fileName = Path.Combine(_testDirectory, $"test_{Guid.NewGuid()}.txt");
        File.WriteAllText(fileName, "test data");
        _testFiles.Add(fileName);
        return fileName;
    }
}