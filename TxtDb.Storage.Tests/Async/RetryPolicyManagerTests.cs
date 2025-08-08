using System;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Models;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for RetryPolicyManager - Phase 4 Infrastructure Hardening Component
/// 
/// CRITICAL REQUIREMENTS:
/// - Exponential backoff retry mechanism
/// - Configurable retry policies (max retries, base delay, max delay)
/// - Circuit breaker integration for fail-fast behavior
/// - Thread-safe operation for concurrent requests
/// - Detailed retry metrics and logging
/// - Support for different failure types and their specific retry strategies
/// 
/// This component is essential for:
/// - Handling transient failures in file I/O operations
/// - Protecting against cascading failures
/// - Providing resilient operation under stress conditions
/// - Maintaining system stability during partial outages
/// </summary>
public class RetryPolicyManagerTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private RetryPolicyManager? _retryPolicyManager;

    public RetryPolicyManagerTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task RetryPolicyManager_CreateWithValidConfig_ShouldSucceed()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 3,
            BaseDelayMs = 100,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };

        // Act & Assert - This will fail until RetryPolicyManager is implemented
        _retryPolicyManager = new RetryPolicyManager(config);
        
        Assert.NotNull(_retryPolicyManager);
        Assert.Equal(config.MaxRetries, _retryPolicyManager.MaxRetries);
        Assert.Equal(config.BaseDelayMs, _retryPolicyManager.BaseDelayMs);
    }

    [Fact]
    public async Task RetryAsync_OperationSucceedsOnFirstTry_ShouldReturnImmediately()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 3,
            BaseDelayMs = 100,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var executionCount = 0;
        var successfulOperation = new Func<Task<string>>(async () =>
        {
            executionCount++;
            await Task.Delay(10); // Simulate work
            return "success";
        });

        var startTime = DateTime.UtcNow;

        // Act
        var result = await _retryPolicyManager.RetryAsync(successfulOperation);

        // Assert
        var elapsed = DateTime.UtcNow - startTime;
        Assert.Equal("success", result);
        Assert.Equal(1, executionCount);
        Assert.True(elapsed.TotalMilliseconds < 50, "Should return immediately on success");
    }

    [Fact]
    public async Task RetryAsync_OperationFailsButSucceedsOnRetry_ShouldReturnSuccess()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 3,
            BaseDelayMs = 50,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var executionCount = 0;
        var eventuallySuccessfulOperation = new Func<Task<string>>(async () =>
        {
            executionCount++;
            await Task.Delay(10);
            
            if (executionCount <= 2)
            {
                throw new InvalidOperationException($"Transient failure #{executionCount}");
            }
            
            return "success_after_retries";
        });

        var startTime = DateTime.UtcNow;

        // Act
        var result = await _retryPolicyManager.RetryAsync(eventuallySuccessfulOperation);

        // Assert
        var elapsed = DateTime.UtcNow - startTime;
        Assert.Equal("success_after_retries", result);
        Assert.Equal(3, executionCount);
        
        // Should have delays: 0ms (first try) + 50ms (first retry) + 100ms (second retry) = ~150ms minimum
        Assert.True(elapsed.TotalMilliseconds >= 140, $"Expected delays not observed. Elapsed: {elapsed.TotalMilliseconds}ms");
    }

    [Fact]
    public async Task RetryAsync_OperationAlwaysFails_ShouldThrowAfterMaxRetries()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 2,
            BaseDelayMs = 10,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var executionCount = 0;
        var alwaysFailingOperation = new Func<Task<string>>(async () =>
        {
            executionCount++;
            await Task.Delay(5);
            throw new InvalidOperationException($"Persistent failure #{executionCount}");
        });

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => 
            _retryPolicyManager.RetryAsync(alwaysFailingOperation));

        Assert.Contains("Persistent failure #3", exception.Message); // Should be on attempt 3 (1 original + 2 retries)
        Assert.Equal(3, executionCount); // 1 original attempt + 2 retries
    }

    [Fact]
    public async Task RetryAsync_ExponentialBackoff_ShouldIncreaseDelaysBetweenRetries()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 3,
            BaseDelayMs = 100,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0,
            UseJitter = false  // Disable jitter for predictable timing tests
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var attemptTimes = new List<DateTime>();
        var alwaysFailingOperation = new Func<Task<string>>(async () =>
        {
            attemptTimes.Add(DateTime.UtcNow);
            await Task.Delay(5);
            throw new InvalidOperationException("Test failure");
        });

        // Act
        try
        {
            await _retryPolicyManager.RetryAsync(alwaysFailingOperation);
        }
        catch (InvalidOperationException)
        {
            // Expected to fail after all retries
        }

        // Assert - Check exponential backoff timing
        Assert.Equal(4, attemptTimes.Count); // 1 original + 3 retries

        // Verify exponential delays: 0ms, 100ms, 200ms, 400ms
        var delay1 = (attemptTimes[1] - attemptTimes[0]).TotalMilliseconds;
        var delay2 = (attemptTimes[2] - attemptTimes[1]).TotalMilliseconds;
        var delay3 = (attemptTimes[3] - attemptTimes[2]).TotalMilliseconds;

        // Allow 20ms tolerance for timing precision
        Assert.True(delay1 >= 80 && delay1 <= 120, $"First retry delay should be ~100ms, was {delay1}ms");
        Assert.True(delay2 >= 180 && delay2 <= 220, $"Second retry delay should be ~200ms, was {delay2}ms");
        Assert.True(delay3 >= 380 && delay3 <= 420, $"Third retry delay should be ~400ms, was {delay3}ms");
    }

    [Fact]
    public async Task RetryAsync_MaxDelayLimit_ShouldCapBackoffDelay()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 5,
            BaseDelayMs = 100,
            MaxDelayMs = 300, // Cap at 300ms
            BackoffMultiplier = 3.0, // Aggressive multiplier
            UseJitter = false  // Disable jitter for predictable timing tests
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var attemptTimes = new List<DateTime>();
        var alwaysFailingOperation = new Func<Task<string>>(async () =>
        {
            attemptTimes.Add(DateTime.UtcNow);
            await Task.Delay(1);
            throw new InvalidOperationException("Test failure");
        });

        // Act
        try
        {
            await _retryPolicyManager.RetryAsync(alwaysFailingOperation);
        }
        catch (InvalidOperationException)
        {
            // Expected to fail
        }

        // Assert - Check that delays are capped at MaxDelayMs
        Assert.Equal(6, attemptTimes.Count); // 1 original + 5 retries

        // Without cap: delays would be 100ms, 300ms, 900ms, 2700ms, 8100ms
        // With cap: delays should be 100ms, 300ms, 300ms, 300ms, 300ms
        for (int i = 1; i < attemptTimes.Count; i++)
        {
            var delay = (attemptTimes[i] - attemptTimes[i - 1]).TotalMilliseconds;
            
            if (i == 1)
            {
                Assert.True(delay >= 80 && delay <= 120, $"First delay should be ~100ms, was {delay}ms");
            }
            else
            {
                Assert.True(delay >= 280 && delay <= 320, $"Capped delay should be ~300ms, was {delay}ms");
            }
        }
    }

    [Fact]
    public async Task GetRetryMetrics_AfterRetryOperations_ShouldProvideAccurateStatistics()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 2,
            BaseDelayMs = 10,
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        // Execute various operations to build metrics
        var executionCount = 0;
        var partiallyFailingOperation = new Func<Task<string>>(async () =>
        {
            executionCount++;
            await Task.Delay(1);
            
            if (executionCount <= 1)
            {
                throw new InvalidOperationException("Transient failure");
            }
            return "success";
        });

        await _retryPolicyManager.RetryAsync(partiallyFailingOperation);
        executionCount = 0; // Reset for next test

        try
        {
            await _retryPolicyManager.RetryAsync<string>(async () =>
            {
                executionCount++;
                await Task.Delay(1);
                throw new InvalidOperationException("Always fails");
            });
        }
        catch (InvalidOperationException) { /* Expected */ }

        // Act
        var metrics = _retryPolicyManager.GetRetryMetrics();

        // Assert
        Assert.NotNull(metrics);
        Assert.Equal(2, metrics.TotalOperations);
        Assert.Equal(1, metrics.SuccessfulOperations);
        Assert.Equal(1, metrics.FailedOperations);
        Assert.True(metrics.TotalRetries >= 3); // At least 1 + 2 retries from the failing operation
        Assert.True(metrics.AverageRetriesPerOperation >= 1.0);
    }

    [Fact]
    public async Task RetryAsync_CancellationRequested_ShouldRespectCancellation()
    {
        // Arrange
        var config = new RetryPolicyConfig
        {
            MaxRetries = 5,
            BaseDelayMs = 1000, // Long delay to allow cancellation
            MaxDelayMs = 5000,
            BackoffMultiplier = 2.0
        };
        _retryPolicyManager = new RetryPolicyManager(config);

        var cancellationTokenSource = new CancellationTokenSource();
        var executionCount = 0;
        
        var slowFailingOperation = new Func<Task<string>>(async () =>
        {
            executionCount++;
            await Task.Delay(10);
            throw new InvalidOperationException("Test failure");
        });

        // Cancel after 100ms
        _ = Task.Delay(100).ContinueWith(_ => cancellationTokenSource.Cancel());

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            _retryPolicyManager.RetryAsync(slowFailingOperation, cancellationTokenSource.Token));

        // Should have been cancelled before completing all retries
        Assert.True(executionCount < 6, $"Should have been cancelled before completing all attempts. Actual attempts: {executionCount}");
    }

    [Fact]
    public void RetryPolicyManager_InvalidConfig_ShouldThrowArgumentException()
    {
        // Test invalid configurations
        Assert.Throws<ArgumentException>(() => new RetryPolicyManager(new RetryPolicyConfig 
        { 
            MaxRetries = -1,  // Invalid
            BaseDelayMs = 100,
            MaxDelayMs = 5000 
        }));

        Assert.Throws<ArgumentException>(() => new RetryPolicyManager(new RetryPolicyConfig 
        { 
            MaxRetries = 3,
            BaseDelayMs = 0,  // Invalid
            MaxDelayMs = 5000 
        }));

        Assert.Throws<ArgumentException>(() => new RetryPolicyManager(new RetryPolicyConfig 
        { 
            MaxRetries = 3,
            BaseDelayMs = 1000,
            MaxDelayMs = 100  // Invalid - less than base delay
        }));
    }

    public void Dispose()
    {
        _retryPolicyManager?.Dispose();
    }
}