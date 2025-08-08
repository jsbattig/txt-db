using System;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// RetryPolicyManager - Phase 4 Infrastructure Hardening Component
/// Provides exponential backoff retry mechanism for transient failures
/// 
/// Key features:
/// - Configurable retry policies with exponential backoff
/// - Jitter to prevent thundering herd problems
/// - Comprehensive metrics and statistics tracking
/// - Thread-safe operation for concurrent requests
/// - Cancellation token support for graceful cancellation
/// - Integration with circuit breaker patterns
/// 
/// This component is critical for:
/// - Handling transient failures in file I/O operations
/// - Protecting against cascading failures during high load
/// - Providing resilient operation under stress conditions
/// - Maintaining system stability during partial outages
/// </summary>
public class RetryPolicyManager : IDisposable
{
    private readonly RetryPolicyConfig _config;
    private readonly Random _jitterRandom = new Random();
    private readonly object _metricsLock = new object();
    private readonly RetryMetrics _metrics = new RetryMetrics();
    private volatile bool _disposed = false;

    /// <summary>
    /// Maximum number of retry attempts configured for this manager
    /// </summary>
    public int MaxRetries => _config.MaxRetries;

    /// <summary>
    /// Base delay in milliseconds for the first retry attempt
    /// </summary>
    public int BaseDelayMs => _config.BaseDelayMs;

    /// <summary>
    /// Maximum delay in milliseconds between retry attempts
    /// </summary>
    public int MaxDelayMs => _config.MaxDelayMs;

    /// <summary>
    /// Creates a new RetryPolicyManager with the specified configuration
    /// </summary>
    /// <param name="config">Retry policy configuration</param>
    /// <exception cref="ArgumentNullException">Thrown when config is null</exception>
    /// <exception cref="ArgumentException">Thrown when config is invalid</exception>
    public RetryPolicyManager(RetryPolicyConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _config.Validate(); // Throws ArgumentException if invalid
    }

    /// <summary>
    /// Executes an async operation with retry logic using exponential backoff
    /// </summary>
    /// <typeparam name="T">Return type of the operation</typeparam>
    /// <param name="operation">The async operation to execute with retries</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>The result of the successful operation</returns>
    /// <exception cref="ArgumentNullException">Thrown when operation is null</exception>
    /// <exception cref="OperationCanceledException">Thrown when operation is cancelled</exception>
    /// <exception cref="ObjectDisposedException">Thrown when manager is disposed</exception>
    public async Task<T> RetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RetryPolicyManager));

        if (operation == null)
            throw new ArgumentNullException(nameof(operation));

        var attemptCount = 0;
        var totalDelayMs = 0L;
        Exception? lastException = null;

        // Update metrics - starting a new operation
        lock (_metricsLock)
        {
            _metrics.TotalOperations++;
        }

        while (attemptCount <= _config.MaxRetries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // Execute the operation
                var result = await operation().ConfigureAwait(false);

                // Success! Update metrics and return result
                lock (_metricsLock)
                {
                    _metrics.SuccessfulOperations++;
                    if (attemptCount > 0)
                    {
                        _metrics.TotalRetries += attemptCount;
                        _metrics.TotalRetryDelayMs += totalDelayMs;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                lastException = ex;
                attemptCount++;

                // If we've exhausted all attempts, record failure and rethrow
                if (attemptCount > _config.MaxRetries)
                {
                    lock (_metricsLock)
                    {
                        _metrics.FailedOperations++;
                        _metrics.TotalRetries += attemptCount - 1; // Don't count the final failed attempt as a "retry"
                        _metrics.TotalRetryDelayMs += totalDelayMs;
                    }
                    throw;
                }

                // Calculate delay for next attempt (exponential backoff)
                var delayMs = CalculateBackoffDelay(attemptCount);
                totalDelayMs += delayMs;

                // Wait before retrying
                if (delayMs > 0)
                {
                    await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        // This should never be reached due to the logic above, but just in case
        throw lastException ?? new InvalidOperationException("Retry operation failed with no exception recorded");
    }

    /// <summary>
    /// Gets current retry metrics and statistics
    /// </summary>
    /// <returns>Copy of current retry metrics</returns>
    public RetryMetrics GetRetryMetrics()
    {
        lock (_metricsLock)
        {
            return new RetryMetrics
            {
                TotalOperations = _metrics.TotalOperations,
                SuccessfulOperations = _metrics.SuccessfulOperations,
                FailedOperations = _metrics.FailedOperations,
                TotalRetries = _metrics.TotalRetries,
                TotalRetryDelayMs = _metrics.TotalRetryDelayMs
            };
        }
    }

    /// <summary>
    /// Resets all retry metrics to zero
    /// Useful for periodic metrics reporting or testing
    /// </summary>
    public void ResetMetrics()
    {
        lock (_metricsLock)
        {
            _metrics.TotalOperations = 0;
            _metrics.SuccessfulOperations = 0;
            _metrics.FailedOperations = 0;
            _metrics.TotalRetries = 0;
            _metrics.TotalRetryDelayMs = 0;
        }
    }

    /// <summary>
    /// Calculates the exponential backoff delay for a given retry attempt
    /// Includes jitter if configured to prevent thundering herd problems
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (1-based)</param>
    /// <returns>Delay in milliseconds for this attempt</returns>
    private int CalculateBackoffDelay(int attemptNumber)
    {
        if (attemptNumber <= 0)
            return 0;

        // Calculate exponential backoff: baseDelay * (multiplier ^ (attemptNumber - 1))
        var exponentialDelay = _config.BaseDelayMs * Math.Pow(_config.BackoffMultiplier, attemptNumber - 1);

        // Cap at maximum delay
        var cappedDelay = Math.Min(exponentialDelay, _config.MaxDelayMs);

        // Add jitter if enabled (up to 25% variation)
        if (_config.UseJitter)
        {
            var jitterRange = cappedDelay * 0.25; // 25% jitter
            var jitterOffset = (_jitterRandom.NextDouble() * 2.0 - 1.0) * jitterRange; // -25% to +25%
            cappedDelay += jitterOffset;

            // Ensure we don't go negative or exceed max delay
            cappedDelay = Math.Max(0, Math.Min(cappedDelay, _config.MaxDelayMs));
        }

        return (int)Math.Round(cappedDelay);
    }

    /// <summary>
    /// Disposes the retry policy manager and cleans up resources
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            
            // RetryPolicyManager doesn't hold any unmanaged resources
            // but we set the flag to prevent further use
        }
    }
}