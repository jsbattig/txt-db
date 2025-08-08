using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// FileIOCircuitBreaker - Phase 4 Infrastructure Hardening Component
/// Provides circuit breaker pattern for file I/O operations to prevent resource exhaustion
/// and cascade failures during high load conditions.
/// 
/// Key features:
/// - Circuit breaker states: Closed, Open, Half-Open
/// - Concurrency limiting to prevent thread pool exhaustion
/// - Operation timeouts to prevent hanging operations
/// - Failure rate tracking and recovery mechanisms
/// - Configurable thresholds and recovery timeouts
/// </summary>
public class FileIOCircuitBreaker : IDisposable
{
    private readonly FileIOCircuitBreakerConfig _config;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly object _stateLock = new object();
    
    // Circuit breaker state management
    private CircuitBreakerState _state = CircuitBreakerState.Closed;
    private int _consecutiveFailures = 0;
    private DateTime _lastFailureTime = DateTime.MinValue;
    private DateTime _circuitOpenTime = DateTime.MinValue;
    private int _halfOpenAttempts = 0;
    
    // Operation metrics tracking
    private long _totalOperations = 0;
    private long _successfulOperations = 0;
    private long _failedOperations = 0;
    private readonly Queue<OperationResult> _recentOperations = new Queue<OperationResult>();
    
    private volatile bool _disposed = false;
    
    /// <summary>
    /// Represents an operation result with timestamp for failure rate calculations
    /// </summary>
    private readonly struct OperationResult
    {
        public DateTime Timestamp { get; init; }
        public bool IsSuccess { get; init; }
        
        public OperationResult(bool isSuccess)
        {
            Timestamp = DateTime.UtcNow;
            IsSuccess = isSuccess;
        }
    }
    
    /// <summary>
    /// Current state of the circuit breaker
    /// </summary>
    public CircuitBreakerState State
    {
        get
        {
            lock (_stateLock)
            {
                return _state;
            }
        }
    }
    
    /// <summary>
    /// Number of consecutive failures recorded
    /// </summary>
    public int ConsecutiveFailures
    {
        get
        {
            lock (_stateLock)
            {
                return _consecutiveFailures;
            }
        }
    }
    
    /// <summary>
    /// Total number of operations attempted
    /// </summary>
    public long TotalOperations => Interlocked.Read(ref _totalOperations);
    
    /// <summary>
    /// Number of successful operations
    /// </summary>
    public long SuccessfulOperations => Interlocked.Read(ref _successfulOperations);
    
    /// <summary>
    /// Number of failed operations
    /// </summary>
    public long FailedOperations => Interlocked.Read(ref _failedOperations);
    
    /// <summary>
    /// Current failure rate within the configured time window
    /// </summary>
    public double FailureRate
    {
        get
        {
            lock (_stateLock)
            {
                CleanupOldOperations();
                
                if (_recentOperations.Count == 0)
                    return 0.0;
                    
                var failures = _recentOperations.Count(op => !op.IsSuccess);
                return (double)failures / _recentOperations.Count;
            }
        }
    }
    
    /// <summary>
    /// Creates a new FileIOCircuitBreaker with default configuration
    /// </summary>
    public FileIOCircuitBreaker() : this(new FileIOCircuitBreakerConfig())
    {
    }
    
    /// <summary>
    /// Creates a new FileIOCircuitBreaker with custom configuration
    /// </summary>
    /// <param name="config">Configuration for circuit breaker behavior</param>
    public FileIOCircuitBreaker(FileIOCircuitBreakerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _concurrencyLimiter = new SemaphoreSlim(config.MaxConcurrentOperations, config.MaxConcurrentOperations);
    }
    
    /// <summary>
    /// Executes a file I/O operation with circuit breaker protection
    /// </summary>
    /// <param name="operation">The file I/O operation to execute</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the operation</returns>
    public async Task ExecuteAsync(Func<Task> operation, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        // Check if circuit breaker allows execution
        if (!CanExecuteOperation())
        {
            var state = State;
            throw new CircuitBreakerOpenException(
                $"Circuit breaker is open due to {_consecutiveFailures} consecutive failures. " +
                $"File I/O operations are temporarily disabled until recovery.");
        }
        
        // Acquire concurrency limit
        await _concurrencyLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
        
        // Track operation start
        Interlocked.Increment(ref _totalOperations);
        
        var operationSuccess = false;
        Exception? operationException = null;
        
        try
        {
            // Execute operation with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_config.OperationTimeoutMs);
            
            try
            {
                await operation().WaitAsync(timeoutCts.Token).ConfigureAwait(false);
                operationSuccess = true;
            }
            catch (OperationCanceledException ex) when (timeoutCts.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // Operation timeout
                operationException = new TimeoutException($"File I/O operation timed out after {_config.OperationTimeoutMs}ms", ex);
            }
            catch (Exception ex)
            {
                operationException = ex;
            }
        }
        finally
        {
            // Record operation result
            RecordOperationResult(operationSuccess);
            
            // Release concurrency limit
            _concurrencyLimiter.Release();
        }
        
        // Throw exception if operation failed
        if (!operationSuccess && operationException != null)
        {
            throw operationException;
        }
    }
    
    /// <summary>
    /// Executes a file I/O operation with circuit breaker protection and returns a result
    /// </summary>
    /// <typeparam name="T">The return type</typeparam>
    /// <param name="operation">The file I/O operation to execute</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the operation result</returns>
    public async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        // Check if circuit breaker allows execution
        if (!CanExecuteOperation())
        {
            var state = State;
            throw new CircuitBreakerOpenException(
                $"Circuit breaker is open due to {_consecutiveFailures} consecutive failures. " +
                $"File I/O operations are temporarily disabled until recovery.");
        }
        
        // Acquire concurrency limit
        await _concurrencyLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
        
        // Track operation start
        Interlocked.Increment(ref _totalOperations);
        
        var operationSuccess = false;
        Exception? operationException = null;
        T? result = default;
        
        try
        {
            // Execute operation with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_config.OperationTimeoutMs);
            
            try
            {
                result = await operation().WaitAsync(timeoutCts.Token).ConfigureAwait(false);
                operationSuccess = true;
            }
            catch (OperationCanceledException ex) when (timeoutCts.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // Operation timeout
                operationException = new TimeoutException($"File I/O operation timed out after {_config.OperationTimeoutMs}ms", ex);
            }
            catch (Exception ex)
            {
                operationException = ex;
            }
        }
        finally
        {
            // Record operation result
            RecordOperationResult(operationSuccess);
            
            // Release concurrency limit
            _concurrencyLimiter.Release();
        }
        
        // Throw exception if operation failed
        if (!operationSuccess && operationException != null)
        {
            throw operationException;
        }
        
        return result!;
    }
    
    /// <summary>
    /// Determines if an operation can be executed based on circuit breaker state
    /// </summary>
    private bool CanExecuteOperation()
    {
        lock (_stateLock)
        {
            switch (_state)
            {
                case CircuitBreakerState.Closed:
                    // Normal operation, check failure rate
                    CleanupOldOperations();
                    var currentFailureRate = GetCurrentFailureRate();
                    
                    if (currentFailureRate >= _config.FailureRateThreshold && 
                        _recentOperations.Count >= 10) // Minimum sample size
                    {
                        TransitionToOpen();
                        return false;
                    }
                    return true;
                    
                case CircuitBreakerState.Open:
                    // Check if timeout has elapsed to transition to half-open
                    if (DateTime.UtcNow - _circuitOpenTime > TimeSpan.FromMilliseconds(_config.TimeoutMs))
                    {
                        TransitionToHalfOpen();
                        return true;
                    }
                    return false;
                    
                case CircuitBreakerState.HalfOpen:
                    // Allow limited test operations
                    if (_halfOpenAttempts < _config.HalfOpenMaxAttempts)
                    {
                        _halfOpenAttempts++;
                        return true;
                    }
                    return false;
                    
                default:
                    return false;
            }
        }
    }
    
    /// <summary>
    /// Records the result of an operation for circuit breaker decision making
    /// </summary>
    private void RecordOperationResult(bool isSuccess)
    {
        lock (_stateLock)
        {
            // Add to metrics
            if (isSuccess)
            {
                Interlocked.Increment(ref _successfulOperations);
                _consecutiveFailures = 0; // Reset consecutive failures on success
            }
            else
            {
                Interlocked.Increment(ref _failedOperations);
                _consecutiveFailures++;
                _lastFailureTime = DateTime.UtcNow;
            }
            
            // Add to recent operations window
            _recentOperations.Enqueue(new OperationResult(isSuccess));
            
            // Handle state transitions based on results
            switch (_state)
            {
                case CircuitBreakerState.Closed:
                    // Check if we should open due to consecutive failures
                    if (_consecutiveFailures >= _config.FailureThreshold)
                    {
                        TransitionToOpen();
                    }
                    break;
                    
                case CircuitBreakerState.HalfOpen:
                    if (isSuccess)
                    {
                        // Success in half-open state transitions back to closed
                        TransitionToClosed();
                    }
                    else
                    {
                        // Failure in half-open state transitions back to open
                        TransitionToOpen();
                    }
                    break;
            }
        }
    }
    
    /// <summary>
    /// Transitions circuit breaker to closed state
    /// </summary>
    private void TransitionToClosed()
    {
        _state = CircuitBreakerState.Closed;
        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
    }
    
    /// <summary>
    /// Transitions circuit breaker to open state
    /// </summary>
    private void TransitionToOpen()
    {
        _state = CircuitBreakerState.Open;
        _circuitOpenTime = DateTime.UtcNow;
        _halfOpenAttempts = 0;
    }
    
    /// <summary>
    /// Transitions circuit breaker to half-open state
    /// </summary>
    private void TransitionToHalfOpen()
    {
        _state = CircuitBreakerState.HalfOpen;
        _halfOpenAttempts = 0;
    }
    
    /// <summary>
    /// Gets the current failure rate within the configured time window
    /// </summary>
    private double GetCurrentFailureRate()
    {
        if (_recentOperations.Count == 0)
            return 0.0;
            
        var failures = _recentOperations.Count(op => !op.IsSuccess);
        return (double)failures / _recentOperations.Count;
    }
    
    /// <summary>
    /// Removes operations outside the configured time window
    /// </summary>
    private void CleanupOldOperations()
    {
        var cutoffTime = DateTime.UtcNow.AddMilliseconds(-_config.FailureRateWindowMs);
        
        while (_recentOperations.Count > 0 && _recentOperations.Peek().Timestamp < cutoffTime)
        {
            _recentOperations.Dequeue();
        }
    }
    
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(FileIOCircuitBreaker));
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
            
        _disposed = true;
        _concurrencyLimiter?.Dispose();
    }
}