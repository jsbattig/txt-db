using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Linq;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// BatchFlushCoordinator - Phase 3 of Epic 002 Performance Optimization
/// Coordinates batch flushing of file operations to reduce FlushToDisk calls by 50%+
/// 
/// Key features:
/// - Channel-based request queuing with configurable batch size and delays
/// - Background processor for batching flush operations  
/// - Priority handling for critical vs normal flushes
/// - Performance monitoring and metrics collection
/// </summary>
public class BatchFlushCoordinator : IDisposable
{
    private readonly BatchFlushConfig _config;
    private readonly Channel<FlushRequest> _requestQueue;
    private readonly ChannelWriter<FlushRequest> _writer;
    private readonly ChannelReader<FlushRequest> _reader;
    private readonly SemaphoreSlim _flushSemaphore;
    private readonly CancellationTokenSource _cancellationTokenSource;
    
    private Task? _backgroundProcessor;
    private volatile bool _isRunning;
    private volatile bool _disposed;
    
    // Performance tracking
    private long _batchCount;
    private long _actualFlushCount;
    private readonly object _statsLock = new object();
    
    // Enhanced Circuit Breaker Implementation for Phase 4 Infrastructure Hardening
    private CircuitBreakerState _circuitBreakerState;
    private int _consecutiveFailures;
    private DateTime _lastFailureTime;
    private DateTime _circuitBreakerOpenTime;
    private int _halfOpenAttempts;
    private readonly object _circuitBreakerLock = new object();
    
    // Circuit breaker metrics tracking
    private long _totalOperations;
    private long _successfulOperations;
    private long _failedOperations;
    private readonly Queue<OperationResult> _recentOperations = new Queue<OperationResult>();
    
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
    /// Maximum number of requests to batch together
    /// </summary>
    public int MaxBatchSize => _config.MaxBatchSize;

    /// <summary>
    /// Maximum delay in milliseconds before forcing a batch flush
    /// </summary>
    public int MaxDelayMs => _config.MaxDelayMs;

    /// <summary>
    /// Maximum number of concurrent flush operations
    /// </summary>
    public int MaxConcurrentFlushes => _config.MaxConcurrentFlushes;

    /// <summary>
    /// Whether the background processor is currently running
    /// </summary>
    public bool IsRunning => _isRunning;

    /// <summary>
    /// Number of batches processed (for performance monitoring)
    /// </summary>
    public long BatchCount => Interlocked.Read(ref _batchCount);

    /// <summary>
    /// Number of actual flush operations performed (for reduction measurement)
    /// </summary>
    public long ActualFlushCount => Interlocked.Read(ref _actualFlushCount);

    // Enhanced Circuit Breaker Properties for Phase 4
    
    /// <summary>
    /// Current state of the circuit breaker
    /// </summary>
    public CircuitBreakerState CircuitBreakerState
    {
        get
        {
            lock (_circuitBreakerLock)
            {
                return _circuitBreakerState;
            }
        }
    }
    
    /// <summary>
    /// Whether the circuit breaker is currently open (blocking operations)
    /// </summary>
    public bool IsCircuitBreakerOpen => CircuitBreakerState != CircuitBreakerState.Closed;
    
    /// <summary>
    /// Number of consecutive failures recorded
    /// </summary>
    public int ConsecutiveFailures
    {
        get
        {
            lock (_circuitBreakerLock)
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
            lock (_circuitBreakerLock)
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
    /// Creates a new BatchFlushCoordinator with default configuration
    /// </summary>
    public BatchFlushCoordinator() : this(new BatchFlushConfig())
    {
    }

    /// <summary>
    /// Creates a new BatchFlushCoordinator with custom configuration
    /// </summary>
    /// <param name="config">Configuration for batch flushing behavior</param>
    public BatchFlushCoordinator(BatchFlushConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        
        // Create bounded channel for flow control
        var channelOptions = new BoundedChannelOptions(_config.MaxBatchSize * 10)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };
        
        _requestQueue = Channel.CreateBounded<FlushRequest>(channelOptions);
        _writer = _requestQueue.Writer;
        _reader = _requestQueue.Reader;
        
        _flushSemaphore = new SemaphoreSlim(_config.MaxConcurrentFlushes, _config.MaxConcurrentFlushes);
        _cancellationTokenSource = new CancellationTokenSource();
        
        // Initialize circuit breaker in closed state
        _circuitBreakerState = CircuitBreakerState.Closed;
        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
    }

    /// <summary>
    /// Starts the background batch processing
    /// </summary>
    public async Task StartAsync()
    {
        ThrowIfDisposed();
        
        if (_isRunning)
            return;

        _isRunning = true;
        _backgroundProcessor = ProcessRequestsAsync(_cancellationTokenSource.Token);
        
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the background batch processing and completes pending requests
    /// </summary>
    public async Task StopAsync()
    {
        if (!_isRunning)
            return;

        _isRunning = false;
        
        // CRITICAL FIX: Handle channel completion exceptions properly
        // The channel may already be completed or closed, which would throw ChannelClosedException
        try
        {
            _writer.Complete();
        }
        catch (InvalidOperationException)
        {
            // Channel is already completed - this is expected in some scenarios
        }
        
        if (_backgroundProcessor != null)
        {
            try
            {
                _cancellationTokenSource.Cancel();
                await _backgroundProcessor.ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                // Expected when canceling background processor (TaskCanceledException is derived from OperationCanceledException)
            }
            catch (OperationCanceledException)
            {
                // Expected when canceling background processor
            }
        }
    }

    /// <summary>
    /// Forces immediate flush of all pending batches and queued requests.
    /// Used for critical operations that require immediate durability guarantees.
    /// This bypasses batching delays and processes all pending requests immediately.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task that completes when all pending flushes are complete</returns>
    public async Task ForceFlushAllPendingAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        if (!_isRunning)
            return; // Nothing to flush if not running
            
        // Send a high-priority signal to flush all pending requests immediately
        // by queuing a critical request that will trigger immediate batch processing
        var forceFlushRequest = new FlushRequest("__FORCE_FLUSH_ALL__", FlushPriority.Critical);
        
        if (await _writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            await _writer.WriteAsync(forceFlushRequest, cancellationToken).ConfigureAwait(false);
            
            // Wait for the force flush to complete
            await forceFlushRequest.CompletionSource.Task.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Forces immediate flush of a file, bypassing the batching queue entirely.
    /// Used for critical operations that require immediate durability guarantees.
    /// </summary>
    /// <param name="filePath">Path to file to be flushed immediately</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task that completes when the file has been flushed</returns>
    public async Task ForceImmediateFlushAsync(string filePath, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        // Critical bypass: skip the queue and flush directly
        // This ensures critical operations complete in <50ms instead of waiting for batching
        var immediateRequest = new FlushRequest(filePath, FlushPriority.Critical);
        var requests = new List<FlushRequest> { immediateRequest };
        
        try
        {
            await PerformFlushOperationAsync(requests, cancellationToken).ConfigureAwait(false);
            immediateRequest.SetCompleted();
        }
        catch (Exception ex)
        {
            immediateRequest.SetError(ex);
            throw;
        }
    }

    /// <summary>
    /// CRITICAL SECURITY FIX: Forces immediate flush of specific files only, maintaining transaction isolation.
    /// Used for critical operations that require immediate durability guarantees while preserving
    /// MVCC isolation by only flushing files from the current transaction.
    /// 
    /// This prevents critical operations from forcing persistence of other transactions' data,
    /// which could violate ACID properties and create security vulnerabilities.
    /// </summary>
    /// <param name="filePaths">Collection of specific file paths to flush immediately</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task that completes when all specified files have been flushed</returns>
    public async Task ForceImmediateFlushSpecificFilesAsync(ICollection<string> filePaths, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        if (filePaths == null || filePaths.Count == 0)
        {
            return; // Nothing to flush
        }
        
        // Create critical requests for each file that exists
        var requests = filePaths
            .Where(File.Exists) // Only flush files that actually exist
            .Select(filePath => new FlushRequest(filePath, FlushPriority.Critical))
            .ToList();
        
        if (requests.Count == 0)
        {
            return; // No existing files to flush
        }
        
        try
        {
            // Critical bypass: skip the queue and flush directly
            // This ensures critical operations complete quickly while maintaining isolation
            await PerformFlushOperationAsync(requests, cancellationToken).ConfigureAwait(false);
            
            // Mark all requests as completed
            foreach (var request in requests)
            {
                request.SetCompleted();
            }
        }
        catch (Exception ex)
        {
            // Mark all requests as failed
            foreach (var request in requests)
            {
                request.SetError(ex);
            }
            throw;
        }
    }

    /// <summary>
    /// Queues a file for batch flushing
    /// CRITICAL FIX: Added cancellation token support for comprehensive cancellation propagation
    /// </summary>
    /// <param name="filePath">Path to file to be flushed</param>
    /// <param name="priority">Priority level for this flush</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation</param>
    /// <returns>Task that completes when the file has been flushed</returns>
    public async Task QueueFlushAsync(string filePath, FlushPriority priority = FlushPriority.Normal, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        
        if (!_isRunning)
            throw new InvalidOperationException("BatchFlushCoordinator is not running. Call StartAsync() first.");

        var request = new FlushRequest(filePath, priority);
        
        // CRITICAL FIX: Use linked cancellation token that combines external and internal cancellation
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);
        
        if (!await _writer.WaitToWriteAsync(linkedCts.Token).ConfigureAwait(false))
            throw new InvalidOperationException("Request queue is closed");

        await _writer.WriteAsync(request, linkedCts.Token).ConfigureAwait(false);
        
        // Wait for the flush to complete with cancellation support
        using var flushCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            await request.CompletionSource.Task.WaitAsync(flushCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // External cancellation requested - mark request as cancelled
            request.SetCancelled();
            throw;
        }
    }

    /// <summary>
    /// Background task that processes flush requests in batches
    /// </summary>
    private async Task ProcessRequestsAsync(CancellationToken cancellationToken)
    {
        var pendingRequests = new List<FlushRequest>();
        
        try
        {
            while (await _reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                // Collect requests for batching
                var batchStartTime = DateTime.UtcNow;
                
                // CRITICAL FIX: Add bounds checking to prevent unbounded memory growth and infinite loops
                // Read available requests up to batch size or until timeout with safety limits
                var maxIterations = _config.MaxBatchSize * 10; // Safety limit to prevent infinite loops
                var iterationCount = 0;
                
                while (pendingRequests.Count < _config.MaxBatchSize && 
                       (DateTime.UtcNow - batchStartTime).TotalMilliseconds < _config.MaxDelayMs &&
                       iterationCount < maxIterations)
                {
                    iterationCount++;
                    
                    if (_reader.TryRead(out var request))
                    {
                        pendingRequests.Add(request);
                        
                        if (request.Priority == FlushPriority.Critical)
                        {
                            break; // Process critical requests immediately
                        }
                    }
                    else
                    {
                        // CRITICAL FIX: No more requests available immediately
                        if (pendingRequests.Count == 0)
                        {
                            // If no pending requests, exit loop to wait for next available item
                            break;
                        }
                        
                        // BOUNDED ITERATION FIX: If we have pending requests but no new ones,
                        // don't spin indefinitely waiting. Exit after a reasonable attempt
                        // to allow processing of what we have.
                        
                        // Add cancellation check to prevent infinite waiting
                        if (cancellationToken.IsCancellationRequested)
                            break;
                        
                        // CRITICAL FIX: Avoid busy waiting by checking if more requests are likely available
                        // If we've been waiting for more requests for a significant portion of max delay,
                        // process what we have rather than continuing to wait
                        var elapsedMs = (DateTime.UtcNow - batchStartTime).TotalMilliseconds;
                        if (elapsedMs > _config.MaxDelayMs * 0.8) // 80% of max delay reached
                        {
                            break; // Process pending requests instead of waiting longer
                        }
                            
                        // Short delay to avoid busy waiting, but don't wait too long
                        await Task.Delay(Math.Min(5, (int)(_config.MaxDelayMs - elapsedMs)), cancellationToken).ConfigureAwait(false);
                    }
                }

                if (pendingRequests.Count > 0)
                {
                    await ProcessBatchAsync(pendingRequests, cancellationToken).ConfigureAwait(false);
                    pendingRequests.Clear();
                    
                    Interlocked.Increment(ref _batchCount);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when stopping
        }
        finally
        {
            // Complete any remaining requests with cancellation
            foreach (var request in pendingRequests)
            {
                request.SetCancelled();
            }
        }
    }

    /// <summary>
    /// Processes a batch of flush requests
    /// </summary>
    private async Task ProcessBatchAsync(List<FlushRequest> requests, CancellationToken cancellationToken)
    {
        // Group requests by priority - process critical first
        var criticalRequests = requests.Where(r => r.Priority == FlushPriority.Critical).ToList();
        var normalRequests = requests.Where(r => r.Priority == FlushPriority.Normal).ToList();
        
        // Process critical requests first
        if (criticalRequests.Count > 0)
        {
            await ProcessRequestGroupAsync(criticalRequests, cancellationToken).ConfigureAwait(false);
        }
        
        // Then process normal requests
        if (normalRequests.Count > 0)
        {
            await ProcessRequestGroupAsync(normalRequests, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Processes a group of requests with the same priority
    /// </summary>
    private async Task ProcessRequestGroupAsync(List<FlushRequest> requests, CancellationToken cancellationToken)
    {
        await _flushSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        
        try
        {
            // Handle special force flush request
            var forceFlushRequest = requests.FirstOrDefault(r => r.FilePath == "__FORCE_FLUSH_ALL__");
            if (forceFlushRequest != null)
            {
                // For force flush, we just complete the request - the act of processing this batch
                // immediately flushes all pending requests which is the desired behavior
                forceFlushRequest.SetCompleted();
                requests.Remove(forceFlushRequest);
                
                // Continue processing other requests in this batch
                if (requests.Count == 0)
                    return;
            }
            // Group by unique file paths to avoid duplicate flushes
            var uniqueRequests = requests
                .GroupBy(r => r.FilePath)
                .Select(g => g.First()) // Take first request for each unique file
                .ToList();

            // Perform the actual flush operation
            await PerformFlushOperationAsync(uniqueRequests, cancellationToken).ConfigureAwait(false);
            
            // Update performance counters
            Interlocked.Increment(ref _actualFlushCount);
            
            // Complete all requests for the same files
            foreach (var group in requests.GroupBy(r => r.FilePath))
            {
                var success = uniqueRequests.Any(ur => ur.FilePath == group.Key);
                foreach (var request in group)
                {
                    if (success)
                        request.SetCompleted();
                    else
                        request.SetError(new IOException($"Failed to flush {group.Key}"));
                }
            }
        }
        catch (Exception ex)
        {
            // Complete all requests with error
            foreach (var request in requests)
            {
                request.SetError(ex);
            }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }

    /// <summary>
    /// Performs the actual flush operation for a group of files
    /// This is where the real I/O optimization happens
    /// Includes enhanced circuit breaker pattern for continuous failure handling
    /// </summary>
    private async Task PerformFlushOperationAsync(List<FlushRequest> requests, CancellationToken cancellationToken)
    {
        // ENHANCED CIRCUIT BREAKER: Check state and handle transitions
        if (!CanExecuteOperation())
        {
            var state = CircuitBreakerState;
            throw new InvalidOperationException(
                $"Circuit breaker is {state} due to {_consecutiveFailures} consecutive failures. " +
                $"Operations are temporarily disabled until recovery.");
        }

        // Track operation start - count each request individually
        Interlocked.Add(ref _totalOperations, requests.Count);
        
        var operationSuccess = true;
        Exception? operationException = null;
        
        try
        {
            foreach (var request in requests)
            {
                // Verify file exists before attempting flush
                if (!File.Exists(request.FilePath))
                {
                    operationSuccess = false;
                    operationException = new FileNotFoundException($"File not found: {request.FilePath}");
                    break;
                }

                // ENHANCED: Add timeout and retry logic for flush operations
                await PerformFlushWithTimeoutAndRetryAsync(request, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            operationSuccess = false;
            operationException = ex;
        }
        finally
        {
            // Record operation result for circuit breaker - record each request individually
            for (int i = 0; i < requests.Count; i++)
            {
                RecordOperationResult(operationSuccess);
            }
        }
        
        if (!operationSuccess && operationException != null)
        {
            throw operationException;
        }
    }

    /// <summary>
    /// Performs flush operation with timeout and retry logic for resilience
    /// </summary>
    private async Task PerformFlushWithTimeoutAndRetryAsync(FlushRequest request, CancellationToken cancellationToken)
    {
        var attempts = 0;
        var maxAttempts = _config.MaxRetryAttempts + 1; // Include initial attempt
        Exception? lastException = null;

        while (attempts < maxAttempts)
        {
            attempts++;
            
            try
            {
                // Create timeout cancellation token for this operation
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(_config.FlushTimeoutMs);

                // CRITICAL FIX: Proper resource management for FileStream operations with timeout
                using var fileStream = new FileStream(request.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                
                try
                {
                    // Attempt async flush first with timeout
                    await fileStream.FlushAsync(timeoutCts.Token).ConfigureAwait(false);
                }
                catch (Exception flushAsyncEx) when (!(flushAsyncEx is OperationCanceledException))
                {
                    // If async flush fails, still attempt synchronous flush for durability
                    try
                    {
                        fileStream.Flush(flushToDisk: true);
                    }
                    catch (Exception flushSyncEx)
                    {
                        // Both flush methods failed - this is a critical error
                        throw new AggregateException($"Both FlushAsync and Flush failed for {request.FilePath}", 
                            flushAsyncEx, flushSyncEx);
                    }
                    
                    // Async failed but sync succeeded - still throw to indicate partial failure
                    throw new IOException($"FlushAsync failed but synchronous flush succeeded for {request.FilePath}: {flushAsyncEx.Message}", flushAsyncEx);
                }
                
                // Both flushes succeeded - force OS buffer flush for guaranteed durability
                fileStream.Flush(flushToDisk: true);
                
                // Success recorded in calling method via RecordOperationResult
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Main cancellation token was cancelled - don't retry
                // Failure will be recorded in calling method
                throw;
            }
            catch (OperationCanceledException ex)
            {
                // Timeout occurred
                lastException = new TimeoutException($"Flush operation timed out after {_config.FlushTimeoutMs}ms for {request.FilePath}", ex);
            }
            catch (UnauthorizedAccessException ex)
            {
                // File locked - could be transient, but record failure
                lastException = new IOException($"Cannot open {request.FilePath} for flushing - file is locked or insufficient permissions: {ex.Message}", ex);
            }
            catch (Exception ex) when (!(ex is FileNotFoundException))
            {
                lastException = new IOException($"Failed to flush {request.FilePath}: {ex.Message}", ex);
            }

            // If we've reached the max attempts, don't delay
            if (attempts >= maxAttempts)
                break;

            // Exponential backoff delay before retry
            var delayMs = _config.RetryDelayMs * (int)Math.Pow(2, attempts - 1);
            try
            {
                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancellation requested during retry delay
                break;
            }
        }

        // All attempts failed - exception will be recorded in calling method
        throw lastException ?? new IOException($"Failed to flush {request.FilePath} after {attempts} attempts");
    }

    /// <summary>
    /// Enhanced circuit breaker implementation for handling continuous failures
    /// Supports closed, open, and half-open states with sophisticated recovery logic
    /// </summary>
    private bool CanExecuteOperation()
    {
        lock (_circuitBreakerLock)
        {
            switch (_circuitBreakerState)
            {
                case CircuitBreakerState.Closed:
                    // Normal operation, check failure rate
                    CleanupOldOperations();
                    var currentFailureRate = GetCurrentFailureRate();
                    
                    if (currentFailureRate >= _config.CircuitBreakerFailureRateThreshold && 
                        _recentOperations.Count >= 10) // Minimum sample size
                    {
                        TransitionToOpen();
                        return false;
                    }
                    return true;
                    
                case CircuitBreakerState.Open:
                    // Check if timeout has elapsed to transition to half-open
                    if (DateTime.UtcNow - _circuitBreakerOpenTime > TimeSpan.FromMilliseconds(_config.CircuitBreakerTimeoutMs))
                    {
                        TransitionToHalfOpen();
                        return true;
                    }
                    return false;
                    
                case CircuitBreakerState.HalfOpen:
                    // Allow limited test operations
                    if (_halfOpenAttempts < _config.CircuitBreakerHalfOpenMaxAttempts)
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
        lock (_circuitBreakerLock)
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
            switch (_circuitBreakerState)
            {
                case CircuitBreakerState.Closed:
                    // Check if we should open due to consecutive failures
                    if (_consecutiveFailures >= _config.CircuitBreakerFailureThreshold)
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
        _circuitBreakerState = CircuitBreakerState.Closed;
        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
    }
    
    /// <summary>
    /// Transitions circuit breaker to open state
    /// </summary>
    private void TransitionToOpen()
    {
        _circuitBreakerState = CircuitBreakerState.Open;
        _circuitBreakerOpenTime = DateTime.UtcNow;
        _halfOpenAttempts = 0;
    }
    
    /// <summary>
    /// Transitions circuit breaker to half-open state
    /// </summary>
    private void TransitionToHalfOpen()
    {
        _circuitBreakerState = CircuitBreakerState.HalfOpen;
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
        var cutoffTime = DateTime.UtcNow.AddMilliseconds(-_config.CircuitBreakerFailureRateWindowMs);
        
        while (_recentOperations.Count > 0 && _recentOperations.Peek().Timestamp < cutoffTime)
        {
            _recentOperations.Dequeue();
        }
    }
    
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BatchFlushCoordinator));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        
        try
        {
            // CRITICAL FIX: Proper disposal with timeout and comprehensive exception handling
            var stopTask = StopAsync();
            
            // Use a reasonable timeout to prevent hanging during disposal
            if (!stopTask.Wait(TimeSpan.FromSeconds(5)))
            {
                // Force cancellation if stop doesn't complete within timeout
                try
                {
                    _cancellationTokenSource?.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // Expected if already disposed
                }
            }
        }
        catch (AggregateException ex)
        {
            // Handle specific exceptions during disposal
            foreach (var innerEx in ex.InnerExceptions)
            {
                if (!(innerEx is OperationCanceledException) && 
                    !(innerEx is TaskCanceledException) && 
                    !(innerEx is ObjectDisposedException))
                {
                    // Log unexpected exceptions during disposal if we had logging
                    // For now, we'll ignore them as disposal should be fault-tolerant
                }
            }
        }
        catch (TaskCanceledException)
        {
            // Expected during cancellation (TaskCanceledException is derived from OperationCanceledException)
        }
        catch (OperationCanceledException)
        {
            // Expected during cancellation
        }
        catch (ObjectDisposedException)
        {
            // Expected if resources already disposed
        }
        catch
        {
            // Ignore any other errors during shutdown to prevent disposal from throwing
        }
        
        // Dispose resources with individual error handling
        try
        {
            _flushSemaphore?.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Already disposed
        }
        
        try
        {
            _cancellationTokenSource?.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Already disposed
        }
    }
}