using System.Collections.Concurrent;
using System.Threading.Channels;
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
        
        // Signal completion and wait for background processor to finish
        _writer.Complete();
        
        if (_backgroundProcessor != null)
        {
            _cancellationTokenSource.Cancel();
            await _backgroundProcessor.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Queues a file for batch flushing
    /// </summary>
    /// <param name="filePath">Path to file to be flushed</param>
    /// <param name="priority">Priority level for this flush</param>
    /// <returns>Task that completes when the file has been flushed</returns>
    public async Task QueueFlushAsync(string filePath, FlushPriority priority = FlushPriority.Normal)
    {
        ThrowIfDisposed();
        
        if (!_isRunning)
            throw new InvalidOperationException("BatchFlushCoordinator is not running. Call StartAsync() first.");

        var request = new FlushRequest(filePath, priority);
        
        if (!await _writer.WaitToWriteAsync(_cancellationTokenSource.Token).ConfigureAwait(false))
            throw new InvalidOperationException("Request queue is closed");

        await _writer.WriteAsync(request, _cancellationTokenSource.Token).ConfigureAwait(false);
        
        // Wait for the flush to complete
        await request.CompletionSource.Task.ConfigureAwait(false);
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
                
                // Read available requests up to batch size or until timeout
                while (pendingRequests.Count < _config.MaxBatchSize && 
                       (DateTime.UtcNow - batchStartTime).TotalMilliseconds < _config.MaxDelayMs)
                {
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
                        // No more requests available, wait a bit for more
                        if (pendingRequests.Count == 0)
                            break;
                            
                        await Task.Delay(5, cancellationToken).ConfigureAwait(false);
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
    /// </summary>
    private async Task PerformFlushOperationAsync(List<FlushRequest> requests, CancellationToken cancellationToken)
    {
        foreach (var request in requests)
        {
            // Verify file exists before attempting flush
            if (!File.Exists(request.FilePath))
            {
                throw new FileNotFoundException($"File not found: {request.FilePath}");
            }

            try
            {
                // Perform the actual flush operation
                // This replaces the individual WriteTextWithFlushAsync calls
                using var fileStream = new FileStream(request.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                await fileStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                
                // Force OS buffer flush for durability
                fileStream.Flush(flushToDisk: true);
            }
            catch (Exception ex) when (!(ex is FileNotFoundException))
            {
                throw new IOException($"Failed to flush {request.FilePath}: {ex.Message}", ex);
            }
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
            StopAsync().Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore errors during shutdown
        }
        
        _flushSemaphore?.Dispose();
        _cancellationTokenSource?.Dispose();
    }
}