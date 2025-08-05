namespace TxtDb.Storage.Models;

/// <summary>
/// Priority levels for flush operations
/// Critical: Transaction commits, immediate durability required
/// Normal: Regular data writes, can be batched for efficiency
/// </summary>
public enum FlushPriority
{
    Normal = 0,
    Critical = 1
}

/// <summary>
/// Configuration for BatchFlushCoordinator
/// Controls batching behavior and performance tuning
/// </summary>
public class BatchFlushConfig
{
    /// <summary>
    /// Maximum number of flush requests to batch together
    /// Default: 100 operations per batch
    /// </summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum delay in milliseconds before forcing a batch flush
    /// Default: 50ms to balance throughput and latency
    /// </summary>
    public int MaxDelayMs { get; set; } = 50;

    /// <summary>
    /// Maximum number of concurrent flush operations
    /// Default: 2 to balance I/O throughput and resource usage
    /// </summary>
    public int MaxConcurrentFlushes { get; set; } = 2;
}

/// <summary>
/// Represents a single flush request in the batch flush system
/// Encapsulates file path, priority, timing, and completion tracking
/// </summary>
public class FlushRequest
{
    /// <summary>
    /// Absolute path to the file that needs to be flushed
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Priority level of this flush request
    /// Critical operations bypass normal batching delays
    /// </summary>
    public FlushPriority Priority { get; }

    /// <summary>
    /// When this request was created
    /// Used for performance monitoring and timeout detection
    /// </summary>
    public DateTime CreatedTime { get; }

    /// <summary>
    /// When this request should be processed (for delayed batching)
    /// </summary>
    public DateTime ScheduledTime { get; }

    /// <summary>
    /// Task completion source for async coordination
    /// Allows callers to await the flush completion
    /// </summary>
    public TaskCompletionSource CompletionSource { get; }

    /// <summary>
    /// Whether this flush request has been completed (successfully or with error)
    /// </summary>
    public bool IsCompleted => CompletionSource.Task.IsCompleted;

    /// <summary>
    /// Creates a new flush request with normal priority and immediate scheduling
    /// </summary>
    /// <param name="filePath">Path to file to be flushed</param>
    /// <param name="priority">Priority level for this flush</param>
    public FlushRequest(string filePath, FlushPriority priority = FlushPriority.Normal)
        : this(filePath, priority, DateTime.UtcNow)
    {
    }

    /// <summary>
    /// Creates a new flush request with specific scheduling time
    /// </summary>
    /// <param name="filePath">Path to file to be flushed</param>
    /// <param name="priority">Priority level for this flush</param>
    /// <param name="scheduledTime">When this flush should be processed</param>
    public FlushRequest(string filePath, FlushPriority priority, DateTime scheduledTime)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        Priority = priority;
        CreatedTime = DateTime.UtcNow;
        ScheduledTime = scheduledTime;
        CompletionSource = new TaskCompletionSource();
    }

    /// <summary>
    /// Marks this flush request as successfully completed
    /// </summary>
    public void SetCompleted()
    {
        CompletionSource.TrySetResult();
    }

    /// <summary>
    /// Marks this flush request as failed with the given error
    /// </summary>
    /// <param name="error">The error that occurred during flushing</param>
    public void SetError(Exception error)
    {
        CompletionSource.TrySetException(error);
    }

    /// <summary>
    /// Marks this flush request as cancelled
    /// </summary>
    public void SetCancelled()
    {
        CompletionSource.TrySetCanceled();
    }
}