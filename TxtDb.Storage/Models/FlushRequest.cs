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
/// Circuit breaker states for infrastructure hardening
/// Used to prevent cascade failures and enable gradual recovery
/// </summary>
public enum CircuitBreakerState
{
    /// <summary>
    /// Circuit breaker is closed, operations flow normally
    /// </summary>
    Closed = 0,

    /// <summary>
    /// Circuit breaker is open, operations are rejected immediately
    /// </summary>
    Open = 1,

    /// <summary>
    /// Circuit breaker is half-open, allowing limited test operations
    /// </summary>
    HalfOpen = 2
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

    /// <summary>
    /// Timeout in milliseconds for individual flush operations
    /// Default: 5000ms (5 seconds) to prevent hanging operations
    /// </summary>
    public int FlushTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// Maximum number of retry attempts for failed flush operations
    /// Default: 2 retries for transient failures
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 2;

    /// <summary>
    /// Delay in milliseconds between retry attempts
    /// Default: 100ms exponential backoff base
    /// </summary>
    public int RetryDelayMs { get; set; } = 100;

    // Circuit Breaker Configuration for Phase 4 Infrastructure Hardening

    /// <summary>
    /// Number of consecutive failures required to open the circuit breaker
    /// Default: 5 failures to balance fault tolerance and fail-fast behavior
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    /// <summary>
    /// Timeout in milliseconds before circuit breaker transitions from open to half-open state
    /// Default: 30000ms (30 seconds) to allow sufficient recovery time
    /// </summary>
    public int CircuitBreakerTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Maximum number of test operations allowed in half-open state
    /// Default: 3 attempts to validate system recovery
    /// </summary>
    public int CircuitBreakerHalfOpenMaxAttempts { get; set; } = 3;

    /// <summary>
    /// Window size in milliseconds for calculating failure rates
    /// Default: 60000ms (1 minute) for meaningful failure rate calculations
    /// </summary>
    public int CircuitBreakerFailureRateWindowMs { get; set; } = 60000;

    /// <summary>
    /// Failure rate threshold (0.0 to 1.0) that triggers circuit breaker opening
    /// Default: 0.5 (50% failure rate) to detect systemic issues while allowing transient failures
    /// </summary>
    public double CircuitBreakerFailureRateThreshold { get; set; } = 0.5;
}

/// <summary>
/// Configuration for FileIOCircuitBreaker infrastructure hardening component
/// Controls file I/O circuit breaker behavior during high load conditions
/// </summary>
public class FileIOCircuitBreakerConfig
{
    /// <summary>
    /// Number of consecutive failures required to open the circuit breaker
    /// Default: 5 failures to balance fault tolerance and fail-fast behavior
    /// </summary>
    public int FailureThreshold { get; set; } = 5;

    /// <summary>
    /// Timeout in milliseconds before circuit breaker transitions from open to half-open state
    /// Default: 30000ms (30 seconds) to allow sufficient recovery time
    /// </summary>
    public int TimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Maximum number of test operations allowed in half-open state
    /// Default: 3 attempts to validate system recovery
    /// </summary>
    public int HalfOpenMaxAttempts { get; set; } = 3;

    /// <summary>
    /// Window size in milliseconds for calculating failure rates
    /// Default: 60000ms (1 minute) for meaningful failure rate calculations
    /// </summary>
    public int FailureRateWindowMs { get; set; } = 60000;

    /// <summary>
    /// Failure rate threshold (0.0 to 1.0) that triggers circuit breaker opening
    /// Default: 0.6 (60% failure rate) for file I/O operations - higher than batch flush due to more volatile nature of file operations
    /// </summary>
    public double FailureRateThreshold { get; set; } = 0.6;

    /// <summary>
    /// Maximum number of concurrent file I/O operations allowed
    /// Default: 10 concurrent operations to prevent resource exhaustion
    /// </summary>
    public int MaxConcurrentOperations { get; set; } = 10;

    /// <summary>
    /// Timeout in milliseconds for individual file I/O operations
    /// Default: 5000ms (5 seconds) to prevent hanging operations
    /// </summary>
    public int OperationTimeoutMs { get; set; } = 5000;
}

/// <summary>
/// Exception thrown when circuit breaker is open and operations are rejected
/// Part of the infrastructure hardening system to prevent cascade failures
/// </summary>
public class CircuitBreakerOpenException : InvalidOperationException
{
    public CircuitBreakerOpenException(string message) : base(message)
    {
    }

    public CircuitBreakerOpenException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Memory pressure levels for infrastructure hardening
/// Used to indicate current system memory pressure and trigger appropriate responses
/// </summary>
public enum MemoryPressureLevel
{
    /// <summary>
    /// Normal memory usage - no restrictions needed
    /// </summary>
    Normal = 0,

    /// <summary>
    /// Warning level - memory usage is elevated, start conservative measures
    /// </summary>
    Warning = 1,

    /// <summary>
    /// Critical level - memory usage is very high, aggressive measures needed
    /// </summary>
    Critical = 2,

    /// <summary>
    /// Emergency level - system is near out-of-memory, emergency actions required
    /// </summary>
    Emergency = 3
}

/// <summary>
/// Configuration for MemoryPressureDetector infrastructure hardening component
/// Controls memory monitoring behavior and response thresholds
/// </summary>
public class MemoryPressureConfig
{
    /// <summary>
    /// Memory usage threshold in MB for warning level pressure
    /// Default: 1024MB (1GB) - conservative threshold for most systems
    /// </summary>
    public long WarningThresholdMB { get; set; } = 1024;

    /// <summary>
    /// Memory usage threshold in MB for critical level pressure
    /// Default: 2048MB (2GB) - aggressive measures needed beyond this
    /// </summary>
    public long CriticalThresholdMB { get; set; } = 2048;

    /// <summary>
    /// Memory usage threshold in MB for emergency level pressure
    /// Default: 3072MB (3GB) - emergency actions required beyond this
    /// </summary>
    public long EmergencyThresholdMB { get; set; } = 3072;

    /// <summary>
    /// Maximum allowed memory usage in MB before blocking new allocations
    /// Default: 4096MB (4GB) - hard limit for memory usage
    /// </summary>
    public long MaxAllowedMemoryMB { get; set; } = 4096;

    /// <summary>
    /// Interval in milliseconds between memory pressure checks
    /// Default: 1000ms (1 second) - balance between responsiveness and overhead
    /// </summary>
    public int MonitoringIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Window size in milliseconds for memory pressure statistics
    /// Default: 60000ms (1 minute) - sufficient for meaningful statistics
    /// </summary>
    public int StatisticsWindowMs { get; set; } = 60000;

    /// <summary>
    /// Whether to automatically trigger garbage collection under memory pressure
    /// Default: true - helps prevent out-of-memory conditions
    /// </summary>
    public bool AutoTriggerGC { get; set; } = true;

    /// <summary>
    /// Minimum interval in milliseconds between automatic GC triggers
    /// Default: 5000ms (5 seconds) - prevents excessive GC overhead
    /// </summary>
    public int MinGCIntervalMs { get; set; } = 5000;
}

/// <summary>
/// Snapshot of current memory information for pressure detection
/// </summary>
public class MemoryPressureInfo
{
    /// <summary>
    /// Total system memory in MB
    /// </summary>
    public long TotalMemoryMB { get; set; }

    /// <summary>
    /// Currently used memory in MB
    /// </summary>
    public long UsedMemoryMB { get; set; }

    /// <summary>
    /// Available memory in MB
    /// </summary>
    public long AvailableMemoryMB { get; set; }

    /// <summary>
    /// Current memory pressure level
    /// </summary>
    public MemoryPressureLevel PressureLevel { get; set; }

    /// <summary>
    /// Timestamp when this information was collected
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// GC generation 0 collection count
    /// </summary>
    public int Gen0Collections { get; set; }

    /// <summary>
    /// GC generation 1 collection count
    /// </summary>
    public int Gen1Collections { get; set; }

    /// <summary>
    /// GC generation 2 collection count
    /// </summary>
    public int Gen2Collections { get; set; }
}

/// <summary>
/// Memory pressure statistics over a time window
/// </summary>
public class MemoryPressureStatistics
{
    /// <summary>
    /// Number of memory samples collected
    /// </summary>
    public int SampleCount { get; set; }

    /// <summary>
    /// Average used memory in MB over the window
    /// </summary>
    public double AverageUsedMemoryMB { get; set; }

    /// <summary>
    /// Peak used memory in MB during the window
    /// </summary>
    public long PeakUsedMemoryMB { get; set; }

    /// <summary>
    /// Minimum used memory in MB during the window
    /// </summary>
    public long MinUsedMemoryMB { get; set; }

    /// <summary>
    /// Time spent in warning level in milliseconds
    /// </summary>
    public long TimeInWarningMs { get; set; }

    /// <summary>
    /// Time spent in critical level in milliseconds
    /// </summary>
    public long TimeInCriticalMs { get; set; }

    /// <summary>
    /// Time spent in emergency level in milliseconds
    /// </summary>
    public long TimeInEmergencyMs { get; set; }

    /// <summary>
    /// Number of times garbage collection was triggered due to pressure
    /// </summary>
    public int PressureTriggeredGCCount { get; set; }
}

/// <summary>
/// Configuration for RetryPolicyManager infrastructure hardening component
/// Controls retry behavior with exponential backoff for transient failures
/// </summary>
public class RetryPolicyConfig
{
    /// <summary>
    /// Maximum number of retry attempts for failed operations
    /// Default: 3 retries - balances resilience with operation timeout
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Base delay in milliseconds for the first retry
    /// Default: 100ms - quick initial retry for transient failures
    /// </summary>
    public int BaseDelayMs { get; set; } = 100;

    /// <summary>
    /// Maximum delay in milliseconds between retry attempts
    /// Default: 30000ms (30 seconds) - prevents unbounded backoff delays
    /// </summary>
    public int MaxDelayMs { get; set; } = 30000;

    /// <summary>
    /// Multiplier for exponential backoff calculation
    /// Default: 2.0 - standard exponential backoff: delay = baseDelay * (multiplier ^ attempt)
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Whether to add random jitter to delays to prevent thundering herd
    /// Default: true - adds up to 25% random variation to delay times
    /// </summary>
    public bool UseJitter { get; set; } = true;

    /// <summary>
    /// Validates the configuration and throws ArgumentException if invalid
    /// </summary>
    public void Validate()
    {
        if (MaxRetries < 0)
            throw new ArgumentException("MaxRetries must be non-negative", nameof(MaxRetries));
        
        if (BaseDelayMs <= 0)
            throw new ArgumentException("BaseDelayMs must be positive", nameof(BaseDelayMs));
        
        if (MaxDelayMs < BaseDelayMs)
            throw new ArgumentException("MaxDelayMs must be greater than or equal to BaseDelayMs", nameof(MaxDelayMs));
        
        if (BackoffMultiplier <= 1.0)
            throw new ArgumentException("BackoffMultiplier must be greater than 1.0", nameof(BackoffMultiplier));
    }
}

/// <summary>
/// Statistics and metrics for retry operations
/// Provides insight into retry behavior and system resilience
/// </summary>
public class RetryMetrics
{
    /// <summary>
    /// Total number of operations attempted through retry manager
    /// </summary>
    public long TotalOperations { get; set; }

    /// <summary>
    /// Number of operations that succeeded (eventually)
    /// </summary>
    public long SuccessfulOperations { get; set; }

    /// <summary>
    /// Number of operations that failed after all retries
    /// </summary>
    public long FailedOperations { get; set; }

    /// <summary>
    /// Total number of retry attempts across all operations
    /// </summary>
    public long TotalRetries { get; set; }

    /// <summary>
    /// Average number of retries per operation
    /// </summary>
    public double AverageRetriesPerOperation => 
        TotalOperations > 0 ? (double)TotalRetries / TotalOperations : 0.0;

    /// <summary>
    /// Success rate (0.0 to 1.0) of operations
    /// </summary>
    public double SuccessRate => 
        TotalOperations > 0 ? (double)SuccessfulOperations / TotalOperations : 0.0;

    /// <summary>
    /// Total time spent in retry delays (milliseconds)
    /// </summary>
    public long TotalRetryDelayMs { get; set; }

    /// <summary>
    /// Average delay time per retry attempt (milliseconds)
    /// </summary>
    public double AverageDelayPerRetry => 
        TotalRetries > 0 ? (double)TotalRetryDelayMs / TotalRetries : 0.0;
}

/// <summary>
/// Configuration for TransactionRecoveryManager infrastructure hardening component
/// Controls transaction recovery behavior for handling partial failures
/// </summary>
public class TransactionRecoveryConfig
{
    /// <summary>
    /// Path to the recovery journal file for persisting recovery state
    /// Default: "./recovery.journal" - stores transaction recovery information
    /// </summary>
    public string JournalPath { get; set; } = "./recovery.journal";

    /// <summary>
    /// Maximum number of recovery attempts for a single transaction
    /// Default: 3 attempts - balances recovery success with operation timeout
    /// </summary>
    public int MaxRecoveryAttempts { get; set; } = 3;

    /// <summary>
    /// Timeout in milliseconds for recovery operations
    /// Default: 30000ms (30 seconds) - prevents hanging recovery operations
    /// </summary>
    public int RecoveryTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Whether to automatically attempt recovery from journal on startup
    /// Default: true - ensures system integrity after crashes
    /// </summary>
    public bool AutoRecoveryOnStartup { get; set; } = true;

    /// <summary>
    /// Maximum age in hours for journal entries before cleanup
    /// Default: 24 hours - prevents journal file from growing indefinitely
    /// </summary>
    public int JournalRetentionHours { get; set; } = 24;

    /// <summary>
    /// Whether to create backup copies of files before recovery operations
    /// Default: true - provides additional safety during recovery
    /// </summary>
    public bool CreateBackups { get; set; } = true;

    /// <summary>
    /// Directory for storing backup files during recovery
    /// Default: "./recovery_backups" - separate location for recovery backups
    /// </summary>
    public string BackupDirectory { get; set; } = "./recovery_backups";
}

/// <summary>
/// Types of operations that can be recovered by TransactionRecoveryManager
/// </summary>
public enum RecoveryOperationType
{
    /// <summary>
    /// File write operation that may need rollback
    /// </summary>
    FileWrite = 0,

    /// <summary>
    /// File deletion operation that may need restoration
    /// </summary>
    FileDelete = 1,

    /// <summary>
    /// File rename/move operation that may need reversal
    /// </summary>
    FileRename = 2,

    /// <summary>
    /// Directory creation operation that may need cleanup
    /// </summary>
    DirectoryCreate = 3,

    /// <summary>
    /// Directory deletion operation that may need restoration
    /// </summary>
    DirectoryDelete = 4,

    /// <summary>
    /// Test operation for simulating slow operations
    /// </summary>
    SlowOperation = 999
}

/// <summary>
/// Current state of a transaction recovery process
/// </summary>
public enum RecoveryState
{
    /// <summary>
    /// Recovery has not started yet
    /// </summary>
    NotStarted = 0,

    /// <summary>
    /// Recovery is currently in progress
    /// </summary>
    InProgress = 1,

    /// <summary>
    /// Recovery completed successfully
    /// </summary>
    Completed = 2,

    /// <summary>
    /// Recovery failed and could not be completed
    /// </summary>
    Failed = 3,

    /// <summary>
    /// Recovery was cancelled before completion
    /// </summary>
    Cancelled = 4
}

/// <summary>
/// Represents a single operation that can be recovered by TransactionRecoveryManager
/// </summary>
public class RecoveryOperation
{
    /// <summary>
    /// Type of operation to be recovered
    /// </summary>
    public RecoveryOperationType OperationType { get; set; }

    /// <summary>
    /// Primary file path involved in the operation
    /// </summary>
    public string FilePath { get; set; } = string.Empty;

    /// <summary>
    /// Secondary file path for operations like rename/move
    /// </summary>
    public string? SecondaryFilePath { get; set; }

    /// <summary>
    /// Original content of the file before the operation
    /// Used for rollback scenarios
    /// </summary>
    public string? OriginalContent { get; set; }

    /// <summary>
    /// New content that the operation intended to write
    /// Used for forward recovery scenarios
    /// </summary>
    public string? NewContent { get; set; }

    /// <summary>
    /// Timestamp when the operation was originally attempted
    /// </summary>
    public DateTime OperationTimestamp { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Whether this operation completed successfully
    /// </summary>
    public bool IsCompleted { get; set; } = false;

    /// <summary>
    /// Error message if the operation failed
    /// </summary>
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Context information for a transaction recovery process
/// </summary>
public class TransactionRecoveryContext
{
    /// <summary>
    /// Unique identifier for the transaction being recovered
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Current state of the recovery process
    /// </summary>
    public RecoveryState State { get; set; } = RecoveryState.NotStarted;

    /// <summary>
    /// List of operations to be recovered as part of this transaction
    /// </summary>
    public List<RecoveryOperation> Operations { get; set; } = new List<RecoveryOperation>();

    /// <summary>
    /// Timestamp when recovery began
    /// </summary>
    public DateTime StartTime { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Timestamp when recovery completed (if applicable)
    /// </summary>
    public DateTime? EndTime { get; set; }

    /// <summary>
    /// Number of recovery attempts made for this transaction
    /// </summary>
    public int AttemptCount { get; set; } = 0;

    /// <summary>
    /// Error message if recovery failed
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Path to journal entry for this recovery context
    /// </summary>
    public string? JournalEntryPath { get; set; }
}

/// <summary>
/// Result of a transaction recovery operation
/// </summary>
public class TransactionRecoveryResult
{
    /// <summary>
    /// Whether the recovery operation succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// Final state of the recovery process
    /// </summary>
    public RecoveryState State { get; set; }

    /// <summary>
    /// Transaction ID that was recovered
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Number of operations that were successfully recovered
    /// </summary>
    public int OperationsRecovered { get; set; }

    /// <summary>
    /// Number of operations that failed to recover
    /// </summary>
    public int OperationsFailed { get; set; }

    /// <summary>
    /// Total time taken for the recovery operation in milliseconds
    /// </summary>
    public long RecoveryTimeMs { get; set; }

    /// <summary>
    /// Error message if recovery failed
    /// </summary>
    public string ErrorMessage { get; set; } = string.Empty;

    /// <summary>
    /// Additional details about the recovery process
    /// </summary>
    public string Details { get; set; } = string.Empty;
}

/// <summary>
/// Statistics and metrics for transaction recovery operations
/// </summary>
public class TransactionRecoveryMetrics
{
    /// <summary>
    /// Total number of recovery attempts made
    /// </summary>
    public long TotalRecoveryAttempts { get; set; }

    /// <summary>
    /// Number of successful recovery operations
    /// </summary>
    public long SuccessfulRecoveries { get; set; }

    /// <summary>
    /// Number of failed recovery operations
    /// </summary>
    public long FailedRecoveries { get; set; }

    /// <summary>
    /// Success rate (0.0 to 1.0) of recovery operations
    /// </summary>
    public double SuccessRate => 
        TotalRecoveryAttempts > 0 ? (double)SuccessfulRecoveries / TotalRecoveryAttempts : 0.0;

    /// <summary>
    /// Total time spent in recovery operations (milliseconds)
    /// </summary>
    public long TotalRecoveryTimeMs { get; set; }

    /// <summary>
    /// Average time per recovery operation (milliseconds)
    /// </summary>
    public double AverageRecoveryTimeMs => 
        TotalRecoveryAttempts > 0 ? (double)TotalRecoveryTimeMs / TotalRecoveryAttempts : 0.0;

    /// <summary>
    /// Number of transactions recovered from journal on startup
    /// </summary>
    public long JournalRecoveries { get; set; }

    /// <summary>
    /// Number of operations rolled back to original state
    /// </summary>
    public long RollbackOperations { get; set; }

    /// <summary>
    /// Number of operations recovered forward to intended state
    /// </summary>
    public long ForwardRecoveries { get; set; }
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