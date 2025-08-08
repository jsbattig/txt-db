namespace TxtDb.Storage.Models;

/// <summary>
/// Configuration for all infrastructure hardening components
/// Phase 4 Infrastructure Hardening - Unified Configuration System
/// </summary>
public class InfrastructureConfig
{
    /// <summary>
    /// Configuration for retry policy behavior
    /// </summary>
    public RetryPolicyConfig RetryPolicy { get; set; } = new();
    
    /// <summary>
    /// Configuration for transaction recovery behavior
    /// </summary>
    public TransactionRecoveryConfig TransactionRecovery { get; set; } = new();
    
    /// <summary>
    /// Configuration for file I/O circuit breaker
    /// </summary>
    public FileIOCircuitBreakerConfig FileIOCircuitBreaker { get; set; } = new();
    
    /// <summary>
    /// Configuration for batch flush circuit breaker
    /// </summary>
    public BatchFlushCircuitBreakerConfig BatchFlushCircuitBreaker { get; set; } = new();
    
    /// <summary>
    /// Configuration for memory pressure detection
    /// </summary>
    public MemoryPressureConfig MemoryPressure { get; set; } = new();
    
    /// <summary>
    /// Whether to enable infrastructure components
    /// Can be disabled for testing or when operating in minimal mode
    /// </summary>
    public bool Enabled { get; set; } = true;
    
    /// <summary>
    /// Whether to auto-start recovery on system initialization
    /// </summary>
    public bool AutoRecoveryOnStartup { get; set; } = true;
    
    /// <summary>
    /// Whether to enable memory pressure monitoring automatically
    /// </summary>
    public bool AutoStartMemoryMonitoring { get; set; } = true;
    
    /// <summary>
    /// Validates the infrastructure configuration
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid</exception>
    public void Validate()
    {
        if (!Enabled)
            return; // Skip validation when infrastructure is disabled
            
        // Validate retry policy config
        RetryPolicy.Validate();
        
        // Note: TransactionRecovery.JournalPath can be empty - it will be set automatically during initialization
        
        // Validate circuit breaker thresholds are reasonable
        if (FileIOCircuitBreaker.FailureThreshold <= 0)
        {
            throw new ArgumentException("FileIOCircuitBreaker.FailureThreshold must be greater than 0");
        }
        
        if (BatchFlushCircuitBreaker.FailureThreshold <= 0)
        {
            throw new ArgumentException("BatchFlushCircuitBreaker.FailureThreshold must be greater than 0");
        }
        
        // Validate memory pressure thresholds
        if (MemoryPressure.WarningThresholdMB <= 0 ||
            MemoryPressure.CriticalThresholdMB <= MemoryPressure.WarningThresholdMB ||
            MemoryPressure.EmergencyThresholdMB <= MemoryPressure.CriticalThresholdMB)
        {
            throw new ArgumentException("MemoryPressure thresholds must be positive and in ascending order (Warning < Critical < Emergency)");
        }
    }
}

/// <summary>
/// Configuration for batch flush circuit breaker behavior
/// </summary>
public class BatchFlushCircuitBreakerConfig
{
    /// <summary>
    /// Number of consecutive failures before opening the circuit
    /// Default: 5 failures
    /// </summary>
    public int FailureThreshold { get; set; } = 5;
    
    /// <summary>
    /// Timeout in milliseconds before attempting to close an open circuit
    /// Default: 30000ms (30 seconds)
    /// </summary>
    public int TimeoutMs { get; set; } = 30000;
    
    /// <summary>
    /// Maximum number of operations to allow in half-open state
    /// Default: 3 operations
    /// </summary>
    public int HalfOpenMaxAttempts { get; set; } = 3;
    
    /// <summary>
    /// Failure rate threshold (0.0 to 1.0) for opening circuit based on percentage
    /// Default: 0.5 (50% failure rate)
    /// </summary>
    public double FailureRateThreshold { get; set; } = 0.5;
    
    /// <summary>
    /// Time window in milliseconds for calculating failure rate
    /// Default: 60000ms (1 minute)
    /// </summary>
    public int FailureRateWindowMs { get; set; } = 60000;
    
    /// <summary>
    /// Maximum number of concurrent batch flush operations
    /// Default: 10 concurrent operations
    /// </summary>
    public int MaxConcurrentOperations { get; set; } = 10;
    
    /// <summary>
    /// Timeout in milliseconds for individual batch flush operations
    /// Default: 10000ms (10 seconds)
    /// </summary>
    public int OperationTimeoutMs { get; set; } = 10000;
}