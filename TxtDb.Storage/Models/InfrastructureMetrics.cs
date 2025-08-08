using TxtDb.Storage.Services.Async;

namespace TxtDb.Storage.Models;

/// <summary>
/// Infrastructure health metrics and monitoring data
/// Phase 4 Infrastructure Hardening - Comprehensive Health Monitoring
/// </summary>
public class InfrastructureHealthMetrics
{
    /// <summary>
    /// Retry policy manager metrics
    /// </summary>
    public RetryMetrics? RetryMetrics { get; set; }
    
    /// <summary>
    /// Transaction recovery manager metrics
    /// </summary>
    public TransactionRecoveryMetrics? RecoveryMetrics { get; set; }
    
    /// <summary>
    /// File I/O circuit breaker status
    /// </summary>
    public CircuitBreakerStatus? FileIOCircuitBreakerStatus { get; set; }
    
    /// <summary>
    /// Batch flush circuit breaker status  
    /// </summary>
    public CircuitBreakerStatus? BatchFlushCircuitBreakerStatus { get; set; }
    
    /// <summary>
    /// Memory pressure information
    /// </summary>
    public MemoryPressureInfo? MemoryPressure { get; set; }
    
    /// <summary>
    /// Memory pressure statistics
    /// </summary>
    public MemoryPressureStatistics? MemoryPressureStats { get; set; }
    
    /// <summary>
    /// Overall infrastructure health status
    /// </summary>
    public InfrastructureHealthStatus OverallStatus { get; set; }
    
    /// <summary>
    /// Timestamp when metrics were collected
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Circuit breaker status information
/// </summary>
public class CircuitBreakerStatus
{
    /// <summary>
    /// Current state of the circuit breaker
    /// </summary>
    public CircuitBreakerState State { get; set; }
    
    /// <summary>
    /// Number of consecutive failures
    /// </summary>
    public int ConsecutiveFailures { get; set; }
    
    /// <summary>
    /// Total operations attempted
    /// </summary>
    public long TotalOperations { get; set; }
    
    /// <summary>
    /// Number of successful operations
    /// </summary>
    public long SuccessfulOperations { get; set; }
    
    /// <summary>
    /// Number of failed operations
    /// </summary>
    public long FailedOperations { get; set; }
    
    /// <summary>
    /// Current failure rate (0.0 to 1.0)
    /// </summary>
    public double FailureRate { get; set; }
    
    /// <summary>
    /// Time when circuit was opened (if applicable)
    /// </summary>
    public DateTime? CircuitOpenTime { get; set; }
    
    /// <summary>
    /// Whether the circuit breaker is currently healthy
    /// </summary>
    public bool IsHealthy => State == CircuitBreakerState.Closed && FailureRate < 0.1;
}

/// <summary>
/// Overall infrastructure health status
/// </summary>
public enum InfrastructureHealthStatus
{
    /// <summary>
    /// All infrastructure components are operating normally
    /// </summary>
    Healthy,
    
    /// <summary>
    /// Some components are experiencing issues but system is still operational
    /// </summary>
    Degraded,
    
    /// <summary>
    /// Critical infrastructure issues that may impact system reliability
    /// </summary>
    Critical,
    
    /// <summary>
    /// Infrastructure components are disabled or not initialized
    /// </summary>
    Disabled
}

/// <summary>
/// Enhanced storage metrics with infrastructure health monitoring
/// </summary>
public static class StorageMetricsExtensions
{
    /// <summary>
    /// Gets comprehensive infrastructure health metrics
    /// </summary>
    public static InfrastructureHealthMetrics GetInfrastructureHealth(this StorageMetrics metrics,
        RetryPolicyManager? retryManager = null,
        TransactionRecoveryManager? recoveryManager = null,
        FileIOCircuitBreaker? fileIOBreaker = null,
        object? batchFlushBreaker = null,
        MemoryPressureDetector? memoryDetector = null)
    {
        var healthMetrics = new InfrastructureHealthMetrics();
        
        // Collect retry metrics
        if (retryManager != null)
        {
            healthMetrics.RetryMetrics = retryManager.GetRetryMetrics();
        }
        
        // Collect recovery metrics
        if (recoveryManager != null)
        {
            healthMetrics.RecoveryMetrics = recoveryManager.GetRecoveryMetrics();
        }
        
        // Collect file I/O circuit breaker status
        if (fileIOBreaker != null)
        {
            healthMetrics.FileIOCircuitBreakerStatus = new CircuitBreakerStatus
            {
                State = fileIOBreaker.State,
                ConsecutiveFailures = fileIOBreaker.ConsecutiveFailures,
                TotalOperations = fileIOBreaker.TotalOperations,
                SuccessfulOperations = fileIOBreaker.SuccessfulOperations,
                FailedOperations = fileIOBreaker.FailedOperations,
                FailureRate = fileIOBreaker.FailureRate
            };
        }
        
        // Collect batch flush circuit breaker status (if available)
        if (batchFlushBreaker != null)
        {
            // Use reflection to get properties since BatchFlushCoordinator has built-in circuit breaker
            var type = batchFlushBreaker.GetType();
            var stateProperty = type.GetProperty("CircuitBreakerState");
            var failuresProperty = type.GetProperty("ConsecutiveFailures");
            var totalOpsProperty = type.GetProperty("TotalOperations");
            var successOpsProperty = type.GetProperty("SuccessfulOperations");
            var failedOpsProperty = type.GetProperty("FailedOperations");
            var failureRateProperty = type.GetProperty("FailureRate");
            
            if (stateProperty != null && failuresProperty != null)
            {
                healthMetrics.BatchFlushCircuitBreakerStatus = new CircuitBreakerStatus
                {
                    State = (CircuitBreakerState)(stateProperty.GetValue(batchFlushBreaker) ?? CircuitBreakerState.Closed),
                    ConsecutiveFailures = (int)(failuresProperty.GetValue(batchFlushBreaker) ?? 0),
                    TotalOperations = (long)(totalOpsProperty?.GetValue(batchFlushBreaker) ?? 0),
                    SuccessfulOperations = (long)(successOpsProperty?.GetValue(batchFlushBreaker) ?? 0),
                    FailedOperations = (long)(failedOpsProperty?.GetValue(batchFlushBreaker) ?? 0),
                    FailureRate = (double)(failureRateProperty?.GetValue(batchFlushBreaker) ?? 0.0)
                };
            }
        }
        
        // Collect memory pressure information
        if (memoryDetector != null)
        {
            healthMetrics.MemoryPressure = memoryDetector.GetCurrentMemoryInfo();
            healthMetrics.MemoryPressureStats = memoryDetector.GetMemoryPressureStatistics();
        }
        
        // Determine overall health status
        healthMetrics.OverallStatus = DetermineOverallHealthStatus(healthMetrics);
        
        return healthMetrics;
    }
    
    /// <summary>
    /// Determines the overall infrastructure health status based on component metrics
    /// </summary>
    private static InfrastructureHealthStatus DetermineOverallHealthStatus(InfrastructureHealthMetrics metrics)
    {
        var issues = 0;
        var criticalIssues = 0;
        
        // Check retry metrics
        if (metrics.RetryMetrics != null)
        {
            var failureRate = metrics.RetryMetrics.TotalOperations > 0 ? 
                (double)metrics.RetryMetrics.FailedOperations / metrics.RetryMetrics.TotalOperations : 0.0;
            if (failureRate > 0.5) criticalIssues++;
            else if (failureRate > 0.2) issues++;
        }
        
        // Check circuit breaker status
        if (metrics.FileIOCircuitBreakerStatus != null)
        {
            if (metrics.FileIOCircuitBreakerStatus.State == CircuitBreakerState.Open)
                criticalIssues++;
            else if (!metrics.FileIOCircuitBreakerStatus.IsHealthy)
                issues++;
        }
        
        if (metrics.BatchFlushCircuitBreakerStatus != null)
        {
            if (metrics.BatchFlushCircuitBreakerStatus.State == CircuitBreakerState.Open)
                criticalIssues++;
            else if (!metrics.BatchFlushCircuitBreakerStatus.IsHealthy)
                issues++;
        }
        
        // Check memory pressure
        if (metrics.MemoryPressure != null)
        {
            if (metrics.MemoryPressure.PressureLevel == MemoryPressureLevel.Emergency)
                criticalIssues++;
            else if (metrics.MemoryPressure.PressureLevel >= MemoryPressureLevel.Critical)
                issues++;
        }
        
        // Determine overall status
        if (criticalIssues > 0) return InfrastructureHealthStatus.Critical;
        if (issues > 0) return InfrastructureHealthStatus.Degraded;
        
        // Check if any components are present
        var hasComponents = metrics.RetryMetrics != null || 
                           metrics.FileIOCircuitBreakerStatus != null ||
                           metrics.BatchFlushCircuitBreakerStatus != null ||
                           metrics.MemoryPressure != null;
                           
        return hasComponents ? InfrastructureHealthStatus.Healthy : InfrastructureHealthStatus.Disabled;
    }
}