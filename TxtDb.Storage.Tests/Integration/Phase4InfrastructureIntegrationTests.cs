using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;

namespace TxtDb.Storage.Tests.Integration;

/// <summary>
/// Phase 4 Infrastructure Hardening - Comprehensive Integration Tests
/// Tests the complete integration of all infrastructure components with AsyncStorageSubsystem
/// </summary>
public class Phase4InfrastructureIntegrationTests : IDisposable
{
    private readonly string _testDataPath;
    
    public Phase4InfrastructureIntegrationTests()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), $"Phase4IntegrationTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDataPath);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_testDataPath))
        {
            Directory.Delete(_testDataPath, recursive: true);
        }
    }
    
    [Fact]
    public async Task CompleteInfrastructureIntegration_ShouldInitializeAllComponents()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        
        // Act
        await storage.InitializeAsync(_testDataPath, config);
        
        // Assert - All infrastructure components should be initialized
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics);
        Assert.NotEqual(InfrastructureHealthStatus.Disabled, healthMetrics.OverallStatus);
        Assert.NotNull(storage.StorageMetrics);
        
        // Verify component initialization
        Assert.NotNull(healthMetrics.RetryMetrics);
        Assert.NotNull(healthMetrics.RecoveryMetrics);
        Assert.NotNull(healthMetrics.FileIOCircuitBreakerStatus);
        Assert.NotNull(healthMetrics.MemoryPressure);
        Assert.NotNull(healthMetrics.MemoryPressureStats);
    }
    
    [Fact]
    public async Task InfrastructureDisabled_ShouldSkipComponentInitialization()
    {
        // Arrange
        var config = new StorageConfig
        {
            Infrastructure = new InfrastructureConfig { Enabled = false }
        };
        var storage = new AsyncStorageSubsystem();
        
        // Act
        await storage.InitializeAsync(_testDataPath, config);
        
        // Assert - Infrastructure should be disabled
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.Equal(InfrastructureHealthStatus.Disabled, healthMetrics.OverallStatus);
        Assert.Null(storage.StorageMetrics);
    }
    
    [Fact]
    public async Task StorageOperationsWithInfrastructure_ShouldRecordMetrics()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Perform storage operations
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "test_namespace");
        
        var insertedPageId = await storage.InsertObjectAsync(transactionId, "test_namespace", "test data 1");
        var readData = await storage.ReadPageAsync(transactionId, "test_namespace", insertedPageId);
        
        await storage.CommitTransactionAsync(transactionId);
        
        // Assert - Operations should be recorded in metrics
        var metrics = storage.StorageMetrics!;
        Assert.True(metrics.TotalOperations > 0);
        Assert.True(metrics.TotalReadOperations > 0);
        Assert.True(metrics.TotalWriteOperations > 0);
        Assert.True(metrics.TotalTransactionOperations > 0);
        
        // Verify health metrics
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.Equal(InfrastructureHealthStatus.Healthy, healthMetrics.OverallStatus);
    }
    
    [Fact]
    public async Task RetryPolicyIntegration_ShouldRetryFailedOperations()
    {
        // Arrange - Configure with aggressive retry policy
        var config = CreateInfrastructureEnabledConfig();
        config.Infrastructure.RetryPolicy.MaxRetries = 3;
        config.Infrastructure.RetryPolicy.BaseDelayMs = 10;
        
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Perform operations that might need retries
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "retry_test");
        
        var pageId = await storage.InsertObjectAsync(transactionId, "retry_test", "test data");
        await storage.CommitTransactionAsync(transactionId);
        
        // Assert - Retry metrics should be available
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics.RetryMetrics);
        Assert.True(healthMetrics.RetryMetrics.TotalOperations > 0);
    }
    
    [Fact]
    public async Task CircuitBreakerIntegration_ShouldTrackOperations()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Perform file I/O operations
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "circuit_test");
        
        for (int i = 0; i < 5; i++)
        {
            await storage.InsertObjectAsync(transactionId, "circuit_test", $"data_{i}");
        }
        
        await storage.CommitTransactionAsync(transactionId);
        
        // Assert - Circuit breaker should track operations
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics.FileIOCircuitBreakerStatus);
        Assert.Equal(CircuitBreakerState.Closed, healthMetrics.FileIOCircuitBreakerStatus.State);
        Assert.True(healthMetrics.FileIOCircuitBreakerStatus.TotalOperations > 0);
        Assert.True(healthMetrics.FileIOCircuitBreakerStatus.IsHealthy);
    }
    
    [Fact]
    public async Task MemoryPressureIntegration_ShouldMonitorMemoryUsage()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        config.Infrastructure.AutoStartMemoryMonitoring = true;
        config.Infrastructure.MemoryPressure.MonitoringIntervalMs = 100; // Fast monitoring for test
        
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Wait for memory monitoring to start
        await Task.Delay(200);
        
        // Act - Perform operations that use memory
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "memory_test");
        
        // Insert multiple objects to use memory
        for (int i = 0; i < 10; i++)
        {
            await storage.InsertObjectAsync(transactionId, "memory_test", $"Large data object {i} with substantial content to use memory");
        }
        
        await storage.CommitTransactionAsync(transactionId);
        
        // Assert - Memory pressure should be monitored
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics.MemoryPressure);
        Assert.NotNull(healthMetrics.MemoryPressureStats);
        Assert.True(healthMetrics.MemoryPressure.UsedMemoryMB > 0);
        Assert.Equal(MemoryPressureLevel.Normal, healthMetrics.MemoryPressure.PressureLevel);
    }
    
    [Fact]
    public async Task TransactionRecoveryIntegration_ShouldHandleStartupRecovery()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        config.Infrastructure.AutoRecoveryOnStartup = true;
        
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Recovery should happen during initialization
        // Since there are no incomplete transactions, recovery should succeed silently
        
        // Assert - Recovery metrics should be available
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics.RecoveryMetrics);
        
        // Perform normal operations to ensure recovery didn't break anything
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "recovery_test");
        var pageId = await storage.InsertObjectAsync(transactionId, "recovery_test", "recovery data");
        await storage.CommitTransactionAsync(transactionId);
        
        // Verify operation succeeded
        var readTransactionId = await storage.BeginTransactionAsync();
        var data = await storage.ReadPageAsync(readTransactionId, "recovery_test", pageId);
        await storage.CommitTransactionAsync(readTransactionId);
        
        Assert.Single(data);
        Assert.Equal("recovery data", data[0]);
    }
    
    [Fact]
    public async Task InfrastructureHealthStatus_ShouldReflectSystemHealth()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Perform normal operations
        var transactionId = await storage.BeginTransactionAsync();
        await storage.CreateNamespaceAsync(transactionId, "health_test");
        await storage.InsertObjectAsync(transactionId, "health_test", "health data");
        await storage.CommitTransactionAsync(transactionId);
        
        // Assert - System should be healthy
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.Equal(InfrastructureHealthStatus.Healthy, healthMetrics.OverallStatus);
        
        // Verify individual component health
        Assert.True(healthMetrics.FileIOCircuitBreakerStatus!.IsHealthy);
        Assert.Equal(CircuitBreakerState.Closed, healthMetrics.FileIOCircuitBreakerStatus.State);
        Assert.Equal(MemoryPressureLevel.Normal, healthMetrics.MemoryPressure!.PressureLevel);
        
        // Verify metrics collection
        Assert.True(healthMetrics.RetryMetrics!.TotalOperations >= 0);
        Assert.True(healthMetrics.RecoveryMetrics!.TotalRecoveryAttempts >= 0);
    }
    
    [Fact]
    public async Task ConcurrentOperationsWithInfrastructure_ShouldMaintainDataIntegrity()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Perform concurrent operations
        var tasks = new List<Task>();
        const int concurrentTransactions = 5;
        const int objectsPerTransaction = 3;
        
        for (int i = 0; i < concurrentTransactions; i++)
        {
            var taskIndex = i;
            tasks.Add(Task.Run(async () =>
            {
                var transactionId = await storage.BeginTransactionAsync();
                await storage.CreateNamespaceAsync(transactionId, $"concurrent_test_{taskIndex}");
                
                var insertedPageIds = new List<string>();
                for (int j = 0; j < objectsPerTransaction; j++)
                {
                    var pageId = await storage.InsertObjectAsync(transactionId, $"concurrent_test_{taskIndex}", $"data_{taskIndex}_{j}");
                    insertedPageIds.Add(pageId);
                }
                
                // Read back the data to verify
                foreach (var pageId in insertedPageIds)
                {
                    var data = await storage.ReadPageAsync(transactionId, $"concurrent_test_{taskIndex}", pageId);
                    Assert.NotEmpty(data);
                }
                
                await storage.CommitTransactionAsync(transactionId);
            }));
        }
        
        await Task.WhenAll(tasks);
        
        // Assert - All operations should complete successfully
        var healthMetrics = storage.GetInfrastructureHealth();
        var metrics = storage.StorageMetrics!;
        
        Assert.True(metrics.TotalOperations >= concurrentTransactions * (1 + objectsPerTransaction * 2)); // namespace + inserts + reads
        Assert.True(metrics.TotalTransactionOperations >= concurrentTransactions * 2); // begin + commit
        Assert.Equal(InfrastructureHealthStatus.Healthy, healthMetrics.OverallStatus);
        
        // Verify all circuit breakers remain closed
        Assert.Equal(CircuitBreakerState.Closed, healthMetrics.FileIOCircuitBreakerStatus!.State);
    }
    
    [Fact]
    public async Task PerformanceTargetsWithInfrastructure_ShouldMaintainEpic002Requirements()
    {
        // Arrange
        var config = CreateInfrastructureEnabledConfig();
        var storage = new AsyncStorageSubsystem();
        await storage.InitializeAsync(_testDataPath, config);
        
        // Act - Measure performance of operations
        const int operationCount = 100;
        var startTime = DateTime.UtcNow;
        
        for (int i = 0; i < operationCount; i++)
        {
            var transactionId = await storage.BeginTransactionAsync();
            var pageId = await storage.InsertObjectAsync(transactionId, "perf_test", $"performance data {i}");
            await storage.CommitTransactionAsync(transactionId, FlushPriority.Critical); // Test critical path performance
        }
        
        var endTime = DateTime.UtcNow;
        var totalTimeMs = (endTime - startTime).TotalMilliseconds;
        var averageTimePerOperation = totalTimeMs / operationCount;
        
        // Assert - Performance should meet Epic002 targets
        Assert.True(averageTimePerOperation < 50, $"Average operation time {averageTimePerOperation:F2}ms exceeds 50ms target");
        
        // Verify metrics
        var metrics = storage.StorageMetrics!;
        Assert.Equal(operationCount, metrics.TotalTransactionOperations / 2); // begin + commit pairs
        Assert.True(metrics.GetSuccessRate() >= 99.0, "Success rate should be at least 99%");
        
        // Verify infrastructure health
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.Equal(InfrastructureHealthStatus.Healthy, healthMetrics.OverallStatus);
    }
    
    /// <summary>
    /// Creates a storage configuration with all infrastructure components enabled
    /// </summary>
    private static StorageConfig CreateInfrastructureEnabledConfig()
    {
        return new StorageConfig
        {
            Infrastructure = new InfrastructureConfig
            {
                Enabled = true,
                AutoRecoveryOnStartup = true,
                AutoStartMemoryMonitoring = false, // Manual control for most tests
                
                RetryPolicy = new RetryPolicyConfig
                {
                    MaxRetries = 3,
                    BaseDelayMs = 50,
                    MaxDelayMs = 1000,
                    BackoffMultiplier = 2.0,
                    UseJitter = true
                },
                
                TransactionRecovery = new TransactionRecoveryConfig
                {
                    MaxRecoveryAttempts = 3,
                    RecoveryTimeoutMs = 30000,
                    JournalPath = "" // Will be set automatically
                },
                
                FileIOCircuitBreaker = new FileIOCircuitBreakerConfig
                {
                    FailureThreshold = 5,
                    TimeoutMs = 30000,
                    HalfOpenMaxAttempts = 3,
                    FailureRateThreshold = 0.5,
                    FailureRateWindowMs = 60000,
                    MaxConcurrentOperations = 10,
                    OperationTimeoutMs = 5000
                },
                
                BatchFlushCircuitBreaker = new BatchFlushCircuitBreakerConfig
                {
                    FailureThreshold = 5,
                    TimeoutMs = 30000,
                    HalfOpenMaxAttempts = 3,
                    FailureRateThreshold = 0.5,
                    FailureRateWindowMs = 60000,
                    MaxConcurrentOperations = 10,
                    OperationTimeoutMs = 10000
                },
                
                MemoryPressure = new MemoryPressureConfig
                {
                    WarningThresholdMB = 1000,
                    CriticalThresholdMB = 1500,
                    EmergencyThresholdMB = 2000,
                    MaxAllowedMemoryMB = 2500,
                    MonitoringIntervalMs = 1000,
                    StatisticsWindowMs = 60000,
                    AutoTriggerGC = false, // Don't trigger GC in tests
                    MinGCIntervalMs = 10000
                }
            }
        };
    }
}