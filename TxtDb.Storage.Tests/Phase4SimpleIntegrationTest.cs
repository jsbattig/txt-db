using System;
using System.IO;
using System.Threading.Tasks;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// Simple Phase 4 Infrastructure Integration Test
/// </summary>
public class Phase4SimpleIntegrationTest : IDisposable
{
    private readonly string _testDataPath;
    
    public Phase4SimpleIntegrationTest()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), $"Phase4SimpleTest_{Guid.NewGuid():N}");
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
    public async Task InfrastructureEnabled_ShouldInitialize()
    {
        // Arrange
        var config = new StorageConfig
        {
            Infrastructure = new InfrastructureConfig
            {
                Enabled = true,
                AutoRecoveryOnStartup = false, // Disable for simplicity
                AutoStartMemoryMonitoring = false, // Disable for simplicity
                RetryPolicy = new RetryPolicyConfig
                {
                    MaxRetries = 1,
                    BaseDelayMs = 10,
                    MaxDelayMs = 100
                }
            }
        };
        
        var storage = new AsyncStorageSubsystem();
        
        // Act
        await storage.InitializeAsync(_testDataPath, config);
        
        // Assert
        Assert.NotNull(storage.StorageMetrics);
        
        var healthMetrics = storage.GetInfrastructureHealth();
        Assert.NotNull(healthMetrics);
        Assert.NotEqual(InfrastructureHealthStatus.Disabled, healthMetrics.OverallStatus);
    }
}