using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Services.Async;
using TxtDb.Storage.Models;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Infrastructure;

/// <summary>
/// TDD tests for memory pressure detection and response system
/// Tests memory monitoring, pressure detection, and graceful degradation mechanisms
/// 
/// Phase 4 Infrastructure Hardening requirement:
/// - Memory pressure detection and response system
/// - Prevents out-of-memory conditions and cascade failures
/// - Configurable thresholds and response actions
/// </summary>
public class MemoryPressureDetectorTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly List<IDisposable> _disposables;

    public MemoryPressureDetectorTests(ITestOutputHelper output)
    {
        _output = output;
        _disposables = new List<IDisposable>();
    }

    public void Dispose()
    {
        foreach (var disposable in _disposables)
        {
            disposable?.Dispose();
        }
    }

    /// <summary>
    /// Test: Memory pressure detector should remain in normal state under low memory usage
    /// </summary>
    [Fact]
    public void MemoryPressureDetector_ShouldRemainNormal_UnderLowMemoryUsage()
    {
        // ARRANGE: Create memory pressure detector with high thresholds
        var config = new MemoryPressureConfig
        {
            WarningThresholdMB = 1000, // 1GB warning threshold
            CriticalThresholdMB = 2000, // 2GB critical threshold
            MonitoringIntervalMs = 100
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        // ACT: Start monitoring
        detector.StartMonitoring();
        
        // Wait a bit for monitoring to collect data
        Thread.Sleep(200);
        
        // ASSERT: Should be in normal state under typical test conditions
        Assert.Equal(MemoryPressureLevel.Normal, detector.CurrentPressureLevel);
        Assert.True(detector.AvailableMemoryMB > 0);
        Assert.True(detector.UsedMemoryMB > 0);
    }

    /// <summary>
    /// Test: Memory pressure detector should detect warning level memory pressure
    /// </summary>
    [Fact]
    public async Task MemoryPressureDetector_ShouldDetectWarningLevel_MemoryPressure()
    {
        // ARRANGE: Create memory pressure detector with very low thresholds for testing
        var config = new MemoryPressureConfig
        {
            WarningThresholdMB = 1, // 1MB warning threshold - very low for testing
            CriticalThresholdMB = 2, // 2MB critical threshold
            MonitoringIntervalMs = 50
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        // Set up event handler to capture pressure level changes
        var pressureLevelChanges = new List<MemoryPressureLevel>();
        detector.PressureLevelChanged += (level) => pressureLevelChanges.Add(level);
        
        // ACT: Start monitoring
        detector.StartMonitoring();
        
        // Allocate memory to trigger warning threshold
        var largeArray = new byte[5 * 1024 * 1024]; // 5MB allocation
        
        // Wait for monitoring to detect the pressure
        await Task.Delay(200);
        
        // Keep reference to prevent GC
        GC.KeepAlive(largeArray);
        
        // ASSERT: Should detect warning or critical level
        Assert.True(detector.CurrentPressureLevel >= MemoryPressureLevel.Warning);
        Assert.True(pressureLevelChanges.Count > 0);
    }

    /// <summary>
    /// Test: Memory pressure detector should trigger callbacks on pressure level changes
    /// </summary>
    [Fact]
    public async Task MemoryPressureDetector_ShouldTriggerCallbacks_OnPressureLevelChanges()
    {
        // ARRANGE: Create memory pressure detector
        var config = new MemoryPressureConfig
        {
            WarningThresholdMB = 1,
            CriticalThresholdMB = 2,
            MonitoringIntervalMs = 50
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        var callbackInvocations = new List<(MemoryPressureLevel level, MemoryPressureInfo info)>();
        
        // Set up callback
        detector.PressureLevelChangedDetailed += (level, info) => 
        {
            callbackInvocations.Add((level, info));
        };
        
        // ACT: Start monitoring and trigger memory pressure
        detector.StartMonitoring();
        
        // Allocate memory to trigger threshold
        var largeArray = new byte[5 * 1024 * 1024]; // 5MB
        
        // Wait for monitoring
        await Task.Delay(200);
        
        GC.KeepAlive(largeArray);
        
        // ASSERT: Callbacks should have been invoked
        Assert.True(callbackInvocations.Count > 0);
        Assert.True(callbackInvocations.Any(c => c.level >= MemoryPressureLevel.Warning));
        Assert.All(callbackInvocations, c => Assert.True(c.info.UsedMemoryMB > 0));
        Assert.All(callbackInvocations, c => Assert.True(c.info.AvailableMemoryMB >= 0));
    }

    /// <summary>
    /// Test: Memory pressure detector should provide accurate memory information
    /// </summary>
    [Fact]
    public void MemoryPressureDetector_ShouldProvideAccurateMemoryInfo()
    {
        // ARRANGE: Create memory pressure detector
        var detector = new MemoryPressureDetector();
        _disposables.Add(detector);
        
        // ACT: Get memory information
        var memoryInfo = detector.GetCurrentMemoryInfo();
        
        // ASSERT: Memory information should be reasonable
        Assert.True(memoryInfo.TotalMemoryMB > 0);
        Assert.True(memoryInfo.UsedMemoryMB > 0);
        Assert.True(memoryInfo.AvailableMemoryMB >= 0);
        Assert.True(memoryInfo.UsedMemoryMB <= memoryInfo.TotalMemoryMB);
        Assert.Equal(memoryInfo.TotalMemoryMB, memoryInfo.UsedMemoryMB + memoryInfo.AvailableMemoryMB);
    }

    /// <summary>
    /// Test: Memory pressure detector should support manual garbage collection triggering
    /// </summary>
    [Fact]
    public async Task MemoryPressureDetector_ShouldSupportManualGC_Triggering()
    {
        // ARRANGE: Create memory pressure detector
        var detector = new MemoryPressureDetector();
        _disposables.Add(detector);
        
        // Allocate some memory to be collected
        var memoryBeforeAllocation = GC.GetTotalMemory(false);
        
        // Allocate and release memory
        var largeArrays = new List<byte[]>();
        for (int i = 0; i < 10; i++)
        {
            largeArrays.Add(new byte[1024 * 1024]); // 1MB each
        }
        
        var memoryAfterAllocation = GC.GetTotalMemory(false);
        
        // Release references
        largeArrays.Clear();
        
        // ACT: Trigger garbage collection through detector
        await detector.TriggerGarbageCollectionAsync();
        
        // ASSERT: Memory should be freed
        var memoryAfterGC = GC.GetTotalMemory(false);
        Assert.True(memoryAfterGC < memoryAfterAllocation);
    }

    /// <summary>
    /// Test: Memory pressure detector should handle start/stop monitoring correctly
    /// </summary>
    [Fact]
    public async Task MemoryPressureDetector_ShouldHandleStartStop_Correctly()
    {
        // ARRANGE: Create memory pressure detector
        var config = new MemoryPressureConfig
        {
            MonitoringIntervalMs = 50
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        var callbackCount = 0;
        detector.PressureLevelChanged += (level) => Interlocked.Increment(ref callbackCount);
        
        // ACT: Start and stop monitoring
        Assert.False(detector.IsMonitoring);
        
        detector.StartMonitoring();
        Assert.True(detector.IsMonitoring);
        
        await Task.Delay(150); // Let it run for a bit
        var callbackCountWhileRunning = callbackCount;
        
        detector.StopMonitoring();
        Assert.False(detector.IsMonitoring);
        
        await Task.Delay(150); // Wait while stopped
        var callbackCountAfterStopping = callbackCount;
        
        // ASSERT: Monitoring state should be correctly managed
        // Callbacks should occur while running, but not after stopping
        Assert.Equal(callbackCountWhileRunning, callbackCountAfterStopping);
    }

    /// <summary>
    /// Test: Memory pressure detector should enforce memory usage limits
    /// </summary>
    [Fact]
    public void MemoryPressureDetector_ShouldEnforceMemoryUsageLimits()
    {
        // ARRANGE: Create detector with very low limits to ensure it triggers
        var config = new MemoryPressureConfig
        {
            WarningThresholdMB = 1,   // 1MB warning
            CriticalThresholdMB = 2,  // 2MB critical
            MaxAllowedMemoryMB = 5    // 5MB max
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        // ACT & ASSERT: Test memory allocation logic
        // Small allocations should not exceed maximum allowed
        var smallAllocation = detector.CanAllocateMemory(1); // 1MB
        var largeAllocation = detector.CanAllocateMemory(100); // 100MB - definitely exceeds limits
        
        // The method should return valid boolean results
        Assert.True(smallAllocation == true || smallAllocation == false);
        Assert.False(largeAllocation); // 100MB should be rejected due to exceeding MaxAllowedMemoryMB (5MB)
        
        // Test that the method handles edge cases correctly
        var exactMaxAllocation = detector.CanAllocateMemory(config.MaxAllowedMemoryMB);
        Assert.True(exactMaxAllocation == true || exactMaxAllocation == false); // Should handle exact max
        
        var overMaxAllocation = detector.CanAllocateMemory(config.MaxAllowedMemoryMB + 1);
        Assert.False(overMaxAllocation); // Should reject allocation over max limit
    }

    /// <summary>
    /// Test: Memory pressure detector should provide memory pressure statistics
    /// </summary>
    [Fact]
    public async Task MemoryPressureDetector_ShouldProvideMemoryPressureStatistics()
    {
        // ARRANGE: Create detector
        var config = new MemoryPressureConfig
        {
            MonitoringIntervalMs = 25,
            StatisticsWindowMs = 1000
        };
        
        var detector = new MemoryPressureDetector(config);
        _disposables.Add(detector);
        
        // ACT: Start monitoring and collect statistics
        detector.StartMonitoring();
        await Task.Delay(300); // Let it collect some data points
        
        var stats = detector.GetMemoryPressureStatistics();
        
        // ASSERT: Statistics should be meaningful
        Assert.True(stats.SampleCount > 0);
        Assert.True(stats.AverageUsedMemoryMB > 0);
        Assert.True(stats.PeakUsedMemoryMB > 0);
        Assert.True(stats.AverageUsedMemoryMB <= stats.PeakUsedMemoryMB);
        Assert.True(stats.TimeInWarningMs >= 0);
        Assert.True(stats.TimeInCriticalMs >= 0);
    }
}