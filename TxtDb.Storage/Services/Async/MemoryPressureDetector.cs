using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// MemoryPressureDetector - Phase 4 Infrastructure Hardening Component
/// Monitors system memory usage and provides pressure detection and response capabilities
/// to prevent out-of-memory conditions and enable graceful degradation.
/// 
/// Key features:
/// - Real-time memory pressure monitoring
/// - Configurable thresholds for warning, critical, and emergency levels
/// - Automatic garbage collection triggering under pressure
/// - Memory usage statistics and trend analysis
/// - Event-based notifications for pressure level changes
/// - Memory allocation gating for resource protection
/// </summary>
public class MemoryPressureDetector : IDisposable
{
    private readonly MemoryPressureConfig _config;
    private readonly Timer? _monitoringTimer;
    private readonly object _stateLock = new object();
    private readonly Queue<MemoryPressureInfo> _memoryHistory = new Queue<MemoryPressureInfo>();
    
    // Current state
    private MemoryPressureLevel _currentPressureLevel = MemoryPressureLevel.Normal;
    private DateTime _lastGCTriggerTime = DateTime.MinValue;
    private bool _isMonitoring = false;
    private volatile bool _disposed = false;
    
    // Statistics tracking
    private readonly Dictionary<MemoryPressureLevel, long> _timeSpentInLevels = new Dictionary<MemoryPressureLevel, long>();
    private DateTime _lastLevelChangeTime = DateTime.UtcNow;
    private int _pressureTriggeredGCCount = 0;
    
    /// <summary>
    /// Event fired when memory pressure level changes
    /// </summary>
    public event Action<MemoryPressureLevel>? PressureLevelChanged;
    
    /// <summary>
    /// Event fired when memory pressure level changes with detailed information
    /// </summary>
    public event Action<MemoryPressureLevel, MemoryPressureInfo>? PressureLevelChangedDetailed;
    
    /// <summary>
    /// Current memory pressure level
    /// </summary>
    public MemoryPressureLevel CurrentPressureLevel
    {
        get
        {
            lock (_stateLock)
            {
                return _currentPressureLevel;
            }
        }
    }
    
    /// <summary>
    /// Whether monitoring is currently active
    /// </summary>
    public bool IsMonitoring
    {
        get
        {
            lock (_stateLock)
            {
                return _isMonitoring;
            }
        }
    }
    
    /// <summary>
    /// Current used memory in MB
    /// </summary>
    public long UsedMemoryMB
    {
        get
        {
            var memoryInfo = GetCurrentMemoryInfo();
            return memoryInfo.UsedMemoryMB;
        }
    }
    
    /// <summary>
    /// Current available memory in MB
    /// </summary>
    public long AvailableMemoryMB
    {
        get
        {
            var memoryInfo = GetCurrentMemoryInfo();
            return memoryInfo.AvailableMemoryMB;
        }
    }
    
    /// <summary>
    /// Creates a new MemoryPressureDetector with default configuration
    /// </summary>
    public MemoryPressureDetector() : this(new MemoryPressureConfig())
    {
    }
    
    /// <summary>
    /// Creates a new MemoryPressureDetector with custom configuration
    /// </summary>
    /// <param name="config">Configuration for memory pressure detection</param>
    public MemoryPressureDetector(MemoryPressureConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        
        // Initialize time spent tracking
        foreach (MemoryPressureLevel level in Enum.GetValues<MemoryPressureLevel>())
        {
            _timeSpentInLevels[level] = 0;
        }
        
        // Create timer for monitoring (but don't start it yet)
        _monitoringTimer = new Timer(MonitoringCallback, null, Timeout.Infinite, Timeout.Infinite);
    }
    
    /// <summary>
    /// Starts memory pressure monitoring
    /// </summary>
    public void StartMonitoring()
    {
        ThrowIfDisposed();
        
        lock (_stateLock)
        {
            if (_isMonitoring)
                return;
                
            _isMonitoring = true;
            _lastLevelChangeTime = DateTime.UtcNow;
            
            // Start the monitoring timer
            _monitoringTimer?.Change(0, _config.MonitoringIntervalMs);
        }
    }
    
    /// <summary>
    /// Stops memory pressure monitoring
    /// </summary>
    public void StopMonitoring()
    {
        lock (_stateLock)
        {
            if (!_isMonitoring)
                return;
                
            _isMonitoring = false;
            
            // Stop the monitoring timer
            _monitoringTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            
            // Record time spent in current level
            var timeSpentInCurrentLevel = DateTime.UtcNow - _lastLevelChangeTime;
            _timeSpentInLevels[_currentPressureLevel] += (long)timeSpentInCurrentLevel.TotalMilliseconds;
        }
    }
    
    /// <summary>
    /// Gets current memory information
    /// </summary>
    public MemoryPressureInfo GetCurrentMemoryInfo()
    {
        ThrowIfDisposed();
        
        // Get current memory usage from GC
        var usedMemoryBytes = GC.GetTotalMemory(false);
        var usedMemoryMB = usedMemoryBytes / 1024 / 1024;
        
        // Get system memory information using Process class
        var process = Process.GetCurrentProcess();
        var workingSetMB = process.WorkingSet64 / 1024 / 1024;
        
        // Use working set as a more accurate measure of actual memory usage
        usedMemoryMB = Math.Max(usedMemoryMB, workingSetMB);
        
        // Estimate total system memory (this is a simplification)
        // In a real implementation, you might use platform-specific APIs
        var totalMemoryMB = Math.Max(usedMemoryMB + 1000, _config.MaxAllowedMemoryMB); // Rough estimate
        
        var availableMemoryMB = Math.Max(0, totalMemoryMB - usedMemoryMB);
        
        // Determine pressure level
        var pressureLevel = DeterminePressureLevel(usedMemoryMB);
        
        return new MemoryPressureInfo
        {
            TotalMemoryMB = totalMemoryMB,
            UsedMemoryMB = usedMemoryMB,
            AvailableMemoryMB = availableMemoryMB,
            PressureLevel = pressureLevel,
            Timestamp = DateTime.UtcNow,
            Gen0Collections = GC.CollectionCount(0),
            Gen1Collections = GC.CollectionCount(1),
            Gen2Collections = GC.CollectionCount(2)
        };
    }
    
    /// <summary>
    /// Determines if memory allocation of the specified size is allowed
    /// </summary>
    /// <param name="requestedSizeMB">Requested allocation size in MB</param>
    /// <returns>True if allocation is allowed, false if it would exceed limits</returns>
    public bool CanAllocateMemory(long requestedSizeMB)
    {
        ThrowIfDisposed();
        
        var memoryInfo = GetCurrentMemoryInfo();
        
        // Check if allocation would exceed maximum allowed memory
        if (memoryInfo.UsedMemoryMB + requestedSizeMB > _config.MaxAllowedMemoryMB)
        {
            return false;
        }
        
        // Allow allocation in normal and warning levels
        if (memoryInfo.PressureLevel <= MemoryPressureLevel.Warning)
        {
            return true;
        }
        
        // In critical level, allow only small allocations
        if (memoryInfo.PressureLevel == MemoryPressureLevel.Critical)
        {
            return requestedSizeMB <= 10; // Max 10MB in critical state
        }
        
        // In emergency level, block all but tiny allocations
        if (memoryInfo.PressureLevel == MemoryPressureLevel.Emergency)
        {
            return requestedSizeMB <= 1; // Max 1MB in emergency state
        }
        
        return false;
    }
    
    /// <summary>
    /// Triggers garbage collection asynchronously
    /// </summary>
    public async Task TriggerGarbageCollectionAsync()
    {
        ThrowIfDisposed();
        
        // Run GC on a background thread to avoid blocking
        await Task.Run(() =>
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }).ConfigureAwait(false);
        
        Interlocked.Increment(ref _pressureTriggeredGCCount);
    }
    
    /// <summary>
    /// Gets memory pressure statistics for the configured time window
    /// </summary>
    public MemoryPressureStatistics GetMemoryPressureStatistics()
    {
        ThrowIfDisposed();
        
        lock (_stateLock)
        {
            // Clean up old history entries
            var cutoffTime = DateTime.UtcNow.AddMilliseconds(-_config.StatisticsWindowMs);
            while (_memoryHistory.Count > 0 && _memoryHistory.Peek().Timestamp < cutoffTime)
            {
                _memoryHistory.Dequeue();
            }
            
            if (_memoryHistory.Count == 0)
            {
                return new MemoryPressureStatistics
                {
                    SampleCount = 0,
                    AverageUsedMemoryMB = 0,
                    PeakUsedMemoryMB = 0,
                    MinUsedMemoryMB = 0,
                    TimeInWarningMs = _timeSpentInLevels[MemoryPressureLevel.Warning],
                    TimeInCriticalMs = _timeSpentInLevels[MemoryPressureLevel.Critical],
                    TimeInEmergencyMs = _timeSpentInLevels[MemoryPressureLevel.Emergency],
                    PressureTriggeredGCCount = _pressureTriggeredGCCount
                };
            }
            
            var samples = _memoryHistory.ToList();
            
            return new MemoryPressureStatistics
            {
                SampleCount = samples.Count,
                AverageUsedMemoryMB = samples.Average(s => s.UsedMemoryMB),
                PeakUsedMemoryMB = samples.Max(s => s.UsedMemoryMB),
                MinUsedMemoryMB = samples.Min(s => s.UsedMemoryMB),
                TimeInWarningMs = _timeSpentInLevels[MemoryPressureLevel.Warning],
                TimeInCriticalMs = _timeSpentInLevels[MemoryPressureLevel.Critical],
                TimeInEmergencyMs = _timeSpentInLevels[MemoryPressureLevel.Emergency],
                PressureTriggeredGCCount = _pressureTriggeredGCCount
            };
        }
    }
    
    /// <summary>
    /// Timer callback for memory monitoring
    /// </summary>
    private void MonitoringCallback(object? state)
    {
        if (_disposed || !_isMonitoring)
            return;
            
        try
        {
            var memoryInfo = GetCurrentMemoryInfo();
            
            lock (_stateLock)
            {
                // Add to history
                _memoryHistory.Enqueue(memoryInfo);
                
                // Clean up old history
                var cutoffTime = DateTime.UtcNow.AddMilliseconds(-_config.StatisticsWindowMs);
                while (_memoryHistory.Count > 0 && _memoryHistory.Peek().Timestamp < cutoffTime)
                {
                    _memoryHistory.Dequeue();
                }
                
                // Check for pressure level changes
                var oldPressureLevel = _currentPressureLevel;
                var newPressureLevel = memoryInfo.PressureLevel;
                
                if (oldPressureLevel != newPressureLevel)
                {
                    // Record time spent in previous level
                    var timeSpentInPreviousLevel = DateTime.UtcNow - _lastLevelChangeTime;
                    _timeSpentInLevels[oldPressureLevel] += (long)timeSpentInPreviousLevel.TotalMilliseconds;
                    
                    _currentPressureLevel = newPressureLevel;
                    _lastLevelChangeTime = DateTime.UtcNow;
                    
                    // Trigger events outside the lock
                    Task.Run(() =>
                    {
                        PressureLevelChanged?.Invoke(newPressureLevel);
                        PressureLevelChangedDetailed?.Invoke(newPressureLevel, memoryInfo);
                    });
                }
                
                // Auto-trigger GC if enabled and needed
                if (_config.AutoTriggerGC && 
                    newPressureLevel >= MemoryPressureLevel.Critical &&
                    DateTime.UtcNow - _lastGCTriggerTime > TimeSpan.FromMilliseconds(_config.MinGCIntervalMs))
                {
                    _lastGCTriggerTime = DateTime.UtcNow;
                    
                    // Trigger GC on background thread
                    Task.Run(async () =>
                    {
                        try
                        {
                            await TriggerGarbageCollectionAsync().ConfigureAwait(false);
                        }
                        catch
                        {
                            // Ignore GC errors - they shouldn't break monitoring
                        }
                    });
                }
            }
        }
        catch
        {
            // Ignore monitoring errors to prevent disruption
        }
    }
    
    /// <summary>
    /// Determines memory pressure level based on used memory
    /// </summary>
    private MemoryPressureLevel DeterminePressureLevel(long usedMemoryMB)
    {
        if (usedMemoryMB >= _config.EmergencyThresholdMB)
            return MemoryPressureLevel.Emergency;
            
        if (usedMemoryMB >= _config.CriticalThresholdMB)
            return MemoryPressureLevel.Critical;
            
        if (usedMemoryMB >= _config.WarningThresholdMB)
            return MemoryPressureLevel.Warning;
            
        return MemoryPressureLevel.Normal;
    }
    
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MemoryPressureDetector));
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
            
        _disposed = true;
        
        // Stop monitoring
        StopMonitoring();
        
        // Dispose timer
        _monitoringTimer?.Dispose();
    }
}