using System.Collections.Concurrent;
using System.Diagnostics;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// StorageMetrics - Comprehensive Performance Monitoring for Epic 002 Phase 4
/// Tracks operation counters, latency histograms, throughput gauges, and resource utilization
/// Thread-safe implementation for concurrent operation tracking
/// </summary>
public class StorageMetrics
{
    // Operation counters (thread-safe)
    private long _totalOperations;
    private long _totalReadOperations;
    private long _totalWriteOperations;
    private long _totalTransactionOperations;
    private long _totalErrors;
    private long _readErrors;
    private long _writeErrors;
    private long _transactionErrors;

    // Latency tracking (thread-safe collections)
    private readonly ConcurrentQueue<double> _readLatencies = new();
    private readonly ConcurrentQueue<double> _writeLatencies = new();
    private readonly ConcurrentQueue<double> _transactionLatencies = new();
    private readonly ConcurrentQueue<double> _flushLatencies = new();

    // Throughput tracking with timestamps
    private readonly ConcurrentQueue<(DateTime timestamp, string operationType)> _operationTimestamps = new();

    // Batch flush metrics
    private long _flushCallsBeforeBatching;
    private long _flushCallsAfterBatching;

    // Resource utilization tracking
    private long _activeAsyncOperations;
    private long _maxConcurrentOperations;
    private readonly object _activeOperationsLock = new object();

    // Thread pool status cache
    private ThreadPoolStatus? _lastThreadPoolStatus;
    private DateTime _lastThreadPoolStatusUpdate = DateTime.MinValue;
    private readonly TimeSpan _threadPoolStatusCacheTimeout = TimeSpan.FromSeconds(1);

    // Memory usage cache
    private MemoryUsage? _lastMemoryUsage;
    private DateTime _lastMemoryUsageUpdate = DateTime.MinValue;
    private readonly TimeSpan _memoryUsageCacheTimeout = TimeSpan.FromSeconds(1);

    // Error categorization
    private readonly ConcurrentDictionary<string, long> _errorsByType = new();
    private readonly ConcurrentQueue<(string Type, string Message)> _errorMessages = new();
    
    // Namespace existence caching metrics
    private long _namespaceExistenceCacheHits;
    private long _namespaceExistenceCacheMisses;

    // Public properties for counters
    public long TotalOperations => Interlocked.Read(ref _totalOperations);
    public long TotalReadOperations => Interlocked.Read(ref _totalReadOperations);
    public long TotalWriteOperations => Interlocked.Read(ref _totalWriteOperations);
    public long TotalTransactionOperations => Interlocked.Read(ref _totalTransactionOperations);
    public long TotalErrors => Interlocked.Read(ref _totalErrors);
    public long ReadErrors => Interlocked.Read(ref _readErrors);
    public long WriteErrors => Interlocked.Read(ref _writeErrors);
    public long TransactionErrors => Interlocked.Read(ref _transactionErrors);

    // Batch flush properties
    public long FlushCallsBeforeBatching => Interlocked.Read(ref _flushCallsBeforeBatching);
    public long FlushCallsAfterBatching => Interlocked.Read(ref _flushCallsAfterBatching);
    public double FlushReductionPercentage
    {
        get
        {
            var before = Interlocked.Read(ref _flushCallsBeforeBatching);
            var after = Interlocked.Read(ref _flushCallsAfterBatching);
            return before == 0 ? 0.0 : (double)(before - after) / before * 100.0;
        }
    }

    // Resource utilization properties
    public int ActiveAsyncOperations => (int)Interlocked.Read(ref _activeAsyncOperations);
    public int MaxConcurrentOperations => (int)Interlocked.Read(ref _maxConcurrentOperations);

    // Cache properties
    public long ErrorCount => TotalErrors;
    public long NamespaceCacheHits => Interlocked.Read(ref _namespaceExistenceCacheHits);
    public long NamespaceCacheMisses => Interlocked.Read(ref _namespaceExistenceCacheMisses);

    /// <summary>
    /// Record a read operation with latency and success status
    /// </summary>
    public void RecordReadOperation(TimeSpan latency, bool success)
    {
        Interlocked.Increment(ref _totalOperations);
        Interlocked.Increment(ref _totalReadOperations);
        
        if (!success)
        {
            Interlocked.Increment(ref _totalErrors);
            Interlocked.Increment(ref _readErrors);
        }

        _readLatencies.Enqueue(latency.TotalMilliseconds);
        _operationTimestamps.Enqueue((DateTime.UtcNow, "read"));
        
        // Trim old latency data to prevent memory leaks (keep last 10000 samples)
        TrimLatencyQueue(_readLatencies, 10000);
    }

    /// <summary>
    /// Record a write operation with latency and success status
    /// </summary>
    public void RecordWriteOperation(TimeSpan latency, bool success)
    {
        Interlocked.Increment(ref _totalOperations);
        Interlocked.Increment(ref _totalWriteOperations);
        
        if (!success)
        {
            Interlocked.Increment(ref _totalErrors);
            Interlocked.Increment(ref _writeErrors);
        }

        _writeLatencies.Enqueue(latency.TotalMilliseconds);
        _operationTimestamps.Enqueue((DateTime.UtcNow, "write"));
        
        TrimLatencyQueue(_writeLatencies, 10000);
    }

    /// <summary>
    /// Record a transaction operation with latency and success status
    /// </summary>
    public void RecordTransactionOperation(TimeSpan latency, bool success)
    {
        Interlocked.Increment(ref _totalOperations);
        Interlocked.Increment(ref _totalTransactionOperations);
        
        if (!success)
        {
            Interlocked.Increment(ref _totalErrors);
            Interlocked.Increment(ref _transactionErrors);
        }

        _transactionLatencies.Enqueue(latency.TotalMilliseconds);
        _operationTimestamps.Enqueue((DateTime.UtcNow, "transaction"));
        
        TrimLatencyQueue(_transactionLatencies, 10000);
    }

    /// <summary>
    /// Record flush operation latency
    /// </summary>
    public void RecordFlushLatency(TimeSpan latency)
    {
        _flushLatencies.Enqueue(latency.TotalMilliseconds);
        TrimLatencyQueue(_flushLatencies, 10000);
    }

    /// <summary>
    /// Record batch flush metrics for optimization tracking
    /// </summary>
    public void RecordBatchFlushMetrics(long beforeFlushCount, long afterFlushCount)
    {
        Interlocked.Add(ref _flushCallsBeforeBatching, beforeFlushCount);
        Interlocked.Add(ref _flushCallsAfterBatching, afterFlushCount);
    }

    /// <summary>
    /// Increment active async operations counter
    /// </summary>
    public void IncrementActiveOperations()
    {
        lock (_activeOperationsLock)
        {
            var current = Interlocked.Increment(ref _activeAsyncOperations);
            
            // Update max concurrent operations if needed
            var currentMax = Interlocked.Read(ref _maxConcurrentOperations);
            if (current > currentMax)
            {
                Interlocked.CompareExchange(ref _maxConcurrentOperations, current, currentMax);
            }
        }
    }

    /// <summary>
    /// Decrement active async operations counter
    /// </summary>
    public void DecrementActiveOperations()
    {
        lock (_activeOperationsLock)
        {
            var current = Interlocked.Read(ref _activeAsyncOperations);
            if (current > 0)
            {
                Interlocked.Decrement(ref _activeAsyncOperations);
            }
        }
    }

    /// <summary>
    /// Record an error with type categorization
    /// </summary>
    public void RecordError(string errorType, string errorMessage)
    {
        Interlocked.Increment(ref _totalErrors);
        _errorsByType.AddOrUpdate(errorType, 1, (key, count) => count + 1);
        _errorMessages.Enqueue((errorType, errorMessage));
        
        // Trim old error messages to prevent memory leaks
        while (_errorMessages.Count > 1000)
        {
            _errorMessages.TryDequeue(out _);
        }
    }

    /// <summary>
    /// Get read latency statistics
    /// </summary>
    public LatencyStatistics GetReadLatencyStatistics()
    {
        return CalculateLatencyStatistics(_readLatencies);
    }

    /// <summary>
    /// Get write latency statistics
    /// </summary>
    public LatencyStatistics GetWriteLatencyStatistics()
    {
        return CalculateLatencyStatistics(_writeLatencies);
    }

    /// <summary>
    /// Get transaction latency statistics
    /// </summary>
    public LatencyStatistics GetTransactionLatencyStatistics()
    {
        return CalculateLatencyStatistics(_transactionLatencies);
    }

    /// <summary>
    /// Get flush latency statistics
    /// </summary>
    public LatencyStatistics GetFlushLatencyStatistics()
    {
        return CalculateLatencyStatistics(_flushLatencies);
    }

    /// <summary>
    /// Calculate throughput over a specified time period
    /// </summary>
    public double CalculateThroughput(DateTime startTime, DateTime endTime)
    {
        var totalSeconds = (endTime - startTime).TotalSeconds;
        if (totalSeconds <= 0) return 0.0;
        
        var operationsInPeriod = _operationTimestamps
            .Where(op => op.timestamp >= startTime && op.timestamp <= endTime)
            .Count();
            
        return operationsInPeriod / totalSeconds;
    }

    /// <summary>
    /// Get current throughput over the last minute
    /// </summary>
    public double GetCurrentThroughput()
    {
        var endTime = DateTime.UtcNow;
        var startTime = endTime.AddMinutes(-1);
        return CalculateThroughput(startTime, endTime);
    }

    /// <summary>
    /// Get rolling throughput over a specified window
    /// </summary>
    public double GetRollingThroughput(TimeSpan window)
    {
        var endTime = DateTime.UtcNow;
        var startTime = endTime.Subtract(window);
        return CalculateThroughput(startTime, endTime);
    }

    /// <summary>
    /// Get overall success rate percentage
    /// </summary>
    public double GetSuccessRate()
    {
        var total = TotalOperations;
        if (total == 0) return 100.0;
        
        var successful = total - TotalErrors;
        return (double)successful / total * 100.0;
    }

    /// <summary>
    /// Get read operation success rate percentage
    /// </summary>
    public double GetReadSuccessRate()
    {
        var total = TotalReadOperations;
        if (total == 0) return 100.0;
        
        var successful = total - ReadErrors;
        return (double)successful / total * 100.0;
    }

    /// <summary>
    /// Get write operation success rate percentage
    /// </summary>
    public double GetWriteSuccessRate()
    {
        var total = TotalWriteOperations;
        if (total == 0) return 100.0;
        
        var successful = total - WriteErrors;
        return (double)successful / total * 100.0;
    }

    /// <summary>
    /// Update thread pool status
    /// </summary>
    public void UpdateThreadPoolStatus()
    {
        var now = DateTime.UtcNow;
        if (now - _lastThreadPoolStatusUpdate < _threadPoolStatusCacheTimeout && _lastThreadPoolStatus != null)
        {
            return; // Use cached value
        }

        ThreadPool.GetAvailableThreads(out int workerThreads, out int completionPortThreads);
        ThreadPool.GetMaxThreads(out int maxWorkerThreads, out int maxCompletionPortThreads);

        _lastThreadPoolStatus = new ThreadPoolStatus
        {
            WorkerThreads = workerThreads,
            CompletionPortThreads = completionPortThreads,
            MaxWorkerThreads = maxWorkerThreads,
            MaxCompletionPortThreads = maxCompletionPortThreads
        };
        
        _lastThreadPoolStatusUpdate = now;
    }

    /// <summary>
    /// Get current thread pool status
    /// </summary>
    public ThreadPoolStatus GetThreadPoolStatus()
    {
        UpdateThreadPoolStatus();
        return _lastThreadPoolStatus ?? new ThreadPoolStatus();
    }

    /// <summary>
    /// Update memory usage statistics
    /// </summary>
    public void UpdateMemoryUsage()
    {
        var now = DateTime.UtcNow;
        if (now - _lastMemoryUsageUpdate < _memoryUsageCacheTimeout && _lastMemoryUsage != null)
        {
            return; // Use cached value
        }

        var process = Process.GetCurrentProcess();
        
        _lastMemoryUsage = new MemoryUsage
        {
            WorkingSetBytes = process.WorkingSet64,
            PrivateMemoryBytes = process.PrivateMemorySize64,
            GC0Collections = GC.CollectionCount(0),
            GC1Collections = GC.CollectionCount(1),
            GC2Collections = GC.CollectionCount(2)
        };
        
        _lastMemoryUsageUpdate = now;
    }

    /// <summary>
    /// Get current memory usage
    /// </summary>
    public MemoryUsage GetMemoryUsage()
    {
        UpdateMemoryUsage();
        return _lastMemoryUsage ?? new MemoryUsage();
    }

    /// <summary>
    /// Get error statistics
    /// </summary>
    public ErrorStatistics GetErrorStatistics()
    {
        return new ErrorStatistics
        {
            TotalErrors = (int)TotalErrors,
            ErrorsByType = new Dictionary<string, int>(_errorsByType.ToDictionary(kvp => kvp.Key, kvp => (int)kvp.Value))
        };
    }

    /// <summary>
    /// Get a complete performance snapshot
    /// </summary>
    public PerformanceSnapshot GetPerformanceSnapshot()
    {
        return new PerformanceSnapshot
        {
            Timestamp = DateTime.UtcNow,
            TotalOperations = TotalOperations,
            TotalReadOperations = TotalReadOperations,
            TotalWriteOperations = TotalWriteOperations,
            TotalTransactionOperations = TotalTransactionOperations,
            TotalErrors = TotalErrors,
            SuccessRate = GetSuccessRate(),
            FlushReductionPercentage = FlushReductionPercentage,
            ActiveAsyncOperations = ActiveAsyncOperations,
            MaxConcurrentOperations = MaxConcurrentOperations,
            CurrentThroughput = GetCurrentThroughput()
        };
    }

    /// <summary>
    /// Get errors by specific type for testing and monitoring purposes
    /// </summary>
    public IEnumerable<(string Type, string Message)> GetErrorsByType(string errorType)
    {
        return _errorMessages.Where(error => error.Type == errorType);
    }

    /// <summary>
    /// Record cache hit for namespace existence checking
    /// </summary>
    public void RecordNamespaceCacheHit()
    {
        Interlocked.Increment(ref _namespaceExistenceCacheHits);
    }

    /// <summary>
    /// Record cache miss for namespace existence checking
    /// </summary>
    public void RecordNamespaceCacheMiss()
    {
        Interlocked.Increment(ref _namespaceExistenceCacheMisses);
    }

    /// <summary>
    /// Get cache metrics for namespace existence caching
    /// </summary>
    public CacheMetrics GetCacheMetrics()
    {
        var hits = NamespaceCacheHits;
        var misses = NamespaceCacheMisses;
        var total = hits + misses;
        
        return new CacheMetrics
        {
            TotalHits = hits,
            TotalMisses = misses,
            HitRatio = total == 0 ? 0.0 : (double)hits / total
        };
    }

    // Helper methods
    private void TrimLatencyQueue(ConcurrentQueue<double> queue, int maxSize)
    {
        while (queue.Count > maxSize)
        {
            queue.TryDequeue(out _);
        }
    }

    private LatencyStatistics CalculateLatencyStatistics(ConcurrentQueue<double> latencies)
    {
        var samples = latencies.ToArray();
        if (samples.Length == 0)
        {
            return new LatencyStatistics
            {
                AverageMs = 0,
                P50Ms = 0,
                P95Ms = 0,
                P99Ms = 0,
                SampleCount = 0
            };
        }

        Array.Sort(samples);
        
        var average = samples.Average();
        var p50 = samples.Length % 2 == 0 ? 
            (samples[samples.Length / 2 - 1] + samples[samples.Length / 2]) / 2.0 : 
            samples[samples.Length / 2];
        var p95 = CalculatePercentile(samples, 0.95);
        var p99 = CalculatePercentile(samples, 0.99);

        return new LatencyStatistics
        {
            AverageMs = average,
            P50Ms = p50,
            P95Ms = p95,
            P99Ms = p99,
            SampleCount = samples.Length
        };
    }

    private double CalculatePercentile(double[] sortedSamples, double percentile)
    {
        if (sortedSamples.Length == 0) return 0;
        if (sortedSamples.Length == 1) return sortedSamples[0];
        
        // Use simpler percentile calculation for consistent results
        var index = (int)Math.Round(percentile * (sortedSamples.Length - 1));
        return sortedSamples[index];
    }
}

/// <summary>
/// Latency statistics data structure
/// </summary>
public class LatencyStatistics
{
    public double AverageMs { get; set; }
    public double P50Ms { get; set; }
    public double P95Ms { get; set; }
    public double P99Ms { get; set; }
    public int SampleCount { get; set; }
}

/// <summary>
/// Thread pool status data structure
/// </summary>
public class ThreadPoolStatus
{
    public int WorkerThreads { get; set; }
    public int CompletionPortThreads { get; set; }
    public int MaxWorkerThreads { get; set; } 
    public int MaxCompletionPortThreads { get; set; }
}

/// <summary>
/// Memory usage data structure
/// </summary>
public class MemoryUsage
{
    public long WorkingSetBytes { get; set; }
    public long PrivateMemoryBytes { get; set; }
    public int GC0Collections { get; set; }
    public int GC1Collections { get; set; }
    public int GC2Collections { get; set; }
}

/// <summary>
/// Error statistics data structure
/// </summary>
public class ErrorStatistics
{
    public int TotalErrors { get; set; }
    public Dictionary<string, int> ErrorsByType { get; set; } = new();
    
    public string GetMostCommonErrorType()
    {
        return ErrorsByType.OrderByDescending(kvp => kvp.Value).FirstOrDefault().Key ?? "";
    }
}

/// <summary>
/// Complete performance snapshot data structure
/// </summary>
public class PerformanceSnapshot
{
    public DateTime Timestamp { get; set; }
    public long TotalOperations { get; set; }
    public long TotalReadOperations { get; set; }
    public long TotalWriteOperations { get; set; }
    public long TotalTransactionOperations { get; set; }
    public long TotalErrors { get; set; }
    public double SuccessRate { get; set; }
    public double FlushReductionPercentage { get; set; }
    public int ActiveAsyncOperations { get; set; }
    public int MaxConcurrentOperations { get; set; }
    public double CurrentThroughput { get; set; }
}

/// <summary>
/// Cache metrics data structure for namespace existence caching
/// </summary>
public class CacheMetrics
{
    public long TotalHits { get; set; }
    public long TotalMisses { get; set; }
    public double HitRatio { get; set; }
}