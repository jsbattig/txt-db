namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Epic002TargetValidator - Performance Target Validation for Epic 002 Phase 4
/// Validates that all Epic 002 performance targets are achieved:
/// - <5ms read latency (P99)
/// - <10ms write latency (P99)  
/// - 200+ operations/second throughput
/// - 50%+ reduction in FlushToDisk calls
/// </summary>
public class Epic002TargetValidator
{
    private readonly StorageMetrics _metrics;
    
    // Epic 002 Performance Targets
    public const double ReadLatencyTargetMs = 5.0;
    public const double WriteLatencyTargetMs = 10.0;
    public const double ThroughputTargetOpsPerSec = 200.0;
    public const double FlushReductionTargetPercent = 50.0;

    public Epic002TargetValidator(StorageMetrics metrics)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    /// <summary>
    /// Validate Epic 002 read latency target: <5ms P99 latency
    /// </summary>
    public TargetValidationResult ValidateReadLatencyTarget()
    {
        var readStats = _metrics.GetReadLatencyStatistics();
        
        if (readStats.SampleCount == 0)
        {
            return new TargetValidationResult
            {
                TargetName = "Read Latency P99",
                TargetDescription = "<5ms P99 latency",
                ActualValue = "No samples available",
                IsMet = false,
                Details = "No read operations recorded to measure latency"
            };
        }

        var isMet = readStats.P99Ms < ReadLatencyTargetMs;
        
        return new TargetValidationResult
        {
            TargetName = "Read Latency P99",
            TargetDescription = $"<{ReadLatencyTargetMs}ms P99 latency",
            ActualValue = $"{readStats.P99Ms:F2}ms",
            IsMet = isMet,
            Details = $"Samples: {readStats.SampleCount}, Avg: {readStats.AverageMs:F2}ms, P50: {readStats.P50Ms:F2}ms, P95: {readStats.P95Ms:F2}ms"
        };
    }

    /// <summary>
    /// Validate Epic 002 write latency target: <10ms P99 latency
    /// </summary>
    public TargetValidationResult ValidateWriteLatencyTarget()
    {
        var writeStats = _metrics.GetWriteLatencyStatistics();
        
        if (writeStats.SampleCount == 0)
        {
            return new TargetValidationResult
            {
                TargetName = "Write Latency P99",
                TargetDescription = "<10ms P99 latency",
                ActualValue = "No samples available",
                IsMet = false,
                Details = "No write operations recorded to measure latency"
            };
        }

        var isMet = writeStats.P99Ms < WriteLatencyTargetMs;
        
        return new TargetValidationResult
        {
            TargetName = "Write Latency P99",
            TargetDescription = $"<{WriteLatencyTargetMs}ms P99 latency",
            ActualValue = $"{writeStats.P99Ms:F2}ms",
            IsMet = isMet,
            Details = $"Samples: {writeStats.SampleCount}, Avg: {writeStats.AverageMs:F2}ms, P50: {writeStats.P50Ms:F2}ms, P95: {writeStats.P95Ms:F2}ms"
        };
    }

    /// <summary>
    /// Validate Epic 002 throughput target: 200+ operations/second
    /// </summary>
    public TargetValidationResult ValidateThroughputTarget()
    {
        var currentThroughput = _metrics.GetCurrentThroughput();
        
        if (_metrics.TotalOperations == 0)
        {
            return new TargetValidationResult
            {
                TargetName = "Throughput",
                TargetDescription = "200+ operations/second",
                ActualValue = "No operations recorded",
                IsMet = false,
                Details = "No operations recorded to measure throughput"
            };
        }

        var isMet = currentThroughput >= ThroughputTargetOpsPerSec;
        
        return new TargetValidationResult
        {
            TargetName = "Throughput",
            TargetDescription = $">={ThroughputTargetOpsPerSec} operations/second",
            ActualValue = $"{currentThroughput:F2} ops/sec",
            IsMet = isMet,
            Details = $"Total operations: {_metrics.TotalOperations}, Success rate: {_metrics.GetSuccessRate():F1}%"
        };
    }

    /// <summary>
    /// Validate Epic 002 flush reduction target: 50%+ reduction in FlushToDisk calls
    /// </summary>
    public TargetValidationResult ValidateFlushReductionTarget()
    {
        var flushReduction = _metrics.FlushReductionPercentage;
        var beforeCount = _metrics.FlushCallsBeforeBatching;
        var afterCount = _metrics.FlushCallsAfterBatching;
        
        if (beforeCount == 0)
        {
            return new TargetValidationResult
            {
                TargetName = "Flush Reduction",
                TargetDescription = "50%+ reduction in flush calls",
                ActualValue = "No flush metrics available",
                IsMet = false,
                Details = "No flush operations recorded to measure reduction"
            };
        }

        var isMet = flushReduction >= FlushReductionTargetPercent;
        
        return new TargetValidationResult
        {
            TargetName = "Flush Reduction",
            TargetDescription = $">={FlushReductionTargetPercent}% reduction in flush calls",
            ActualValue = $"{flushReduction:F1}% reduction",
            IsMet = isMet,
            Details = $"Before batching: {beforeCount} flushes, After batching: {afterCount} flushes"
        };
    }

    /// <summary>
    /// Validate all Epic 002 targets simultaneously
    /// </summary>
    public List<TargetValidationResult> ValidateAllTargets()
    {
        return new List<TargetValidationResult>
        {
            ValidateReadLatencyTarget(),
            ValidateWriteLatencyTarget(),
            ValidateThroughputTarget(),
            ValidateFlushReductionTarget()
        };
    }

    /// <summary>
    /// Get a comprehensive Epic 002 validation report
    /// </summary>
    public Epic002ValidationReport GetValidationReport()
    {
        var results = ValidateAllTargets();
        var allTargetsMet = results.All(r => r.IsMet);
        
        return new Epic002ValidationReport
        {
            ValidationTimestamp = DateTime.UtcNow,
            AllTargetsMet = allTargetsMet,
            TargetResults = results,
            Summary = GenerateValidationSummary(results, allTargetsMet),
            PerformanceSnapshot = _metrics.GetPerformanceSnapshot()
        };
    }

    /// <summary>
    /// Check if Epic 002 targets are achievable based on current baseline performance
    /// </summary>
    public TargetAchievabilityAnalysis AnalyzeTargetAchievability()
    {
        var readStats = _metrics.GetReadLatencyStatistics();
        var writeStats = _metrics.GetWriteLatencyStatistics();
        var currentThroughput = _metrics.GetCurrentThroughput();
        var flushReduction = _metrics.FlushReductionPercentage;

        return new TargetAchievabilityAnalysis
        {
            ReadLatencyAchievable = readStats.SampleCount == 0 || readStats.P99Ms < ReadLatencyTargetMs * 1.2, // 20% buffer
            WriteLatencyAchievable = writeStats.SampleCount == 0 || writeStats.P99Ms < WriteLatencyTargetMs * 1.2, // 20% buffer
            ThroughputAchievable = _metrics.TotalOperations == 0 || currentThroughput > ThroughputTargetOpsPerSec * 0.8, // 80% of target
            FlushReductionAchievable = _metrics.FlushCallsBeforeBatching == 0 || flushReduction > FlushReductionTargetPercent * 0.8, // 80% of target
            
            ReadLatencyGap = readStats.SampleCount > 0 ? Math.Max(0, readStats.P99Ms - ReadLatencyTargetMs) : 0,
            WriteLatencyGap = writeStats.SampleCount > 0 ? Math.Max(0, writeStats.P99Ms - WriteLatencyTargetMs) : 0,
            ThroughputGap = _metrics.TotalOperations > 0 ? Math.Max(0, ThroughputTargetOpsPerSec - currentThroughput) : ThroughputTargetOpsPerSec,
            FlushReductionGap = _metrics.FlushCallsBeforeBatching > 0 ? Math.Max(0, FlushReductionTargetPercent - flushReduction) : FlushReductionTargetPercent
        };
    }

    private string GenerateValidationSummary(List<TargetValidationResult> results, bool allTargetsMet)
    {
        var summary = $"Epic 002 Validation Summary - {(allTargetsMet ? "ALL TARGETS ACHIEVED" : "TARGETS NOT MET")}\n";
        summary += $"Validation performed at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC\n\n";
        
        var metTargets = results.Count(r => r.IsMet);
        summary += $"Targets Met: {metTargets}/{results.Count}\n\n";
        
        foreach (var result in results)
        {
            var status = result.IsMet ? "✅ ACHIEVED" : "❌ NOT MET";
            summary += $"{result.TargetName}: {status}\n";
            summary += $"  Target: {result.TargetDescription}\n";
            summary += $"  Actual: {result.ActualValue}\n";
            if (!string.IsNullOrEmpty(result.Details))
            {
                summary += $"  Details: {result.Details}\n";
            }
            summary += "\n";
        }
        
        if (!allTargetsMet)
        {
            summary += "Recommendations:\n";
            foreach (var result in results.Where(r => !r.IsMet))
            {
                summary += $"- {GetRecommendationForTarget(result.TargetName)}\n";
            }
        }
        
        return summary;
    }

    private string GetRecommendationForTarget(string targetName)
    {
        return targetName switch
        {
            "Read Latency P99" => "Optimize read operations with caching, index improvements, or async I/O enhancements",
            "Write Latency P99" => "Optimize write operations with batch processing, async I/O, or storage optimizations",
            "Throughput" => "Improve concurrency handling, reduce lock contention, and optimize critical path operations",
            "Flush Reduction" => "Enhance batch flush coordination and increase batching thresholds",
            _ => $"Optimize {targetName} performance"
        };
    }
}

/// <summary>
/// Result of Epic 002 target validation
/// </summary>
public class TargetValidationResult
{
    public string TargetName { get; set; } = "";
    public string TargetDescription { get; set; } = "";
    public string ActualValue { get; set; } = "";
    public bool IsMet { get; set; }
    public string Details { get; set; } = "";
}

/// <summary>
/// Comprehensive Epic 002 validation report
/// </summary>
public class Epic002ValidationReport
{
    public DateTime ValidationTimestamp { get; set; }
    public bool AllTargetsMet { get; set; }
    public List<TargetValidationResult> TargetResults { get; set; } = new();
    public string Summary { get; set; } = "";
    public PerformanceSnapshot PerformanceSnapshot { get; set; } = new();
}

/// <summary>
/// Analysis of Epic 002 target achievability
/// </summary>
public class TargetAchievabilityAnalysis
{
    public bool ReadLatencyAchievable { get; set; }
    public bool WriteLatencyAchievable { get; set; }
    public bool ThroughputAchievable { get; set; }
    public bool FlushReductionAchievable { get; set; }
    
    public double ReadLatencyGap { get; set; }
    public double WriteLatencyGap { get; set; }
    public double ThroughputGap { get; set; }
    public double FlushReductionGap { get; set; }
    
    public bool AllTargetsAchievable => ReadLatencyAchievable && WriteLatencyAchievable && 
                                       ThroughputAchievable && FlushReductionAchievable;
}