namespace TxtDb.Storage.Models;

public class StorageConfig
{
    public SerializationFormat Format { get; set; } = SerializationFormat.Json;
    public int MaxPageSizeKB { get; set; } = 8;
    public string NamespaceDelimiter { get; set; } = ".";
    public int VersionCleanupIntervalMinutes { get; set; } = 15;
    
    /// <summary>
    /// When true, forces each object to be inserted into its own page for maximum MVCC isolation.
    /// When false (default), objects are packed into pages for better space efficiency.
    /// </summary>
    public bool ForceOneObjectPerPage { get; set; } = false;
    
    /// <summary>
    /// When true, enables batch flushing to reduce FlushToDisk calls and improve performance.
    /// When false (default), uses individual flush operations for each write.
    /// </summary>
    public bool EnableBatchFlushing { get; set; } = false;
    
    /// <summary>
    /// Configuration for batch flushing behavior when EnableBatchFlushing is true.
    /// Ignored when EnableBatchFlushing is false.
    /// </summary>
    public BatchFlushConfig? BatchFlushConfig { get; set; }
    
    /// <summary>
    /// Deadlock detection timeout in milliseconds. When a transaction waits for a lock
    /// longer than this timeout, it will be aborted to prevent deadlocks.
    /// Default is 30 seconds (30000ms). Set to 0 to disable deadlock detection.
    /// </summary>
    public int DeadlockTimeoutMs { get; set; } = 30000;
    
    /// <summary>
    /// When true, enables Wait-Die deadlock prevention algorithm for high-contention scenarios.
    /// When false (default), uses timeout-based locking which is more suitable for version cleanup operations.
    /// The Wait-Die algorithm immediately aborts younger transactions when they conflict with older ones,
    /// which can be too aggressive for routine operations like version cleanup.
    /// </summary>
    public bool EnableWaitDieDeadlockPrevention { get; set; } = false;
    
    /// <summary>
    /// Configuration for infrastructure hardening components (Phase 4)
    /// Includes retry policies, circuit breakers, memory pressure detection, and transaction recovery.
    /// </summary>
    public InfrastructureConfig Infrastructure { get; set; } = new();
}

public enum SerializationFormat
{
    Json,
    Xml,
    Yaml
}