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
}

public enum SerializationFormat
{
    Json,
    Xml,
    Yaml
}