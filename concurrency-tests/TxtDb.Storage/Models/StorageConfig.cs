namespace TxtDb.Storage.Models;

public class StorageConfig
{
    public SerializationFormat Format { get; set; } = SerializationFormat.Json;
    public int MaxPageSizeKB { get; set; } = 8;
    public string NamespaceDelimiter { get; set; } = ".";
    public int VersionCleanupIntervalMinutes { get; set; } = 15;
}

public enum SerializationFormat
{
    Json,
    Xml,
    Yaml
}