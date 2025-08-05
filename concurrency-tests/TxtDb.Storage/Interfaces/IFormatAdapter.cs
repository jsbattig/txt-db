namespace TxtDb.Storage.Interfaces;

/// <summary>
/// Pluggable serialization adapter for different formats (JSON, XML, YAML)
/// </summary>
public interface IFormatAdapter
{
    string FileExtension { get; }
    string Serialize<T>(T obj);
    T Deserialize<T>(string content);
    object[] DeserializeArray(string content, Type elementType);
    string SerializeArray(object[] objects);
}