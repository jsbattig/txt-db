namespace TxtDb.Storage.Interfaces.Async;

/// <summary>
/// Async Format Adapter Interface for Performance Optimization
/// Phase 2: Core Async Storage - Extends serialization with async methods
/// CRITICAL: Maintains serialization correctness while providing async benefits
/// </summary>
public interface IAsyncFormatAdapter
{
    /// <summary>
    /// File extension for this format adapter
    /// </summary>
    string FileExtension { get; }

    /// <summary>
    /// Asynchronously serializes an object to string
    /// </summary>
    /// <typeparam name="T">Type of object to serialize</typeparam>
    /// <param name="obj">Object to serialize</param>
    /// <returns>Serialized string</returns>
    Task<string> SerializeAsync<T>(T obj);

    /// <summary>
    /// Asynchronously deserializes a string to object
    /// </summary>
    /// <typeparam name="T">Type of object to deserialize to</typeparam>
    /// <param name="content">String content to deserialize</param>
    /// <returns>Deserialized object</returns>
    Task<T> DeserializeAsync<T>(string content);

    /// <summary>
    /// Asynchronously deserializes array content
    /// </summary>
    /// <param name="content">String content to deserialize</param>
    /// <param name="elementType">Type of array elements</param>
    /// <returns>Deserialized object array</returns>
    Task<object[]> DeserializeArrayAsync(string content, Type elementType);

    /// <summary>
    /// Asynchronously serializes an object array
    /// </summary>
    /// <param name="objects">Object array to serialize</param>
    /// <returns>Serialized string</returns>
    Task<string> SerializeArrayAsync(object[] objects);
}