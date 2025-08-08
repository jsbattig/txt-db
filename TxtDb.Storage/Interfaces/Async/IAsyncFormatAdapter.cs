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
    /// CRITICAL FIX: Added cancellation token support for comprehensive cancellation propagation
    /// </summary>
    /// <typeparam name="T">Type of object to serialize</typeparam>
    /// <param name="obj">Object to serialize</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Serialized string</returns>
    Task<string> SerializeAsync<T>(T obj, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deserializes a string to object
    /// CRITICAL FIX: Added cancellation token support for comprehensive cancellation propagation
    /// </summary>
    /// <typeparam name="T">Type of object to deserialize to</typeparam>
    /// <param name="content">String content to deserialize</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Deserialized object</returns>
    Task<T> DeserializeAsync<T>(string content, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deserializes array content
    /// CRITICAL FIX: Added cancellation token support for comprehensive cancellation propagation
    /// </summary>
    /// <param name="content">String content to deserialize</param>
    /// <param name="elementType">Type of array elements</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Deserialized object array</returns>
    Task<object[]> DeserializeArrayAsync(string content, Type elementType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously serializes an object array
    /// CRITICAL FIX: Added cancellation token support for comprehensive cancellation propagation
    /// </summary>
    /// <param name="objects">Object array to serialize</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Serialized string</returns>
    Task<string> SerializeArrayAsync(object[] objects, CancellationToken cancellationToken = default);
}