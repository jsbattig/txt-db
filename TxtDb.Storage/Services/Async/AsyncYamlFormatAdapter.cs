using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using TxtDb.Storage.Interfaces.Async;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async YAML Format Adapter for Performance Optimization
/// Phase 2: Core Async Storage - Provides async YAML serialization
/// CRITICAL: Maintains YAML serialization correctness while enabling async I/O benefits
/// </summary>
public class AsyncYamlFormatAdapter : IAsyncFormatAdapter
{
    private readonly ISerializer _serializer;
    private readonly IDeserializer _deserializer;

    public string FileExtension => ".yaml";

    public AsyncYamlFormatAdapter()
    {
        _serializer = new SerializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();

        _deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();
    }

    public async Task<string> SerializeAsync<T>(T obj, CancellationToken cancellationToken = default)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));

        try
        {
            // Use Task.Run to make synchronous YAML serialization async-friendly
            // YAML serialization is CPU-bound, so we offload to thread pool
            return await Task.Run(() => _serializer.Serialize(obj)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public async Task<T> DeserializeAsync<T>(string content, CancellationToken cancellationToken = default)
    {
        if (content == null)
            throw new ArgumentNullException(nameof(content));

        try
        {
            // Use Task.Run to make synchronous YAML deserialization async-friendly
            var result = await Task.Run(() => _deserializer.Deserialize<T>(content)).ConfigureAwait(false);
            return result ?? throw new InvalidOperationException($"YAML deserialization returned null for type {typeof(T).Name}");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to deserialize YAML content to type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public async Task<object[]> DeserializeArrayAsync(string content, Type elementType, CancellationToken cancellationToken = default)
    {
        if (content == null)
            throw new ArgumentNullException(nameof(content));

        try
        {
            var arrayType = elementType.MakeArrayType();
            var result = await Task.Run(() => _deserializer.Deserialize(content, arrayType)).ConfigureAwait(false);
            
            if (result is object[] array)
                return array;
                
            if (result is Array genericArray)
            {
                var objects = new object[genericArray.Length];
                Array.Copy(genericArray, objects, genericArray.Length);
                return objects;
            }
            
            throw new InvalidOperationException($"YAML deserialization did not return an array for element type {elementType.Name}");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to deserialize YAML array with element type {elementType.Name}: {ex.Message}", ex);
        }
    }

    public async Task<string> SerializeArrayAsync(object[] objects, CancellationToken cancellationToken = default)
    {
        if (objects == null)
            throw new ArgumentNullException(nameof(objects));

        try
        {
            return await Task.Run(() => _serializer.Serialize(objects)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to serialize object array: {ex.Message}", ex);
        }
    }
}