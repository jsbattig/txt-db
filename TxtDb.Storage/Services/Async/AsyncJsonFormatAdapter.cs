using Newtonsoft.Json;
using TxtDb.Storage.Interfaces.Async;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async JSON Format Adapter for Performance Optimization
/// Phase 2: Core Async Storage - Provides async JSON serialization
/// CRITICAL: Maintains JSON serialization correctness while enabling async I/O benefits
/// </summary>
public class AsyncJsonFormatAdapter : IAsyncFormatAdapter
{
    private readonly JsonSerializerSettings _settings;

    public string FileExtension => ".json";

    public AsyncJsonFormatAdapter()
    {
        _settings = new JsonSerializerSettings
        {
            Formatting = Formatting.Indented, // Pretty-print for git diff readability
            NullValueHandling = NullValueHandling.Include,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ReferenceLoopHandling = ReferenceLoopHandling.Error // Detect circular references
        };
    }

    public async Task<string> SerializeAsync<T>(T obj)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));

        try
        {
            // Use Task.Run to make synchronous JSON serialization async-friendly
            // JSON.NET serialization is CPU-bound, so we offload to thread pool
            return await Task.Run(() => JsonConvert.SerializeObject(obj, _settings)).ConfigureAwait(false);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public async Task<T> DeserializeAsync<T>(string content)
    {
        if (content == null)
            throw new ArgumentNullException(nameof(content));

        try
        {
            // Use Task.Run to make synchronous JSON deserialization async-friendly
            var result = await Task.Run(() => JsonConvert.DeserializeObject<T>(content, _settings)).ConfigureAwait(false);
            return result ?? throw new InvalidOperationException($"Deserialization returned null for type {typeof(T).Name}");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize JSON content to type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public async Task<object[]> DeserializeArrayAsync(string content, Type elementType)
    {
        if (content == null)
            throw new ArgumentNullException(nameof(content));

        try
        {
            var arrayType = elementType.MakeArrayType();
            var result = await Task.Run(() => JsonConvert.DeserializeObject(content, arrayType, _settings)).ConfigureAwait(false);
            
            if (result is object[] array)
                return array;
                
            if (result is Array genericArray)
            {
                var objects = new object[genericArray.Length];
                Array.Copy(genericArray, objects, genericArray.Length);
                return objects;
            }
            
            throw new InvalidOperationException($"Deserialization did not return an array for element type {elementType.Name}");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize JSON array with element type {elementType.Name}: {ex.Message}", ex);
        }
    }

    public async Task<string> SerializeArrayAsync(object[] objects)
    {
        if (objects == null)
            throw new ArgumentNullException(nameof(objects));

        try
        {
            return await Task.Run(() => JsonConvert.SerializeObject(objects, _settings)).ConfigureAwait(false);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to serialize object array: {ex.Message}", ex);
        }
    }
}