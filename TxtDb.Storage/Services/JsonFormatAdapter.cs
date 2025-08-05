using Newtonsoft.Json;
using TxtDb.Storage.Interfaces;

namespace TxtDb.Storage.Services;

public class JsonFormatAdapter : IFormatAdapter
{
    private readonly JsonSerializerSettings _settings;

    public string FileExtension => ".json";

    public JsonFormatAdapter()
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

    public string Serialize<T>(T obj)
    {
        try
        {
            return JsonConvert.SerializeObject(obj, _settings);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public T Deserialize<T>(string content)
    {
        try
        {
            var result = JsonConvert.DeserializeObject<T>(content, _settings);
            return result ?? throw new InvalidOperationException($"Deserialization returned null for type {typeof(T).Name}");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize JSON content to type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public object[] DeserializeArray(string content, Type elementType)
    {
        try
        {
            var arrayType = elementType.MakeArrayType();
            var result = JsonConvert.DeserializeObject(content, arrayType, _settings);
            
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

    public string SerializeArray(object[] objects)
    {
        try
        {
            return JsonConvert.SerializeObject(objects, _settings);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to serialize object array: {ex.Message}", ex);
        }
    }
}