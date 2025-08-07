using System.Dynamic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
            ReferenceLoopHandling = ReferenceLoopHandling.Error, // Detect circular references
            TypeNameHandling = TypeNameHandling.Auto // Preserve type information for complex objects
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
            {
                // Unwrap JValue/JObject types to proper primitives
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = UnwrapJTokens(array[i]);
                }
                return array;
            }
                
            if (result is Array genericArray)
            {
                var objects = new object[genericArray.Length];
                Array.Copy(genericArray, objects, genericArray.Length);
                // Unwrap JValue/JObject types to proper primitives
                for (int i = 0; i < objects.Length; i++)
                {
                    objects[i] = UnwrapJTokens(objects[i]);
                }
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

    /// <summary>
    /// Unwraps JValue, JObject, and JArray tokens to their underlying .NET primitive types.
    /// When TypeNameHandling is enabled, JObject instances with type information are preserved
    /// as-is to maintain proper deserialization. Otherwise, they are converted to ExpandoObject.
    /// </summary>
    /// <param name="obj">The object to unwrap</param>
    /// <returns>The unwrapped object with proper .NET types</returns>
    private static object UnwrapJTokens(object obj)
    {
        if (obj is JObject jObject)
        {
            // If the JObject has type information ($type property), leave it as JObject
            // so the calling code can deserialize it properly to the original type
            if (jObject.Property("$type") != null)
            {
                return jObject;
            }
            
            // Otherwise, convert to ExpandoObject for dynamic access
            var expando = new ExpandoObject();
            var dict = (IDictionary<string, object?>)expando;
            foreach (var property in jObject.Properties())
            {
                dict[property.Name] = UnwrapJTokens(property.Value);
            }
            return expando;
        }
        if (obj is JValue jValue)
        {
            return jValue.Value ?? obj;
        }
        if (obj is JArray jArray)
        {
            return jArray.Select(UnwrapJTokens).ToArray();
        }
        return obj;
    }
}