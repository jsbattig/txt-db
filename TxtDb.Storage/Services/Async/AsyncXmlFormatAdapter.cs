using System.Xml;
using System.Xml.Serialization;
using TxtDb.Storage.Interfaces.Async;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async XML Format Adapter for Performance Optimization
/// Phase 2: Core Async Storage - Provides async XML serialization
/// CRITICAL: Maintains XML serialization correctness while enabling async I/O benefits
/// </summary>
public class AsyncXmlFormatAdapter : IAsyncFormatAdapter
{
    public string FileExtension => ".xml";

    public async Task<string> SerializeAsync<T>(T obj, CancellationToken cancellationToken = default)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));

        try
        {
            return await Task.Run(() =>
            {
                var serializer = new XmlSerializer(typeof(T));
                using var stringWriter = new StringWriter();
                using var xmlWriter = XmlWriter.Create(stringWriter, new XmlWriterSettings 
                { 
                    Indent = true,
                    OmitXmlDeclaration = true
                });
                
                serializer.Serialize(xmlWriter, obj);
                return stringWriter.ToString();
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is XmlException || ex is InvalidOperationException)
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
            var result = await Task.Run(() =>
            {
                var serializer = new XmlSerializer(typeof(T));
                using var stringReader = new StringReader(content);
                return (T?)serializer.Deserialize(stringReader);
            }).ConfigureAwait(false);
            
            return result ?? throw new InvalidOperationException($"XML deserialization returned null for type {typeof(T).Name}");
        }
        catch (Exception ex) when (ex is XmlException || ex is InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to deserialize XML content to type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public async Task<object[]> DeserializeArrayAsync(string content, Type elementType, CancellationToken cancellationToken = default)
    {
        if (content == null)
            throw new ArgumentNullException(nameof(content));

        try
        {
            var arrayType = elementType.MakeArrayType();
            var result = await Task.Run(() =>
            {
                var serializer = new XmlSerializer(arrayType);
                using var stringReader = new StringReader(content);
                return serializer.Deserialize(stringReader);
            }).ConfigureAwait(false);
            
            if (result is object[] array)
                return array;
                
            if (result is Array genericArray)
            {
                var objects = new object[genericArray.Length];
                Array.Copy(genericArray, objects, genericArray.Length);
                return objects;
            }
            
            throw new InvalidOperationException($"XML deserialization did not return an array for element type {elementType.Name}");
        }
        catch (Exception ex) when (ex is XmlException || ex is InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to deserialize XML array with element type {elementType.Name}: {ex.Message}", ex);
        }
    }

    public async Task<string> SerializeArrayAsync(object[] objects, CancellationToken cancellationToken = default)
    {
        if (objects == null)
            throw new ArgumentNullException(nameof(objects));

        try
        {
            return await Task.Run(() =>
            {
                var serializer = new XmlSerializer(objects.GetType());
                using var stringWriter = new StringWriter();
                using var xmlWriter = XmlWriter.Create(stringWriter, new XmlWriterSettings 
                { 
                    Indent = true,
                    OmitXmlDeclaration = true
                });
                
                serializer.Serialize(xmlWriter, objects);
                return stringWriter.ToString();
            }).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is XmlException || ex is InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to serialize object array: {ex.Message}", ex);
        }
    }
}