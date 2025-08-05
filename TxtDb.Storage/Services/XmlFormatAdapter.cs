using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using TxtDb.Storage.Interfaces;

namespace TxtDb.Storage.Services;

public class XmlFormatAdapter : IFormatAdapter
{
    public string FileExtension => ".xml";

    public string Serialize<T>(T obj)
    {
        try
        {
            var serializer = new XmlSerializer(typeof(T));
            var settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = "  ", // 2 spaces for readability
                Encoding = Encoding.UTF8,
                OmitXmlDeclaration = false
            };

            using var stringWriter = new StringWriter();
            using var xmlWriter = XmlWriter.Create(stringWriter, settings);
            
            serializer.Serialize(xmlWriter, obj);
            return stringWriter.ToString();
        }
        catch (Exception ex) when (ex is InvalidOperationException or XmlException)
        {
            throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).Name} to XML: {ex.Message}", ex);
        }
    }

    public T Deserialize<T>(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
            throw new ArgumentException("Content cannot be null, empty, or whitespace.", nameof(content));
            
        try
        {
            // First validate XML is well-formed and parseable
            var doc = new XmlDocument();
            doc.LoadXml(content); // This will throw XmlException if XML is malformed
            
            var serializer = new XmlSerializer(typeof(T));
            using var stringReader = new StringReader(content);
            
            var result = serializer.Deserialize(stringReader);
            return result is T typedResult 
                ? typedResult 
                : throw new InvalidOperationException($"XML deserialization returned wrong type. Expected {typeof(T).Name}");
        }
        catch (XmlException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize XML content to type {typeof(T).Name}: Malformed XML - {ex.Message}", ex);
        }
        catch (Exception ex) when (ex is InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to deserialize XML content to type {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public object[] DeserializeArray(string content, Type elementType)
    {
        if (string.IsNullOrWhiteSpace(content))
            throw new ArgumentException("Content cannot be null, empty, or whitespace.", nameof(content));
            
        try
        {
            // Parse XML to detect the actual structure
            var doc = new XmlDocument();
            doc.LoadXml(content);
            
            // Handle empty arrays
            if (doc.DocumentElement == null || !doc.DocumentElement.HasChildNodes)
                return Array.Empty<object>();
            
            // For generic object arrays, we need to handle different XML structures
            if (elementType == typeof(object))
            {
                return DeserializeGenericObjectArray(content, doc);
            }
            
            // For strongly typed arrays, use standard XML serialization
            var arrayType = elementType.MakeArrayType();
            var serializer = new XmlSerializer(arrayType);
            
            using var stringReader = new StringReader(content);
            var result = serializer.Deserialize(stringReader);
            
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
        catch (Exception ex) when (ex is InvalidOperationException or XmlException)
        {
            throw new InvalidOperationException($"Failed to deserialize XML array with element type {elementType.Name}: {ex.Message}", ex);
        }
    }
    
    private object[] DeserializeGenericObjectArray(string content, XmlDocument doc)
    {
        var objects = new List<object>();
        
        // Get all child elements (they represent individual objects)
        var childNodes = doc.DocumentElement?.ChildNodes;
        if (childNodes == null) return Array.Empty<object>();
        
        foreach (XmlNode child in childNodes)
        {
            if (child.NodeType != XmlNodeType.Element) continue;
            
            // Create a dictionary representation of the XML element
            var obj = XmlElementToDictionary(child);
            objects.Add(obj);
        }
        
        return objects.ToArray();
    }
    
    private object XmlElementToDictionary(XmlNode element)
    {
        var result = new Dictionary<string, object>();
        
        foreach (XmlNode child in element.ChildNodes)
        {
            if (child.NodeType == XmlNodeType.Element)
            {
                var value = child.InnerText;
                
                // Try to parse common types
                if (int.TryParse(value, out var intVal))
                    result[child.Name] = intVal;
                else if (bool.TryParse(value, out var boolVal))
                    result[child.Name] = boolVal;
                else if (double.TryParse(value, out var doubleVal))
                    result[child.Name] = doubleVal;
                else
                    result[child.Name] = value;
            }
        }
        
        return result;
    }

    public string SerializeArray(object[] objects)
    {
        if (objects == null)
            throw new ArgumentNullException(nameof(objects));
            
        try
        {
            if (objects.Length == 0)
                return SerializeEmptyArray();

            // Find the first non-null element to determine the type
            var elementType = typeof(object);
            for (int i = 0; i < objects.Length; i++)
            {
                if (objects[i] != null)
                {
                    elementType = objects[i].GetType();
                    break;
                }
            }
            
            // CRITICAL XML ARRAY FIX: Keep using the original concrete type approach
            // The DeserializeArray method already handles different XML element names correctly
            // No need for special handling - the core issue was elsewhere
            
            var arrayType = elementType.MakeArrayType();
            var serializer = new XmlSerializer(arrayType);
            
            var settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = "  ",
                Encoding = Encoding.UTF8,
                OmitXmlDeclaration = false
            };

            using var stringWriter = new StringWriter();
            using var xmlWriter = XmlWriter.Create(stringWriter, settings);
            
            // Convert to strongly typed array for serialization, handling nulls
            var typedArray = Array.CreateInstance(elementType, objects.Length);
            for (int i = 0; i < objects.Length; i++)
            {
                if (objects[i] != null && elementType.IsAssignableFrom(objects[i].GetType()))
                {
                    typedArray.SetValue(objects[i], i);
                }
                // Leave null elements as null in the typed array
            }
            
            serializer.Serialize(xmlWriter, typedArray);
            return stringWriter.ToString();
        }
        catch (Exception ex) when (ex is InvalidOperationException or XmlException)
        {
            throw new InvalidOperationException($"Failed to serialize object array to XML: {ex.Message}", ex);
        }
    }

    private static string SerializeEmptyArray()
    {
        return """
            <?xml version="1.0" encoding="utf-8"?>
            <ArrayOfObject xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" />
            """;
    }

    // REMOVED: Unused helper methods from experimental complex serialization approach
    // The final solution uses the original concrete type approach which works correctly
}