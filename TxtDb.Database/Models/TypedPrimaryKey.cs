using System;
using System.Text.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TxtDb.Database.Models;

/// <summary>
/// Type-safe wrapper for primary key values that preserves type information through JSON serialization.
/// Solves the core issue where GUID, DateTime, and other non-primitive types lose their type information
/// when serialized/deserialized, causing key comparison failures in Table operations.
/// 
/// CRITICAL FIX: This addresses the Phase 6 type safety failures where:
/// - Insert: Uses System.Guid type
/// - Retrieve: Extracts as System.String type  
/// - Result: Key comparison fails because "System.Guid:value" != "System.String:value"
/// </summary>
public class TypedPrimaryKey : IEquatable<TypedPrimaryKey>
{
    /// <summary>
    /// The actual primary key value (e.g., GUID, int, string)
    /// </summary>
    public object Value { get; }
    
    /// <summary>
    /// The .NET type of the primary key value for type-safe comparison
    /// </summary>
    public Type ValueType { get; }

    /// <summary>
    /// Creates a new typed primary key wrapper
    /// </summary>
    /// <param name="value">The primary key value</param>
    /// <exception cref="ArgumentNullException">If value is null</exception>
    public TypedPrimaryKey(object value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value), "Primary key value cannot be null");
        ValueType = value.GetType();
    }

    /// <summary>
    /// Creates a typed primary key from a potentially serialized/deserialized object.
    /// Handles type reconstruction for objects that may have lost type information during JSON processing.
    /// </summary>
    /// <param name="value">The value (may be JValue, string representation, etc.)</param>
    /// <param name="originalType">The original expected type (optional)</param>
    /// <returns>A typed primary key with the correct type</returns>
    public static TypedPrimaryKey FromValue(object value, Type? originalType = null)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value), "Primary key value cannot be null");

        // Handle JValue unwrapping - common case from JSON deserialization
        if (value is JValue jValue)
        {
            value = jValue.Value ?? throw new InvalidOperationException("JValue contains null value");
        }

        // If we have type information, try to reconstruct the original type
        if (originalType != null && value.GetType() != originalType)
        {
            try
            {
                // Common type conversions that get lost during JSON serialization
                if (originalType == typeof(Guid) && value is string guidString)
                {
                    return new TypedPrimaryKey(Guid.Parse(guidString));
                }
                
                if (originalType == typeof(DateTime) && value is string dateTimeString)
                {
                    return new TypedPrimaryKey(DateTime.Parse(dateTimeString));
                }
                
                if (originalType == typeof(DateTimeOffset) && value is string dateTimeOffsetString)
                {
                    return new TypedPrimaryKey(DateTimeOffset.Parse(dateTimeOffsetString));
                }
                
                // Handle numeric type conversions (JSON often converts int to long)
                if (originalType == typeof(int) && value is long longValue && longValue >= int.MinValue && longValue <= int.MaxValue)
                {
                    return new TypedPrimaryKey((int)longValue);
                }
                
                if (originalType == typeof(short) && value is long shortLongValue && shortLongValue >= short.MinValue && shortLongValue <= short.MaxValue)
                {
                    return new TypedPrimaryKey((short)shortLongValue);
                }

                // Try generic conversion using Convert.ChangeType
                var convertedValue = Convert.ChangeType(value, originalType);
                return new TypedPrimaryKey(convertedValue);
            }
            catch
            {
                // If conversion fails, use the value as-is
                return new TypedPrimaryKey(value);
            }
        }

        return new TypedPrimaryKey(value);
    }

    /// <summary>
    /// Type-safe equality comparison that ensures type and value match
    /// </summary>
    public bool Equals(TypedPrimaryKey? other)
    {
        if (other == null) return false;
        return ValueType == other.ValueType && Equals(Value, other.Value);
    }

    /// <summary>
    /// Type-safe equality comparison with object
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (obj is TypedPrimaryKey other) return Equals(other);
        if (obj == null) return false;
        
        // Allow direct comparison with the underlying value, but check type compatibility
        return ValueType == obj.GetType() && Equals(Value, obj);
    }

    /// <summary>
    /// Hash code based on both type and value for reliable dictionary operations
    /// </summary>
    public override int GetHashCode()
    {
        return HashCode.Combine(ValueType, Value);
    }

    /// <summary>
    /// String representation for debugging and normalization
    /// </summary>
    public override string ToString()
    {
        return $"{ValueType.FullName}:{Value}";
    }

    /// <summary>
    /// Creates a normalized string key for reliable dictionary operations.
    /// This ensures that different instances with the same type and value produce the same key.
    /// </summary>
    public string ToNormalizedKey()
    {
        return ToString();
    }

    /// <summary>
    /// Implicit conversion from common types to TypedPrimaryKey
    /// </summary>
    public static implicit operator TypedPrimaryKey(int value) => new(value);
    public static implicit operator TypedPrimaryKey(long value) => new(value);
    public static implicit operator TypedPrimaryKey(string value) => new(value);
    public static implicit operator TypedPrimaryKey(Guid value) => new(value);
    public static implicit operator TypedPrimaryKey(DateTime value) => new(value);

    /// <summary>
    /// Equality operators for convenience
    /// </summary>
    public static bool operator ==(TypedPrimaryKey? left, TypedPrimaryKey? right)
    {
        return Equals(left, right);
    }

    public static bool operator !=(TypedPrimaryKey? left, TypedPrimaryKey? right)
    {
        return !Equals(left, right);
    }
}

/// <summary>
/// JSON converter for TypedPrimaryKey to handle serialization/deserialization correctly
/// </summary>
public class TypedPrimaryKeyJsonConverter : JsonConverter<TypedPrimaryKey>
{
    public override void WriteJson(JsonWriter writer, TypedPrimaryKey? value, Newtonsoft.Json.JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }

        // Serialize as an object with type information
        writer.WriteStartObject();
        writer.WritePropertyName("Type");
        writer.WriteValue(value.ValueType.FullName);
        writer.WritePropertyName("Value");
        serializer.Serialize(writer, value.Value);
        writer.WriteEndObject();
    }

    public override TypedPrimaryKey? ReadJson(JsonReader reader, Type objectType, TypedPrimaryKey? existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;

        var jObject = JObject.Load(reader);
        var typeName = jObject["Type"]?.Value<string>();
        var valueToken = jObject["Value"];

        if (typeName == null || valueToken == null)
            throw new JsonSerializationException("TypedPrimaryKey JSON must contain Type and Value properties");

        var type = Type.GetType(typeName) ?? throw new JsonSerializationException($"Could not resolve type: {typeName}");
        var value = valueToken.ToObject(type);

        return value == null ? null : new TypedPrimaryKey(value);
    }
}