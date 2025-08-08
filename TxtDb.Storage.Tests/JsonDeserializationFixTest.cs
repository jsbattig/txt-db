using System.Dynamic;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// Test to verify the JSON deserialization fix for JValue/JObject unwrapping
/// Addresses the issue where integers were deserialized as Int64 instead of Int32,
/// and objects were JObject instead of ExpandoObject/primitives.
/// </summary>
public class JsonDeserializationFixTest
{
    [Fact]
    public void DeserializeArray_WithIntegers_ShouldReturnInt32NotInt64()
    {
        // Arrange
        var adapter = new JsonFormatAdapter();
        var objects = new object[]
        {
            new { Id = 42, Name = "Test" },
            new { Id = 123, Name = "Another" }
        };
        
        // Act - Serialize then deserialize
        var json = adapter.SerializeArray(objects);
        var deserializedArray = adapter.DeserializeArray(json, typeof(object));
        
        // Assert - Verify the objects are properly unwrapped
        Assert.NotNull(deserializedArray);
        Assert.Equal(2, deserializedArray.Length);
        
        // Check first object - expect anonymous type, not ExpandoObject
        var first = deserializedArray[0];
        Assert.True(first.GetType().Name.StartsWith("<>f__AnonymousType"), 
            $"Expected anonymous type, got {first.GetType().Name}");
        
        // Use reflection to access properties of anonymous type
        var firstType = first.GetType();
        var idProperty = firstType.GetProperty("Id")!;
        var nameProperty = firstType.GetProperty("Name")!;
        // Verify properties exist and have correct values
        Assert.NotNull(idProperty);
        Assert.NotNull(nameProperty);
        
        // Critical test: Id should be Int32 (or Int64), not JValue
        var idValue = idProperty.GetValue(first);
        Assert.True(idValue is int || idValue is long, 
            $"Expected int or long, got {idValue?.GetType().Name}. Value: {idValue}");
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", idValue?.GetType().FullName);
        
        // Name should be string, not JValue
        var nameValue = nameProperty.GetValue(first);
        Assert.IsType<string>(nameValue);
        Assert.Equal("Test", nameValue);
        
        // Check second object - expect anonymous type, not ExpandoObject
        var second = deserializedArray[1];
        Assert.True(second.GetType().Name.StartsWith("<>f__AnonymousType"), 
            $"Expected anonymous type, got {second.GetType().Name}");
        
        // Use reflection to access properties of anonymous type
        var secondType = second.GetType();
        var secondIdProperty = secondType.GetProperty("Id")!;
        var secondNameProperty = secondType.GetProperty("Name")!;
        
        var secondIdValue = secondIdProperty.GetValue(second);
        Assert.True(secondIdValue is int || secondIdValue is long,
            $"Expected int or long, got {secondIdValue?.GetType().Name}. Value: {secondIdValue}");
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", secondIdValue?.GetType().FullName);
        
        var secondNameValue = secondNameProperty.GetValue(second);
        Assert.IsType<string>(secondNameValue);
        Assert.Equal("Another", secondNameValue);
    }
    
    [Fact]
    public void DeserializeArray_WithNestedObjects_ShouldUnwrapRecursively()
    {
        // Arrange
        var adapter = new JsonFormatAdapter();
        var objects = new object[]
        {
            new { 
                Id = 1, 
                Metadata = new { 
                    Version = 42, 
                    IsActive = true 
                } 
            }
        };
        
        // Act
        var json = adapter.SerializeArray(objects);
        var deserializedArray = adapter.DeserializeArray(json, typeof(object));
        
        // Assert
        Assert.NotNull(deserializedArray);
        Assert.Single(deserializedArray);
        
        var obj = deserializedArray[0];
        Assert.True(obj.GetType().Name.StartsWith("<>f__AnonymousType"), 
            $"Expected anonymous type, got {obj.GetType().Name}");
        
        // Use reflection to access properties of anonymous type
        var objType = obj.GetType();
        var idProperty = objType.GetProperty("Id")!;
        var metadataProperty = objType.GetProperty("Metadata")!;
        
        // Check Id
        var idValue = idProperty.GetValue(obj);
        Assert.True(idValue is int || idValue is long);
        
        // Check nested Metadata object - should also be anonymous type
        var metadataValue = metadataProperty.GetValue(obj);
        Assert.True(metadataValue?.GetType().Name.StartsWith("<>f__AnonymousType"), 
            $"Expected anonymous type for nested object, got {metadataValue?.GetType().Name}");
        
        // Use reflection to access nested object properties
        var metadataType = metadataValue!.GetType();
        var versionProperty = metadataType.GetProperty("Version")!;
        var isActiveProperty = metadataType.GetProperty("IsActive")!;
        
        // Check Version inside nested object
        var versionValue = versionProperty.GetValue(metadataValue);
        Assert.True(versionValue is int || versionValue is long);
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", versionValue?.GetType().FullName);
        
        // Check boolean value
        var isActiveValue = isActiveProperty.GetValue(metadataValue);
        Assert.IsType<bool>(isActiveValue);
        Assert.True((bool)isActiveValue!);
    }
    
    [Fact]
    public void DeserializeArray_WithArrayValues_ShouldUnwrapArrays()
    {
        // Arrange  
        var adapter = new JsonFormatAdapter();
        var objects = new object[]
        {
            new { 
                Tags = new string[] { "tag1", "tag2" },
                Numbers = new int[] { 1, 2, 3 }
            }
        };
        
        // Act
        var json = adapter.SerializeArray(objects);
        var deserializedArray = adapter.DeserializeArray(json, typeof(object));
        
        // Assert
        Assert.NotNull(deserializedArray);
        Assert.Single(deserializedArray);
        
        var obj = deserializedArray[0];
        Assert.True(obj.GetType().Name.StartsWith("<>f__AnonymousType"), 
            $"Expected anonymous type, got {obj.GetType().Name}");
        
        // Use reflection to access properties of anonymous type
        var objType = obj.GetType();
        var tagsProperty = objType.GetProperty("Tags")!;
        var numbersProperty = objType.GetProperty("Numbers")!;
        
        // Check Tags array
        var tagsValue = tagsProperty.GetValue(obj);
        Assert.IsType<string[]>(tagsValue);
        var tagsArray = (string[])tagsValue!;
        Assert.Equal(2, tagsArray.Length);
        Assert.IsType<string>(tagsArray[0]);
        Assert.Equal("tag1", tagsArray[0]);
        
        // Check Numbers array  
        var numbersValue = numbersProperty.GetValue(obj);
        Assert.IsType<int[]>(numbersValue);
        var numbersArray = (int[])numbersValue!;
        Assert.Equal(3, numbersArray.Length);
        Assert.True(numbersArray[0] is int || numbersArray[0] is long);
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", numbersArray[0].GetType().FullName);
    }
}