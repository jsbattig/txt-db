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
        
        // Check first object
        var first = deserializedArray[0];
        Assert.IsType<ExpandoObject>(first);
        
        var firstDict = (IDictionary<string, object?>)first;
        Assert.True(firstDict.ContainsKey("Id"));
        Assert.True(firstDict.ContainsKey("Name"));
        
        // Critical test: Id should be Int32 (or Int64), not JValue
        var idValue = firstDict["Id"];
        Assert.True(idValue is int || idValue is long, 
            $"Expected int or long, got {idValue?.GetType().Name}. Value: {idValue}");
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", idValue?.GetType().FullName);
        
        // Name should be string, not JValue
        var nameValue = firstDict["Name"];
        Assert.IsType<string>(nameValue);
        Assert.Equal("Test", nameValue);
        
        // Check second object 
        var second = deserializedArray[1];
        Assert.IsType<ExpandoObject>(second);
        
        var secondDict = (IDictionary<string, object?>)second;
        var secondIdValue = secondDict["Id"];
        Assert.True(secondIdValue is int || secondIdValue is long,
            $"Expected int or long, got {secondIdValue?.GetType().Name}. Value: {secondIdValue}");
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", secondIdValue?.GetType().FullName);
        
        var secondNameValue = secondDict["Name"];
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
        Assert.IsType<ExpandoObject>(obj);
        
        var dict = (IDictionary<string, object?>)obj;
        
        // Check Id
        var idValue = dict["Id"];
        Assert.True(idValue is int || idValue is long);
        
        // Check nested Metadata object
        var metadataValue = dict["Metadata"];
        Assert.IsType<ExpandoObject>(metadataValue);
        
        var metadataDict = (IDictionary<string, object?>)metadataValue;
        
        // Check Version inside nested object
        var versionValue = metadataDict["Version"];
        Assert.True(versionValue is int || versionValue is long);
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", versionValue?.GetType().FullName);
        
        // Check boolean value
        var isActiveValue = metadataDict["IsActive"];
        Assert.IsType<bool>(isActiveValue);
        Assert.True((bool)isActiveValue);
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
        Assert.IsType<ExpandoObject>(obj);
        
        var dict = (IDictionary<string, object?>)obj;
        
        // Check Tags array
        var tagsValue = dict["Tags"];
        Assert.IsType<object[]>(tagsValue);
        var tagsArray = (object[])tagsValue;
        Assert.Equal(2, tagsArray.Length);
        Assert.IsType<string>(tagsArray[0]);
        Assert.Equal("tag1", tagsArray[0]);
        
        // Check Numbers array  
        var numbersValue = dict["Numbers"];
        Assert.IsType<object[]>(numbersValue);
        var numbersArray = (object[])numbersValue;
        Assert.Equal(3, numbersArray.Length);
        Assert.True(numbersArray[0] is int || numbersArray[0] is long);
        Assert.NotEqual("Newtonsoft.Json.Linq.JValue", numbersArray[0]?.GetType().FullName);
    }
}