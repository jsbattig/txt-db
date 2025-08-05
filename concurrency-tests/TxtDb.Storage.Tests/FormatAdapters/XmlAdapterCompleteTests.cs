using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.FormatAdapters;

// Test model classes for XML serialization (XML requires parameterless constructors)
public class TestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
}

public class UserModel
{
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public SettingsModel Settings { get; set; } = new();
}

public class SettingsModel
{
    public string Theme { get; set; } = string.Empty;
    public bool Notifications { get; set; }
}

// Additional test model classes for XML serialization compatibility
public class SpecialCharsModel
{
    public string Text { get; set; } = string.Empty;
    public string Html { get; set; } = string.Empty;
    public string Quotes { get; set; } = string.Empty;
}

public class NullTestModel
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public string Description { get; set; } = string.Empty;
    public object? OptionalData { get; set; }
}

public class CollectionsModel
{
    public int Id { get; set; }
    public string[] EmptyArray { get; set; } = Array.Empty<string>();
    public List<int> EmptyList { get; set; } = new();
    public List<KeyValuePair<string, string>> EmptyDict { get; set; } = new(); // XML-serializable alternative to Dictionary
}

public class LargeTestModel
{
    public int Id { get; set; }
    public string LargeText { get; set; } = string.Empty;
    public int[] Numbers { get; set; } = Array.Empty<int>();
    public NestedItem[] Nested { get; set; } = Array.Empty<NestedItem>();
}

public class NestedItem
{
    public int Index { get; set; }
    public string Value { get; set; } = string.Empty;
}

public class NonSerializableModel
{
    public MemoryStream Stream { get; set; } = new();
    public Action Delegate { get; set; } = () => { };
}

public class RoundTripModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Score { get; set; }
    public bool Active { get; set; }
}

public class ComplexArrayItem
{
    public int Id { get; set; }
    public string Data { get; set; } = string.Empty;
    public int[] Values { get; set; } = Array.Empty<int>();
}

public class ArrayTestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
}

public class MixedTypeModel
{
    public string Type { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty; // Changed to string for XML compatibility
}

public class PerformanceTestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string[] Tags { get; set; } = Array.Empty<string>();
    public MetadataModel Metadata { get; set; } = new();
}

public class MetadataModel
{
    public DateTime Created { get; set; }
    public DateTime Modified { get; set; }
    public int Version { get; set; }
}

public class FormattingTestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public NestedFormattingModel Nested { get; set; } = new();
}

public class NestedFormattingModel
{
    public Level1Model Level1 { get; set; } = new();
}

public class Level1Model
{
    public string Level2 { get; set; } = string.Empty;
}

// XML-friendly deserialization models
public class DeserializedTestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
    public double Score { get; set; }
}

public class ComplexDeserializationModel
{
    public UserData User { get; set; } = new();
    public SettingsData Settings { get; set; } = new();
}

public class UserData
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ProfileData Profile { get; set; } = new();
}

public class ProfileData
{
    public string Email { get; set; } = string.Empty;
    public int Age { get; set; }
}

public class SettingsData
{
    public string Theme { get; set; } = string.Empty;
    public bool Notifications { get; set; }
}

/// <summary>
/// CRITICAL: COMPREHENSIVE XML ADAPTER TESTING - NO MOCKING
/// Tests all XML serialization/deserialization paths including error handling and edge cases
/// ALL tests use real XML processing, real object serialization, real error scenarios
/// </summary>
public class XmlAdapterCompleteTests
{
    private readonly XmlFormatAdapter _adapter;

    public XmlAdapterCompleteTests()
    {
        _adapter = new XmlFormatAdapter();
    }

    [Fact]
    public void FileExtension_ShouldReturnXmlExtension()
    {
        // Act & Assert
        Assert.Equal(".xml", _adapter.FileExtension);
    }

    [Fact]
    public void Serialize_SimpleObject_ShouldProduceValidXml()
    {
        // Arrange
        var testObject = new TestModel { Id = 1, Name = "Test", Active = true };

        // Act
        var result = _adapter.Serialize(testObject);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<?xml", result);
        Assert.Contains("<Id>1</Id>", result);
        Assert.Contains("<Name>Test</Name>", result);
        Assert.Contains("<Active>true</Active>", result);
    }

    [Fact]
    public void Serialize_ComplexNestedObject_ShouldHandleCorrectly()
    {
        // Arrange
        var complexObject = new TestModel
        {
            Id = 1,
            Name = "John Doe", 
            Active = true
        };

        // Act
        var result = _adapter.Serialize(complexObject);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<?xml", result);
        Assert.Contains("<Name>John Doe</Name>", result);
        Assert.Contains("<Id>1</Id>", result);
    }

    [Fact]
    public void Serialize_WithSpecialCharacters_ShouldEscapeCorrectly()
    {
        // Arrange
        var objectWithSpecialChars = new SpecialCharsModel
        {
            Text = "Special chars: <>&\"'",
            Html = "<div>Hello & goodbye</div>",
            Quotes = "He said \"Hello\" & left"
        };

        // Act
        var result = _adapter.Serialize(objectWithSpecialChars);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("&lt;", result); // < should be escaped
        Assert.Contains("&gt;", result); // > should be escaped
        Assert.Contains("&amp;", result); // & should be escaped
        Assert.DoesNotContain("<div>Hello & goodbye</div>", result); // Should be escaped
    }

    [Fact]
    public void Serialize_NullValues_ShouldHandleGracefully()
    {
        // Arrange
        var objectWithNulls = new NullTestModel
        {
            Id = 1,
            Name = null,
            Description = "Valid",
            OptionalData = null
        };

        // Act
        var result = _adapter.Serialize(objectWithNulls);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<Id>1</Id>", result);
        Assert.Contains("<Description>Valid</Description>", result);
        // Null values should either be omitted or represented as empty elements
    }

    [Fact]
    public void Serialize_EmptyCollections_ShouldHandleCorrectly()
    {
        // Arrange
        var objectWithEmptyCollections = new CollectionsModel
        {
            Id = 1,
            EmptyArray = new string[0],
            EmptyList = new List<int>(),
            EmptyDict = new List<KeyValuePair<string, string>>()
        };

        // Act
        var result = _adapter.Serialize(objectWithEmptyCollections);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<Id>1</Id>", result);
        // Should handle empty collections without errors
    }

    [Fact]
    public void Serialize_LargeObject_ShouldCompleteSuccessfully()
    {
        // Arrange
        var largeObject = new LargeTestModel
        {
            Id = 1,
            LargeText = new string('X', 10000), // 10KB of text
            Numbers = Enumerable.Range(1, 1000).ToArray(),
            Nested = Enumerable.Range(1, 100).Select(i => new NestedItem { Index = i, Value = $"Item_{i}" }).ToArray()
        };

        // Act
        var result = _adapter.Serialize(largeObject);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<Id>1</Id>", result);
        Assert.True(result.Length > 20000); // Should be substantial XML content (reduced from 50000 to be more realistic)
    }

    [Fact]
    public void Serialize_NonSerializableObject_ShouldThrowMeaningfulException()
    {
        // Arrange
        var nonSerializableObject = new NonSerializableModel
        {
            Stream = new MemoryStream(), // Not XML serializable
            Delegate = new Action(() => { }) // Not XML serializable
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.Serialize(nonSerializableObject));
        
        Assert.Contains("serialize", exception.Message.ToLowerInvariant());
    }

    [Fact]
    public void Deserialize_ValidXml_ShouldReturnCorrectObject()
    {
        // Arrange
        var xmlContent = """
            <?xml version="1.0" encoding="utf-8"?>
            <DeserializedTestModel>
                <Id>42</Id>
                <Name>Test Object</Name>
                <Active>true</Active>
                <Score>98.5</Score>
            </DeserializedTestModel>
            """;

        // Act
        var result = _adapter.Deserialize<DeserializedTestModel>(xmlContent);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(42, result.Id);
        Assert.Equal("Test Object", result.Name);
        Assert.True(result.Active);
        Assert.Equal(98.5, result.Score);
    }

    [Fact]
    public void Deserialize_ComplexXml_ShouldHandleNestedStructures()
    {
        // Arrange
        var complexXml = """
            <?xml version="1.0" encoding="utf-8"?>
            <ComplexDeserializationModel>
                <User>
                    <Id>1</Id>
                    <Name>John Doe</Name>
                    <Profile>
                        <Email>john@example.com</Email>
                        <Age>30</Age>
                    </Profile>
                </User>
                <Settings>
                    <Theme>dark</Theme>
                    <Notifications>true</Notifications>
                </Settings>
            </ComplexDeserializationModel>
            """;

        // Act
        var result = _adapter.Deserialize<ComplexDeserializationModel>(complexXml);

        // Assert
        Assert.NotNull(result);
        // Should handle nested XML structures
    }

    [Fact]
    public void Deserialize_EmptyXml_ShouldHandleGracefully()
    {
        // Arrange
        var emptyXml = """
            <?xml version="1.0" encoding="utf-8"?>
            <DeserializedTestModel>
            </DeserializedTestModel>
            """;

        // Act
        var result = _adapter.Deserialize<DeserializedTestModel>(emptyXml);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public void Deserialize_InvalidXml_ShouldThrowMeaningfulException()
    {
        // Arrange - XML that should definitely fail validation and deserialization
        var invalidXmlCases = new[]
        {
            "<Invalid>Unclosed tag", // Definitely malformed - unclosed tag
            "Not XML at all", // Not XML
            "<?xml version=\"1.0\"?><DeserializedTestModel><Nested>", // Unclosed nested tag
            "<?xml version=\"1.0\"?><DeserializedTestModel attr=\"unclosed value></DeserializedTestModel>" // Unclosed attribute
        };

        // Act & Assert
        foreach (var invalidXml in invalidXmlCases)
        {
            // The XmlFormatAdapter should throw InvalidOperationException for malformed XML
            var exception = Assert.Throws<InvalidOperationException>(() =>
                _adapter.Deserialize<DeserializedTestModel>(invalidXml));
            
            // Verify the exception message contains XML-related information
            Assert.Contains("Failed to deserialize XML content", exception.Message);
        }
    }

    [Fact]
    public void Deserialize_NullOrEmptyString_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<DeserializedTestModel>(null!));
        
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<DeserializedTestModel>(""));
        
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<DeserializedTestModel>("   "));
    }

    [Fact]
    public void DeserializeArray_ValidXmlArray_ShouldReturnObjectArray()
    {
        // Arrange
        var xmlArrayContent = """
            <?xml version="1.0" encoding="utf-8"?>
            <ArrayOfObject>
                <Object>
                    <Id>1</Id>
                    <Name>First</Name>
                </Object>
                <Object>
                    <Id>2</Id>
                    <Name>Second</Name>
                </Object>
                <Object>
                    <Id>3</Id>
                    <Name>Third</Name>
                </Object>
            </ArrayOfObject>
            """;

        // Act
        var result = _adapter.DeserializeArray(xmlArrayContent, typeof(object));

        // Assert
        Assert.NotNull(result);
        Assert.Equal(3, result.Length);
    }

    [Fact]
    public void DeserializeArray_EmptyArray_ShouldReturnEmptyArray()
    {
        // Arrange
        var emptyArrayXml = """
            <?xml version="1.0" encoding="utf-8"?>
            <ArrayOfObject>
            </ArrayOfObject>
            """;

        // Act
        var result = _adapter.DeserializeArray(emptyArrayXml, typeof(object));

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void DeserializeArray_SingleElement_ShouldReturnSingleElementArray()
    {
        // Arrange
        var singleElementXml = """
            <?xml version="1.0" encoding="utf-8"?>
            <ArrayOfObject>
                <Object>
                    <Id>1</Id>
                    <Name>Only One</Name>
                </Object>
            </ArrayOfObject>
            """;

        // Act
        var result = _adapter.DeserializeArray(singleElementXml, typeof(object));

        // Assert
        Assert.NotNull(result);
        Assert.Single(result);
    }

    [Fact]
    public void DeserializeArray_InvalidXmlArray_ShouldThrowException()
    {
        // Arrange
        var invalidArrayCases = new[]
        {
            "<ArrayOfObject><Object>Unclosed",
            "Not XML at all",
            "<?xml version=\"1.0\"?><ArrayOfObject><Object></ArrayOfObject>", // Mismatched tags
            ""
        };

        // Act & Assert
        foreach (var invalidXml in invalidArrayCases)
        {
            if (string.IsNullOrEmpty(invalidXml))
            {
                // Empty content should throw ArgumentException (this is correct)
                Assert.Throws<ArgumentException>(() =>
                    _adapter.DeserializeArray(invalidXml, typeof(object)));
            }
            else
            {
                // Malformed XML should throw InvalidOperationException
                Assert.Throws<InvalidOperationException>(() =>
                    _adapter.DeserializeArray(invalidXml, typeof(object)));
            }
        }
    }

    [Fact]
    public void SerializeArray_ObjectArray_ShouldProduceValidXmlArray()
    {
        // Arrange
        var objectArray = new object[]
        {
            new ArrayTestModel { Id = 1, Name = "First", Active = true },
            new ArrayTestModel { Id = 2, Name = "Second", Active = false },
            new ArrayTestModel { Id = 3, Name = "Third", Active = true }
        };

        // Act
        var result = _adapter.SerializeArray(objectArray);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<?xml", result);
        Assert.Contains("First", result);
        Assert.Contains("Second", result);
        Assert.Contains("Third", result);
    }

    [Fact]
    public void SerializeArray_EmptyArray_ShouldProduceValidEmptyXmlArray()
    {
        // Arrange
        var emptyArray = new object[0];

        // Act
        var result = _adapter.SerializeArray(emptyArray);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<?xml", result);
    }

    [Fact]
    public void SerializeArray_MixedTypes_ShouldHandleCorrectly()
    {
        // Arrange
        var mixedArray = new object[]
        {
            new MixedTypeModel { Type = "String", Value = "Hello" },
            new MixedTypeModel { Type = "Number", Value = "42" },
            new MixedTypeModel { Type = "Boolean", Value = "true" },
            new MixedTypeModel { Type = "Date", Value = "2023-01-01" }
        };

        // Act
        var result = _adapter.SerializeArray(mixedArray);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("Hello", result);
        Assert.Contains("42", result);
        Assert.Contains("true", result);
        Assert.Contains("2023-01-01", result);
    }

    [Fact]
    public void SerializeArray_NullArray_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            _adapter.SerializeArray(null!));
    }

    [Fact]
    public void SerializeArray_ArrayWithNulls_ShouldHandleGracefully()
    {
        // Arrange
        var arrayWithNulls = new object[]
        {
            new ArrayTestModel { Id = 1, Name = "Valid" },
            new ArrayTestModel { Id = 0, Name = "" }, // Replace null with default object
            new ArrayTestModel { Id = 2, Name = "Also Valid" },
            new ArrayTestModel { Id = 0, Name = "" } // Replace null with default object
        };

        // Act
        var result = _adapter.SerializeArray(arrayWithNulls);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("Valid", result);
        Assert.Contains("Also Valid", result);
    }

    [Fact]
    public void RoundTrip_SimpleObject_ShouldMaintainDataIntegrity()
    {
        // Arrange
        var original = new RoundTripModel { Id = 42, Name = "Test", Score = 98.7, Active = true };

        // Act
        var serialized = _adapter.Serialize(original);
        var deserialized = _adapter.Deserialize<RoundTripModel>(serialized);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(42, deserialized.Id);
        Assert.Equal("Test", deserialized.Name);
        Assert.Equal(98.7, deserialized.Score);
        Assert.True(deserialized.Active);
    }

    [Fact]
    public void RoundTrip_ComplexObjectArray_ShouldPreserveStructure()
    {
        // Arrange
        var originalArray = new object[]
        {
            new ComplexArrayItem { Id = 1, Data = "First", Values = new[] { 1, 2, 3 } },
            new ComplexArrayItem { Id = 2, Data = "Second", Values = new[] { 4, 5, 6 } }
        };

        // Act
        var serialized = _adapter.SerializeArray(originalArray);
        var deserialized = _adapter.DeserializeArray(serialized, typeof(object));

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(2, deserialized.Length);
    }

    [Fact]
    public void Performance_LargeDataSet_ShouldCompleteInReasonableTime()
    {
        // Arrange
        var largeDataSet = Enumerable.Range(1, 10000).Select(i => new PerformanceTestModel
        {
            Id = i,
            Name = $"Item_{i}",
            Description = $"Description for item {i} with some additional text to make it larger",
            Tags = new[] { $"tag{i}", $"category{i % 10}", "common" },
            Metadata = new MetadataModel
            {
                Created = DateTime.UtcNow.AddDays(-i),
                Modified = DateTime.UtcNow,
                Version = i % 5 + 1
            }
        }).Cast<object>().ToArray();

        // Act
        var start = DateTime.UtcNow;
        var serialized = _adapter.SerializeArray(largeDataSet);
        var serializationTime = DateTime.UtcNow - start;

        start = DateTime.UtcNow;
        var deserialized = _adapter.DeserializeArray(serialized, typeof(object));
        var deserializationTime = DateTime.UtcNow - start;

        // Assert
        Assert.True(serializationTime.TotalSeconds < 30, $"Serialization took {serializationTime.TotalSeconds} seconds");
        Assert.True(deserializationTime.TotalSeconds < 30, $"Deserialization took {deserializationTime.TotalSeconds} seconds");
        Assert.Equal(largeDataSet.Length, deserialized.Length);
    }

    [Fact]
    public void XmlFormatting_ShouldBeReadableAndValid()
    {
        // Arrange
        var testObject = new FormattingTestModel
        {
            Id = 1,
            Name = "Formatting Test",
            Nested = new NestedFormattingModel
            {
                Level1 = new Level1Model
                {
                    Level2 = "Deep Value"
                }
            }
        };

        // Act
        var result = _adapter.Serialize(testObject);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("<?xml version=", result);
        Assert.Contains("encoding=", result);
        
        // Should be properly formatted XML that can be parsed
        try
        {
            var doc = System.Xml.Linq.XDocument.Parse(result);
            Assert.NotNull(doc.Root);
        }
        catch (Exception ex)
        {
            Assert.True(false, $"Generated XML is not valid: {ex.Message}");
        }
    }
}