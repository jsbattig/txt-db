using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

/// <summary>
/// CRITICAL: NO MOCKING - Tests real serialization/deserialization with actual libraries
/// Tests for JSON, XML, and YAML format adapters with complex objects and edge cases
/// </summary>
public class FormatAdapterTests
{
    [Fact]
    public void JsonAdapter_SerializeDeserialize_SimpleObject_ShouldRoundTrip()
    {
        // Arrange
        var adapter = new JsonFormatAdapter();
        var originalObject = new TestObject
        {
            Id = 123,
            Name = "Test Object",
            Created = DateTime.UtcNow,
            IsActive = true,
            Tags = new[] { "tag1", "tag2", "tag3" }
        };

        // Act
        var serialized = adapter.Serialize(originalObject);
        var deserialized = adapter.Deserialize<TestObject>(serialized);

        // Assert
        Assert.NotNull(serialized);
        Assert.Contains("Test Object", serialized);
        Assert.Equal(originalObject.Id, deserialized.Id);
        Assert.Equal(originalObject.Name, deserialized.Name);
        Assert.Equal(originalObject.IsActive, deserialized.IsActive);
        Assert.Equal(originalObject.Tags, deserialized.Tags);
        Assert.Equal(originalObject.Created.ToString("O"), deserialized.Created.ToString("O"));
    }

    [Fact]
    public void JsonAdapter_SerializeArray_MultipleObjects_ShouldRoundTrip()
    {
        // Arrange
        var adapter = new JsonFormatAdapter();
        var objects = new object[]
        {
            new TestObject { Id = 1, Name = "First", IsActive = true },
            new TestObject { Id = 2, Name = "Second", IsActive = false },
            new TestObject { Id = 3, Name = "Third", IsActive = true }
        };

        // Act
        var serialized = adapter.SerializeArray(objects);
        var deserialized = adapter.DeserializeArray(serialized, typeof(TestObject));

        // Assert
        Assert.Equal(objects.Length, deserialized.Length);
        for (int i = 0; i < objects.Length; i++)
        {
            var original = (TestObject)objects[i];
            var restored = (TestObject)deserialized[i];
            Assert.Equal(original.Id, restored.Id);
            Assert.Equal(original.Name, restored.Name);
            Assert.Equal(original.IsActive, restored.IsActive);
        }
    }

    [Fact]
    public void XmlAdapter_SerializeDeserialize_ComplexObject_ShouldRoundTrip()
    {
        // Arrange
        var adapter = new XmlFormatAdapter();
        var originalObject = new TestObject
        {
            Id = 456,
            Name = "XML Test",
            Created = DateTime.UtcNow,
            IsActive = false,
            Tags = new[] { "xml", "test" },
            Nested = new NestedObject { Value = "Nested Value", Count = 42 }
        };

        // Act
        var serialized = adapter.Serialize(originalObject);
        var deserialized = adapter.Deserialize<TestObject>(serialized);

        // Assert
        Assert.NotNull(serialized);
        Assert.Contains("XML Test", serialized);
        Assert.Contains("<TestObject", serialized);
        Assert.Equal(originalObject.Id, deserialized.Id);
        Assert.Equal(originalObject.Name, deserialized.Name);
        Assert.Equal(originalObject.IsActive, deserialized.IsActive);
        Assert.Equal(originalObject.Nested.Value, deserialized.Nested.Value);
        Assert.Equal(originalObject.Nested.Count, deserialized.Nested.Count);
    }

    [Fact]
    public void YamlAdapter_SerializeDeserialize_WithSpecialCharacters_ShouldRoundTrip()
    {
        // Arrange
        var adapter = new YamlFormatAdapter();
        var originalObject = new TestObject
        {
            Id = 789,
            Name = "YAML: Test with \"quotes\" and 'apostrophes' and\nnewlines",
            Created = DateTime.UtcNow,
            IsActive = true,
            Tags = new[] { "yaml", "special-chars", "multi\nline" }
        };

        // Act
        var serialized = adapter.Serialize(originalObject);
        var deserialized = adapter.Deserialize<TestObject>(serialized);

        // Assert
        Assert.NotNull(serialized);
        Assert.Contains("Id: 789", serialized);
        Assert.Equal(originalObject.Id, deserialized.Id);
        Assert.Equal(originalObject.Name, deserialized.Name);
        Assert.Equal(originalObject.IsActive, deserialized.IsActive);
        Assert.Equal(originalObject.Tags, deserialized.Tags);
    }

    [Fact]
    public void AllAdapters_LargeObject_ShouldHandlePerformanceTest()
    {
        // Arrange
        var adapters = new IFormatAdapter[]
        {
            new JsonFormatAdapter(),
            new XmlFormatAdapter(),
            new YamlFormatAdapter()
        };

        var largeObject = new TestObject
        {
            Id = 1,
            Name = "Large Object Test",
            Created = DateTime.UtcNow,
            IsActive = true,
            Tags = Enumerable.Range(0, 1000).Select(i => $"tag-{i}").ToArray(),
            LargeText = string.Join("", Enumerable.Range(0, 5000).Select(i => $"Line {i}\n"))
        };

        foreach (var adapter in adapters)
        {
            // Act
            var startTime = DateTime.UtcNow;
            var serialized = adapter.Serialize(largeObject);
            var deserialized = adapter.Deserialize<TestObject>(serialized);
            var elapsed = DateTime.UtcNow - startTime;

            // Assert
            Assert.True(elapsed.TotalSeconds < 5, $"{adapter.GetType().Name} took too long: {elapsed.TotalSeconds}s");
            Assert.Equal(largeObject.Id, deserialized.Id);
            Assert.Equal(largeObject.Tags.Length, deserialized.Tags.Length);
            Assert.Equal(largeObject.LargeText.Length, deserialized.LargeText.Length);
        }
    }

    [Fact]
    public void JsonAdapter_CorruptedData_ShouldThrowMeaningfulException()
    {
        // Arrange
        var adapter = new JsonFormatAdapter();
        var corruptedJson = "{ \"Id\": 123, \"Name\": \"Test\", invalid json here }";

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => adapter.Deserialize<TestObject>(corruptedJson));
        Assert.NotNull(exception.Message);
    }

    [Fact]
    public void XmlAdapter_CorruptedData_ShouldThrowMeaningfulException()
    {
        // Arrange
        var adapter = new XmlFormatAdapter();
        var corruptedXml = "<TestObject><Id>123</Id><Name>Test</Name><InvalidTag>";

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => adapter.Deserialize<TestObject>(corruptedXml));
        Assert.NotNull(exception.Message);
    }

    [Fact]
    public void YamlAdapter_CorruptedData_ShouldThrowMeaningfulException()
    {
        // Arrange
        var adapter = new YamlFormatAdapter();
        var corruptedYaml = "Id: 123\nName: Test\n  InvalidIndentation:\nNoValue";

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => adapter.Deserialize<TestObject>(corruptedYaml));
        Assert.NotNull(exception.Message);
    }

    [Fact]
    public void AllAdapters_NullAndEmptyValues_ShouldHandleGracefully()
    {
        // Arrange
        var adapters = new IFormatAdapter[]
        {
            new JsonFormatAdapter(),
            new XmlFormatAdapter(),
            new YamlFormatAdapter()
        };

        var testObject = new TestObject
        {
            Id = 0,
            Name = null,
            Created = default,
            IsActive = false,
            Tags = null,
            Nested = null
        };

        foreach (var adapter in adapters)
        {
            // Act
            var serialized = adapter.Serialize(testObject);
            var deserialized = adapter.Deserialize<TestObject>(serialized);

            // Assert
            Assert.Equal(testObject.Id, deserialized.Id);
            Assert.Equal(testObject.IsActive, deserialized.IsActive);
            // Null handling may vary by format - just ensure no exceptions
            Assert.NotNull(serialized);
        }
    }

    [Theory]
    [InlineData(".json")]
    [InlineData(".xml")]
    [InlineData(".yaml")]
    public void FileExtensions_ShouldMatchFormat(string expectedExtension)
    {
        // Arrange & Act
        IFormatAdapter adapter = expectedExtension switch
        {
            ".json" => new JsonFormatAdapter(),
            ".xml" => new XmlFormatAdapter(),
            ".yaml" => new YamlFormatAdapter(),
            _ => throw new ArgumentException("Unknown extension")
        };

        // Assert
        Assert.Equal(expectedExtension, adapter.FileExtension);
    }
}

// Test data classes
public class TestObject
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public DateTime Created { get; set; }
    public bool IsActive { get; set; }
    public string[]? Tags { get; set; }
    public string? LargeText { get; set; }
    public NestedObject? Nested { get; set; }
}

public class NestedObject
{
    public string? Value { get; set; }
    public int Count { get; set; }
}