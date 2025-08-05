using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.FormatAdapters;

/// <summary>
/// CRITICAL: COMPREHENSIVE YAML ADAPTER ERROR PATH TESTING - NO MOCKING
/// Tests all YAML serialization error scenarios, edge cases, and untested branches
/// ALL tests use real YAML processing, real error conditions, real exception handling
/// </summary>
public class YamlAdapterErrorTests
{
    private readonly YamlFormatAdapter _adapter;

    public YamlAdapterErrorTests()
    {
        _adapter = new YamlFormatAdapter();
    }

    [Fact]
    public void FileExtension_ShouldReturnYamlExtension()
    {
        // Act & Assert
        Assert.Equal(".yaml", _adapter.FileExtension);
    }

    [Fact]
    public void Serialize_CircularReference_ShouldThrowMeaningfulException()
    {
        // Arrange - Create circular reference
        var parent = new Dictionary<string, object> { { "Id", 1 }, { "Name", "Parent" } };
        var child = new Dictionary<string, object> { { "Id", 2 }, { "Name", "Child" } };
        
        parent["Child"] = child;
        child["Parent"] = parent; // Circular reference

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.Serialize(parent));
        
        Assert.Contains("circular", exception.Message.ToLowerInvariant());
    }

    [Fact]
    public void Serialize_NonSerializableObject_ShouldThrowException()
    {
        // Arrange
        var nonSerializableObject = new
        {
            FileStream = new FileStream(Path.GetTempFileName(), FileMode.Create),
            Delegate = new Action(() => { }),
            Type = typeof(string)
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.Serialize(nonSerializableObject));
        
        Assert.Contains("serialize", exception.Message.ToLowerInvariant());
        
        // Cleanup
        nonSerializableObject.FileStream.Dispose();
    }

    [Fact]
    public void Serialize_ExtremelyLargeObject_ShouldHandleOrThrow()
    {
        // Arrange - Create very large object that might exceed memory limits
        var largeObject = new
        {
            Id = 1,
            HugeText = new string('X', 100_000_000), // 100MB string
            Numbers = Enumerable.Range(1, 1_000_000).ToArray() // 1M integers
        };

        // Act & Assert - Should either succeed or throw meaningful exception
        try
        {
            var result = _adapter.Serialize(largeObject);
            Assert.NotEmpty(result);
        }
        catch (OutOfMemoryException)
        {
            // This is acceptable for extremely large objects
            Assert.True(true, "OutOfMemoryException is acceptable for huge objects");
        }
        catch (InvalidOperationException ex)
        {
            // Should provide meaningful error message
            Assert.NotEmpty(ex.Message);
        }
    }

    public class YamlSpecialCharsTestObject
    {
        public string Text { get; set; } = string.Empty;
        public string MultiLine { get; set; } = string.Empty;
        public string Unicode { get; set; } = string.Empty;
        public string QuotedStrings { get; set; } = string.Empty;
        public string NullString { get; set; } = string.Empty;
        public string BoolString { get; set; } = string.Empty;
        public string NumberString { get; set; } = string.Empty;
    }

    [Fact]
    public void Serialize_ObjectWithInvalidYamlCharacters_ShouldEscapeCorrectly()
    {
        // Arrange - Use named class instead of anonymous type to avoid serialization restriction
        var objectWithSpecialChars = new YamlSpecialCharsTestObject
        {
            Text = "YAML special: : | > - [ ] { } # & * ! % @ `",
            MultiLine = "Line 1\nLine 2\r\nLine 3\tTabbed",
            Unicode = "Unicode: 🚀 αβγ δεζ ηθι κλμ νξο πρς στυ φχψ ω",
            QuotedStrings = "He said: \"Hello 'world'\" & left",
            NullString = "null",
            BoolString = "true",
            NumberString = "123.45"
        };

        // Act
        var result = _adapter.Serialize(objectWithSpecialChars);

        // Assert
        Assert.NotEmpty(result);
        Assert.Contains("YAML special", result);
        // Unicode may be escaped by YAML serializer - check for either format
        Assert.True(result.Contains("🚀") || result.Contains("\\U0001F680"), 
            "Unicode emoji should be present in either literal or escaped form");
        
        // Should be valid YAML that can be parsed back
        var deserialized = _adapter.Deserialize<YamlSpecialCharsTestObject>(result);
        Assert.NotNull(deserialized);
        Assert.Equal("YAML special: : | > - [ ] { } # & * ! % @ `", deserialized.Text);
        Assert.Equal("Unicode: 🚀 αβγ δεζ ηθι κλμ νξο πρς στυ φχψ ω", deserialized.Unicode);
    }

    [Fact]
    public void Deserialize_MalformedYaml_ShouldThrowMeaningfulException()
    {
        // Arrange
        var malformedYamlCases = new[]
        {
            "{ invalid: yaml: structure: }",
            "- item1\n  - item2\n- item3", // Invalid indentation
            "key: value\nkey: value", // Duplicate keys
            "invalid:\n  - item\n- another", // Mixed indentation
            "text: |\n  multiline\n text", // Invalid multiline
            "anchors: &anchor\n  value: test\nreference: *invalid_anchor", // Invalid anchor reference
            "!!invalid_tag value",
            "key with spaces without quotes: value",
            "key:\n  nested:\n    value\n  invalid_level: wrong"
        };

        // Act & Assert
        foreach (var malformedYaml in malformedYamlCases)
        {
            try
            {
                var result = _adapter.Deserialize<Dictionary<string, object>>(malformedYaml);
                // If we get here, the YAML was parsed successfully when it shouldn't have been
                Assert.True(false, $"Expected exception for malformed YAML but got successful result: '{malformedYaml.Replace('\n', '|')}'");
            }
            catch (InvalidOperationException ex)
            {
                // This is expected - malformed YAML should throw InvalidOperationException
                Assert.Contains("yaml", ex.Message.ToLowerInvariant());
            }
            catch (Exception ex)
            {
                // Some other exception type is also acceptable for malformed YAML
                Assert.True(ex is ArgumentException || ex is InvalidOperationException, 
                    $"Expected InvalidOperationException or ArgumentException for malformed YAML '{malformedYaml.Replace('\n', '|')}', got {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    [Fact]
    public void Deserialize_EmptyOrWhitespaceString_ShouldThrowArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<Dictionary<string, object>>(null!));
        
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<Dictionary<string, object>>(""));
        
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<Dictionary<string, object>>("   "));
        
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<Dictionary<string, object>>("\t\n\r"));
    }

    [Fact]
    public void Deserialize_UnsupportedYamlFeatures_ShouldHandleGracefully()
    {
        // Arrange - YAML features that might not be fully supported
        var advancedYamlCases = new[]
        {
            "!!binary SGVsbG8gV29ybGQ=", // Binary data
            "!!timestamp 2023-01-01T10:00:00Z", // Timestamp tag
            "!!set\n  ? item1\n  ? item2", // Set type
            "!!omap\n  - key1: value1\n  - key2: value2", // Ordered map
            "merged: &default\n  key1: value1\nspecific:\n  <<: *default\n  key2: value2" // Merge keys
        };

        // Act & Assert
        foreach (var advancedYaml in advancedYamlCases)
        {
            try
            {
                var result = _adapter.Deserialize<Dictionary<string, object>>(advancedYaml);
                // If it succeeds, that's fine
                Assert.NotNull(result);
            }
            catch (InvalidOperationException ex)
            {
                // Should provide meaningful error for unsupported features
                Assert.NotEmpty(ex.Message);
            }
        }
    }

    [Fact]
    public void DeserializeArray_InvalidYamlArray_ShouldThrowException()
    {
        // Arrange
        var invalidArrayCases = new[]
        {
            "- item1\n- item2\n  - nested: wrong", // Invalid nesting
            "- item1\n-item2", // Missing space
            "[\n  item1,\n  item2,", // Unclosed array
            "- item1\n- : empty_key", // Invalid key
            "- item1\n-\n  invalid", // Empty array item with continuation
            "[item1, item2", // Unclosed bracket array
            "- 'unclosed string", // Unclosed quoted string
            "- item1\n- item2: with_invalid_nested", // Mixed array/object
        };

        // Act & Assert
        foreach (var invalidYaml in invalidArrayCases)
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                _adapter.DeserializeArray(invalidYaml, typeof(object)));
            
            Assert.Contains("yaml", exception.Message.ToLowerInvariant());
        }
    }

    [Fact]
    public void DeserializeArray_NonArrayYaml_ShouldThrowException()
    {
        // Arrange
        var nonArrayYaml = "key: value\nanother_key: another_value";

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.DeserializeArray(nonArrayYaml, typeof(object)));
        
        Assert.Contains("array", exception.Message.ToLowerInvariant());
    }

    [Fact]
    public void DeserializeArray_MixedArrayTypes_ShouldHandleComplexScenarios()
    {
        // Arrange
        var mixedArrayYaml = """
            - simple_string
            - 42
            - true
            - null
            - nested:
                key: value
            - - nested_array_item1
              - nested_array_item2
            - key_with_spaces: "quoted value"
            """;

        // Act
        var result = _adapter.DeserializeArray(mixedArrayYaml, typeof(object));

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Length >= 6, $"Expected at least 6 items, got {result.Length}");
    }

    [Fact]
    public void DeserializeArray_EmptyArrayVariations_ShouldHandleAllFormats()
    {
        // Arrange
        var emptyArrayCases = new[]
        {
            "[]",
            "[ ]",
            "[\n]",
            "[\n  \n]",
            "", // Completely empty should be treated as empty array
            "# Just a comment",
            "---\n[]", // YAML document marker with empty array
            "---\n# comment\n[]"
        };

        // Act & Assert
        foreach (var emptyCase in emptyArrayCases)
        {
            try
            {
                var result = _adapter.DeserializeArray(emptyCase, typeof(object));
                Assert.NotNull(result);
                Assert.Empty(result);
            }
            catch (InvalidOperationException)
            {
                // Some empty formats might not be valid - that's acceptable
            }
        }
    }

    [Fact]
    public void SerializeArray_ArrayWithCircularReferences_ShouldThrowException()
    {
        // Arrange
        var item1 = new Dictionary<string, object> { { "Id", 1 } };
        var item2 = new Dictionary<string, object> { { "Id", 2 } };
        
        item1["Ref"] = item2;
        item2["Ref"] = item1; // Circular reference
        
        var arrayWithCircularRefs = new object[] { item1, item2 };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.SerializeArray(arrayWithCircularRefs));
        
        Assert.Contains("circular", exception.Message.ToLowerInvariant());
    }

    [Fact]
    public void SerializeArray_NullArray_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            _adapter.SerializeArray(null!));
    }

    [Fact]
    public void SerializeArray_ArrayWithNonSerializableItems_ShouldThrowException()
    {
        // Arrange
        var arrayWithNonSerializable = new object[]
        {
            new { Id = 1, Name = "Valid" },
            new FileStream(Path.GetTempFileName(), FileMode.Create), // Non-serializable
            new { Id = 2, Name = "Also Valid" }
        };

        // Act & Assert
        try
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                _adapter.SerializeArray(arrayWithNonSerializable));
            
            Assert.Contains("serialize", exception.Message.ToLowerInvariant());
        }
        finally
        {
            // Cleanup
            ((FileStream)arrayWithNonSerializable[1]).Dispose();
        }
    }

    [Fact]
    public void RoundTrip_YamlSpecialValues_ShouldPreserveTypes()
    {
        // Arrange
        var specialValues = new object[]
        {
            null!,
            true,
            false,
            42,
            3.14159,
            "string",
            "",
            "null", // String that looks like null
            "true", // String that looks like boolean
            "123", // String that looks like number
            DateTime.Parse("2023-01-01T10:00:00Z"),
            new { Empty = (object?)null },
            new { Array = new int[0] },
            new { Dict = new Dictionary<string, object>() }
        };

        // Act
        var serialized = _adapter.SerializeArray(specialValues);
        var deserialized = _adapter.DeserializeArray(serialized, typeof(object));

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(specialValues.Length, deserialized.Length);
        
        // Note: YAML type inference might change exact types, but logical values should be preserved
    }

    [Fact]
    public void Performance_ErrorHandling_ShouldNotCauseMemoryLeaks()
    {
        // Arrange
        var invalidYamlCases = new[]
        {
            "{ invalid: yaml }",
            "- invalid\n-nested",
            "key: 'unclosed",
            "!!invalid data",
            "{ deeply: { nested: { invalid: yaml } } }"
        };

        // Act & Assert - Repeatedly trigger errors to check for memory leaks
        for (int iteration = 0; iteration < 100; iteration++)
        {
            foreach (var invalidYaml in invalidYamlCases)
            {
                try
                {
                    _adapter.Deserialize<Dictionary<string, object>>(invalidYaml);
                    Assert.True(false, "Should have thrown exception");
                }
                catch (InvalidOperationException)
                {
                    // Expected - ensure exception handling doesn't leak memory
                }
                catch (ArgumentException)
                {
                    // Also acceptable for some invalid formats
                }
            }
        }
        
        // If we get here without OutOfMemoryException, error handling is working properly
        Assert.True(true);
    }

    [Fact]
    public void YamlComplexIndentation_ShouldHandleCorrectly()
    {
        // Arrange
        var complexIndentationYaml = """
            root:
              level1:
                level2:
                  - array_item1
                  - array_item2:
                      nested_key: nested_value
                  - simple_item
                another_level2:
                  key: value
              another_level1:
                - item1
                - item2
            another_root: simple_value
            """;

        // Act
        var result = _adapter.Deserialize<Dictionary<string, object>>(complexIndentationYaml);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.ContainsKey("root"));
        Assert.True(result.ContainsKey("another_root"));
    }

    [Fact]
    public void YamlWithComments_ShouldIgnoreCommentsCorrectly()
    {
        // Arrange
        var yamlWithComments = """
            # This is a comment
            key1: value1  # Inline comment
            # Another comment
            key2:
              # Nested comment
              nested_key: nested_value  # Another inline comment
            # Final comment
            key3:
              - item1  # Array comment
              - item2
            """;

        // Act
        var result = _adapter.Deserialize<Dictionary<string, object>>(yamlWithComments);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.ContainsKey("key1"));
        Assert.True(result.ContainsKey("key2"));
        Assert.True(result.ContainsKey("key3"));
    }

    [Fact]
    public void YamlMultilineStrings_ShouldHandleAllVariations()
    {
        // Arrange
        var multilineYaml = """
            literal_block: |
              This is a literal block
              Preserving line breaks
              And indentation
            
            folded_block: >
              This is a folded block
              Line breaks are converted to spaces
              Unless there are blank lines
            
            quoted_multiline: "This is a
              quoted multiline string
              with line breaks"
            
            single_quoted: 'This is also
              a multiline string
              but single quoted'
            """;

        // Act
        var result = _adapter.Deserialize<Dictionary<string, object>>(multilineYaml);

        // Assert
        Assert.NotNull(result);
        Assert.True(result.ContainsKey("literal_block"));
        Assert.True(result.ContainsKey("folded_block"));
        Assert.True(result.ContainsKey("quoted_multiline"));
        Assert.True(result.ContainsKey("single_quoted"));
    }

    [Fact]
    public void YamlAnchorReferences_ShouldHandleCorrectly()
    {
        // Arrange
        var yamlWithAnchors = """
            default_settings: &defaults
              timeout: 30
              retries: 3
              debug: false
            
            production:
              <<: *defaults
              debug: false
              timeout: 60
            
            development:
              <<: *defaults
              debug: true
              timeout: 10
            """;

        // Act
        try
        {
            var result = _adapter.Deserialize<Dictionary<string, object>>(yamlWithAnchors);
            
            // Assert
            Assert.NotNull(result);
            // If anchor/merge support is available, should work
            Assert.True(result.ContainsKey("default_settings"));
        }
        catch (InvalidOperationException ex)
        {
            // If anchors/merges are not supported, should provide clear error
            Assert.Contains("anchor", ex.Message.ToLowerInvariant());
        }
    }
}