using System.Linq;
using TxtDb.Storage.Services;
using TxtDb.Storage.Tests.FormatAdapters;
using Xunit;

namespace TxtDb.Storage.Tests;

public class XmlArrayDebugTest
{
    [Fact]
    public void DebugLargeObjectSerialization()
    {
        // Arrange
        var adapter = new XmlFormatAdapter();
        var largeObject = new LargeTestModel
        {
            Id = 1,
            LargeText = new string('X', 10000), // 10KB of text
            Numbers = Enumerable.Range(1, 1000).ToArray(),
            Nested = Enumerable.Range(1, 100).Select(i => new NestedItem { Index = i, Value = $"Item_{i}" }).ToArray()
        };

        // Act
        var result = adapter.Serialize(largeObject);

        // Debug output
        System.Console.WriteLine($"Result length: {result.Length}");
        System.Console.WriteLine($"First 500 characters: {result.Substring(0, Math.Min(500, result.Length))}");
        
        // Assert for debugging
        Assert.NotEmpty(result);
    }
}