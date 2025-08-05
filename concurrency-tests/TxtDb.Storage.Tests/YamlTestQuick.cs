using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests;

public class YamlTestQuick
{
    private readonly YamlFormatAdapter _adapter = new();

    [Fact]
    public void AnonymousType_ShouldThrowException()
    {
        var anonymousObj = new { Id = 1, Name = "Test" };
        
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.Serialize(anonymousObj));
        
        Assert.Contains("anonymous types", exception.Message.ToLowerInvariant());
    }
    
    [Fact]
    public void EmptyString_ShouldThrowArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<object>(""));
            
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<object>("   "));
    }
    
    [Fact]
    public void NullString_ShouldThrowArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            _adapter.Deserialize<object>(null!));
    }
    
    [Fact]
    public void MalformedYaml_ShouldThrowException()
    {
        var malformedYaml = "{ invalid: yaml }";
        
        var exception = Assert.Throws<InvalidOperationException>(() =>
            _adapter.Deserialize<object>(malformedYaml));
        
        Assert.Contains("invalid syntax", exception.Message.ToLowerInvariant());
    }
    
    [Fact]
    public void ValidObject_ShouldSerializeWithPascalCase()
    {
        var obj = new QuickTestObj { Id = 123, Name = "Test" };
        
        var yaml = _adapter.Serialize(obj);
        
        Assert.Contains("Id: 123", yaml);
        Assert.Contains("Name:", yaml);
    }
}

public class QuickTestObj
{
    public int Id { get; set; }
    public string? Name { get; set; }
}