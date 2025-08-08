using System.Collections.Generic;
using System.Linq;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// TDD Tests for ResourceOrderingManager - Phase 1 of Deadlock Prevention Implementation
/// Tests deterministic resource ordering to prevent circular dependencies
/// </summary>
public class ResourceOrderingManagerTests
{
    private readonly ITestOutputHelper _output;

    public ResourceOrderingManagerTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void CompareResources_SameResource_ShouldReturnZero()
    {
        // Act
        var result = ResourceOrderingManager.CompareResources("resource1", "resource1");

        // Assert
        Assert.Equal(0, result);
    }

    [Fact]
    public void CompareResources_DifferentResources_ShouldReturnConsistentOrdering()
    {
        // Arrange
        var resource1 = "namespace1::object1";
        var resource2 = "namespace1::object2";

        // Act
        var result1 = ResourceOrderingManager.CompareResources(resource1, resource2);
        var result2 = ResourceOrderingManager.CompareResources(resource2, resource1);

        // Assert
        Assert.True(result1 != 0, "Different resources should not compare as equal");
        Assert.Equal(-result1, result2); // Consistent ordering
        Assert.True((result1 > 0) != (result2 > 0), "Ordering should be opposite when parameters are swapped");
    }

    [Fact]
    public void OrderResources_MultipleResources_ShouldReturnDeterministicOrder()
    {
        // Arrange
        var resources = new List<string>
        {
            "namespace2::object1",
            "namespace1::object1", 
            "namespace3::object2",
            "namespace1::object2"
        };

        // Act
        var ordered1 = ResourceOrderingManager.OrderResources(resources);
        var ordered2 = ResourceOrderingManager.OrderResources(resources.AsEnumerable().Reverse());
        var ordered3 = ResourceOrderingManager.OrderResources(resources.OrderBy(r => Guid.NewGuid()));

        // Assert
        Assert.Equal(4, ordered1.Count);
        Assert.Equal(ordered1, ordered2); // Same ordering regardless of input order
        Assert.Equal(ordered1, ordered3); // Same ordering regardless of input order
        
        // Verify ordering is lexicographic
        for (int i = 1; i < ordered1.Count; i++)
        {
            var compareResult = string.Compare(ordered1[i-1], ordered1[i], StringComparison.Ordinal);
            Assert.True(compareResult < 0, $"Resource {ordered1[i-1]} should come before {ordered1[i]}");
        }
    }

    [Fact]
    public void OrderResources_EmptyList_ShouldReturnEmptyList()
    {
        // Act
        var result = ResourceOrderingManager.OrderResources(new List<string>());

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void OrderResources_SingleResource_ShouldReturnSingleItem()
    {
        // Arrange
        var resource = "test::resource";

        // Act
        var result = ResourceOrderingManager.OrderResources(new[] { resource });

        // Assert
        Assert.Single(result);
        Assert.Equal(resource, result[0]);
    }

    [Fact]
    public void OrderResources_DeadlockScenario_ShouldPreventCircularDependencies()
    {
        // Arrange - Classic deadlock scenario resources
        var resourceA = "deadlock::resourceA";
        var resourceB = "deadlock::resourceB";
        
        // Act - Both transactions should get same ordering
        var orderingT1 = ResourceOrderingManager.OrderResources(new[] { resourceA, resourceB });
        var orderingT2 = ResourceOrderingManager.OrderResources(new[] { resourceB, resourceA }); // Different input order

        // Assert - Same deterministic order prevents deadlock
        Assert.Equal(2, orderingT1.Count);
        Assert.Equal(2, orderingT2.Count);
        Assert.Equal(orderingT1[0], orderingT2[0]); // First resource is same
        Assert.Equal(orderingT1[1], orderingT2[1]); // Second resource is same
        
        _output.WriteLine($"Deterministic order: {orderingT1[0]} -> {orderingT1[1]}");
    }
}