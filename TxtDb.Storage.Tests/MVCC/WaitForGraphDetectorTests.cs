using System.Collections.Generic;
using System.Linq;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// TDD Tests for WaitForGraphDetector - Phase 2 of Deadlock Prevention Implementation
/// Tests wait-for graph construction and cycle detection for immediate deadlock prevention
/// </summary>
public class WaitForGraphDetectorTests
{
    private readonly ITestOutputHelper _output;

    public WaitForGraphDetectorTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void AddWaitRelation_NewRelation_ShouldAddToGraph()
    {
        // Arrange
        var detector = new WaitForGraphDetector();

        // Act
        var hasCycle = detector.AddWaitRelation(transactionId: 1, resourceId: "resource1", holderTransactionId: 2);

        // Assert
        Assert.False(hasCycle, "Single wait relation should not create a cycle");
        Assert.True(detector.HasWaitRelation(1, "resource1", 2));
    }

    [Fact]
    public void AddWaitRelation_CircularDependency_ShouldDetectCycle()
    {
        // Arrange
        var detector = new WaitForGraphDetector();

        // Act - Create circular dependency: T1 waits for T2, T2 waits for T1
        var cycle1 = detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        var cycle2 = detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 1);

        // Assert
        Assert.False(cycle1, "First wait relation should not detect cycle");
        Assert.True(cycle2, "Second wait relation should detect cycle");
    }

    [Fact]
    public void AddWaitRelation_IndirectCircularDependency_ShouldDetectCycle()
    {
        // Arrange
        var detector = new WaitForGraphDetector();

        // Act - Create indirect circular dependency: T1 -> T2 -> T3 -> T1
        var cycle1 = detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        var cycle2 = detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 3);
        var cycle3 = detector.AddWaitRelation(transactionId: 3, resourceId: "resourceC", holderTransactionId: 1);

        // Assert
        Assert.False(cycle1, "First wait relation should not detect cycle");
        Assert.False(cycle2, "Second wait relation should not detect cycle");
        Assert.True(cycle3, "Third wait relation should detect indirect cycle");
    }

    [Fact]
    public void RemoveWaitRelation_ExistingRelation_ShouldRemoveFromGraph()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resource1", holderTransactionId: 2);

        // Act
        detector.RemoveWaitRelation(transactionId: 1, resourceId: "resource1");

        // Assert
        Assert.False(detector.HasWaitRelation(1, "resource1", 2));
    }

    [Fact]
    public void RemoveAllWaitsForTransaction_MultipleWaits_ShouldRemoveAllRelations()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceB", holderTransactionId: 3);
        detector.AddWaitRelation(transactionId: 2, resourceId: "resourceC", holderTransactionId: 1);

        // Act
        detector.RemoveAllWaitsForTransaction(1);

        // Assert
        Assert.False(detector.HasWaitRelation(1, "resourceA", 2));
        Assert.False(detector.HasWaitRelation(1, "resourceB", 3));
        Assert.True(detector.HasWaitRelation(2, "resourceC", 1)); // Other transactions not affected
    }

    [Fact]
    public void DetectDeadlock_NoCycles_ShouldReturnEmpty()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 3);

        // Act
        var cycle = detector.DetectDeadlock();

        // Assert
        Assert.Empty(cycle);
    }

    [Fact]
    public void DetectDeadlock_WithCycle_ShouldReturnCycleTransactions()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 3);
        detector.AddWaitRelation(transactionId: 3, resourceId: "resourceC", holderTransactionId: 1);

        // Act
        var cycle = detector.DetectDeadlock();

        // Assert
        Assert.Equal(3, cycle.Count);
        Assert.Contains(1L, cycle);
        Assert.Contains(2L, cycle);
        Assert.Contains(3L, cycle);

        _output.WriteLine($"Detected deadlock cycle: {string.Join(" -> ", cycle)}");
    }

    [Fact]
    public void GetDiagnostics_WithWaitRelations_ShouldReturnCurrentState()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 3);

        // Act
        var diagnostics = detector.GetDiagnostics();

        // Assert
        Assert.Equal(2, diagnostics.ActiveWaitRelations);
        Assert.Equal(3, diagnostics.InvolvedTransactions); // T1, T2, T3
        Assert.False(diagnostics.HasDeadlock);
    }

    [Fact]
    public void GetDiagnostics_WithDeadlock_ShouldIndicateDeadlock()
    {
        // Arrange
        var detector = new WaitForGraphDetector();
        detector.AddWaitRelation(transactionId: 1, resourceId: "resourceA", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 2, resourceId: "resourceB", holderTransactionId: 1);

        // Act
        var diagnostics = detector.GetDiagnostics();

        // Assert
        Assert.True(diagnostics.HasDeadlock);
        Assert.NotEmpty(diagnostics.DeadlockCycle);
    }

    [Fact]
    public void WaitForGraphDetector_ComplexScenario_ShouldHandleCorrectly()
    {
        // Arrange - Complex multi-transaction, multi-resource scenario
        var detector = new WaitForGraphDetector();

        // Act - Build complex wait graph
        detector.AddWaitRelation(transactionId: 1, resourceId: "A", holderTransactionId: 2);
        detector.AddWaitRelation(transactionId: 2, resourceId: "B", holderTransactionId: 3);
        detector.AddWaitRelation(transactionId: 4, resourceId: "C", holderTransactionId: 5);
        detector.AddWaitRelation(transactionId: 5, resourceId: "D", holderTransactionId: 6);

        // No cycles yet
        Assert.Empty(detector.DetectDeadlock());

        // Add cycle-creating edge
        var hasCycle = detector.AddWaitRelation(transactionId: 3, resourceId: "E", holderTransactionId: 1);

        // Assert
        Assert.True(hasCycle, "Should detect cycle when T3 waits for T1 (closing T1->T2->T3->T1 cycle)");
        
        var cycle = detector.DetectDeadlock();
        Assert.Contains(1L, cycle);
        Assert.Contains(2L, cycle);
        Assert.Contains(3L, cycle);
        Assert.DoesNotContain(4L, cycle); // T4, T5, T6 are not part of the cycle
        Assert.DoesNotContain(5L, cycle);
        Assert.DoesNotContain(6L, cycle);
    }
}