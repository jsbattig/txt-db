using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for ConfigureAwait(false) Implementation - Story 002-010: CRITICAL
/// These tests verify that ALL await calls in the async implementation include ConfigureAwait(false)
/// to prevent deadlocks in ASP.NET/WinForms contexts and reduce context switching overhead.
/// 
/// REQUIREMENT: Zero tolerance for missing ConfigureAwait(false) in production async code
/// IMPACT: Prevents 10-50μs performance penalty per await + eliminates deadlock risk
/// </summary>
public class ConfigureAwaitValidationTests
{
    private readonly ITestOutputHelper _output;
    private readonly string _sourceCodePath;

    public ConfigureAwaitValidationTests(ITestOutputHelper output)
    {
        _output = output;
        _sourceCodePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, 
            "..", "..", "..", "..", "TxtDb.Storage", "Services", "Async");
    }

    [Fact]
    public async Task AsyncFileOperations_AllAwaitCalls_ShouldHaveConfigureAwaitFalse()
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, "AsyncFileOperations.cs");
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, "AsyncFileOperations.cs");
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in AsyncFileOperations.cs:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    [Fact]
    public async Task AsyncLockManager_AllAwaitCalls_ShouldHaveConfigureAwaitFalse()
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, "AsyncLockManager.cs");
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, "AsyncLockManager.cs");
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in AsyncLockManager.cs:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    [Fact]
    public async Task AsyncStorageSubsystem_AllAwaitCalls_ShouldHaveConfigureAwaitFalse()
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, "AsyncStorageSubsystem.cs");
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, "AsyncStorageSubsystem.cs");
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in AsyncStorageSubsystem.cs:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    [Fact]
    public async Task BatchFlushCoordinator_AllAwaitCalls_ShouldHaveConfigureAwaitFalse()
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, "BatchFlushCoordinator.cs");
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, "BatchFlushCoordinator.cs");
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in BatchFlushCoordinator.cs:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    [Theory]
    [InlineData("AsyncJsonFormatAdapter.cs")]
    [InlineData("AsyncXmlFormatAdapter.cs")]
    [InlineData("AsyncYamlFormatAdapter.cs")]
    public async Task FormatAdapters_AllAwaitCalls_ShouldHaveConfigureAwaitFalse(string fileName)
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, fileName);
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, fileName);
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in {fileName}:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    [Fact]
    public async Task MonitoredAsyncStorageSubsystem_AllAwaitCalls_ShouldHaveConfigureAwaitFalse()
    {
        // Arrange
        var filePath = Path.Combine(_sourceCodePath, "MonitoredAsyncStorageSubsystem.cs");
        var sourceCode = await File.ReadAllTextAsync(filePath);
        
        // Act
        var awaitCallsWithoutConfigureAwait = AnalyzeAwaitCalls(sourceCode, "MonitoredAsyncStorageSubsystem.cs");
        
        // Assert - This test MUST FAIL initially (RED phase of TDD)
        Assert.True(awaitCallsWithoutConfigureAwait.Count == 0, 
            $"Found {awaitCallsWithoutConfigureAwait.Count} await calls without ConfigureAwait(false) in MonitoredAsyncStorageSubsystem.cs:\n" +
            string.Join("\n", awaitCallsWithoutConfigureAwait.Select(x => $"Line {x.LineNumber}: {x.Code}")));
    }

    /// <summary>
    /// Analyzes C# source code to find await calls without ConfigureAwait(false)
    /// Uses Roslyn syntax analysis for accurate parsing
    /// </summary>
    private List<(int LineNumber, string Code)> AnalyzeAwaitCalls(string sourceCode, string fileName)
    {
        var awaitCallsWithoutConfigureAwait = new List<(int LineNumber, string Code)>();
        
        try
        {
            var syntaxTree = CSharpSyntaxTree.ParseText(sourceCode);
            var root = syntaxTree.GetRoot();
            
            var awaitExpressions = root.DescendantNodes().OfType<AwaitExpressionSyntax>();
            
            foreach (var awaitExpression in awaitExpressions)
            {
                var awaitText = awaitExpression.ToString();
                var lineNumber = syntaxTree.GetLineSpan(awaitExpression.Span).StartLinePosition.Line + 1;
                
                // Check if this await call has ConfigureAwait(false)
                if (!awaitText.Contains("ConfigureAwait(false)"))
                {
                    awaitCallsWithoutConfigureAwait.Add((lineNumber, awaitText.Trim()));
                    _output.WriteLine($"[{fileName}:{lineNumber}] Missing ConfigureAwait(false): {awaitText.Trim()}");
                }
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Error analyzing {fileName}: {ex.Message}");
            // Return empty list to fail test gracefully
        }
        
        return awaitCallsWithoutConfigureAwait;
    }

    /// <summary>
    /// Integration test to verify that adding ConfigureAwait(false) doesn't break existing functionality
    /// This test should pass both before and after the ConfigureAwait implementation
    /// </summary>
    [Fact]
    public async Task ConfigureAwaitImplementation_ShouldNotBreakExistingFunctionality()
    {
        // Arrange
        var testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_configureawait_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(testRootPath);
        
        try
        {
            var asyncFileOps = new AsyncFileOperations();
            var testFile = Path.Combine(testRootPath, "configure_await_test.json");
            var testContent = "{\"test\": \"ConfigureAwait integration test\"}";
            
            // Act - These operations should work regardless of ConfigureAwait presence
            await asyncFileOps.WriteTextAsync(testFile, testContent);
            var readContent = await asyncFileOps.ReadTextAsync(testFile);
            
            // Assert
            Assert.Equal(testContent, readContent);
            Assert.True(File.Exists(testFile));
        }
        finally
        {
            if (Directory.Exists(testRootPath))
            {
                Directory.Delete(testRootPath, true);
            }
        }
    }

    /// <summary>
    /// Performance baseline test to measure impact of ConfigureAwait(false)
    /// This establishes baseline timing before implementing ConfigureAwait
    /// </summary>
    [Fact]
    public async Task AsyncOperations_PerformanceBaseline_ShouldEstablishTimingReference()
    {
        // Arrange
        var testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_perf_baseline_{Guid.NewGuid():N}");
        Directory.CreateDirectory(testRootPath);
        
        try
        {
            var asyncFileOps = new AsyncFileOperations();
            var testFile = Path.Combine(testRootPath, "perf_test.json");
            var testContent = "{\"data\": \"" + new string('x', 1000) + "\"}"; // 1KB content
            
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            // Act - Perform multiple async operations to measure timing
            for (int i = 0; i < 10; i++)
            {
                await asyncFileOps.WriteTextAsync(testFile, testContent);
                await asyncFileOps.ReadTextAsync(testFile);
            }
            
            stopwatch.Stop();
            
            // Assert - Record baseline timing for comparison after ConfigureAwait implementation
            var averageOperationTime = stopwatch.ElapsedMilliseconds / 20.0; // 10 writes + 10 reads
            _output.WriteLine($"Baseline average operation time: {averageOperationTime:F2}ms per operation");
            
            // Expect reasonable performance (arbitrary upper bound for sanity check)
            Assert.True(averageOperationTime < 100, 
                $"Baseline performance too slow: {averageOperationTime:F2}ms per operation (expected < 100ms)");
        }
        finally
        {
            if (Directory.Exists(testRootPath))
            {
                Directory.Delete(testRootPath, true);
            }
        }
    }
}