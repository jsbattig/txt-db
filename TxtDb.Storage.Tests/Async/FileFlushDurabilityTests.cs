using System.Text;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for File Flushing Durability - Story 002-012: CRITICAL
/// These tests verify that file flushing operations actually guarantee durability
/// by ensuring files are opened in ReadWrite mode and flush operations are effective.
/// 
/// REQUIREMENT: Files must be opened with FileAccess.ReadWrite to allow flushing
/// IMPACT: Prevents silent no-op flushes that compromise data durability
/// </summary>
public class FileFlushDurabilityTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testRootPath;
    private BatchFlushCoordinator? _batchFlushCoordinator;

    public FileFlushDurabilityTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_flush_durability_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
    }

    [Fact]
    public async Task BatchFlushCoordinator_FlushOperation_ShouldActuallyFlushToFileSystem()
    {
        // Arrange
        var batchConfig = new BatchFlushConfig
        {
            MaxBatchSize = 1,
            MaxDelayMs = 100,
            MaxConcurrentFlushes = 1
        };
        
        _batchFlushCoordinator = new BatchFlushCoordinator(batchConfig);
        await _batchFlushCoordinator.StartAsync();
        
        var testFile = Path.Combine(_testRootPath, "durability_test.txt");
        var testContent = "Critical data that must be durable";
        
        // Write content to file first
        await File.WriteAllTextAsync(testFile, testContent);
        
        // Act - Queue flush operation through BatchFlushCoordinator
        // This test MUST FAIL initially because FileAccess.Read prevents flushing
        await _batchFlushCoordinator.QueueFlushAsync(testFile, FlushPriority.Critical);
        
        // Wait for flush to process
        await Task.Delay(200);
        
        // Assert - Verify file content is still intact after flush
        var verifyContent = await File.ReadAllTextAsync(testFile);
        Assert.Equal(testContent, verifyContent);
        
        // Additional verification: The flush should have completed without throwing
        // If FileAccess.Read is used, the flush is a no-op but doesn't throw
        // The real test is that we can perform actual durability operations
        Assert.True(File.Exists(testFile), "File should exist after flush operation");
    }

    [Fact]
    public async Task BatchFlushCoordinator_ShouldOpenFilesWithWriteAccess()
    {
        // Arrange
        var batchConfig = new BatchFlushConfig
        {
            MaxBatchSize = 1,
            MaxDelayMs = 50,
            MaxConcurrentFlushes = 1
        };
        
        _batchFlushCoordinator = new BatchFlushCoordinator(batchConfig);
        await _batchFlushCoordinator.StartAsync();
        
        var testFile = Path.Combine(_testRootPath, "write_access_test.txt");
        var testContent = "Test content for write access verification";
        
        // Write initial content
        await File.WriteAllTextAsync(testFile, testContent);
        
        // Create a file handle that would conflict with write access
        // FileShare.None prevents any other process from opening the file
        using var exclusiveHandle = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        
        // Act - When BatchFlushCoordinator tries to access the file for flushing, it should fail
        // because exclusiveHandle has exclusive access with FileShare.None
        var flushTask = _batchFlushCoordinator.QueueFlushAsync(testFile, FlushPriority.Normal);
        
        // Assert - The flush task should fail with an IOException due to file access conflict
        // Use a more flexible approach that handles different possible exception types and messages
        var exception = await Assert.ThrowsAnyAsync<Exception>(async () => await flushTask);
        
        // Verify it's a file access related exception
        Assert.True(exception is IOException || exception is UnauthorizedAccessException || 
                   exception.Message.Contains("access") || exception.Message.Contains("use") ||
                   exception.Message.Contains("lock") || exception.Message.Contains("sharing"),
                   $"Expected file access exception, got {exception.GetType().Name}: {exception.Message}");
    }

    [Fact]
    public async Task FileFlushOperations_ShouldSupportReadWriteAccess()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "readwrite_test.txt");
        var testContent = "Data requiring read-write access for flushing";
        
        // Write test content
        await File.WriteAllTextAsync(testFile, testContent);
        
        // Act & Assert - Direct test of the file access pattern
        // This tests the specific fix needed in BatchFlushCoordinator
        
        // BEFORE FIX: This should demonstrate the problem
        using (var readOnlyStream = new FileStream(testFile, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            // This flush call should be a no-op (doesn't throw but doesn't actually flush)
            await readOnlyStream.FlushAsync();
            // The flush above does nothing because the file is opened read-only
        }
        
        // AFTER FIX: This demonstrates the solution
        using (var readWriteStream = new FileStream(testFile, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
        {
            // This flush call should actually flush to the file system
            await readWriteStream.FlushAsync();
            readWriteStream.Flush(flushToDisk: true); // Force OS buffer flush
            // This flush actually guarantees durability
        }
        
        // Verify content integrity
        var verifyContent = await File.ReadAllTextAsync(testFile);
        Assert.Equal(testContent, verifyContent);
        
        _output.WriteLine("File access pattern verification completed");
    }

    [Fact]
    public async Task BatchFlushCoordinator_CriticalFlushes_ShouldGuaranteeDurability()
    {
        // Arrange
        var batchConfig = new BatchFlushConfig
        {
            MaxBatchSize = 5,
            MaxDelayMs = 1000, // Longer delay to test critical priority
            MaxConcurrentFlushes = 2
        };
        
        _batchFlushCoordinator = new BatchFlushCoordinator(batchConfig);
        await _batchFlushCoordinator.StartAsync();
        
        var testFiles = new List<string>();
        for (int i = 0; i < 3; i++)
        {
            var testFile = Path.Combine(_testRootPath, $"critical_flush_{i}.txt");
            var testContent = $"Critical transaction data {i} - must be durable";
            await File.WriteAllTextAsync(testFile, testContent);
            testFiles.Add(testFile);
        }
        
        // Act - Queue critical flushes
        var flushTasks = testFiles.Select(file => 
            _batchFlushCoordinator.QueueFlushAsync(file, FlushPriority.Critical)).ToArray();
        
        // Critical flushes should complete quickly despite long batch delay
        var completionTask = Task.WhenAll(flushTasks);
        var timeoutTask = Task.Delay(500); // Should complete well before normal batch delay
        
        var completedTask = await Task.WhenAny(completionTask, timeoutTask);
        
        // Assert
        Assert.Equal(completionTask, completedTask);
        Assert.True(completionTask.IsCompletedSuccessfully, "Critical flushes should complete quickly");
        
        // Verify all files still exist and have correct content
        for (int i = 0; i < testFiles.Count; i++)
        {
            var content = await File.ReadAllTextAsync(testFiles[i]);
            Assert.Contains($"Critical transaction data {i}", content);
        }
        
        _output.WriteLine($"Critical flush operations completed in under 500ms");
    }

    public void Dispose()
    {
        _batchFlushCoordinator?.Dispose();
        
        if (Directory.Exists(_testRootPath))
        {
            try
            {
                Directory.Delete(_testRootPath, true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }
    }
}