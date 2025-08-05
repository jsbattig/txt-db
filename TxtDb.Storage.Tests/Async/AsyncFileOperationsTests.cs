using TxtDb.Storage.Services.Async;
using Xunit;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.Async;

/// <summary>
/// TDD TESTS for AsyncFileOperations - Phase 1: Foundation
/// These tests define the interface and behavior for async file I/O operations
/// CRITICAL: All async operations must maintain atomicity and durability guarantees
/// </summary>
public class AsyncFileOperationsTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly ITestOutputHelper _output;

    public AsyncFileOperationsTests(ITestOutputHelper output)
    {
        _output = output;
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_async_fileops_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
    }

    [Fact]
    public async Task ReadTextAsync_FileExists_ShouldReturnContentAsynchronously()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "test_read.json");
        var expectedContent = "{\"test\": \"async read operation\", \"timestamp\": \"2025-01-01T00:00:00Z\"}";
        await File.WriteAllTextAsync(testFile, expectedContent);
        
        var asyncFileOps = new AsyncFileOperations();

        // Act
        var actualContent = await asyncFileOps.ReadTextAsync(testFile);

        // Assert
        Assert.Equal(expectedContent, actualContent);
    }

    [Fact]
    public async Task ReadTextAsync_FileDoesNotExist_ShouldThrowFileNotFoundException()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_testRootPath, "nonexistent.json");
        var asyncFileOps = new AsyncFileOperations();

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => asyncFileOps.ReadTextAsync(nonExistentFile));
    }

    [Fact]
    public async Task WriteTextAsync_ValidContent_ShouldWriteAsynchronously()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "test_write.json");
        var contentToWrite = "{\"test\": \"async write operation\", \"id\": 12345}";
        var asyncFileOps = new AsyncFileOperations();

        // Act
        await asyncFileOps.WriteTextAsync(testFile, contentToWrite);

        // Assert
        Assert.True(File.Exists(testFile));
        var writtenContent = await File.ReadAllTextAsync(testFile);
        Assert.Equal(contentToWrite, writtenContent);
    }

    [Fact]
    public async Task WriteTextAsync_OverwriteExistingFile_ShouldReplaceContent()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "test_overwrite.json");
        var originalContent = "{\"original\": \"content\"}";
        var newContent = "{\"new\": \"content\", \"updated\": true}";
        
        await File.WriteAllTextAsync(testFile, originalContent);
        var asyncFileOps = new AsyncFileOperations();

        // Act
        await asyncFileOps.WriteTextAsync(testFile, newContent);

        // Assert
        var finalContent = await File.ReadAllTextAsync(testFile);
        Assert.Equal(newContent, finalContent);
        Assert.NotEqual(originalContent, finalContent);
    }

    [Fact]
    public async Task WriteTextWithFlushAsync_ValidContent_ShouldWriteAndFlushToDisk()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "test_write_flush.json");
        var contentToWrite = "{\"critical\": \"data\", \"requiresFlush\": true}";
        var asyncFileOps = new AsyncFileOperations();

        // Act
        await asyncFileOps.WriteTextWithFlushAsync(testFile, contentToWrite);

        // Assert
        Assert.True(File.Exists(testFile));
        var writtenContent = await File.ReadAllTextAsync(testFile);
        Assert.Equal(contentToWrite, writtenContent);
        
        // Verify that file is actually flushed (difficult to test directly, but we can verify it doesn't throw)
        // The flush operation should complete without exceptions
    }

    [Fact]
    public async Task FileExistsAsync_ExistingFile_ShouldReturnTrue()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "existing_file.json");
        await File.WriteAllTextAsync(testFile, "{}");
        var asyncFileOps = new AsyncFileOperations();

        // Act
        var exists = await asyncFileOps.FileExistsAsync(testFile);

        // Assert
        Assert.True(exists);
    }

    [Fact]
    public async Task FileExistsAsync_NonExistentFile_ShouldReturnFalse()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_testRootPath, "nonexistent.json");
        var asyncFileOps = new AsyncFileOperations();

        // Act
        var exists = await asyncFileOps.FileExistsAsync(nonExistentFile);

        // Assert
        Assert.False(exists);
    }

    [Fact]
    public async Task DeleteFileAsync_ExistingFile_ShouldDeleteSuccessfully()
    {
        // Arrange
        var testFile = Path.Combine(_testRootPath, "to_delete.json");
        await File.WriteAllTextAsync(testFile, "{\"toDelete\": true}");
        var asyncFileOps = new AsyncFileOperations();

        // Act
        await asyncFileOps.DeleteFileAsync(testFile);

        // Assert
        Assert.False(File.Exists(testFile));
    }

    [Fact]
    public async Task DeleteFileAsync_NonExistentFile_ShouldNotThrow()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_testRootPath, "nonexistent_delete.json");
        var asyncFileOps = new AsyncFileOperations();

        // Act & Assert (should not throw)
        await asyncFileOps.DeleteFileAsync(nonExistentFile);
    }

    [Fact]
    public async Task ConcurrentReadOperations_MultipleFiles_ShouldHandleAsynchronously()
    {
        // Arrange
        var fileCount = 10;
        var files = new List<string>();
        var expectedContents = new List<string>();
        
        for (int i = 0; i < fileCount; i++)
        {
            var fileName = Path.Combine(_testRootPath, $"concurrent_read_{i}.json");
            var content = $"{{\"id\": {i}, \"data\": \"concurrent test {i}\"}}";
            await File.WriteAllTextAsync(fileName, content);
            files.Add(fileName);
            expectedContents.Add(content);
        }
        
        var asyncFileOps = new AsyncFileOperations();

        // Act
        var readTasks = files.Select(file => asyncFileOps.ReadTextAsync(file)).ToArray();
        var actualContents = await Task.WhenAll(readTasks);

        // Assert
        Assert.Equal(fileCount, actualContents.Length);
        for (int i = 0; i < fileCount; i++)
        {
            Assert.Equal(expectedContents[i], actualContents[i]);
        }
    }

    [Fact]
    public async Task ConcurrentWriteOperations_MultipleFiles_ShouldHandleAsynchronously()
    {
        // Arrange
        var fileCount = 10;
        var files = new List<string>();
        var contents = new List<string>();
        
        for (int i = 0; i < fileCount; i++)
        {
            var fileName = Path.Combine(_testRootPath, $"concurrent_write_{i}.json");
            var content = $"{{\"id\": {i}, \"data\": \"concurrent write test {i}\", \"timestamp\": \"{DateTime.UtcNow:O}\"}}";
            files.Add(fileName);
            contents.Add(content);
        }
        
        var asyncFileOps = new AsyncFileOperations();

        // Act
        var writeTasks = files.Zip(contents, (file, content) => 
            asyncFileOps.WriteTextAsync(file, content)).ToArray();
        await Task.WhenAll(writeTasks);

        // Assert
        for (int i = 0; i < fileCount; i++)
        {
            Assert.True(File.Exists(files[i]));
            var writtenContent = await File.ReadAllTextAsync(files[i]);
            Assert.Equal(contents[i], writtenContent);
        }
    }

    [Fact]
    public async Task PerformanceTest_AsyncVsSync_ShouldShowAsyncBenefit()
    {
        // Arrange
        var fileCount = 50;
        var asyncFileOps = new AsyncFileOperations();
        var testContents = Enumerable.Range(0, fileCount)
            .Select(i => $"{{\"performance_test\": {i}, \"data\": \"{new string('X', 1000)}\"}}") // 1KB content
            .ToArray();

        // Test async operations
        var asyncFiles = Enumerable.Range(0, fileCount)
            .Select(i => Path.Combine(_testRootPath, $"perf_async_{i}.json"))
            .ToArray();

        // Act - Async operations
        var asyncStopwatch = System.Diagnostics.Stopwatch.StartNew();
        var asyncWriteTasks = asyncFiles.Zip(testContents, (file, content) => 
            asyncFileOps.WriteTextAsync(file, content)).ToArray();
        await Task.WhenAll(asyncWriteTasks);
        
        var asyncReadTasks = asyncFiles.Select(file => asyncFileOps.ReadTextAsync(file)).ToArray();
        var asyncResults = await Task.WhenAll(asyncReadTasks);
        asyncStopwatch.Stop();

        // Test sync operations for comparison
        var syncFiles = Enumerable.Range(0, fileCount)
            .Select(i => Path.Combine(_testRootPath, $"perf_sync_{i}.json"))
            .ToArray();

        var syncStopwatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < fileCount; i++)
        {
            await File.WriteAllTextAsync(syncFiles[i], testContents[i]);
        }
        var syncResults = new string[fileCount];
        for (int i = 0; i < fileCount; i++)
        {
            syncResults[i] = await File.ReadAllTextAsync(syncFiles[i]);
        }
        syncStopwatch.Stop();

        // Assert
        Assert.Equal(fileCount, asyncResults.Length);
        Assert.All(asyncResults.Zip(testContents), pair => Assert.Equal(pair.Second, pair.First));
        
        _output.WriteLine($"Async operations took: {asyncStopwatch.ElapsedMilliseconds}ms");
        _output.WriteLine($"Sync operations took: {syncStopwatch.ElapsedMilliseconds}ms");
        
        // Async should be faster or at least comparable for I/O bound operations
        // Note: On some systems, the difference might be minimal for small file counts
        Assert.True(asyncStopwatch.ElapsedMilliseconds <= syncStopwatch.ElapsedMilliseconds * 1.5,
            $"Async operations should be competitive: {asyncStopwatch.ElapsedMilliseconds}ms vs {syncStopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task DirectoryOperations_CreateAndExists_ShouldWorkAsynchronously()
    {
        // Arrange
        var testDirectory = Path.Combine(_testRootPath, "async_test_dir");
        var asyncFileOps = new AsyncFileOperations();

        // Act & Assert - Directory should not exist initially
        var existsInitially = await asyncFileOps.DirectoryExistsAsync(testDirectory);
        Assert.False(existsInitially);

        // Create directory
        await asyncFileOps.CreateDirectoryAsync(testDirectory);
        
        // Directory should now exist
        var existsAfterCreation = await asyncFileOps.DirectoryExistsAsync(testDirectory);
        Assert.True(existsAfterCreation);
        Assert.True(Directory.Exists(testDirectory));
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup failed - not critical for tests
        }
    }
}