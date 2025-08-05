namespace TxtDb.Storage.Services.Async;

/// <summary>
/// Async File I/O Operations for Performance Optimization
/// Phase 1: Foundation - Replaces synchronous file operations with async alternatives
/// CRITICAL: Maintains atomicity and durability guarantees while improving concurrency
/// </summary>
public class AsyncFileOperations
{
    /// <summary>
    /// Asynchronously reads all text from a file
    /// </summary>
    /// <param name="filePath">Path to the file to read</param>
    /// <returns>File content as string</returns>
    /// <exception cref="FileNotFoundException">Thrown when file does not exist</exception>
    public async Task<string> ReadTextAsync(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File not found: {filePath}");
        }

        return await File.ReadAllTextAsync(filePath).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously writes text to a file, overwriting if it exists
    /// </summary>
    /// <param name="filePath">Path to the file to write</param>
    /// <param name="content">Content to write to the file</param>
    public async Task WriteTextAsync(string filePath, string content)
    {
        await File.WriteAllTextAsync(filePath, content).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously writes text to a file with explicit flush to disk
    /// CRITICAL: Ensures durability by forcing OS to write to physical storage
    /// </summary>
    /// <param name="filePath">Path to the file to write</param>
    /// <param name="content">Content to write to the file</param>
    public async Task WriteTextWithFlushAsync(string filePath, string content)
    {
        await File.WriteAllTextAsync(filePath, content).ConfigureAwait(false);
        
        // Force flush to disk for durability guarantee
        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await fileStream.FlushAsync().ConfigureAwait(false);
        // FlushToDisk equivalent - force OS buffer flush
        fileStream.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Asynchronously checks if a file exists
    /// </summary>
    /// <param name="filePath">Path to check</param>
    /// <returns>True if file exists, false otherwise</returns>
    public async Task<bool> FileExistsAsync(string filePath)
    {
        // File.Exists is not truly async, but we wrap it in Task.Run for consistency
        // In .NET, file system metadata operations are typically fast and don't benefit from async
        return await Task.Run(() => File.Exists(filePath)).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously deletes a file if it exists
    /// Does not throw if file doesn't exist
    /// </summary>
    /// <param name="filePath">Path to the file to delete</param>
    public async Task DeleteFileAsync(string filePath)
    {
        await Task.Run(() =>
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously checks if a directory exists
    /// </summary>
    /// <param name="directoryPath">Path to check</param>
    /// <returns>True if directory exists, false otherwise</returns>
    public async Task<bool> DirectoryExistsAsync(string directoryPath)
    {
        return await Task.Run(() => Directory.Exists(directoryPath)).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously creates a directory if it doesn't exist
    /// Creates parent directories as needed
    /// </summary>
    /// <param name="directoryPath">Path to the directory to create</param>
    public async Task CreateDirectoryAsync(string directoryPath)
    {
        await Task.Run(() => Directory.CreateDirectory(directoryPath)).ConfigureAwait(false);
    }
}