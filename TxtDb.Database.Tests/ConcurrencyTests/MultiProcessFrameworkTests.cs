using System;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Database.Services;

namespace TxtDb.Database.Tests.ConcurrencyTests;

/// <summary>
/// RED PHASE: Tests for the new multi-process framework
/// These tests verify that the multi-process test runner executable works correctly,
/// replacing the broken approach that tried to run C# source files as executables.
/// 
/// CRITICAL FIX: Uses real executable instead of "dotnet run --project {scriptPath}"
/// </summary>
public class MultiProcessFrameworkTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;
    private readonly string _storageDirectory;
    private readonly string _runnerExecutablePath;

    public MultiProcessFrameworkTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "txtdb_multiprocess_framework", Guid.NewGuid().ToString());
        _storageDirectory = Path.Combine(_testDirectory, "storage");
        _runnerExecutablePath = Path.Combine(Environment.CurrentDirectory, "..", "..", "..", "..", 
            "TxtDb.Database.Tests.MultiProcess.Runner", "bin", "Release", "net8.0", 
            "TxtDb.Database.Tests.MultiProcess.Runner");
        
        Directory.CreateDirectory(_testDirectory);
        Directory.CreateDirectory(_storageDirectory);
        
        _output.WriteLine($"Test directory: {_testDirectory}");
        _output.WriteLine($"Storage directory: {_storageDirectory}");
        _output.WriteLine($"Runner executable: {_runnerExecutablePath}");
    }

    /// <summary>
    /// RED TEST: Verify multi-process executable framework works
    /// This test SHOULD PASS once the framework is properly implemented
    /// </summary>
    [Fact]
    public async Task MultiProcessFramework_ShouldExecuteRealProcesses_NotSourceFiles()
    {
        // Arrange - Verify executable exists
        Assert.True(File.Exists(_runnerExecutablePath), $"Multi-process runner executable not found at: {_runnerExecutablePath}");
        
        _output.WriteLine("Phase 1: Test database creation via multi-process executable");

        // Act - Create database using multi-process runner
        var processInfo = new ProcessStartInfo
        {
            FileName = _runnerExecutablePath,
            Arguments = $"CreateDatabaseWithTables --STORAGE_DIRECTORY \"{_storageDirectory}\" --DATABASE_NAME \"framework_test_db\" --TABLE_COUNT \"5\"",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var process = Process.Start(processInfo);
        Assert.NotNull(process);
        
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        
        await process.WaitForExitAsync();
        
        _output.WriteLine($"Process exit code: {process.ExitCode}");
        _output.WriteLine($"Process output: {output}");
        if (!string.IsNullOrEmpty(error))
        {
            _output.WriteLine($"Process error: {error}");
        }

        // Assert - Process should complete successfully
        Assert.Equal(0, process.ExitCode);
        Assert.Contains("Database creation completed", output);
        
        _output.WriteLine("Phase 2: Verify database and tables were created");

        // Verify the database was actually created
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var database = await databaseLayer.GetDatabaseAsync("framework_test_db");
        
        Assert.NotNull(database);
        Assert.Equal("framework_test_db", database.Name);
        
        var tables = await database.ListTablesAsync();
        Assert.Equal(5, tables.Length);
        
        _output.WriteLine($"Verified database with {tables.Length} tables created via multi-process framework");
    }

    /// <summary>
    /// RED TEST: Verify concurrent multi-process operations work
    /// This test demonstrates real multi-process coordination
    /// </summary>
    [Fact]
    public async Task MultiProcessFramework_ShouldSupportConcurrentProcesses()
    {
        // Arrange
        const int processCount = 3;
        const int databasesPerProcess = 5;
        
        _output.WriteLine($"Phase 1: Launch {processCount} concurrent processes");

        // Act - Launch multiple processes concurrently
        var processTasks = new List<Task<Process>>();
        
        for (int i = 0; i < processCount; i++)
        {
            var processId = i;
            var task = Task.Run(() =>
            {
                var processInfo = new ProcessStartInfo
                {
                    FileName = _runnerExecutablePath,
                    Arguments = $"ConcurrentDatabaseCreation --STORAGE_DIRECTORY \"{_storageDirectory}\" --PROCESS_ID \"{processId}\" --DATABASES_PER_PROCESS \"{databasesPerProcess}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                return Process.Start(processInfo)!;
            });
            
            processTasks.Add(task);
        }

        var processes = await Task.WhenAll(processTasks);

        // Wait for all processes to complete
        var completionTasks = processes.Select(async process =>
        {
            var output = await process.StandardOutput.ReadToEndAsync();
            var error = await process.StandardError.ReadToEndAsync();
            
            await process.WaitForExitAsync();
            
            _output.WriteLine($"Process {process.Id} exit code: {process.ExitCode}");
            _output.WriteLine($"Process {process.Id} output: {output}");
            if (!string.IsNullOrEmpty(error))
            {
                _output.WriteLine($"Process {process.Id} error: {error}");
            }
            
            return new { Process = process, Output = output, Error = error };
        });

        var results = await Task.WhenAll(completionTasks);

        // Assert - All processes should complete successfully
        foreach (var result in results)
        {
            Assert.Equal(0, result.Process.ExitCode);
            Assert.Contains("Completed database creation", result.Output);
        }

        _output.WriteLine("Phase 2: Verify all databases were created correctly");

        // Verify all databases were created
        var databaseLayer = await DatabaseLayer.CreateAsync(_storageDirectory);
        var allDatabases = await databaseLayer.ListDatabasesAsync();
        
        var expectedDatabaseCount = processCount * databasesPerProcess;
        Assert.Equal(expectedDatabaseCount, allDatabases.Length);
        
        _output.WriteLine($"Verified {allDatabases.Length} databases created by concurrent processes");

        // Clean up processes
        foreach (var process in processes)
        {
            process.Dispose();
        }
    }

    /// <summary>
    /// RED TEST: Verify argument parsing and error handling work correctly
    /// This test ensures the framework is robust
    /// </summary>
    [Fact]
    public async Task MultiProcessFramework_ShouldHandleArgumentsAndErrors()
    {
        _output.WriteLine("Phase 1: Test invalid operation");

        // Test invalid operation
        var invalidProcessInfo = new ProcessStartInfo
        {
            FileName = _runnerExecutablePath,
            Arguments = "InvalidOperation",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var invalidProcess = Process.Start(invalidProcessInfo);
        Assert.NotNull(invalidProcess);
        
        var invalidOutput = await invalidProcess.StandardOutput.ReadToEndAsync();
        var invalidError = await invalidProcess.StandardError.ReadToEndAsync();
        
        await invalidProcess.WaitForExitAsync();
        
        _output.WriteLine($"Invalid operation exit code: {invalidProcess.ExitCode}");
        _output.WriteLine($"Invalid operation output: {invalidOutput}");
        _output.WriteLine($"Invalid operation error: {invalidError}");

        // Should fail with non-zero exit code
        Assert.NotEqual(0, invalidProcess.ExitCode);
        Assert.Contains("Unknown operation", invalidError);

        _output.WriteLine("Phase 2: Test missing arguments");

        // Test missing required arguments
        var missingArgsProcessInfo = new ProcessStartInfo
        {
            FileName = _runnerExecutablePath,
            Arguments = "CreateDatabaseWithTables --DATABASE_NAME \"test\"", // Missing STORAGE_DIRECTORY
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var missingArgsProcess = Process.Start(missingArgsProcessInfo);
        Assert.NotNull(missingArgsProcess);
        
        var missingArgsOutput = await missingArgsProcess.StandardOutput.ReadToEndAsync();
        var missingArgsError = await missingArgsProcess.StandardError.ReadToEndAsync();
        
        await missingArgsProcess.WaitForExitAsync();
        
        _output.WriteLine($"Missing args exit code: {missingArgsProcess.ExitCode}");
        _output.WriteLine($"Missing args output: {missingArgsOutput}");
        _output.WriteLine($"Missing args error: {missingArgsError}");

        // Should fail with non-zero exit code
        Assert.NotEqual(0, missingArgsProcess.ExitCode);
        Assert.Contains("Required argument", missingArgsError);

        _output.WriteLine("Multi-process framework error handling verified");

        // Clean up
        invalidProcess.Dispose();
        missingArgsProcess.Dispose();
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}