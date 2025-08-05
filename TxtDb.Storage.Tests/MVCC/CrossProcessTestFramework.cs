using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// Cross-Process Test Framework for Epic 003 Phase 1
    /// 
    /// Implements true multi-process testing using:
    /// - Process.Start() for launching test processes
    /// - File-based barrier synchronization
    /// - Inter-process communication through files
    /// - Process lifecycle management
    /// 
    /// CRITICAL: Enables true cross-process validation scenarios
    /// </summary>
    public class CrossProcessTestFramework : IDisposable
    {
        private readonly string _testDirectory;
        private readonly string _barrierDirectory;
        private readonly string _communicationDirectory;
        private readonly string _resultsDirectory;
        private readonly List<Process> _processes;
        private readonly ITestOutputHelper? _output;
        private volatile bool _disposed = false;

        /// <summary>
        /// Test process configuration
        /// </summary>
        public class ProcessConfig
        {
            public string ProcessId { get; set; } = Guid.NewGuid().ToString();
            public string TestAssembly { get; set; } = "";
            public string TestClass { get; set; } = "";
            public string TestMethod { get; set; } = "";
            public Dictionary<string, string> Parameters { get; set; } = new();
            public int TimeoutSeconds { get; set; } = 60;
        }

        /// <summary>
        /// Process test result
        /// </summary>
        public class ProcessTestResult
        {
            public string ProcessId { get; set; } = "";
            public bool Success { get; set; }
            public string Output { get; set; } = "";
            public string Error { get; set; } = "";
            public int ExitCode { get; set; }
            public TimeSpan Duration { get; set; }
            public Dictionary<string, object> Data { get; set; } = new();
        }

        /// <summary>
        /// File-based barrier for process synchronization
        /// </summary>
        public class FileBarrier
        {
            private readonly string _barrierPath;
            private readonly int _participantCount;
            private readonly string _barrierId;

            public FileBarrier(string barrierDirectory, string barrierId, int participantCount)
            {
                _barrierId = barrierId;
                _participantCount = participantCount;
                _barrierPath = Path.Combine(barrierDirectory, _barrierId);
                Directory.CreateDirectory(_barrierPath);
            }

            public async Task SignalAndWaitAsync(string processId, TimeSpan timeout)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': Signaling arrival...");
                
                // Signal arrival
                var arrivalFile = Path.Combine(_barrierPath, $"arrived_{processId}.txt");
                await File.WriteAllTextAsync(arrivalFile, DateTime.UtcNow.ToString());
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': Arrival signaled at {arrivalFile}");

                // Wait for all participants
                var deadline = DateTime.UtcNow.Add(timeout);
                var lastArrivedCount = 0;
                
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': Waiting for {_participantCount} participants until {deadline:HH:mm:ss.fff}");
                
                while (DateTime.UtcNow < deadline)
                {
                    var arrivedFiles = Directory.GetFiles(_barrierPath, "arrived_*.txt");
                    var arrivedCount = arrivedFiles.Length;
                    
                    if (arrivedCount != lastArrivedCount)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': {arrivedCount}/{_participantCount} participants arrived");
                        foreach (var file in arrivedFiles)
                        {
                            var fileName = Path.GetFileName(file);
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': Found arrival file: {fileName}");
                        }
                        lastArrivedCount = arrivedCount;
                    }
                    
                    if (arrivedCount >= _participantCount)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': All {_participantCount} participants arrived. Proceeding.");
                        return; // All arrived
                    }
                    await Task.Delay(50);
                }

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{processId}] Barrier '{_barrierId}': TIMEOUT! Only {lastArrivedCount}/{_participantCount} participants arrived");
                throw new TimeoutException($"Barrier timeout waiting for {_participantCount} participants");
            }

            public void Reset()
            {
                if (Directory.Exists(_barrierPath))
                {
                    foreach (var file in Directory.GetFiles(_barrierPath))
                    {
                        try { File.Delete(file); } catch { }
                    }
                }
            }
        }

        public CrossProcessTestFramework(string testDirectory, ITestOutputHelper? output = null)
        {
            _testDirectory = testDirectory ?? throw new ArgumentNullException(nameof(testDirectory));
            _barrierDirectory = Path.Combine(_testDirectory, "barriers");
            _communicationDirectory = Path.Combine(_testDirectory, "communication");
            _resultsDirectory = Path.Combine(_testDirectory, "results");
            _processes = new List<Process>();
            _output = output;

            // Ensure directories exist
            Directory.CreateDirectory(_testDirectory);
            Directory.CreateDirectory(_barrierDirectory);
            Directory.CreateDirectory(_communicationDirectory);
            Directory.CreateDirectory(_resultsDirectory);
        }

        /// <summary>
        /// Launches a test process
        /// </summary>
        public async Task<Process> LaunchTestProcessAsync(ProcessConfig config)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CrossProcessTestFramework));

            // Create process start info with proper dotnet exec command
            var startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"exec \"{Assembly.GetExecutingAssembly().Location}\"",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = Environment.CurrentDirectory
            };

            // Add environment variables properly (not as command line arguments)
            startInfo.Environment["TXTDB_TEST_PROCESS_ID"] = config.ProcessId;
            startInfo.Environment["TXTDB_TEST_DIRECTORY"] = _testDirectory;
            startInfo.Environment["TXTDB_TEST_CLASS"] = config.TestClass;
            startInfo.Environment["TXTDB_TEST_METHOD"] = config.TestMethod;
            startInfo.Environment["TXTDB_TEST_FRAMEWORK"] = "true";
            
            // Add all parameters as environment variables
            foreach (var kvp in config.Parameters)
            {
                startInfo.Environment[kvp.Key] = kvp.Value;
            }

            var process = new Process { StartInfo = startInfo };
            
            // Capture output
            var outputBuilder = new System.Text.StringBuilder();
            var errorBuilder = new System.Text.StringBuilder();
            
            process.OutputDataReceived += (sender, e) => 
            {
                if (e.Data != null)
                {
                    outputBuilder.AppendLine(e.Data);
                    _output?.WriteLine($"[{config.ProcessId}] OUT: {e.Data}");
                }
            };
            
            process.ErrorDataReceived += (sender, e) => 
            {
                if (e.Data != null)
                {
                    errorBuilder.AppendLine(e.Data);
                    _output?.WriteLine($"[{config.ProcessId}] ERR: {e.Data}");
                }
            };

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            
            _processes.Add(process);
            
            return process;
        }

        /// <summary>
        /// Runs a multi-process test scenario
        /// </summary>
        public async Task<List<ProcessTestResult>> RunMultiProcessTestAsync(
            string testName,
            List<ProcessConfig> processConfigs,
            Func<CrossProcessTestContext, Task> coordinatorLogic)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CrossProcessTestFramework));

            var results = new List<ProcessTestResult>();
            var context = new CrossProcessTestContext(this, processConfigs.Count);

            try
            {
                // Launch all processes
                var tasks = new List<Task<ProcessTestResult>>();
                foreach (var config in processConfigs)
                {
                    var process = await LaunchTestProcessAsync(config);
                    tasks.Add(WaitForProcessCompletionAsync(process, config));
                }

                // Run coordinator logic in parallel
                var coordinatorTask = coordinatorLogic(context);

                // Wait for all to complete
                await Task.WhenAll(tasks.Concat(new[] { coordinatorTask }));

                // Collect results
                foreach (var task in tasks)
                {
                    results.Add(await task);
                }

                return results;
            }
            finally
            {
                // Ensure all processes are terminated
                foreach (var process in _processes.ToList())
                {
                    if (!process.HasExited)
                    {
                        try
                        {
                            process.Kill();
                            process.WaitForExit(5000);
                        }
                        catch { }
                    }
                }
            }
        }

        /// <summary>
        /// Creates a file-based barrier for process synchronization
        /// </summary>
        public FileBarrier CreateBarrier(string barrierId, int participantCount)
        {
            return new FileBarrier(_barrierDirectory, barrierId, participantCount);
        }

        /// <summary>
        /// Sends data to a specific process
        /// </summary>
        public async Task SendToProcessAsync(string processId, string messageType, object data)
        {
            var message = new ProcessMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                FromProcess = "coordinator",
                ToProcess = processId,
                MessageType = messageType,
                Data = JsonSerializer.Serialize(data),
                Timestamp = DateTime.UtcNow
            };

            var messagePath = Path.Combine(_communicationDirectory, processId, $"{message.MessageId}.json");
            var dir = Path.GetDirectoryName(messagePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            var json = JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(messagePath, json);
        }

        /// <summary>
        /// Receives messages for a process
        /// </summary>
        public async Task<List<ProcessMessage>> ReceiveMessagesAsync(string processId, string? messageType = null)
        {
            var messages = new List<ProcessMessage>();
            var processDir = Path.Combine(_communicationDirectory, processId);
            
            if (!Directory.Exists(processDir))
                return messages;

            var files = Directory.GetFiles(processDir, "*.json");
            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var message = JsonSerializer.Deserialize<ProcessMessage>(json);
                    
                    if (message != null && (messageType == null || message.MessageType == messageType))
                    {
                        messages.Add(message);
                    }
                }
                catch
                {
                    // Skip corrupted messages
                }
            }

            return messages.OrderBy(m => m.Timestamp).ToList();
        }

        /// <summary>
        /// Waits for a process to complete and collects results
        /// </summary>
        private async Task<ProcessTestResult> WaitForProcessCompletionAsync(Process process, ProcessConfig config)
        {
            var stopwatch = Stopwatch.StartNew();
            var timeout = TimeSpan.FromSeconds(config.TimeoutSeconds);
            
            var completed = await Task.Run(() => process.WaitForExit((int)timeout.TotalMilliseconds));
            stopwatch.Stop();

            if (!completed)
            {
                // Timeout - kill process
                try
                {
                    process.Kill();
                    process.WaitForExit(5000);
                }
                catch { }
            }

            // Load result file if exists
            var resultPath = Path.Combine(_resultsDirectory, $"{config.ProcessId}.json");
            ProcessTestResult? result = null;
            
            if (File.Exists(resultPath))
            {
                try
                {
                    var json = await File.ReadAllTextAsync(resultPath);
                    result = JsonSerializer.Deserialize<ProcessTestResult>(json);
                }
                catch { }
            }

            if (result == null)
            {
                result = new ProcessTestResult
                {
                    ProcessId = config.ProcessId,
                    Success = process.ExitCode == 0,
                    ExitCode = process.ExitCode,
                    Duration = stopwatch.Elapsed,
                    Error = completed ? "" : "Process timed out"
                };
            }

            return result;
        }

        /// <summary>
        /// Gets the test executable path
        /// </summary>
        private string GetTestExecutablePath()
        {
            // This would need to be configured based on the test runner
            // For now, using the current process path
            return Process.GetCurrentProcess().MainModule?.FileName ?? "dotnet";
        }

        /// <summary>
        /// Inter-process message
        /// </summary>
        public class ProcessMessage
        {
            public string MessageId { get; set; } = "";
            public string FromProcess { get; set; } = "";
            public string ToProcess { get; set; } = "";
            public string MessageType { get; set; } = "";
            public string Data { get; set; } = "";
            public DateTime Timestamp { get; set; }
        }

        /// <summary>
        /// Test context for coordinator logic
        /// </summary>
        public class CrossProcessTestContext
        {
            private readonly CrossProcessTestFramework _framework;
            private readonly int _processCount;

            public CrossProcessTestContext(CrossProcessTestFramework framework, int processCount)
            {
                _framework = framework;
                _processCount = processCount;
            }

            public FileBarrier CreateBarrier(string name)
            {
                return _framework.CreateBarrier(name, _processCount);
            }

            public Task SendToProcessAsync(string processId, string messageType, object data)
            {
                return _framework.SendToProcessAsync(processId, messageType, data);
            }

            public Task<List<ProcessMessage>> ReceiveMessagesAsync(string processId, string? messageType = null)
            {
                return _framework.ReceiveMessagesAsync(processId, messageType);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;

            // Terminate all processes
            foreach (var process in _processes)
            {
                try
                {
                    if (!process.HasExited)
                    {
                        process.Kill();
                        process.WaitForExit(5000);
                    }
                    process.Dispose();
                }
                catch { }
            }
            _processes.Clear();

            // Clean up test directory
            try
            {
                if (Directory.Exists(_testDirectory))
                {
                    Directory.Delete(_testDirectory, true);
                }
            }
            catch { }
        }
    }

    /// <summary>
    /// Base class for cross-process test participants
    /// </summary>
    public abstract class CrossProcessTestParticipant
    {
        protected string ProcessId { get; }
        protected string TestDirectory { get; }
        protected string CommunicationDirectory { get; }
        protected string ResultsDirectory { get; }
        protected Dictionary<string, object> TestResultData { get; } = new();

        protected CrossProcessTestParticipant()
        {
            ProcessId = Environment.GetEnvironmentVariable("TXTDB_TEST_PROCESS_ID") ?? Guid.NewGuid().ToString();
            TestDirectory = Environment.GetEnvironmentVariable("TXTDB_TEST_DIRECTORY") ?? Path.GetTempPath();
            CommunicationDirectory = Path.Combine(TestDirectory, "communication");
            ResultsDirectory = Path.Combine(TestDirectory, "results");
        }

        /// <summary>
        /// Main entry point for test execution
        /// </summary>
        public async Task<int> RunAsync()
        {
            var result = new CrossProcessTestFramework.ProcessTestResult
            {
                ProcessId = ProcessId
            };

            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                await ExecuteTestAsync();
                result.Success = true;
                result.ExitCode = 0;
                
                // Copy test result data from child class
                foreach (var kvp in TestResultData)
                {
                    result.Data[kvp.Key] = kvp.Value;
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.ToString();
                result.ExitCode = 1;
            }
            finally
            {
                stopwatch.Stop();
                result.Duration = stopwatch.Elapsed;
                
                // Save result
                await SaveResultAsync(result);
            }

            return result.ExitCode;
        }

        /// <summary>
        /// Implement test logic here
        /// </summary>
        protected abstract Task ExecuteTestAsync();

        /// <summary>
        /// Waits at a barrier
        /// </summary>
        protected async Task WaitAtBarrierAsync(string barrierId, int participantCount, TimeSpan timeout)
        {
            var barrier = new CrossProcessTestFramework.FileBarrier(
                Path.Combine(TestDirectory, "barriers"), 
                barrierId, 
                participantCount);
            
            await barrier.SignalAndWaitAsync(ProcessId, timeout);
        }

        /// <summary>
        /// Sends a message to another process
        /// </summary>
        protected async Task SendMessageAsync(string toProcess, string messageType, object data)
        {
            var message = new CrossProcessTestFramework.ProcessMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                FromProcess = ProcessId,
                ToProcess = toProcess,
                MessageType = messageType,
                Data = JsonSerializer.Serialize(data),
                Timestamp = DateTime.UtcNow
            };

            var messagePath = Path.Combine(CommunicationDirectory, toProcess, $"{message.MessageId}.json");
            var dir = Path.GetDirectoryName(messagePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            var json = JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(messagePath, json);
        }

        /// <summary>
        /// Receives messages
        /// </summary>
        protected async Task<List<CrossProcessTestFramework.ProcessMessage>> ReceiveMessagesAsync(string? messageType = null)
        {
            var messages = new List<CrossProcessTestFramework.ProcessMessage>();
            var processDir = Path.Combine(CommunicationDirectory, ProcessId);
            
            if (!Directory.Exists(processDir))
                return messages;

            var files = Directory.GetFiles(processDir, "*.json");
            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var message = JsonSerializer.Deserialize<CrossProcessTestFramework.ProcessMessage>(json);
                    
                    if (message != null && (messageType == null || message.MessageType == messageType))
                    {
                        messages.Add(message);
                    }
                }
                catch { }
            }

            return messages.OrderBy(m => m.Timestamp).ToList();
        }

        /// <summary>
        /// Saves test result
        /// </summary>
        private async Task SaveResultAsync(CrossProcessTestFramework.ProcessTestResult result)
        {
            Directory.CreateDirectory(ResultsDirectory);
            var resultPath = Path.Combine(ResultsDirectory, $"{ProcessId}.json");
            var json = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(resultPath, json);
        }
    }
}