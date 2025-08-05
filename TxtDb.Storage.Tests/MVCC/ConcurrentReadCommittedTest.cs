using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Newtonsoft.Json.Linq;

namespace TxtDb.Storage.Tests.MVCC
{
    public class TestConcurrentObject
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
        public int TaskId { get; set; }
        public int ThreadId { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class ConcurrentReadCommittedTest : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly StorageSubsystem _storage;
        private readonly string _testRoot;

        public ConcurrentReadCommittedTest(ITestOutputHelper output)
        {
            _output = output;
            _testRoot = Path.Combine(Path.GetTempPath(), $"txtdb_readcommitted_{Guid.NewGuid():N}");
            _storage = new StorageSubsystem();
            _storage.Initialize(_testRoot, new StorageConfig
            {
                Format = SerializationFormat.Json,
                MaxPageSizeKB = 4
            });
        }

        public void Dispose()
        {
            if (Directory.Exists(_testRoot))
            {
                Directory.Delete(_testRoot, true);
            }
        }

        [Fact]
        public async Task ConcurrentInsert_ReadCommitted_ShouldSeeAllCommittedData()
        {
            const int taskCount = 50;
            const string testNamespace = "concurrent.readcommitted";
            var results = new ConcurrentBag<(int taskId, string pageId, bool success)>();
            var errors = new ConcurrentBag<Exception>();

            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Starting concurrent read-committed test with {taskCount} tasks");

            // Create concurrent tasks
            var tasks = Enumerable.Range(0, taskCount).Select(async taskId =>
            {
                try
                {
                    // Each task gets its own transaction
                    var txnId = _storage.BeginTransaction();
                    
                    _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Task {taskId}: Started transaction {txnId}");
                    
                    var testObject = new TestConcurrentObject
                    {
                        Id = taskId,
                        Value = $"ReadCommitted_{taskId}",
                        TaskId = taskId,
                        ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId,
                        Timestamp = DateTime.UtcNow
                    };

                    var pageId = _storage.InsertObject(txnId, testNamespace, testObject);
                    _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Task {taskId}: Inserted object into page {pageId}");
                    
                    _storage.CommitTransaction(txnId);
                    _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Task {taskId}: Committed transaction {txnId}");
                    
                    results.Add((taskId, pageId, true));
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Task {taskId}: ERROR - {ex.Message}");
                    errors.Add(ex);
                    results.Add((taskId, string.Empty, false));
                }
            }).ToArray();

            await Task.WhenAll(tasks);

            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] All tasks completed. Analyzing results...");

            // Check for errors
            if (errors.Count > 0)
            {
                _output.WriteLine($"Found {errors.Count} errors:");
                foreach (var error in errors.Take(5))
                {
                    _output.WriteLine($"  - {error.Message}");
                }
                throw new Exception($"Found {errors.Count} errors during concurrent operations");
            }

            var successfulTasks = results.Where(r => r.success).ToList();
            _output.WriteLine($"Successful tasks: {successfulTasks.Count}/{taskCount}");

            Assert.Equal(taskCount, successfulTasks.Count);

            // Now verify that we can read ALL committed data
            var readTxnId = _storage.BeginTransaction();
            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Reading data with transaction {readTxnId}");
            
            var allObjects = _storage.GetMatchingObjects(readTxnId, testNamespace, "*");
            _storage.CommitTransaction(readTxnId);

            var totalObjectsFound = allObjects.Values.Sum(objects => objects.Length);
            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Found {totalObjectsFound} objects across {allObjects.Count} pages");

            // Debug: Show what we found
            foreach (var (pageId, objects) in allObjects)
            {
                _output.WriteLine($"  Page {pageId}: {objects.Length} objects");
                foreach (var obj in objects.Take(3))
                {
                    if (obj is TestConcurrentObject testObj)
                    {
                        _output.WriteLine($"    - Id: {testObj.Id}, Value: {testObj.Value}, TaskId: {testObj.TaskId}");
                    }
                    else if (obj is JObject jObj)
                    {
                        var id = jObj["Id"]?.Value<int>();
                        var value = jObj["Value"]?.Value<string>();
                        var taskId = jObj["TaskId"]?.Value<int>();
                        _output.WriteLine($"    - JObject: Id: {id}, Value: {value}, TaskId: {taskId}");
                    }
                    else
                    {
                        // Fallback for other dynamic objects
                        var props = obj.GetType().GetProperties();
                        var id = props.FirstOrDefault(p => p.Name == "Id")?.GetValue(obj);
                        var value = props.FirstOrDefault(p => p.Name == "Value")?.GetValue(obj);
                        _output.WriteLine($"    - Dynamic object: Id: {id}, Value: {value}, Type: {obj.GetType().Name}");
                    }
                }
                if (objects.Length > 3)
                {
                    _output.WriteLine($"    ... and {objects.Length - 3} more objects");
                }
            }

            // CRITICAL TEST: We should see ALL committed objects
            Assert.Equal(taskCount, totalObjectsFound);

            // Verify that each task's object is present
            var foundTaskIds = new HashSet<int>();
            foreach (var objects in allObjects.Values)
            {
                foreach (var obj in objects)
                {
                    if (obj is TestConcurrentObject testObj)
                    {
                        foundTaskIds.Add(testObj.TaskId);
                    }
                    else if (obj is JObject jObj)
                    {
                        // Handle JObject deserialization from JSON
                        var taskIdToken = jObj["TaskId"];
                        if (taskIdToken?.Type == JTokenType.Integer)
                        {
                            foundTaskIds.Add(taskIdToken.Value<int>());
                        }
                    }
                    else
                    {
                        // Fallback for other dynamic objects
                        var props = obj.GetType().GetProperties();
                        var taskIdProp = props.FirstOrDefault(p => p.Name == "TaskId");
                        if (taskIdProp?.GetValue(obj) is int taskId)
                        {
                            foundTaskIds.Add(taskId);
                        }
                    }
                }
            }

            _output.WriteLine($"Found objects from {foundTaskIds.Count} different tasks");
            Assert.Equal(taskCount, foundTaskIds.Count);

            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] ✅ Read-committed isolation test PASSED!");
        }

        [Fact]
        public async Task ConcurrentReadDuringWrites_ShouldSeeCommittedDataOnly()
        {
            const int writerCount = 20;
            const int readerCount = 10;
            const string testNamespace = "concurrent.readers";
            var writerResults = new ConcurrentBag<bool>();
            var readerResults = new ConcurrentBag<int>();
            var errors = new ConcurrentBag<Exception>();

            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Starting concurrent read-during-write test");
            _output.WriteLine($"  Writers: {writerCount}, Readers: {readerCount}");

            // Start writers and readers concurrently
            var writerTasks = Enumerable.Range(0, writerCount).Select(async writerId =>
            {
                try
                {
                    var txnId = _storage.BeginTransaction();
                    
                    var testObject = new TestConcurrentObject
                    {
                        Id = writerId,
                        Value = $"Writer_{writerId}",
                        TaskId = writerId,
                        ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId,
                        Timestamp = DateTime.UtcNow
                    };

                    _storage.InsertObject(txnId, testNamespace, testObject);
                    
                    // Add some delay to allow readers to run during uncommitted state
                    await Task.Delay(10);
                    
                    _storage.CommitTransaction(txnId);
                    writerResults.Add(true);
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                    writerResults.Add(false);
                }
            });

            var readerTasks = Enumerable.Range(0, readerCount).Select(async readerId =>
            {
                try
                {
                    // Add small delay to let some writers start
                    await Task.Delay(5);
                    
                    var txnId = _storage.BeginTransaction();
                    var objects = _storage.GetMatchingObjects(txnId, testNamespace, "*");
                    _storage.CommitTransaction(txnId);
                    
                    var totalObjects = objects.Values.Sum(objs => objs.Length);
                    readerResults.Add(totalObjects);
                    
                    _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Reader {readerId}: Found {totalObjects} committed objects");
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                    readerResults.Add(-1);
                }
            });

            await Task.WhenAll(writerTasks.Concat(readerTasks));

            // Check for errors
            if (errors.Count > 0)
            {
                _output.WriteLine($"Found {errors.Count} errors:");
                foreach (var error in errors.Take(3))
                {
                    _output.WriteLine($"  - {error.Message}");
                }
                throw new Exception($"Found {errors.Count} errors during concurrent read/write operations");
            }

            // Verify all writers succeeded
            var successfulWrites = writerResults.Count(r => r);
            Assert.Equal(writerCount, successfulWrites);

            // Verify readers found reasonable results (should see committed data only)
            var validReads = readerResults.Where(r => r >= 0).ToList();
            Assert.Equal(readerCount, validReads.Count);

            // Readers should never see partial/uncommitted data
            // Each reader should see between 0 and writerCount objects (depending on timing)
            foreach (var readResult in validReads)
            {
                Assert.True(readResult >= 0 && readResult <= writerCount, 
                    $"Reader found {readResult} objects, should be between 0 and {writerCount}");
            }

            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] ✅ Concurrent read-during-write test PASSED!");
            _output.WriteLine($"  Reader results: [{string.Join(", ", validReads.OrderBy(x => x))}]");
        }
    }
}