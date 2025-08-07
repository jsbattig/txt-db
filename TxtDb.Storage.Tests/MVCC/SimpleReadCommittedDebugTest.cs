using System;
using System.IO;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;
using Xunit.Abstractions;
using Newtonsoft.Json.Linq;

namespace TxtDb.Storage.Tests.MVCC
{
    public class SimpleReadCommittedDebugTest : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly StorageSubsystem _storage;
        private readonly string _testRoot;

        public SimpleReadCommittedDebugTest(ITestOutputHelper output)
        {
            _output = output;
            _testRoot = Path.Combine(Path.GetTempPath(), $"txtdb_debug_{Guid.NewGuid():N}");
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
        public void SimpleReadCommitted_DebugDeserialization()
        {
            const string testNamespace = "debug.test";

            // Insert a single object to debug
            var txnId1 = _storage.BeginTransaction();
            var testObject = new TestConcurrentObject
            {
                Id = 999,
                Value = "TestValue_999",
                TaskId = 999,
                ThreadId = 123,
                Timestamp = DateTime.UtcNow
            };
            
            var pageId = _storage.InsertObject(txnId1, testNamespace, testObject);
            _storage.CommitTransaction(txnId1);
            
            _output.WriteLine($"Inserted object with TaskId 999 into page {pageId}");

            // Read it back
            var txnId2 = _storage.BeginTransaction();
            var allObjects = _storage.GetMatchingObjects(txnId2, testNamespace, "*");
            _storage.CommitTransaction(txnId2);

            _output.WriteLine($"Found {allObjects.Count} pages with total {allObjects.Values.Sum(objs => objs.Length)} objects");

            foreach (var (retrievedPageId, objects) in allObjects)
            {
                _output.WriteLine($"Page {retrievedPageId}: {objects.Length} objects");
                
                for (int i = 0; i < objects.Length; i++)
                {
                    var obj = objects[i];
                    _output.WriteLine($"  Object {i}: Type = {obj.GetType().Name}");
                    
                    if (obj is JObject jObj)
                    {
                        _output.WriteLine($"    JObject properties: {string.Join(", ", jObj.Properties().Select(p => $"{p.Name}={p.Value}"))}");
                        
                        // Check if this JObject has type information and can be deserialized
                        if (jObj.Property("$type") != null)
                        {
                            _output.WriteLine($"    JObject has type info: {jObj["$type"]}");
                            try
                            {
                                var deserializedObj = jObj.ToObject<TestConcurrentObject>();
                                if (deserializedObj != null)
                                {
                                    _output.WriteLine($"    Successfully deserialized to TestConcurrentObject: Id={deserializedObj.Id}, TaskId={deserializedObj.TaskId}");
                                }
                            }
                            catch (Exception ex)
                            {
                                _output.WriteLine($"    Failed to deserialize JObject with type info: {ex.Message}");
                            }
                        }
                        
                        // Try to extract TaskId from JObject
                        var taskIdToken = jObj["TaskId"];
                        _output.WriteLine($"    TaskId token: {taskIdToken} (type: {taskIdToken?.Type})");
                        
                        if (taskIdToken?.Type == JTokenType.Integer)
                        {
                            var taskId = taskIdToken.Value<int>();
                            _output.WriteLine($"    Successfully extracted TaskId: {taskId}");
                        }
                    }
                    else if (obj is TestConcurrentObject testObj)
                    {
                        _output.WriteLine($"    TestConcurrentObject: Id={testObj.Id}, TaskId={testObj.TaskId}, Value={testObj.Value}");
                    }
                    else
                    {
                        // Use reflection for other object types
                        var props = obj.GetType().GetProperties();
                        foreach (var prop in props)
                        {
                            try
                            {
                                var value = prop.GetValue(obj);
                                _output.WriteLine($"    {prop.Name}: {value} (type: {prop.PropertyType.Name})");
                            }
                            catch (Exception ex)
                            {
                                _output.WriteLine($"    {prop.Name}: ERROR - {ex.Message}");
                            }
                        }
                    }
                }
            }

            // The critical test - make sure we can extract the TaskId properly
            var foundTaskIds = new HashSet<int>();
            foreach (var objects in allObjects.Values)
            {
                foreach (var obj in objects)
                {
                    if (obj is JObject jObj)
                    {
                        // Try to deserialize if it has type information
                        if (jObj.Property("$type") != null)
                        {
                            try
                            {
                                var deserializedObj = jObj.ToObject<TestConcurrentObject>();
                                if (deserializedObj != null)
                                {
                                    foundTaskIds.Add(deserializedObj.TaskId);
                                    continue;
                                }
                            }
                            catch
                            {
                                // Fall back to token extraction
                            }
                        }
                        
                        // Extract from JObject tokens
                        var taskIdToken = jObj["TaskId"];
                        if (taskIdToken?.Type == JTokenType.Integer)
                        {
                            foundTaskIds.Add(taskIdToken.Value<int>());
                        }
                    }
                    else if (obj is TestConcurrentObject testObj)
                    {
                        foundTaskIds.Add(testObj.TaskId);
                    }
                }
            }

            _output.WriteLine($"Final result: Found TaskIds: [{string.Join(", ", foundTaskIds)}]");
            Assert.Equal(1, foundTaskIds.Count);
            Assert.Contains(999, foundTaskIds);
        }
    }
}