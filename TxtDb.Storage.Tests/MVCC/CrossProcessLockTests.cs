using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using TxtDb.Storage.Services.MVCC;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// TDD tests for Cross-Process Locking Infrastructure (Story 003-001)
    /// 
    /// Test Scenarios:
    /// 1. Single process lock acquisition and release
    /// 2. Lock timeout mechanisms 
    /// 3. Cross-process lock conflict detection
    /// 4. Automatic cleanup of abandoned locks
    /// 5. Process crash recovery scenarios
    /// 6. Lock acquisition latency under 10ms P95
    /// </summary>
    public class CrossProcessLockTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _testLockDirectory;
        private readonly List<string> _createdFiles;

        public CrossProcessLockTests(ITestOutputHelper output)
        {
            _output = output;
            _testLockDirectory = Path.Combine(Path.GetTempPath(), "TxtDb_CrossProcessLock_Tests", Guid.NewGuid().ToString());
            Directory.CreateDirectory(_testLockDirectory);
            _createdFiles = new List<string>();
        }

        [Fact]
        public async Task CrossProcessLock_BasicAcquisition_ShouldAcquireAndReleaseLock()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "test.lock");
            _createdFiles.Add(lockPath);

            // ACT & ASSERT - This should fail initially (TDD Red phase)
            using (var crossProcessLock = new CrossProcessLock(lockPath))
            {
                var acquired = await crossProcessLock.TryAcquireAsync(TimeSpan.FromSeconds(1));
                
                Assert.True(acquired, "Lock should be acquired on first attempt");
                Assert.True(File.Exists(lockPath), "Lock file should exist when lock is acquired");
                
                // Note: We can't read the lock file content while it's exclusively held
                // The lock mechanism is working if the file exists and the lock was acquired
            }
            
            // Lock should be released after disposal (DeleteOnClose will handle cleanup)
            Assert.False(File.Exists(lockPath), "Lock file should be deleted after disposal");
        }

        [Fact]
        public async Task CrossProcessLock_TimeoutMechanism_ShouldRespectTimeout()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "timeout_test.lock");
            _createdFiles.Add(lockPath);

            // First acquire the lock with one instance to simulate a held lock
            using (var holdingLock = new CrossProcessLock(lockPath))
            {
                var holdingLockAcquired = await holdingLock.TryAcquireAsync(TimeSpan.FromSeconds(1));
                Assert.True(holdingLockAcquired, "First lock should be acquired");

                // ACT - Try to acquire with another instance while the first is held
                var stopwatch = Stopwatch.StartNew();
                using (var crossProcessLock = new CrossProcessLock(lockPath))
                {
                    var acquired = await crossProcessLock.TryAcquireAsync(TimeSpan.FromMilliseconds(500));
                    stopwatch.Stop();

                    // ASSERT
                    Assert.False(acquired, "Lock should not be acquired when held by another process");
                    Assert.InRange(stopwatch.ElapsedMilliseconds, 450, 750); // Allow some tolerance
                }
            }
        }

        [Fact]
        public async Task CrossProcessLock_ConflictDetection_ShouldPreventMultipleLocks()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "conflict_test.lock");
            _createdFiles.Add(lockPath);

            using (var lock1 = new CrossProcessLock(lockPath))
            using (var lock2 = new CrossProcessLock(lockPath))
            {
                // ACT
                var acquired1 = await lock1.TryAcquireAsync(TimeSpan.FromMilliseconds(100));
                var acquired2 = await lock2.TryAcquireAsync(TimeSpan.FromMilliseconds(100));

                // ASSERT
                Assert.True(acquired1, "First lock should be acquired");
                Assert.False(acquired2, "Second lock should fail due to conflict");
            }
        }

        [Fact]
        public async Task CrossProcessLock_AcquisitionLatency_ShouldBeLessThan10msP95()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "latency_test.lock");
            _createdFiles.Add(lockPath);
            var latencies = new List<double>();

            // ACT - Measure lock acquisition latency over multiple iterations
            for (int i = 0; i < 100; i++)
            {
                using (var crossProcessLock = new CrossProcessLock($"{lockPath}_{i}"))
                {
                    _createdFiles.Add($"{lockPath}_{i}");
                    
                    var stopwatch = Stopwatch.StartNew();
                    await crossProcessLock.TryAcquireAsync(TimeSpan.FromSeconds(1));
                    stopwatch.Stop();
                    
                    latencies.Add(stopwatch.Elapsed.TotalMilliseconds);
                }
            }

            // ASSERT - Calculate P95 latency
            latencies.Sort();
            var p95Index = (int)Math.Ceiling(latencies.Count * 0.95) - 1;
            var p95Latency = latencies[p95Index];

            _output.WriteLine($"Lock acquisition P95 latency: {p95Latency:F2}ms");
            _output.WriteLine($"Average latency: {latencies.Average():F2}ms");

            Assert.True(p95Latency < 10.0, $"P95 lock acquisition latency should be under 10ms, actual: {p95Latency:F2}ms");
        }

        [Fact]
        public async Task CrossProcessLock_AutomaticCleanup_ShouldCleanupAbandonedLocks()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "cleanup_test.lock");
            _createdFiles.Add(lockPath);

            // Simulate abandoned lock by creating lock file with old timestamp
            var abandonedLockInfo = new
            {
                ProcessId = 99999, // Non-existent process
                AcquiredAt = DateTime.UtcNow.AddMinutes(-10), // Old timestamp
                MachineName = Environment.MachineName
            };
            
            await File.WriteAllTextAsync(lockPath, System.Text.Json.JsonSerializer.Serialize(abandonedLockInfo));

            // ACT
            using (var crossProcessLock = new CrossProcessLock(lockPath))
            {
                var acquired = await crossProcessLock.TryAcquireAsync(TimeSpan.FromSeconds(2));

                // ASSERT
                Assert.True(acquired, "Should be able to acquire abandoned lock after cleanup");
            }
        }

        [Fact]
        public async Task CrossProcessLock_ConcurrentAccess_ShouldMaintainExclusivity()
        {
            // ARRANGE
            var lockPath = Path.Combine(_testLockDirectory, "concurrent_test.lock");
            _createdFiles.Add(lockPath);
            var successCount = 0;
            var concurrentAttempts = 0;
            var startBarrier = new Barrier(10); // Ensure all tasks start simultaneously
            var tasks = new List<Task>();

            // ACT - Multiple tasks trying to acquire the same lock SIMULTANEOUSLY
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    // Wait for all tasks to be ready before attempting lock acquisition
                    startBarrier.SignalAndWait();
                    
                    using (var crossProcessLock = new CrossProcessLock(lockPath))
                    {
                        Interlocked.Increment(ref concurrentAttempts);
                        
                        // Use a very short timeout to ensure true concurrency testing
                        if (await crossProcessLock.TryAcquireAsync(TimeSpan.FromMilliseconds(100)))
                        {
                            var currentCount = Interlocked.Increment(ref successCount);
                            
                            // Verify that no other task acquired the lock simultaneously
                            Assert.Equal(1, currentCount);
                            
                            await Task.Delay(150); // Hold lock long enough to ensure other tasks timeout
                            
                            Interlocked.Decrement(ref successCount);
                        }
                    }
                }));
            }

            await Task.WhenAll(tasks);

            // ASSERT - All tasks should have attempted, but only one should succeed at a time
            Assert.Equal(10, concurrentAttempts);
            Assert.Equal(0, successCount); // Should be back to 0 after decrement
        }

        public void Dispose()
        {
            // Cleanup test files
            foreach (var file in _createdFiles)
            {
                try
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }

            try
            {
                if (Directory.Exists(_testLockDirectory))
                    Directory.Delete(_testLockDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}