using System;
using System.Threading.Tasks;
using TxtDb.Storage.Tests.MVCC;

namespace TxtDb.Storage.Tests
{
    /// <summary>
    /// Program entry point that can handle both test execution and multi-process test scenarios
    /// </summary>
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            // Check if this is being launched as a child process for multi-process testing
            var isTestFramework = Environment.GetEnvironmentVariable("TXTDB_TEST_FRAMEWORK");
            
            Console.WriteLine($"DEBUG: Program started. TXTDB_TEST_FRAMEWORK={isTestFramework}");
            
            if (isTestFramework == "true")
            {
                // This is a child process launched by the test framework
                Console.WriteLine("DEBUG: Starting ProcessTestRunner");
                var runner = new ProcessTestRunner();
                var result = await runner.RunAsync();
                Console.WriteLine($"DEBUG: ProcessTestRunner completed with exit code: {result}");
                return result;
            }
            
            // Regular execution (tests will be run by test runner)
            Console.WriteLine("TxtDb.Storage.Tests - Use 'dotnet test' to run tests");
            return 0;
        }
    }
}