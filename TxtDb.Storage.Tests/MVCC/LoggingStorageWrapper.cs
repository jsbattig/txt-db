using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;

namespace TxtDb.Storage.Tests.MVCC;

/// <summary>
/// Wrapper around StorageSubsystem that adds comprehensive logging for debugging
/// </summary>
public class LoggingStorageWrapper : IStorageSubsystem
{
    private readonly IStorageSubsystem _inner;
    private readonly ConcurrentBag<string> _operationLog = new();
    private readonly object _logLock = new object();

    public LoggingStorageWrapper(IStorageSubsystem inner)
    {
        _inner = inner;
        Log("LoggingStorageWrapper initialized");
    }

    private void Log(string message)
    {
        var timestamp = DateTime.UtcNow.ToString("HH:mm:ss.fff");
        var threadId = Thread.CurrentThread.ManagedThreadId;
        var logEntry = $"[{timestamp}] T{threadId:D2}: {message}";
        
        lock (_logLock)
        {
            _operationLog.Add(logEntry);
            Console.WriteLine($"STORAGE: {logEntry}");
        }
    }

    public IEnumerable<string> GetOperationLog() => _operationLog.OrderBy(x => x);

    public void Initialize(string rootPath, StorageConfig? config = null)
    {
        Log($"Initialize(rootPath: {rootPath}, config: {config?.Format})");
        _inner.Initialize(rootPath, config);
        Log("Initialize completed");
    }

    public long BeginTransaction()
    {
        Log("BeginTransaction called");
        var txnId = _inner.BeginTransaction();
        Log($"BeginTransaction returned: {txnId}");
        return txnId;
    }

    public void CommitTransaction(long transactionId)
    {
        Log($"CommitTransaction({transactionId}) called");
        try
        {
            _inner.CommitTransaction(transactionId);
            Log($"CommitTransaction({transactionId}) completed successfully");
        }
        catch (Exception ex)
        {
            Log($"CommitTransaction({transactionId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void RollbackTransaction(long transactionId)
    {
        Log($"RollbackTransaction({transactionId}) called");
        try
        {
            _inner.RollbackTransaction(transactionId);
            Log($"RollbackTransaction({transactionId}) completed");
        }
        catch (Exception ex)
        {
            Log($"RollbackTransaction({transactionId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public string InsertObject(long transactionId, string @namespace, object data)
    {
        Log($"InsertObject(txn: {transactionId}, ns: {@namespace}, data: {System.Text.Json.JsonSerializer.Serialize(data).Substring(0, Math.Min(100, System.Text.Json.JsonSerializer.Serialize(data).Length))})");
        try
        {
            var pageId = _inner.InsertObject(transactionId, @namespace, data);
            Log($"InsertObject(txn: {transactionId}) returned pageId: {pageId}");
            return pageId;
        }
        catch (Exception ex)
        {
            Log($"InsertObject(txn: {transactionId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void UpdatePage(long transactionId, string @namespace, string pageId, object[] pageContent)
    {
        Log($"UpdatePage(txn: {transactionId}, ns: {@namespace}, page: {pageId}, objects: {pageContent.Length})");
        try
        {
            _inner.UpdatePage(transactionId, @namespace, pageId, pageContent);
            Log($"UpdatePage(txn: {transactionId}, page: {pageId}) completed successfully");
        }
        catch (Exception ex)
        {
            Log($"UpdatePage(txn: {transactionId}, page: {pageId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public object[] ReadPage(long transactionId, string @namespace, string pageId)
    {
        Log($"ReadPage(txn: {transactionId}, ns: {@namespace}, page: {pageId}) called");
        try
        {
            var result = _inner.ReadPage(transactionId, @namespace, pageId);
            Log($"ReadPage(txn: {transactionId}, page: {pageId}) returned {result.Length} objects");
            return result;
        }
        catch (Exception ex)
        {
            Log($"ReadPage(txn: {transactionId}, page: {pageId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public Dictionary<string, object[]> GetMatchingObjects(long transactionId, string @namespace, string pattern)
    {
        Log($"GetMatchingObjects(txn: {transactionId}, ns: {@namespace}, pattern: {pattern}) called");
        try
        {
            var result = _inner.GetMatchingObjects(transactionId, @namespace, pattern);
            var totalObjects = result.Values.Sum(pages => pages.Length);
            Log($"GetMatchingObjects(txn: {transactionId}) returned {result.Count} pages with {totalObjects} total objects");
            
            foreach (var kvp in result)
            {
                Log($"  Page {kvp.Key}: {kvp.Value.Length} objects");
            }
            
            return result;
        }
        catch (Exception ex)
        {
            Log($"GetMatchingObjects(txn: {transactionId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void CreateNamespace(long transactionId, string @namespace)
    {
        Log($"CreateNamespace(txn: {transactionId}, ns: {@namespace}) called");
        try
        {
            _inner.CreateNamespace(transactionId, @namespace);
            Log($"CreateNamespace(txn: {transactionId}, ns: {@namespace}) completed");
        }
        catch (Exception ex)
        {
            Log($"CreateNamespace(txn: {transactionId}, ns: {@namespace}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void DeleteNamespace(long transactionId, string @namespace)
    {
        Log($"DeleteNamespace(txn: {transactionId}, ns: {@namespace}) called");
        try
        {
            _inner.DeleteNamespace(transactionId, @namespace);
            Log($"DeleteNamespace(txn: {transactionId}, ns: {@namespace}) completed");
        }
        catch (Exception ex)
        {
            Log($"DeleteNamespace(txn: {transactionId}, ns: {@namespace}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void RenameNamespace(long transactionId, string oldName, string newName)
    {
        Log($"RenameNamespace(txn: {transactionId}, old: {oldName}, new: {newName}) called");
        try
        {
            _inner.RenameNamespace(transactionId, oldName, newName);
            Log($"RenameNamespace(txn: {transactionId}) completed");
        }
        catch (Exception ex)
        {
            Log($"RenameNamespace(txn: {transactionId}) FAILED: {ex.Message}");
            throw;
        }
    }

    public void StartVersionCleanup(int intervalMinutes = 15)
    {
        Log($"StartVersionCleanup(interval: {intervalMinutes}) called");
        _inner.StartVersionCleanup(intervalMinutes);
        Log("StartVersionCleanup completed");
    }
}