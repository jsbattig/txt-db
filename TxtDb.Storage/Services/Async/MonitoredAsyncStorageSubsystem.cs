using System.Diagnostics;
using TxtDb.Storage.Models;

namespace TxtDb.Storage.Services.Async;

/// <summary>
/// MonitoredAsyncStorageSubsystem - Performance Monitoring Wrapper for Epic 002 Phase 4
/// Wraps AsyncStorageSubsystem to automatically collect comprehensive performance metrics
/// Provides real-time monitoring of all async operations with latency and throughput tracking
/// </summary>
public class MonitoredAsyncStorageSubsystem : IDisposable
{
    private readonly AsyncStorageSubsystem _innerStorage;
    private readonly StorageMetrics _metrics;
    private bool _disposed;

    /// <summary>
    /// Performance metrics collected by this wrapper
    /// </summary>
    public StorageMetrics Metrics => _metrics;

    public MonitoredAsyncStorageSubsystem()
    {
        _innerStorage = new AsyncStorageSubsystem();
        _metrics = new StorageMetrics();
    }

    // Delegate basic properties and methods that exist
    public long FlushOperationCount => _innerStorage.FlushOperationCount;

    public void Initialize(string rootPath, StorageConfig config)
    {
        _innerStorage.Initialize(rootPath, config);
    }

    // Async Transaction operations with metrics
    public async Task<long> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            var result = await _innerStorage.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task CommitTransactionAsync(long transactionId, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.CommitTransactionAsync(transactionId, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task CommitTransactionAsync(long transactionId, FlushPriority flushPriority, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.CommitTransactionAsync(transactionId, flushPriority, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task RollbackTransactionAsync(long transactionId, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.RollbackTransactionAsync(transactionId, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    // Async Read operations with metrics (using string pageId as per interface)
    public async Task<object[]> ReadPageAsync(long transactionId, string namespaceName, string pageId, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            var result = await _innerStorage.ReadPageAsync(transactionId, namespaceName, pageId, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task<Dictionary<string, object[]>> GetMatchingObjectsAsync(long transactionId, string namespaceName, string pattern, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            var result = await _innerStorage.GetMatchingObjectsAsync(transactionId, namespaceName, pattern, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    // Async Write operations with metrics  
    public async Task<string> InsertObjectAsync(long transactionId, string namespaceName, object obj, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            var result = await _innerStorage.InsertObjectAsync(transactionId, namespaceName, obj, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task UpdatePageAsync(long transactionId, string namespaceName, string pageId, object[] objects, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.UpdatePageAsync(transactionId, namespaceName, pageId, objects, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    // Async Namespace operations with metrics
    public async Task CreateNamespaceAsync(long transactionId, string namespaceName, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.CreateNamespaceAsync(transactionId, namespaceName, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task DeleteNamespaceAsync(long transactionId, string namespaceName, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.DeleteNamespaceAsync(transactionId, namespaceName, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    public async Task RenameNamespaceAsync(long transactionId, string oldName, string newName, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        _metrics.IncrementActiveOperations();
        
        try
        {
            await _innerStorage.RenameNamespaceAsync(transactionId, oldName, newName, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
        finally
        {
            _metrics.DecrementActiveOperations();
        }
    }

    // Synchronous method delegation (without async metrics, but still tracked)
    public long BeginTransaction()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = _innerStorage.BeginTransaction();
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void CommitTransaction(long transactionId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.CommitTransaction(transactionId);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void RollbackTransaction(long transactionId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.RollbackTransaction(transactionId);
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordTransactionOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    // Synchronous methods delegated to base class (AsyncStorageSubsystem inherits from StorageSubsystem)    
    public object[] ReadPage(long transactionId, string namespaceName, string pageId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = _innerStorage.ReadPage(transactionId, namespaceName, pageId);
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public Dictionary<string, object[]> GetMatchingObjects(long transactionId, string namespaceName, string pattern)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = _innerStorage.GetMatchingObjects(transactionId, namespaceName, pattern);
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordReadOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public string InsertObject(long transactionId, string namespaceName, object obj)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = _innerStorage.InsertObject(transactionId, namespaceName, obj);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void UpdatePage(long transactionId, string namespaceName, string pageId, object[] objects)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.UpdatePage(transactionId, namespaceName, pageId, objects);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void CreateNamespace(long transactionId, string namespaceName)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.CreateNamespace(transactionId, namespaceName);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void DeleteNamespace(long transactionId, string namespaceName)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.DeleteNamespace(transactionId, namespaceName);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    public void RenameNamespace(long transactionId, string oldName, string newName)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _innerStorage.RenameNamespace(transactionId, oldName, newName);
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: true);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.RecordWriteOperation(stopwatch.Elapsed, success: false);
            _metrics.RecordError(ex.GetType().Name, ex.Message);
            throw;
        }
    }

    // Note: ListNamespaces and NamespaceExists are not available in AsyncStorageSubsystem
    // These would need to be added to the base class if needed

    public void Dispose()
    {
        if (!_disposed)
        {
            _innerStorage?.Dispose();
            _disposed = true;
        }
    }
}