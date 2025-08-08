using TxtDb.Storage.Models;

namespace TxtDb.Storage.Interfaces.Async;

/// <summary>
/// Async storage subsystem providing MVCC-based object persistence with async/await patterns.
/// CRITICAL: Maintains all ACID guarantees while releasing threads during I/O waits.
/// Phase 2: Core Async Storage - Target 200+ ops/sec throughput improvement
/// </summary>
public interface IAsyncStorageSubsystem 
{
    // Async Transaction operations (MVCC with immediate metadata persistence)
    Task<long> BeginTransactionAsync(CancellationToken cancellationToken = default);
    Task CommitTransactionAsync(long transactionId, CancellationToken cancellationToken = default);
    Task CommitTransactionAsync(long transactionId, FlushPriority flushPriority, CancellationToken cancellationToken = default);
    Task RollbackTransactionAsync(long transactionId, CancellationToken cancellationToken = default);
    
    // Async Object operations (ALL require transactionId for snapshot isolation)
    Task<string> InsertObjectAsync(long transactionId, string @namespace, object data, CancellationToken cancellationToken = default);
    Task UpdatePageAsync(long transactionId, string @namespace, string pageId, object[] pageContent, CancellationToken cancellationToken = default);
    Task<object[]> ReadPageAsync(long transactionId, string @namespace, string pageId, CancellationToken cancellationToken = default);
    Task<Dictionary<string, object[]>> GetMatchingObjectsAsync(long transactionId, string @namespace, string pattern, CancellationToken cancellationToken = default);
    
    // Async Structural operations (require transactionId and namespace-level operation locks)
    Task CreateNamespaceAsync(long transactionId, string @namespace, CancellationToken cancellationToken = default);
    Task DeleteNamespaceAsync(long transactionId, string @namespace, CancellationToken cancellationToken = default); // Blocks if operations active
    Task RenameNamespaceAsync(long transactionId, string oldName, string newName, CancellationToken cancellationToken = default);
    
    // Async Configuration (immediate disk persistence)
    Task InitializeAsync(string rootPath, Models.StorageConfig? config = null, CancellationToken cancellationToken = default);
    Task StartVersionCleanupAsync(int intervalMinutes = 15, CancellationToken cancellationToken = default);
    
    // Performance monitoring for batch flushing and critical operations
    long FlushOperationCount { get; }
    long CriticalOperationCount { get; }
}