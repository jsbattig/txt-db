namespace TxtDb.Storage.Interfaces;

/// <summary>
/// Generic storage subsystem providing MVCC-based object persistence with immediate disk durability.
/// CRITICAL: ALL data operations require an active transaction for proper snapshot isolation.
/// </summary>
public interface IStorageSubsystem 
{
    // Transaction operations (MVCC with immediate metadata persistence)
    long BeginTransaction();
    void CommitTransaction(long transactionId);
    void RollbackTransaction(long transactionId);
    
    // Object operations (ALL require transactionId for snapshot isolation)
    string InsertObject(long transactionId, string @namespace, object data);
    void UpdatePage(long transactionId, string @namespace, string pageId, object[] pageContent);
    object[] ReadPage(long transactionId, string @namespace, string pageId);
    Dictionary<string, object[]> GetMatchingObjects(long transactionId, string @namespace, string pattern);
    
    // Structural operations (require transactionId and namespace-level operation locks)
    void CreateNamespace(long transactionId, string @namespace);
    void DeleteNamespace(long transactionId, string @namespace); // Blocks if operations active
    void RenameNamespace(long transactionId, string oldName, string newName);
    
    // Configuration (immediate disk persistence)
    void Initialize(string rootPath, Models.StorageConfig? config = null);
    void StartVersionCleanup(int intervalMinutes = 15);
}