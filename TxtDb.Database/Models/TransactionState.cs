namespace TxtDb.Database.Models;

/// <summary>
/// Transaction state enumeration.
/// </summary>
public enum TransactionState
{
    Active,
    Committed,
    RolledBack,
    Aborted
}