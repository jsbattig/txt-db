namespace TxtDb.Database.Interfaces;

/// <summary>
/// Subscriber interface for page modification events.
/// Used for cache invalidation and cross-process coordination.
/// </summary>
public interface IPageEventSubscriber
{
    /// <summary>
    /// Called when a page is modified after transaction commit.
    /// </summary>
    /// <param name="database">Database name</param>
    /// <param name="table">Table name</param>
    /// <param name="pageId">Page identifier that was modified</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task for async processing</returns>
    Task OnPageModified(string database, string table, string pageId, CancellationToken cancellationToken = default);
}