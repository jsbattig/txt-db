namespace TxtDb.Database.Interfaces;

/// <summary>
/// Represents an index on a table field.
/// Indexes map field values to page IDs containing objects with those values.
/// </summary>
public interface IIndex
{
    /// <summary>
    /// Index name.
    /// </summary>
    string Name { get; }
    
    /// <summary>
    /// JSON path to indexed field.
    /// </summary>
    string FieldPath { get; }
    
    /// <summary>
    /// Finds objects matching the specified value.
    /// Returns expando objects directly.
    /// </summary>
    /// <param name="value">Value to search for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Matching objects</returns>
    Task<IList<dynamic>> FindAsync(object value, CancellationToken cancellationToken = default);
}