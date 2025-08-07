namespace TxtDb.Database.Interfaces;

/// <summary>
/// Query filter specification for table queries.
/// </summary>
public interface IQueryFilter
{
    /// <summary>
    /// Evaluates whether an object matches the filter.
    /// </summary>
    /// <param name="obj">Object to evaluate</param>
    /// <returns>True if matches</returns>
    bool Matches(dynamic obj);
}