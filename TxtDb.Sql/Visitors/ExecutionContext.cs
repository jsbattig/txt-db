using System.Collections.Concurrent;

namespace TxtDb.Sql.Visitors;

/// <summary>
/// Execution context that carries state between visitor methods during SQL statement processing.
/// Provides a thread-safe way to pass data and configuration between different phases of SQL execution.
/// 
/// This context allows the visitor pattern to maintain state without requiring global variables
/// or complex parameter passing between methods.
/// </summary>
public class ExecutionContext
{
    private readonly ConcurrentDictionary<string, object> _values = new();
    
    /// <summary>
    /// Sets a value in the execution context.
    /// </summary>
    /// <typeparam name="T">The type of the value</typeparam>
    /// <param name="key">The key to associate with the value</param>
    /// <param name="value">The value to store</param>
    public void SetValue<T>(string key, T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        
        _values[key] = value;
    }
    
    /// <summary>
    /// Gets a value from the execution context.
    /// </summary>
    /// <typeparam name="T">The expected type of the value</typeparam>
    /// <param name="key">The key to look up</param>
    /// <returns>The value associated with the key</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the key is not found</exception>
    /// <exception cref="InvalidCastException">Thrown when the value cannot be cast to the expected type</exception>
    public T GetValue<T>(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        
        if (!_values.TryGetValue(key, out var value))
        {
            throw new KeyNotFoundException($"Key '{key}' not found in execution context");
        }
        
        if (value is not T typedValue)
        {
            throw new InvalidCastException($"Value for key '{key}' is of type {value.GetType().Name}, but expected {typeof(T).Name}");
        }
        
        return typedValue;
    }
    
    /// <summary>
    /// Tries to get a value from the execution context.
    /// </summary>
    /// <typeparam name="T">The expected type of the value</typeparam>
    /// <param name="key">The key to look up</param>
    /// <param name="value">The value associated with the key, if found</param>
    /// <returns>True if the key was found and the value could be cast to the expected type; otherwise, false</returns>
    public bool TryGetValue<T>(string key, out T? value)
    {
        ArgumentNullException.ThrowIfNull(key);
        
        value = default;
        
        if (!_values.TryGetValue(key, out var obj))
        {
            return false;
        }
        
        if (obj is T typedValue)
        {
            value = typedValue;
            return true;
        }
        
        return false;
    }
    
    /// <summary>
    /// Checks if a key exists in the execution context.
    /// </summary>
    /// <param name="key">The key to check</param>
    /// <returns>True if the key exists; otherwise, false</returns>
    public bool ContainsKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _values.ContainsKey(key);
    }
    
    /// <summary>
    /// Removes a value from the execution context.
    /// </summary>
    /// <param name="key">The key to remove</param>
    /// <returns>True if the key was found and removed; otherwise, false</returns>
    public bool RemoveValue(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _values.TryRemove(key, out _);
    }
    
    /// <summary>
    /// Gets all keys currently in the execution context.
    /// </summary>
    /// <returns>A collection of all keys</returns>
    public IEnumerable<string> Keys => _values.Keys;
    
    /// <summary>
    /// Clears all values from the execution context.
    /// </summary>
    public void Clear()
    {
        _values.Clear();
    }
}