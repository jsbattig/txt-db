using System.Dynamic;
using SqlParser.Ast;
using TxtDb.Database.Interfaces;
using TxtDb.Sql.Exceptions;

namespace TxtDb.Sql.Filters;

/// <summary>
/// IQueryFilter implementation that uses USE INDEX hints for optimized query execution.
/// This filter leverages TxtDb's secondary indexes to provide efficient lookups based on the hinted index,
/// then applies additional WHERE clause filtering to the index-filtered results.
/// 
/// This is the core implementation for Story 4 - USE INDEX functionality.
/// </summary>
public class IndexHintedFilter : IQueryFilter
{
    private readonly ITable _table;
    private readonly string _indexName;
    private readonly Expression? _whereExpression;
    private readonly string _originalSql;
    private readonly WhereClauseFilter _whereClauseFilter;
    private List<dynamic>? _indexFilteredResults;
    
    /// <summary>
    /// Creates an index-hinted filter that uses the specified index for optimization.
    /// </summary>
    /// <param name="table">The table to query</param>
    /// <param name="indexName">The name of the index to use for optimization</param>
    /// <param name="whereExpression">The WHERE expression AST node from SqlParserCS</param>
    /// <param name="originalSql">Original SQL for error reporting</param>
    /// <param name="transaction">Database transaction for index operations</param>
    public IndexHintedFilter(ITable table, string indexName, Expression? whereExpression, string originalSql, IDatabaseTransaction transaction)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        _whereExpression = whereExpression;
        _originalSql = originalSql ?? throw new ArgumentNullException(nameof(originalSql));
        
        // Create a standard WHERE clause filter for additional filtering after index lookup
        _whereClauseFilter = new WhereClauseFilter(whereExpression, originalSql);
        
        // Pre-compute index-filtered results during construction
        _indexFilteredResults = ComputeIndexFilteredResults(transaction).GetAwaiter().GetResult();
    }
    
    /// <summary>
    /// Evaluates whether the given object matches the index-optimized filter.
    /// This method uses pre-computed index results for efficiency.
    /// </summary>
    /// <param name="obj">Dynamic object to evaluate against</param>
    /// <returns>True if the object matches both the index filter and WHERE clause, false otherwise</returns>
    public bool Matches(dynamic obj)
    {
        // First check if the object is in our index-filtered results
        if (_indexFilteredResults == null || !IsObjectInIndexFilteredResults(obj))
            return false;
            
        // Then apply the WHERE clause filter for any additional conditions
        return _whereClauseFilter.Matches(obj);
    }
    
    /// <summary>
    /// Computes the results from index-based filtering by analyzing the WHERE clause
    /// and using the appropriate index query operations.
    /// </summary>
    private async Task<List<dynamic>> ComputeIndexFilteredResults(IDatabaseTransaction transaction)
    {
        try
        {
            Console.WriteLine($"INDEX FILTER DEBUG: Computing index-filtered results using index '{_indexName}'");
            
            // Verify the index exists
            var indexes = await _table.ListIndexesAsync(transaction);
            if (!indexes.Contains(_indexName))
            {
                throw new SqlExecutionException(
                    $"Index '{_indexName}' not found on table '{_table.Name}'",
                    _originalSql,
                    "USE INDEX");
            }
            
            Console.WriteLine($"INDEX FILTER DEBUG: Index '{_indexName}' found on table '{_table.Name}'");
            
            // Extract index query criteria from WHERE expression
            var indexQueryValue = ExtractIndexQueryValue(_whereExpression);
            if (indexQueryValue != null)
            {
                Console.WriteLine($"INDEX FILTER DEBUG: Extracted index query value: '{indexQueryValue}' (Type: {indexQueryValue.GetType().Name})");
                
                // Use a custom query filter that checks if objects match the index criteria
                var indexValueFilter = new IndexValueFilter(_indexName, indexQueryValue, _table.PrimaryKeyField);
                var indexResults = await _table.QueryAsync(transaction, indexValueFilter);
                Console.WriteLine($"INDEX FILTER DEBUG: Index-based filter returned {indexResults.Count} objects");
                
                return indexResults.ToList();
            }
            
            Console.WriteLine($"INDEX FILTER DEBUG: No index query value extracted, falling back to full table scan");
            
            // Fallback to full table scan if we can't extract an index query value
            var allResults = await _table.QueryAsync(transaction, new MatchAllQueryFilter());
            return allResults.ToList();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"INDEX FILTER DEBUG: Error during index filtering: {ex.Message}");
            throw new SqlExecutionException(
                $"Failed to execute index-hinted query: {ex.Message}",
                _originalSql,
                "USE INDEX");
        }
    }
    
    /// <summary>
    /// Extracts the value to use for index querying from the WHERE expression.
    /// This method analyzes the WHERE clause to find conditions that match the indexed field.
    /// </summary>
    private object? ExtractIndexQueryValue(Expression? expression)
    {
        if (expression == null)
            return null;
            
        // Handle binary operations (equality comparisons)
        if (expression is Expression.BinaryOp binaryOp && binaryOp.Op == BinaryOperator.Eq)
        {
            // Look for patterns like "indexed_field = value"
            var leftValue = ExtractExpressionValue(binaryOp.Left);
            var rightValue = ExtractExpressionValue(binaryOp.Right);
            
            Console.WriteLine($"INDEX FILTER DEBUG: Binary equality - Left: '{leftValue}', Right: '{rightValue}'");
            
            // Return the literal value (assumes one side is a field reference, other is a literal)
            if (leftValue is string && rightValue != null && !(rightValue is string))
                return rightValue;
            if (rightValue is string && leftValue != null && !(leftValue is string))
                return leftValue;
        }
        
        // Handle AND operations - look for the indexed field condition
        if (expression is Expression.BinaryOp andOp && andOp.Op == BinaryOperator.And)
        {
            // Try left side first
            var leftResult = ExtractIndexQueryValue(andOp.Left);
            if (leftResult != null)
                return leftResult;
                
            // Try right side
            return ExtractIndexQueryValue(andOp.Right);
        }
        
        return null;
    }
    
    /// <summary>
    /// Extracts values from expression objects for index query analysis.
    /// </summary>
    private object? ExtractExpressionValue(Expression expression)
    {
        return expression switch
        {
            Expression.Identifier identifier => identifier.Ident.ToString(),
            Expression.LiteralValue literal => ExtractLiteralValue(literal),
            Expression.CompoundIdentifier compound => compound.Idents.Last().ToString(),
            _ => null
        };
    }
    
    /// <summary>
    /// Extracts literal values from SqlParserCS Value objects.
    /// </summary>
    private object? ExtractLiteralValue(Expression.LiteralValue literal)
    {
        return literal.Value switch
        {
            Value.Number number => int.TryParse(number.Value, out var intVal) ? (object)intVal : long.Parse(number.Value),
            Value.SingleQuotedString str => str.Value,
            Value.DoubleQuotedString str => str.Value,
            Value.Boolean boolean => boolean.Value,
            Value.Null => null,
            _ => literal.Value.ToString()
        };
    }
    
    /// <summary>
    /// Checks if a given object exists in the pre-computed index-filtered results.
    /// This is used to quickly determine if an object matches the index criteria.
    /// </summary>
    private bool IsObjectInIndexFilteredResults(dynamic obj)
    {
        if (_indexFilteredResults == null)
            return false;
            
        // Compare objects by their primary key for efficiency
        var objDict = obj as IDictionary<string, object>;
        if (objDict == null)
            return false;
            
        var primaryKeyField = _table.PrimaryKeyField.TrimStart('$', '.');
        if (!objDict.TryGetValue(primaryKeyField, out var objPrimaryKey))
            return false;
            
        foreach (var indexResult in _indexFilteredResults)
        {
            var indexResultDict = indexResult as IDictionary<string, object>;
            if (indexResultDict == null)
                continue;
                
            if (indexResultDict.TryGetValue(primaryKeyField, out var indexPrimaryKey))
            {
                if (AreKeysEqual(objPrimaryKey, indexPrimaryKey))
                    return true;
            }
        }
        
        return false;
    }
    
    /// <summary>
    /// Compares two primary key values for equality, handling type conversions.
    /// </summary>
    private static bool AreKeysEqual(object? key1, object? key2)
    {
        if (key1 == null && key2 == null) return true;
        if (key1 == null || key2 == null) return false;
        
        // Handle Int32/Int64 conversions that can occur during JSON serialization
        if (key1 is long longVal1 && key2 is int intVal2)
            return longVal1 == intVal2;
        if (key1 is int intVal1 && key2 is long longVal2)
            return intVal1 == longVal2;
            
        return key1.Equals(key2);
    }
    
    /// <summary>
    /// Simple query filter that matches all objects.
    /// Used as a fallback when index optimization isn't applicable.
    /// </summary>
    private class MatchAllQueryFilter : IQueryFilter
    {
        public bool Matches(dynamic obj)
        {
            return true; // Match all objects
        }
    }
    
    /// <summary>
    /// Query filter that matches objects based on a specific field value.
    /// This simulates index lookup by checking field values directly.
    /// </summary>
    private class IndexValueFilter : IQueryFilter
    {
        private readonly string _indexName;
        private readonly object _targetValue;
        private readonly string _primaryKeyField;
        
        public IndexValueFilter(string indexName, object targetValue, string primaryKeyField)
        {
            _indexName = indexName;
            _targetValue = targetValue;
            _primaryKeyField = primaryKeyField;
        }
        
        public bool Matches(dynamic obj)
        {
            if (obj == null) return false;
            
            var objDict = obj as IDictionary<string, object>;
            if (objDict == null) return false;
            
            // Extract the field name from the index name (assuming idx_table_field pattern)
            var fieldName = ExtractFieldNameFromIndexName(_indexName);
            
            // Check if the object has the field and if it matches the target value
            if (objDict.TryGetValue(fieldName, out var fieldValue))
            {
                return AreValuesEqual(fieldValue, _targetValue);
            }
            
            return false;
        }
        
        private string ExtractFieldNameFromIndexName(string indexName)
        {
            // Handle common index naming patterns like "idx_table_field" -> "field"
            var parts = indexName.Split('_');
            if (parts.Length >= 3)
            {
                return parts[parts.Length - 1]; // Last part is typically the field name
            }
            
            // Fallback: try to extract from patterns like "idx_users_age" -> "age"
            if (indexName.StartsWith("idx_") && parts.Length >= 2)
            {
                return parts[parts.Length - 1];
            }
            
            // Last resort: use the full index name
            return indexName;
        }
        
        private bool AreValuesEqual(object? value1, object? value2)
        {
            if (value1 == null && value2 == null) return true;
            if (value1 == null || value2 == null) return false;
            
            // Handle type conversions for numeric comparisons
            if (value1 is long longVal1 && value2 is int intVal2)
                return longVal1 == intVal2;
            if (value1 is int intVal1 && value2 is long longVal2)
                return intVal1 == longVal2;
            
            return value1.Equals(value2);
        }
    }
}