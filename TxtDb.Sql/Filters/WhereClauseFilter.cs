using System.Dynamic;
using System.Reflection;
using System.Text.RegularExpressions;
using SqlParser.Ast;
using TxtDb.Database.Interfaces;
using TxtDb.Sql.Exceptions;
using TxtDb.Sql.Models;

namespace TxtDb.Sql.Filters;

/// <summary>
/// IQueryFilter implementation that evaluates SQL WHERE clause expressions against dynamic objects.
/// Supports all SQL comparison operators, logical operators, and handles JSON/dynamic object properties.
/// 
/// This filter bridges SqlParserCS AST expressions with TxtDb's IQueryFilter interface,
/// providing full WHERE clause evaluation for UPDATE and DELETE operations.
/// </summary>
public class WhereClauseFilter : IQueryFilter
{
    private readonly Expression? _whereExpression;
    private readonly string _originalSql;
    
    /// <summary>
    /// Creates a WHERE clause filter from a SqlParserCS Expression AST node.
    /// </summary>
    /// <param name="whereExpression">The WHERE expression AST node from SqlParserCS</param>
    /// <param name="originalSql">Original SQL for error reporting</param>
    public WhereClauseFilter(Expression? whereExpression, string originalSql)
    {
        _whereExpression = whereExpression;
        _originalSql = originalSql ?? throw new ArgumentNullException(nameof(originalSql));
    }
    
    /// <summary>
    /// Evaluates the WHERE clause expression against a dynamic object.
    /// Supports all SQL operators and handles type coercion between SQL types and JSON values.
    /// </summary>
    /// <param name="obj">Dynamic object to evaluate against</param>
    /// <returns>True if the object matches the WHERE clause, false otherwise</returns>
    public bool Matches(dynamic obj)
    {
        // If no WHERE clause, match all objects
        if (_whereExpression == null)
            return true;
            
        try
        {
            return EvaluateExpression(_whereExpression, obj);
        }
        catch (Exception ex)
        {
            throw new SqlExecutionException(
                $"Failed to evaluate WHERE clause: {ex.Message}",
                _originalSql,
                "WHERE");
        }
    }
    
    /// <summary>
    /// Recursively evaluates SQL expressions against dynamic objects.
    /// Handles comparison operators, logical operators, and value extraction.
    /// </summary>
    private bool EvaluateExpression(Expression expression, dynamic obj)
    {
        return expression switch
        {
            // Binary operations (comparison and logical operators)
            Expression.BinaryOp binaryOp => EvaluateBinaryOperation(binaryOp, obj),
            
            // Unary operations (NOT, IS NULL, IS NOT NULL)
            Expression.UnaryOp unaryOp => EvaluateUnaryOperation(unaryOp, obj),
            
            // LIKE expressions
            Expression.Like like => EvaluateLikeExpression(like, obj),
            
            // IS NULL / IS NOT NULL expressions
            Expression.IsNull isNull => EvaluateIsNullExpression(isNull, obj),
            Expression.IsNotNull isNotNull => EvaluateIsNotNullExpression(isNotNull, obj),
            
            // Parenthesized expressions
            Expression.Nested nested => EvaluateExpression(nested.Expression, obj),
            
            // Literal values (for constant expressions like "WHERE 1 = 1")
            Expression.LiteralValue literal => EvaluateLiteralAsBoolean(literal),
            
            _ => throw new SqlExecutionException(
                $"Unsupported WHERE clause expression type: {expression.GetType().Name}",
                _originalSql,
                "WHERE")
        };
    }
    
    /// <summary>
    /// Evaluates binary operations including comparison (=, !=, >, <, etc.) and logical (AND, OR).
    /// </summary>
    private bool EvaluateBinaryOperation(Expression.BinaryOp binaryOp, dynamic obj)
    {
        return binaryOp.Op switch
        {
            // Comparison operators
            BinaryOperator.Eq => CompareValuesEqual(binaryOp.Left, binaryOp.Right, obj),
            BinaryOperator.NotEq => !CompareValuesEqual(binaryOp.Left, binaryOp.Right, obj),
            BinaryOperator.Gt => CompareValuesGreater(binaryOp.Left, binaryOp.Right, obj),
            BinaryOperator.GtEq => CompareValuesGreaterEqual(binaryOp.Left, binaryOp.Right, obj),
            BinaryOperator.Lt => CompareValuesLess(binaryOp.Left, binaryOp.Right, obj),
            BinaryOperator.LtEq => CompareValuesLessEqual(binaryOp.Left, binaryOp.Right, obj),
            
            // Logical operators
            BinaryOperator.And => EvaluateExpression(binaryOp.Left, obj) && EvaluateExpression(binaryOp.Right, obj),
            BinaryOperator.Or => EvaluateExpression(binaryOp.Left, obj) || EvaluateExpression(binaryOp.Right, obj),
            
            _ => throw new SqlExecutionException(
                $"Unsupported binary operator: {binaryOp.Op}",
                _originalSql,
                "WHERE")
        };
    }
    
    /// <summary>
    /// Evaluates unary operations like NOT.
    /// </summary>
    private bool EvaluateUnaryOperation(Expression.UnaryOp unaryOp, dynamic obj)
    {
        return unaryOp.Op switch
        {
            UnaryOperator.Not => !EvaluateExpression(unaryOp.Expression, obj),
            
            _ => throw new SqlExecutionException(
                $"Unsupported unary operator: {unaryOp.Op}",
                _originalSql,
                "WHERE")
        };
    }
    
    /// <summary>
    /// Evaluates LIKE expressions with pattern matching using % and _.
    /// </summary>
    private bool EvaluateLikeExpression(Expression.Like like, dynamic obj)
    {
        var leftValue = ExtractValue(like.Expression, obj);
        var patternValue = ExtractValue(like.Pattern, obj);
        
        var leftString = ConvertToString(leftValue);
        var patternString = ConvertToString(patternValue);
        
        // Convert SQL LIKE pattern to regex
        var regexPattern = ConvertLikePatternToRegex(patternString);
        var regex = new Regex(regexPattern, RegexOptions.IgnoreCase);
        
        // DEBUG: Log LIKE pattern matching
        Console.WriteLine($"LIKE DEBUG: '{leftString}' LIKE '{patternString}' -> regex: '{regexPattern}'");
        bool matches = regex.IsMatch(leftString);
        Console.WriteLine($"LIKE DEBUG: Match result: {matches}");
        
        // Handle LIKE vs NOT LIKE
        return like.Negated ? !matches : matches;
    }
    
    /// <summary>
    /// Evaluates IS NULL and IS NOT NULL expressions.
    /// </summary>
    private bool EvaluateIsNullExpression(Expression.IsNull isNull, dynamic obj)
    {
        var value = ExtractValue(isNull.Expression, obj);
        bool isValueNull = value == null;
        
        // Handle IS NULL vs IS NOT NULL
        // IMPLEMENTATION: Check if IsNull has a Negated property like LIKE expressions do
        // Use reflection to inspect the structure and determine if this is IS NULL or IS NOT NULL
        bool isNegated = false;
        var isNullType = isNull.GetType();
        var negatedProperty = isNullType.GetProperty("Negated");
        
        if (negatedProperty != null)
        {
            isNegated = (bool)negatedProperty.GetValue(isNull)!;
        }
        
        // Return the correct result based on negation
        return isNegated ? !isValueNull : isValueNull;
    }
    
    /// <summary>
    /// Evaluates IS NOT NULL expressions.
    /// This handles the separate SqlParserCS expression type for IS NOT NULL.
    /// </summary>
    private bool EvaluateIsNotNullExpression(Expression.IsNotNull isNotNull, dynamic obj)
    {
        var value = ExtractValue(isNotNull.Expression, obj);
        bool isValueNull = value == null;
        
        // IS NOT NULL should return true when the value is NOT null
        // i.e., the opposite of IS NULL
        return !isValueNull;
    }
    
    /// <summary>
    /// Evaluates literal values as boolean expressions.
    /// </summary>
    private bool EvaluateLiteralAsBoolean(Expression.LiteralValue literal)
    {
        return literal.Value switch
        {
            Value.Boolean boolean => boolean.Value,
            Value.Number number => int.TryParse(number.Value, out var intVal) && intVal != 0,
            _ => false
        };
    }
    
    /// <summary>
    /// Helper methods for specific comparison operations.
    /// </summary>
    private bool CompareValuesEqual(Expression left, Expression right, dynamic obj)
    {
        var leftValue = ExtractValue(left, obj);
        var rightValue = ExtractValue(right, obj);
        return AreEqual(leftValue, rightValue);
    }
    
    private bool CompareValuesGreater(Expression left, Expression right, dynamic obj)
    {
        var leftValue = ExtractValue(left, obj);
        var rightValue = ExtractValue(right, obj);
        return CompareNumeric(leftValue, rightValue) > 0;
    }
    
    private bool CompareValuesGreaterEqual(Expression left, Expression right, dynamic obj)
    {
        var leftValue = ExtractValue(left, obj);
        var rightValue = ExtractValue(right, obj);
        return CompareNumeric(leftValue, rightValue) >= 0;
    }
    
    private bool CompareValuesLess(Expression left, Expression right, dynamic obj)
    {
        var leftValue = ExtractValue(left, obj);
        var rightValue = ExtractValue(right, obj);
        return CompareNumeric(leftValue, rightValue) < 0;
    }
    
    private bool CompareValuesLessEqual(Expression left, Expression right, dynamic obj)
    {
        var leftValue = ExtractValue(left, obj);
        var rightValue = ExtractValue(right, obj);
        return CompareNumeric(leftValue, rightValue) <= 0;
    }
    
    /// <summary>
    /// Extracts a value from an expression, handling identifiers (column references) and literals.
    /// </summary>
    private object? ExtractValue(Expression expression, dynamic obj)
    {
        return expression switch
        {
            // Column reference - extract from dynamic object
            Expression.Identifier identifier => ExtractPropertyValue(obj, identifier.Ident.ToString()),
            
            // Literal value
            Expression.LiteralValue literal => ExtractLiteralValue(literal),
            
            // Nested/parenthesized value
            Expression.Nested nested => ExtractValue(nested.Expression, obj),
            
            // Compound identifier (e.g., table.column)
            Expression.CompoundIdentifier compound => ExtractPropertyValue(obj, compound.Idents.Last().ToString()),
            
            _ => throw new SqlExecutionException(
                $"Unsupported expression type in WHERE clause: {expression.GetType().Name}",
                _originalSql,
                "WHERE")
        };
    }
    
    /// <summary>
    /// Extracts a property value from a dynamic object by name.
    /// Handles both ExpandoObject and IDictionary&lt;string, object&gt; representations.
    /// </summary>
    private object? ExtractPropertyValue(dynamic obj, string propertyName)
    {
        if (obj == null)
            return null;
            
        // Try as IDictionary<string, object> (most common case for TxtDb)
        if (obj is IDictionary<string, object> dict)
        {
            return dict.TryGetValue(propertyName, out var value) ? value : null;
        }
        
        // Try as ExpandoObject
        if (obj is ExpandoObject expando)
        {
            var expandoDict = expando as IDictionary<string, object>;
            return expandoDict!.TryGetValue(propertyName, out var value) ? value : null;
        }
        
        // Try reflection for other dynamic types
        try
        {
            var type = obj.GetType();
            var property = type.GetProperty(propertyName);
            return property?.GetValue(obj);
        }
        catch
        {
            return null;
        }
    }
    
    /// <summary>
    /// Extracts literal values from SqlParserCS Value objects.
    /// </summary>
    private object? ExtractLiteralValue(Expression.LiteralValue literal)
    {
        return literal.Value switch
        {
            Value.Number number => ParseNumericValue(number.Value),
            Value.SingleQuotedString str => str.Value,
            Value.DoubleQuotedString str => str.Value,
            Value.Boolean boolean => boolean.Value,
            Value.Null => null,
            _ => literal.Value.ToString()
        };
    }
    
    /// <summary>
    /// Parses numeric values with proper type detection (int, long, double).
    /// </summary>
    private object ParseNumericValue(string value)
    {
        if (int.TryParse(value, out var intVal))
            return intVal;
        if (long.TryParse(value, out var longVal))
            return longVal;
        if (double.TryParse(value, out var doubleVal))
            return doubleVal;
        return value; // Return as string if parsing fails
    }
    
    /// <summary>
    /// Compares two values for equality with type coercion.
    /// Handles string-to-number comparisons and null values.
    /// </summary>
    private bool AreEqual(object? left, object? right)
    {
        // Handle null cases
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        // Direct equality
        if (left.Equals(right)) return true;
        
        // Type coercion for numeric comparisons
        if (TryConvertToNumeric(left, out var leftNum) && TryConvertToNumeric(right, out var rightNum))
        {
            return Math.Abs(leftNum - rightNum) < double.Epsilon;
        }
        
        // String comparison (case-insensitive)
        var leftStr = ConvertToString(left);
        var rightStr = ConvertToString(right);
        return string.Equals(leftStr, rightStr, StringComparison.OrdinalIgnoreCase);
    }
    
    /// <summary>
    /// Compares two values numerically.
    /// Returns positive if left > right, negative if left < right, zero if equal.
    /// </summary>
    private double CompareNumeric(object? left, object? right)
    {
        if (!TryConvertToNumeric(left, out var leftNum))
            throw new SqlExecutionException(
                $"Cannot compare non-numeric value: {left}",
                _originalSql,
                "WHERE");
                
        if (!TryConvertToNumeric(right, out var rightNum))
            throw new SqlExecutionException(
                $"Cannot compare non-numeric value: {right}",
                _originalSql,
                "WHERE");
                
        return leftNum - rightNum;
    }
    
    /// <summary>
    /// Attempts to convert a value to a numeric double.
    /// </summary>
    private bool TryConvertToNumeric(object? value, out double result)
    {
        result = 0;
        
        if (value == null) return false;
        
        return value switch
        {
            int intVal => (result = intVal) == result,
            long longVal => (result = longVal) == result,
            float floatVal => (result = floatVal) == result,
            double doubleVal => (result = doubleVal) == result,
            decimal decimalVal => (result = (double)decimalVal) == result,
            string strVal => double.TryParse(strVal, out result),
            _ => false
        };
    }
    
    /// <summary>
    /// Converts a value to string representation.
    /// </summary>
    private string ConvertToString(object? value)
    {
        return value?.ToString() ?? string.Empty;
    }
    
    /// <summary>
    /// Converts SQL LIKE pattern to .NET regex pattern.
    /// % becomes .*, _ becomes ., and literal characters are escaped.
    /// </summary>
    private string ConvertLikePatternToRegex(string likePattern)
    {
        // CRITICAL FIX: Don't escape first - handle wildcards directly
        // Replace SQL wildcards with regex equivalents first
        var pattern = likePattern
            .Replace("%", ".*")  // % becomes .*
            .Replace("_", ".");  // _ becomes .
            
        // Now escape any remaining regex special characters (but preserve our .* and . wildcards)
        // We need to be careful not to escape the . and * we just added
        var escaped = Regex.Escape(pattern)
            .Replace("\\*", "*")   // Restore * that we want
            .Replace("\\.", ".");  // Restore . that we want
        
        // Anchor the pattern for full string matching
        return $"^{escaped}$";
    }
}