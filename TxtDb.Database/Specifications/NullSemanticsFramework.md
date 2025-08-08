# TxtDb NULL Semantics Framework Specification
## Version 1.0 - Comprehensive Design Document

## Executive Summary

The NULL Semantics Framework addresses the fundamental impedance mismatch between SQL's three-valued logic (True, False, Unknown) and .NET's two-valued boolean logic, while seamlessly integrating with JSON null handling and TxtDb's ExpandoObject-based storage architecture. This specification defines a complete NULL handling system that maintains SQL standard compliance while providing efficient execution within the .NET runtime environment.

### Key Architectural Decisions

1. **Ternary Logic Engine**: Implementation of a dedicated three-valued logic system that operates independently of .NET's boolean type
2. **NULL Propagation Pipeline**: Explicit NULL tracking through all expression evaluation paths
3. **Type System Bridge**: Bidirectional conversion layer between SQL NULL, .NET null, and JSON null representations
4. **Performance-Optimized Evaluation**: Short-circuit evaluation with NULL-aware optimization paths
5. **Configurable Strictness Modes**: Support for both ANSI SQL strict NULL semantics and permissive application-friendly modes

## System Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Query Layer                          │
│                  (SqlParserCS AST)                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│              NULL Semantics Framework                       │
│  ┌────────────────────────────────────────────────────┐    │
│  │           Ternary Logic Engine                     │    │
│  │  - ThreeValuedBoolean Type                        │    │
│  │  - Logic Operations (AND, OR, NOT)                │    │
│  │  - Truth Table Evaluator                          │    │
│  └────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────┐    │
│  │           NULL-Aware Expression Evaluator          │    │
│  │  - Comparison Operations                          │    │
│  │  - Arithmetic Operations                          │    │
│  │  - String Operations                              │    │
│  └────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────┐    │
│  │           Aggregation NULL Handler                 │    │
│  │  - COUNT, SUM, AVG, MIN, MAX                      │    │
│  │  - GROUP BY NULL Handling                         │    │
│  │  - DISTINCT NULL Semantics                        │    │
│  └────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────┐    │
│  │           JSON Path NULL Navigator                 │    │
│  │  - Deep Path Resolution                           │    │
│  │  - Missing Property Handling                      │    │
│  │  - Array NULL Element Processing                  │    │
│  └────────────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                 TxtDb Storage Layer                         │
│            (ExpandoObject / Dynamic Objects)                │
└─────────────────────────────────────────────────────────────┘
```

## Detailed Component Specifications

### 1. Ternary Logic Engine

#### Interface Definition

```pseudo-code
interface IThreeValuedBoolean {
    enum TernaryValue {
        TRUE,
        FALSE,
        UNKNOWN  // Represents NULL in boolean context
    }
    
    property Value: TernaryValue
    
    // Conversion methods
    method ToBoolean(unknownDefault: bool): bool
    method ToNullableBoolean(): bool?
    method ToString(): string
    
    // Factory methods
    static method FromBoolean(value: bool): IThreeValuedBoolean
    static method FromNullableBoolean(value: bool?): IThreeValuedBoolean
    static method Unknown(): IThreeValuedBoolean
}

interface ITernaryLogicEvaluator {
    method And(left: IThreeValuedBoolean, right: IThreeValuedBoolean): IThreeValuedBoolean
    method Or(left: IThreeValuedBoolean, right: IThreeValuedBoolean): IThreeValuedBoolean
    method Not(value: IThreeValuedBoolean): IThreeValuedBoolean
    method Xor(left: IThreeValuedBoolean, right: IThreeValuedBoolean): IThreeValuedBoolean
    method Implies(left: IThreeValuedBoolean, right: IThreeValuedBoolean): IThreeValuedBoolean
}
```

#### Three-Valued Logic Truth Tables

```pseudo-code
// AND Operation Truth Table
algorithm EvaluateAnd(left: TernaryValue, right: TernaryValue): TernaryValue {
    match (left, right) {
        (TRUE, TRUE) => TRUE
        (TRUE, FALSE) => FALSE
        (TRUE, UNKNOWN) => UNKNOWN
        (FALSE, TRUE) => FALSE
        (FALSE, FALSE) => FALSE
        (FALSE, UNKNOWN) => FALSE    // FALSE dominates UNKNOWN in AND
        (UNKNOWN, TRUE) => UNKNOWN
        (UNKNOWN, FALSE) => FALSE    // FALSE dominates UNKNOWN in AND
        (UNKNOWN, UNKNOWN) => UNKNOWN
    }
}

// OR Operation Truth Table
algorithm EvaluateOr(left: TernaryValue, right: TernaryValue): TernaryValue {
    match (left, right) {
        (TRUE, TRUE) => TRUE
        (TRUE, FALSE) => TRUE
        (TRUE, UNKNOWN) => TRUE      // TRUE dominates UNKNOWN in OR
        (FALSE, TRUE) => TRUE
        (FALSE, FALSE) => FALSE
        (FALSE, UNKNOWN) => UNKNOWN
        (UNKNOWN, TRUE) => TRUE      // TRUE dominates UNKNOWN in OR
        (UNKNOWN, FALSE) => UNKNOWN
        (UNKNOWN, UNKNOWN) => UNKNOWN
    }
}

// NOT Operation Truth Table
algorithm EvaluateNot(value: TernaryValue): TernaryValue {
    match value {
        TRUE => FALSE
        FALSE => TRUE
        UNKNOWN => UNKNOWN          // NOT NULL is still NULL
    }
}
```

### 2. NULL-Aware Expression Evaluator

#### Interface Definition

```pseudo-code
interface INullAwareExpressionEvaluator {
    method EvaluateExpression(
        expression: SqlExpression,
        context: IEvaluationContext,
        nullSemantics: NullSemanticsMode
    ): ExpressionResult
    
    method EvaluateComparison(
        operator: ComparisonOperator,
        left: object?,
        right: object?
    ): IThreeValuedBoolean
    
    method EvaluateArithmetic(
        operator: ArithmeticOperator,
        left: object?,
        right: object?
    ): object?
    
    method EvaluateStringOperation(
        operator: StringOperator,
        operands: object?[]
    ): object?
}

enum NullSemanticsMode {
    ANSI_STRICT,      // Full ANSI SQL compliance
    PERMISSIVE,       // Application-friendly NULL handling
    ORACLE_COMPATIBLE // Oracle-style empty string = NULL
}

class ExpressionResult {
    property Value: object?
    property IsNull: bool
    property Type: SqlDataType
    property Metadata: Dictionary<string, object>
}
```

#### NULL-Safe Comparison Algorithm

```pseudo-code
algorithm EvaluateNullSafeComparison(
    operator: ComparisonOperator,
    left: object?,
    right: object?
): IThreeValuedBoolean {
    
    // Handle IS NULL / IS NOT NULL operators
    if operator == IS_NULL {
        return ThreeValuedBoolean.FromBoolean(left == null)
    }
    if operator == IS_NOT_NULL {
        return ThreeValuedBoolean.FromBoolean(left != null)
    }
    
    // NULL propagation for standard comparisons
    if left == null OR right == null {
        // Special case: NULL-safe equality operator (<=>)
        if operator == NULL_SAFE_EQUALS {
            if left == null AND right == null {
                return ThreeValuedBoolean.True
            }
            if left == null OR right == null {
                return ThreeValuedBoolean.False
            }
        }
        
        // Standard operators return UNKNOWN for NULL operands
        return ThreeValuedBoolean.Unknown
    }
    
    // Perform type-safe comparison
    result = PerformTypedComparison(operator, left, right)
    return ThreeValuedBoolean.FromBoolean(result)
}

algorithm PerformTypedComparison(
    operator: ComparisonOperator,
    left: object,
    right: object
): bool {
    // Type coercion and comparison logic
    leftType = GetSqlType(left)
    rightType = GetSqlType(right)
    
    // Promote types to common type
    commonType = PromoteTypes(leftType, rightType)
    leftConverted = ConvertToType(left, commonType)
    rightConverted = ConvertToType(right, commonType)
    
    match operator {
        EQUALS => leftConverted == rightConverted
        NOT_EQUALS => leftConverted != rightConverted
        LESS_THAN => leftConverted < rightConverted
        LESS_THAN_OR_EQUAL => leftConverted <= rightConverted
        GREATER_THAN => leftConverted > rightConverted
        GREATER_THAN_OR_EQUAL => leftConverted >= rightConverted
        LIKE => EvaluateLikePattern(leftConverted, rightConverted)
        IN => EvaluateInList(leftConverted, rightConverted)
        BETWEEN => EvaluateBetween(leftConverted, rightConverted)
    }
}
```

### 3. Aggregation NULL Handler

#### Interface Definition

```pseudo-code
interface IAggregationNullHandler {
    method Count(values: object?[], includeNulls: bool): int
    method Sum(values: object?[]): object?
    method Average(values: object?[]): object?
    method Min(values: object?[]): object?
    method Max(values: object?[]): object?
    method StringAgg(values: object?[], separator: string): string?
    method GroupBy(values: object?[], groupingKeys: string[]): GroupedResult[]
}

class GroupedResult {
    property Key: object?  // Can be NULL for NULL group
    property Values: object?[]
    property Count: int
}
```

#### Aggregation NULL Handling Algorithms

```pseudo-code
algorithm CalculateSum(values: object?[]): object? {
    // NULL values are ignored in SUM
    nonNullValues = values.Where(v => v != null)
    
    if nonNullValues.IsEmpty() {
        return null  // SUM of all NULLs is NULL
    }
    
    sum = 0
    for value in nonNullValues {
        numericValue = ConvertToNumeric(value)
        if numericValue == null {
            continue  // Skip non-numeric values
        }
        sum += numericValue
    }
    
    return sum
}

algorithm CalculateAverage(values: object?[]): object? {
    // NULL values are ignored in AVG
    nonNullValues = values.Where(v => v != null)
    
    if nonNullValues.IsEmpty() {
        return null  // AVG of all NULLs is NULL
    }
    
    sum = 0
    count = 0
    
    for value in nonNullValues {
        numericValue = ConvertToNumeric(value)
        if numericValue == null {
            continue  // Skip non-numeric values
        }
        sum += numericValue
        count++
    }
    
    if count == 0 {
        return null
    }
    
    return sum / count
}

algorithm CalculateCount(values: object?[], includeNulls: bool): int {
    if includeNulls {
        return values.Length  // COUNT(*) includes NULLs
    } else {
        return values.Where(v => v != null).Count()  // COUNT(column) excludes NULLs
    }
}

algorithm CalculateMinMax(values: object?[], isMax: bool): object? {
    // NULL values are ignored in MIN/MAX
    nonNullValues = values.Where(v => v != null)
    
    if nonNullValues.IsEmpty() {
        return null  // MIN/MAX of all NULLs is NULL
    }
    
    result = nonNullValues[0]
    
    for i from 1 to nonNullValues.Length - 1 {
        comparison = CompareValues(result, nonNullValues[i])
        if (isMax AND comparison < 0) OR (!isMax AND comparison > 0) {
            result = nonNullValues[i]
        }
    }
    
    return result
}
```

### 4. COALESCE and NULL Functions

#### Interface Definition

```pseudo-code
interface INullFunctions {
    method Coalesce(values: object?[]): object?
    method NullIf(value1: object?, value2: object?): object?
    method IfNull(value: object?, defaultValue: object?): object?
    method Greatest(values: object?[]): object?
    method Least(values: object?[]): object?
    method IsDistinctFrom(left: object?, right: object?): bool
}
```

#### NULL Function Algorithms

```pseudo-code
algorithm EvaluateCoalesce(values: object?[]): object? {
    // Return first non-NULL value
    for value in values {
        if value != null {
            return value
        }
    }
    return null
}

algorithm EvaluateNullIf(value1: object?, value2: object?): object? {
    // Return NULL if values are equal, otherwise return value1
    if value1 == null OR value2 == null {
        return value1  // NULL comparison always false
    }
    
    if AreValuesEqual(value1, value2) {
        return null
    }
    
    return value1
}

algorithm EvaluateIsDistinctFrom(left: object?, right: object?): bool {
    // NULL-safe inequality check
    if left == null AND right == null {
        return false  // Both NULL means not distinct
    }
    if left == null OR right == null {
        return true   // One NULL means distinct
    }
    return !AreValuesEqual(left, right)
}
```

### 5. JSON Path NULL Navigator

#### Interface Definition

```pseudo-code
interface IJsonPathNullNavigator {
    method NavigatePath(
        root: ExpandoObject,
        path: JsonPath,
        nullMode: JsonNullMode
    ): NavigationResult
    
    method ExtractValue(
        obj: dynamic,
        propertyPath: string[]
    ): object?
    
    method SetValue(
        obj: dynamic,
        propertyPath: string[],
        value: object?,
        createMissing: bool
    ): bool
}

enum JsonNullMode {
    STRICT,        // Missing property throws error
    LAX,           // Missing property returns NULL
    CREATE_PATH    // Missing intermediate objects are created
}

class NavigationResult {
    property Value: object?
    property PathExists: bool
    property PartialPath: string[]
    property MissingFrom: int
}
```

#### JSON Path Navigation Algorithm

```pseudo-code
algorithm NavigateJsonPath(
    root: ExpandoObject,
    pathSegments: string[],
    nullMode: JsonNullMode
): NavigationResult {
    
    current = root
    result = new NavigationResult()
    
    for i from 0 to pathSegments.Length - 1 {
        segment = pathSegments[i]
        
        // Handle array indexing
        if IsArrayIndex(segment) {
            index = ParseArrayIndex(segment)
            if current is not Array {
                if nullMode == STRICT {
                    throw PathNavigationError("Expected array at " + segment)
                }
                result.PathExists = false
                result.PartialPath = pathSegments[0..i]
                result.MissingFrom = i
                result.Value = null
                return result
            }
            
            array = current as Array
            if index < 0 OR index >= array.Length {
                if nullMode == STRICT {
                    throw IndexOutOfBoundsError(index)
                }
                result.PathExists = false
                result.Value = null
                return result
            }
            
            current = array[index]
        }
        // Handle object property
        else {
            if current == null {
                result.PathExists = false
                result.PartialPath = pathSegments[0..i]
                result.MissingFrom = i
                result.Value = null
                return result
            }
            
            if current is ExpandoObject {
                dictionary = current as IDictionary<string, object>
                if !dictionary.ContainsKey(segment) {
                    if nullMode == STRICT {
                        throw PropertyNotFoundError(segment)
                    }
                    if nullMode == CREATE_PATH AND i < pathSegments.Length - 1 {
                        // Create intermediate object
                        newObject = new ExpandoObject()
                        dictionary[segment] = newObject
                        current = newObject
                    } else {
                        result.PathExists = false
                        result.Value = null
                        return result
                    }
                } else {
                    current = dictionary[segment]
                }
            }
            else if current is JObject {
                jobject = current as JObject
                token = jobject[segment]
                if token == null {
                    if nullMode == STRICT {
                        throw PropertyNotFoundError(segment)
                    }
                    result.PathExists = false
                    result.Value = null
                    return result
                }
                current = ConvertJTokenToObject(token)
            }
            else {
                // Try reflection for regular objects
                property = current.GetType().GetProperty(segment)
                if property == null {
                    if nullMode == STRICT {
                        throw PropertyNotFoundError(segment)
                    }
                    result.PathExists = false
                    result.Value = null
                    return result
                }
                current = property.GetValue(current)
            }
        }
    }
    
    result.PathExists = true
    result.Value = current
    result.PartialPath = pathSegments
    return result
}
```

### 6. Predicate Logic Integration

#### WHERE Clause NULL Handling

```pseudo-code
algorithm EvaluateWhereClause(
    row: ExpandoObject,
    predicate: SqlExpression,
    evaluator: INullAwareExpressionEvaluator
): bool {
    
    result = evaluator.EvaluateExpression(predicate, row, ANSI_STRICT)
    
    // Convert three-valued result to boolean for filtering
    if result.Value is IThreeValuedBoolean {
        ternaryResult = result.Value as IThreeValuedBoolean
        
        // In WHERE clause, only TRUE passes the filter
        // FALSE and UNKNOWN (NULL) are filtered out
        return ternaryResult.Value == TernaryValue.TRUE
    }
    
    // Non-boolean results are errors in WHERE clause
    throw TypeError("WHERE clause must evaluate to boolean")
}
```

#### JOIN Condition NULL Handling

```pseudo-code
algorithm EvaluateJoinCondition(
    leftRow: ExpandoObject,
    rightRow: ExpandoObject,
    joinPredicate: SqlExpression,
    joinType: JoinType,
    evaluator: INullAwareExpressionEvaluator
): JoinResult {
    
    // Combine rows for evaluation context
    combinedContext = MergeRowContexts(leftRow, rightRow)
    
    result = evaluator.EvaluateExpression(joinPredicate, combinedContext, ANSI_STRICT)
    
    if result.Value is IThreeValuedBoolean {
        ternaryResult = result.Value as IThreeValuedBoolean
        
        match joinType {
            INNER_JOIN => {
                // Only TRUE matches for INNER JOIN
                if ternaryResult.Value == TernaryValue.TRUE {
                    return JoinResult.Match(leftRow, rightRow)
                }
                return JoinResult.NoMatch()
            }
            LEFT_JOIN => {
                // TRUE matches, FALSE/UNKNOWN preserves left row with NULL right
                if ternaryResult.Value == TernaryValue.TRUE {
                    return JoinResult.Match(leftRow, rightRow)
                }
                return JoinResult.LeftOnly(leftRow)
            }
            RIGHT_JOIN => {
                // TRUE matches, FALSE/UNKNOWN preserves right row with NULL left
                if ternaryResult.Value == TernaryValue.TRUE {
                    return JoinResult.Match(leftRow, rightRow)
                }
                return JoinResult.RightOnly(rightRow)
            }
            FULL_OUTER_JOIN => {
                // TRUE matches, FALSE/UNKNOWN preserves both rows
                if ternaryResult.Value == TernaryValue.TRUE {
                    return JoinResult.Match(leftRow, rightRow)
                }
                return JoinResult.BothUnmatched(leftRow, rightRow)
            }
        }
    }
    
    throw TypeError("JOIN condition must evaluate to boolean")
}
```

### 7. Type System Integration

#### Type Conversion Bridge

```pseudo-code
interface ITypeConversionBridge {
    method SqlToClr(sqlValue: SqlValue): object?
    method ClrToSql(clrValue: object?): SqlValue
    method JsonToSql(jsonValue: JToken?): SqlValue
    method SqlToJson(sqlValue: SqlValue): JToken?
}

class SqlValue {
    property Value: object?
    property Type: SqlDataType
    property IsNull: bool
    
    static method Null(type: SqlDataType): SqlValue
    static method FromValue(value: object, type: SqlDataType): SqlValue
}

algorithm ConvertSqlToClr(sqlValue: SqlValue): object? {
    if sqlValue.IsNull {
        return null
    }
    
    match sqlValue.Type {
        INTEGER => Convert.ToInt64(sqlValue.Value)
        DECIMAL => Convert.ToDecimal(sqlValue.Value)
        VARCHAR => sqlValue.Value?.ToString()
        BOOLEAN => {
            // Special handling for three-valued boolean
            if sqlValue.Value is IThreeValuedBoolean {
                ternary = sqlValue.Value as IThreeValuedBoolean
                return ternary.ToNullableBoolean()
            }
            return Convert.ToBoolean(sqlValue.Value)
        }
        TIMESTAMP => Convert.ToDateTime(sqlValue.Value)
        JSON => {
            // Keep as ExpandoObject or JObject
            return sqlValue.Value
        }
        default => sqlValue.Value
    }
}

algorithm ConvertClrToSql(clrValue: object?): SqlValue {
    if clrValue == null {
        // Infer type from context or use VARCHAR as default
        return SqlValue.Null(SqlDataType.VARCHAR)
    }
    
    type = clrValue.GetType()
    
    match type {
        Int32, Int64 => SqlValue.FromValue(clrValue, SqlDataType.INTEGER)
        Decimal, Double, Float => SqlValue.FromValue(clrValue, SqlDataType.DECIMAL)
        String => {
            // Handle Oracle-style empty string = NULL
            str = clrValue as String
            if str.Length == 0 AND NullSemanticsMode == ORACLE_COMPATIBLE {
                return SqlValue.Null(SqlDataType.VARCHAR)
            }
            return SqlValue.FromValue(clrValue, SqlDataType.VARCHAR)
        }
        Boolean => {
            // Convert to three-valued boolean
            boolValue = clrValue as Boolean
            ternary = ThreeValuedBoolean.FromBoolean(boolValue)
            return SqlValue.FromValue(ternary, SqlDataType.BOOLEAN)
        }
        DateTime => SqlValue.FromValue(clrValue, SqlDataType.TIMESTAMP)
        ExpandoObject, JObject => SqlValue.FromValue(clrValue, SqlDataType.JSON)
        default => SqlValue.FromValue(clrValue, SqlDataType.VARCHAR)
    }
}
```

## Performance Optimizations

### Short-Circuit Evaluation

```pseudo-code
algorithm OptimizedAndEvaluation(
    leftExpression: SqlExpression,
    rightExpression: SqlExpression,
    context: IEvaluationContext
): IThreeValuedBoolean {
    
    // Evaluate left side first
    leftResult = EvaluateExpression(leftExpression, context)
    
    // Short-circuit: FALSE AND anything = FALSE
    if leftResult.Value == TernaryValue.FALSE {
        return ThreeValuedBoolean.False
    }
    
    // Must evaluate right side
    rightResult = EvaluateExpression(rightExpression, context)
    
    // Apply three-valued AND logic
    return EvaluateAnd(leftResult.Value, rightResult.Value)
}

algorithm OptimizedOrEvaluation(
    leftExpression: SqlExpression,
    rightExpression: SqlExpression,
    context: IEvaluationContext
): IThreeValuedBoolean {
    
    // Evaluate left side first
    leftResult = EvaluateExpression(leftExpression, context)
    
    // Short-circuit: TRUE OR anything = TRUE
    if leftResult.Value == TernaryValue.TRUE {
        return ThreeValuedBoolean.True
    }
    
    // Must evaluate right side
    rightResult = EvaluateExpression(rightExpression, context)
    
    // Apply three-valued OR logic
    return EvaluateOr(leftResult.Value, rightResult.Value)
}
```

### NULL Bitmap Optimization

```pseudo-code
class NullBitmap {
    private bitArray: BitArray
    private columnCount: int
    
    method IsNull(columnIndex: int): bool {
        return bitArray[columnIndex]
    }
    
    method SetNull(columnIndex: int) {
        bitArray[columnIndex] = true
    }
    
    method HasAnyNulls(): bool {
        // Fast check for any NULL values
        return bitArray.HasAnySet()
    }
    
    method GetNullColumns(): int[] {
        // Return indices of NULL columns
        result = []
        for i from 0 to columnCount - 1 {
            if bitArray[i] {
                result.Add(i)
            }
        }
        return result
    }
}
```

## Configuration and Modes

### NULL Semantics Configuration

```pseudo-code
class NullSemanticsConfiguration {
    property Mode: NullSemanticsMode
    property EmptyStringAsNull: bool
    property NullSortOrder: NullSortOrder
    property CoalesceEmptyCollections: bool
    property StrictTypeComparison: bool
    property WarningOnUnknownComparison: bool
    
    static method AnsiStrict(): NullSemanticsConfiguration {
        return new NullSemanticsConfiguration {
            Mode = ANSI_STRICT,
            EmptyStringAsNull = false,
            NullSortOrder = NULLS_LAST,
            CoalesceEmptyCollections = false,
            StrictTypeComparison = true,
            WarningOnUnknownComparison = true
        }
    }
    
    static method Permissive(): NullSemanticsConfiguration {
        return new NullSemanticsConfiguration {
            Mode = PERMISSIVE,
            EmptyStringAsNull = false,
            NullSortOrder = NULLS_FIRST,
            CoalesceEmptyCollections = true,
            StrictTypeComparison = false,
            WarningOnUnknownComparison = false
        }
    }
    
    static method OracleCompatible(): NullSemanticsConfiguration {
        return new NullSemanticsConfiguration {
            Mode = ORACLE_COMPATIBLE,
            EmptyStringAsNull = true,
            NullSortOrder = NULLS_LAST,
            CoalesceEmptyCollections = false,
            StrictTypeComparison = true,
            WarningOnUnknownComparison = true
        }
    }
}

enum NullSortOrder {
    NULLS_FIRST,  // NULLs sort before all values
    NULLS_LAST    // NULLs sort after all values
}
```

## Integration Points

### SqlParserCS AST Integration

```pseudo-code
class NullAwareAstVisitor : IAstVisitor {
    private evaluator: INullAwareExpressionEvaluator
    private configuration: NullSemanticsConfiguration
    
    method VisitBinaryExpression(node: BinaryExpression): ExpressionResult {
        leftResult = Visit(node.Left)
        rightResult = Visit(node.Right)
        
        match node.Operator {
            AND, OR => {
                leftTernary = ConvertToTernary(leftResult)
                rightTernary = ConvertToTernary(rightResult)
                return EvaluateLogicalOperation(node.Operator, leftTernary, rightTernary)
            }
            EQUALS, NOT_EQUALS, LESS_THAN, etc => {
                return evaluator.EvaluateComparison(
                    node.Operator,
                    leftResult.Value,
                    rightResult.Value
                )
            }
            PLUS, MINUS, MULTIPLY, DIVIDE => {
                return evaluator.EvaluateArithmetic(
                    node.Operator,
                    leftResult.Value,
                    rightResult.Value
                )
            }
        }
    }
    
    method VisitIsNullExpression(node: IsNullExpression): ExpressionResult {
        targetResult = Visit(node.Expression)
        isNull = targetResult.Value == null
        
        if node.IsNegated {
            return ExpressionResult.FromBoolean(!isNull)
        }
        return ExpressionResult.FromBoolean(isNull)
    }
    
    method VisitCaseExpression(node: CaseExpression): ExpressionResult {
        // CASE expressions with NULL handling
        for whenClause in node.WhenClauses {
            condition = Visit(whenClause.Condition)
            
            if condition.Value is IThreeValuedBoolean {
                ternary = condition.Value as IThreeValuedBoolean
                if ternary.Value == TernaryValue.TRUE {
                    return Visit(whenClause.Result)
                }
                // Continue on FALSE or UNKNOWN
            }
        }
        
        if node.ElseClause != null {
            return Visit(node.ElseClause)
        }
        
        return ExpressionResult.Null()
    }
}
```

### TxtDb Storage Integration

```pseudo-code
class NullAwareStorageAdapter {
    private storage: IAsyncStorageSubsystem
    private nullHandler: INullAwareExpressionEvaluator
    
    method QueryWithNullSemantics(
        namespace: string,
        predicate: SqlExpression,
        transaction: ITransactionScope
    ): async Task<ExpandoObject[]> {
        
        // Retrieve all objects in namespace
        allObjects = await storage.GetObjectsAsync(transaction, namespace)
        
        results = []
        for obj in allObjects {
            // Convert ExpandoObject to evaluation context
            context = CreateEvaluationContext(obj)
            
            // Evaluate predicate with NULL semantics
            predicateResult = nullHandler.EvaluateExpression(
                predicate,
                context,
                configuration.Mode
            )
            
            // Only include if predicate is TRUE (not FALSE or UNKNOWN)
            if predicateResult.Value is IThreeValuedBoolean {
                ternary = predicateResult.Value as IThreeValuedBoolean
                if ternary.Value == TernaryValue.TRUE {
                    results.Add(obj)
                }
            }
        }
        
        return results
    }
    
    method CreateEvaluationContext(obj: ExpandoObject): IEvaluationContext {
        return new EvaluationContext {
            CurrentRow = obj,
            Variables = new Dictionary<string, object>(),
            Functions = GetBuiltInFunctions(),
            NullNavigator = new JsonPathNullNavigator()
        }
    }
}
```

## Error Handling and Diagnostics

### NULL-Related Exceptions

```pseudo-code
class NullSemanticException : Exception {
    property ErrorCode: string
    property Context: string
    property Expression: string
}

class UnexpectedNullException : NullSemanticException {
    constructor(expression: string, context: string) {
        ErrorCode = "NULL_001"
        Message = "Unexpected NULL value in expression"
        Expression = expression
        Context = context
    }
}

class InvalidNullComparisonException : NullSemanticException {
    constructor(operator: string, leftType: string, rightType: string) {
        ErrorCode = "NULL_002"
        Message = $"Cannot use {operator} with NULL values"
    }
}

class NullPropagationException : NullSemanticException {
    constructor(operation: string, reason: string) {
        ErrorCode = "NULL_003"
        Message = $"NULL propagation in {operation}: {reason}"
    }
}
```

### Diagnostic Logging

```pseudo-code
interface INullSemanticsLogger {
    method LogNullComparison(
        expression: string,
        result: TernaryValue,
        context: IEvaluationContext
    )
    
    method LogNullPropagation(
        sourceExpression: string,
        targetExpression: string,
        propagationType: string
    )
    
    method LogCoalesceEvaluation(
        values: object?[],
        result: object?,
        elapsed: TimeSpan
    )
    
    method LogThreeValuedLogic(
        operation: string,
        left: TernaryValue,
        right: TernaryValue?,
        result: TernaryValue
    )
}
```

## Testing Strategy

### Unit Test Categories

1. **Ternary Logic Tests**
   - Truth table validation
   - Complex expression evaluation
   - Short-circuit optimization verification

2. **NULL Propagation Tests**
   - Arithmetic operations with NULLs
   - String concatenation with NULLs
   - Comparison operations with NULLs

3. **Aggregation Tests**
   - COUNT with/without NULLs
   - SUM/AVG with all NULLs
   - GROUP BY with NULL keys

4. **JSON Path Tests**
   - Missing property navigation
   - NULL array elements
   - Deep path resolution with NULLs

5. **Integration Tests**
   - Complex queries with multiple NULL conditions
   - JOIN operations with NULL handling
   - Subqueries with NULL results

### Performance Benchmarks

```pseudo-code
class NullSemanticsBenchmarks {
    method BenchmarkThreeValuedLogic() {
        // Measure overhead of ternary logic vs boolean
        iterations = 1000000
        
        // Boolean baseline
        booleanTime = MeasureTime(() => {
            for i from 0 to iterations {
                result = true AND false OR true
            }
        })
        
        // Ternary logic
        ternaryTime = MeasureTime(() => {
            for i from 0 to iterations {
                result = EvaluateOr(
                    EvaluateAnd(TRUE, FALSE),
                    TRUE
                )
            }
        })
        
        overhead = (ternaryTime - booleanTime) / booleanTime * 100
        Assert(overhead < 50, "Ternary logic overhead should be < 50%")
    }
    
    method BenchmarkNullPropagation() {
        // Measure NULL check overhead in expressions
        testData = GenerateTestData(10000)
        
        withoutNullChecks = MeasureTime(() => {
            for row in testData {
                result = row.Value1 + row.Value2 * row.Value3
            }
        })
        
        withNullChecks = MeasureTime(() => {
            for row in testData {
                result = EvaluateArithmeticWithNulls(
                    row.Value1,
                    row.Value2,
                    row.Value3
                )
            }
        })
        
        overhead = (withNullChecks - withoutNullChecks) / withoutNullChecks * 100
        Assert(overhead < 25, "NULL checking overhead should be < 25%")
    }
}
```

## Migration and Adoption

### Phased Implementation Plan

**Phase 1: Core Infrastructure (Week 1-2)**
- Implement IThreeValuedBoolean and ITernaryLogicEvaluator
- Create basic NULL-aware expression evaluator
- Unit tests for ternary logic

**Phase 2: Comparison Operations (Week 3)**
- Implement NULL-safe comparison operations
- IS NULL / IS NOT NULL support
- Integration with SqlParserCS AST

**Phase 3: Aggregation Functions (Week 4)**
- Implement aggregation NULL handling
- GROUP BY NULL support
- Performance optimization for large datasets

**Phase 4: JSON Integration (Week 5)**
- JSON path NULL navigation
- ExpandoObject NULL handling
- Dynamic property access with NULLs

**Phase 5: Advanced Features (Week 6)**
- COALESCE and NULL functions
- Configurable NULL semantics modes
- Performance tuning and optimization

**Phase 6: Testing and Documentation (Week 7-8)**
- Comprehensive test suite
- Performance benchmarking
- API documentation and examples

## Risk Assessment

### Technical Risks

1. **Performance Impact**
   - Risk: Ternary logic adds overhead to all boolean operations
   - Mitigation: Implement short-circuit evaluation and bitmap optimizations

2. **Type System Complexity**
   - Risk: Complex conversions between SQL/CLR/JSON types
   - Mitigation: Clear conversion rules and extensive testing

3. **Backward Compatibility**
   - Risk: Existing queries may behave differently with proper NULL semantics
   - Mitigation: Configurable modes to support legacy behavior

### Implementation Risks

1. **SqlParserCS Limitations**
   - Risk: Parser may not support all SQL NULL constructs
   - Mitigation: Extend parser or implement workarounds for missing features

2. **ExpandoObject Constraints**
   - Risk: Dynamic objects complicate type checking
   - Mitigation: Runtime type inspection and careful NULL handling

## Conclusion

This NULL Semantics Framework provides a comprehensive solution for handling SQL's three-valued logic within TxtDb's .NET-based architecture. The design prioritizes correctness while maintaining performance through careful optimization. The modular architecture allows for incremental implementation and testing, reducing risk while delivering value early in the development process.

The framework's integration with SqlParserCS, ExpandoObject storage, and JSON handling ensures seamless operation within TxtDb's existing ecosystem while providing the flexibility to support different NULL handling modes for various use cases.