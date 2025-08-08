# SQL Parser Research for TxtDb SQL Layer

## Overview
Research into pluggable SQL parsers for .NET to build a SQL layer on top of our TxtDb storage engine. The goal is to support a subset of SQL that translates into calls to our underlying database layer.

## Top SQL Parser Options for .NET

### 1. **SqlParser-cs (Recommended for General SQL)** ⭐⭐⭐⭐⭐
- **Repository**: https://github.com/TylerBrinks/SqlParser-cs
- **Installation**: `dotnet add package SqlParserCS`
- **License**: MIT
- **Status**: Actively maintained (2024)

**Strengths:**
- Supports 11+ SQL dialects (ANSI, PostgreSQL, MySQL, MS SQL, SQLite, BigQuery, etc.)
- Clean, modern .NET implementation (ported from Rust sqlparser-rs)
- Produces traversable Abstract Syntax Tree (AST)
- Multiple output formats (ToString, ToSql, JSON)
- Visitor pattern support for AST traversal
- Extensible dialect system

**Basic Usage:**
```csharp
var sql = "SELECT TOP 10 * FROM users WHERE age > 25 ORDER BY name";
var ast = new SqlQueryParser().Parse(sql);
// AST can be traversed and transformed
```

**Perfect for:** Custom SQL subset implementation, multi-dialect support, modern .NET applications

### 2. **Microsoft ScriptDOM (Best for T-SQL)** ⭐⭐⭐⭐
- **Repository**: https://github.com/microsoft/SqlScriptDOM  
- **Installation**: `Microsoft.SqlServer.TransactSql.ScriptDom` (NuGet)
- **License**: MIT
- **Status**: Microsoft-maintained, actively developed

**Strengths:**
- Official Microsoft parser used by SQL Server tooling
- Most comprehensive T-SQL support (based on SQL Server grammar)
- Mature, production-proven (used by DacFX, SqlPackage, PowerShell)
- Full AST manipulation capabilities
- Excellent for T-SQL code analysis and transformation

**Usage Example:**
```csharp
var parser = new TSql160Parser(true); // SQL Server 2019/Azure SQL
IList<ParseError> errors;
var ast = parser.Parse(new StringReader(sql), out errors);
```

**Perfect for:** T-SQL compatibility, enterprise SQL Server integration, maximum T-SQL fidelity

### 3. **ANTLR4 with SQL Grammars (Most Flexible)** ⭐⭐⭐⭐
- **Repository**: https://github.com/antlr/grammars-v4/tree/master/sql
- **Installation**: `Antlr4` + `Antlr4.Runtime` (NuGet)
- **License**: BSD
- **Status**: Community-maintained grammars

**Available SQL Dialects:**
- T-SQL, PostgreSQL, MySQL, SQLite, Oracle (PL/SQL)
- ClickHouse, Snowflake, Teradata, Athena, Hive
- 17+ different SQL dialects supported

**Strengths:**
- Maximum flexibility and customization
- Can create custom SQL subset grammars
- Industry standard parser generator
- Excellent documentation and tooling
- Perfect for domain-specific languages

**Usage:**
```csharp
// Custom grammar compilation generates lexer/parser classes
var inputStream = new AntlrInputStream(sql);
var lexer = new SqlLexer(inputStream);
var tokens = new CommonTokenStream(lexer);
var parser = new SqlParser(tokens);
var tree = parser.sql_stmt();
```

**Perfect for:** Custom SQL subsets, academic research, maximum control over parsing

## Comparison Matrix

| Feature | SqlParser-cs | ScriptDOM | ANTLR4 |
|---------|-------------|-----------|---------|
| **Ease of Use** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **Multi-Dialect** | ⭐⭐⭐⭐⭐ | ⭐⭐ (T-SQL only) | ⭐⭐⭐⭐⭐ |
| **Customization** | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Performance** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Documentation** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Community** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Maintenance** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |

## Recommendation for TxtDb

**Primary Recommendation: SqlParser-cs**

For TxtDb's SQL layer, I recommend **SqlParser-cs** because:

1. **Multi-dialect Support**: Allows us to support different SQL flavors
2. **Modern .NET Implementation**: Clean, idiomatic C# code
3. **Extensible Architecture**: Easy to customize for our subset needs
4. **Active Development**: Regular updates and community support
5. **Clean AST**: Easy to traverse and convert to TxtDb operations

**Implementation Strategy:**
1. Use SqlParser-cs as the base parser
2. Define our supported SQL subset through AST filtering
3. Create a SQL-to-TxtDb translator that walks the AST
4. Add custom validation for unsupported operations

**SQL Subset to Support Initially:**
```sql
-- Basic queries
SELECT column1, column2 FROM table WHERE condition;
SELECT * FROM table WHERE id = 123;
SELECT COUNT(*) FROM table WHERE active = true;

-- Simple joins  
SELECT a.*, b.name FROM tableA a JOIN tableB b ON a.id = b.a_id;

-- Basic DML
INSERT INTO table (col1, col2) VALUES ('value1', 'value2');
UPDATE table SET column = 'value' WHERE id = 123;
DELETE FROM table WHERE condition;

-- DDL
CREATE TABLE table (id INT, name VARCHAR(255));
DROP TABLE table;
```

## Next Steps

1. **Prototype Integration**: Create proof-of-concept with SqlParser-cs
2. **Define SQL Subset**: Specify exactly which SQL features to support
3. **Design Translation Layer**: Map SQL AST to TxtDb operations
4. **Performance Testing**: Ensure SQL layer doesn't impact TxtDb performance
5. **Error Handling**: Provide meaningful error messages for unsupported features

## Alternative Approaches Considered

- **JSqlParser (Java)**: Excellent but requires JVM integration
- **Custom Parser**: Too much development overhead
- **Linq Expression Trees**: Limited SQL compatibility
- **Entity Framework Query Translation**: Too heavyweight for our needs

---

**Research Date**: January 2025  
**Status**: Ready for implementation phase