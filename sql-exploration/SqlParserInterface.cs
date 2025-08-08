using SqlParser;
using SqlParser.Ast;
using System;
using System.Linq;

namespace TxtDb.SqlExploration
{
    /// <summary>
    /// Detailed exploration of SqlParserCS interfaces and AST structure
    /// This helps us understand how to build the SQL-to-TxtDb translator
    /// </summary>
    public class SqlParserInterfaceExploration
    {
        public static void ExploreSelectStatement()
        {
            var sql = "SELECT id, name, age FROM users WHERE age > 25 AND status = 'active' ORDER BY name LIMIT 10";
            Console.WriteLine("=== SELECT Statement Analysis ===");
            Console.WriteLine($"SQL: {sql}");
            
            var statements = new SqlQueryParser().Parse(sql);
            
            if (statements.Count > 0 && statements[0] is Statement.Select selectStmt)
            {
                Console.WriteLine("Parsed as SELECT statement");
                // This shows us the AST structure we need to work with
                var query = selectStmt.Query;
                
                // Explore query structure for our translator
                Console.WriteLine("Query structure exploration...");
                // We'll need to extract:
                // - SELECT columns
                // - FROM tables  
                // - WHERE conditions
                // - ORDER BY clauses
                // - LIMIT values
            }
        }

        public static void ExploreInsertStatement()
        {
            var sql = "INSERT INTO users (name, age, email) VALUES ('John', 30, 'john@example.com')";
            Console.WriteLine("=== INSERT Statement Analysis ===");
            Console.WriteLine($"SQL: {sql}");
            
            var statements = new SqlQueryParser().Parse(sql);
            
            if (statements.Count > 0 && statements[0] is Statement.Insert insertStmt)
            {
                Console.WriteLine("Parsed as INSERT statement");
                // This shows us what we need for INSERT translation
                Console.WriteLine("Insert structure exploration...");
                // We'll need to extract:
                // - Table name
                // - Column names
                // - Values to insert
            }
        }

        public static void ExploreUpdateStatement()
        {
            var sql = "UPDATE users SET age = 31, email = 'newemail@example.com' WHERE id = 1";
            Console.WriteLine("=== UPDATE Statement Analysis ===");
            Console.WriteLine($"SQL: {sql}");
            
            var statements = new SqlQueryParser().Parse(sql);
            
            if (statements.Count > 0 && statements[0] is Statement.Update updateStmt)
            {
                Console.WriteLine("Parsed as UPDATE statement");
                // This shows us what we need for UPDATE translation
                Console.WriteLine("Update structure exploration...");
                // We'll need to extract:
                // - Table name
                // - SET clauses (column = value pairs)
                // - WHERE conditions
            }
        }

        public static void ExploreDeleteStatement()
        {
            var sql = "DELETE FROM users WHERE age < 18 OR status = 'inactive'";
            Console.WriteLine("=== DELETE Statement Analysis ===");
            Console.WriteLine($"SQL: {sql}");
            
            var statements = new SqlQueryParser().Parse(sql);
            
            if (statements.Count > 0 && statements[0] is Statement.Delete deleteStmt)
            {
                Console.WriteLine("Parsed as DELETE statement");
                // This shows us what we need for DELETE translation
                Console.WriteLine("Delete structure exploration...");
                // We'll need to extract:
                // - Table name
                // - WHERE conditions
            }
        }

        public static void ExploreCreateTableStatement()
        {
            var sql = @"CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                age INT,
                email VARCHAR(255) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";
            Console.WriteLine("=== CREATE TABLE Statement Analysis ===");
            Console.WriteLine($"SQL: {sql}");
            
            try
            {
                var statements = new SqlQueryParser().Parse(sql);
                
                if (statements.Count > 0 && statements[0] is Statement.CreateTable createStmt)
                {
                    Console.WriteLine("Parsed as CREATE TABLE statement");
                    Console.WriteLine("Create table structure exploration...");
                    // We'll need to extract:
                    // - Table name
                    // - Column definitions (name, type, constraints)
                    // - Primary key information
                    // - Unique constraints
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error (expected for complex DDL): {ex.Message}");
                Console.WriteLine("Note: We may need to simplify DDL support initially");
            }
        }

        public static void ExploreDialects()
        {
            var sql = "SELECT TOP 10 * FROM users";
            Console.WriteLine("=== Dialect Support Exploration ===");
            Console.WriteLine($"SQL: {sql}");
            
            // Test different dialects
            var genericParser = new SqlQueryParser();
            var mssqlParser = new SqlQueryParser(SqlDialect.MSSqlServer);
            
            try
            {
                Console.WriteLine("Generic parser:");
                var genericResult = genericParser.Parse(sql);
                Console.WriteLine($"Parsed {genericResult.Count} statements");
                
                Console.WriteLine("MS SQL parser:");
                var mssqlResult = mssqlParser.Parse(sql);
                Console.WriteLine($"Parsed {mssqlResult.Count} statements");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Dialect test error: {ex.Message}");
            }
        }
    }
}