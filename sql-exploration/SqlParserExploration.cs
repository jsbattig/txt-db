using SqlParser;
using System;

namespace TxtDb.SqlExploration
{
    /// <summary>
    /// Quick exploration of SqlParserCS to understand the API and AST structure
    /// </summary>
    public class SqlParserExploration
    {
        public static void ExploreBasicQueries()
        {
            // Test basic SELECT query
            var selectSql = "SELECT id, name, age FROM users WHERE age > 25 ORDER BY name";
            Console.WriteLine("Parsing SELECT query:");
            Console.WriteLine(selectSql);
            
            try
            {
                var ast = new SqlQueryParser().Parse(selectSql);
                Console.WriteLine($"Parsed AST: {ast}");
                Console.WriteLine($"AST Type: {ast.GetType().Name}");
                Console.WriteLine("---");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error: {ex.Message}");
            }

            // Test INSERT query
            var insertSql = "INSERT INTO users (name, age, email) VALUES ('John Doe', 30, 'john@example.com')";
            Console.WriteLine("Parsing INSERT query:");
            Console.WriteLine(insertSql);
            
            try
            {
                var ast = new SqlQueryParser().Parse(insertSql);
                Console.WriteLine($"Parsed AST: {ast}");
                Console.WriteLine($"AST Type: {ast.GetType().Name}");
                Console.WriteLine("---");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error: {ex.Message}");
            }

            // Test UPDATE query
            var updateSql = "UPDATE users SET age = 31 WHERE id = 1";
            Console.WriteLine("Parsing UPDATE query:");
            Console.WriteLine(updateSql);
            
            try
            {
                var ast = new SqlQueryParser().Parse(updateSql);
                Console.WriteLine($"Parsed AST: {ast}");
                Console.WriteLine($"AST Type: {ast.GetType().Name}");
                Console.WriteLine("---");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error: {ex.Message}");
            }

            // Test DELETE query
            var deleteSql = "DELETE FROM users WHERE age < 18";
            Console.WriteLine("Parsing DELETE query:");
            Console.WriteLine(deleteSql);
            
            try
            {
                var ast = new SqlQueryParser().Parse(deleteSql);
                Console.WriteLine($"Parsed AST: {ast}");
                Console.WriteLine($"AST Type: {ast.GetType().Name}");
                Console.WriteLine("---");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error: {ex.Message}");
            }
        }

        public static void ExploreDDLQueries()
        {
            // Test CREATE TABLE
            var createTableSql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), age INT)";
            Console.WriteLine("Parsing CREATE TABLE query:");
            Console.WriteLine(createTableSql);
            
            try
            {
                var ast = new SqlQueryParser().Parse(createTableSql);
                Console.WriteLine($"Parsed AST: {ast}");
                Console.WriteLine($"AST Type: {ast.GetType().Name}");
                Console.WriteLine("---");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parse error: {ex.Message}");
            }
        }
    }
}