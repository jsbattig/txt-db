using System.Reflection;
using TxtDb.Sql.Interfaces;
using TxtDb.Sql.Models;

namespace TxtDb.Sql.Tests.Architecture;

/// <summary>
/// Test suite to verify interface compliance according to architectural requirements.
/// These tests enforce that interfaces conform to the exact specification.
/// 
/// CRITICAL: These tests are failing by design until interface is fixed.
/// They define the EXACT interface contract required by the architecture plan.
/// </summary>
public class InterfaceComplianceTests
{
    /// <summary>
    /// Test that ISqlResult interface DOES NOT contain HasError property.
    /// Per architecture requirements, error handling should be done via exceptions, not result flags.
    /// </summary>
    [Fact]
    public void ISqlResult_ShouldNotHave_HasErrorProperty()
    {
        // Act
        var interfaceType = typeof(ISqlResult);
        var hasErrorProperty = interfaceType.GetProperty("HasError");
        
        // Assert
        Assert.Null(hasErrorProperty); // Property should NOT exist
    }
    
    /// <summary>
    /// Test that ISqlResult interface DOES NOT contain ErrorMessage property.
    /// Per architecture requirements, error handling should be done via exceptions, not result flags.
    /// </summary>
    [Fact]
    public void ISqlResult_ShouldNotHave_ErrorMessageProperty()
    {
        // Act
        var interfaceType = typeof(ISqlResult);
        var errorMessageProperty = interfaceType.GetProperty("ErrorMessage");
        
        // Assert
        Assert.Null(errorMessageProperty); // Property should NOT exist
    }
    
    /// <summary>
    /// Test that ISqlResult interface contains exactly the expected properties.
    /// This ensures no extra unauthorized properties exist.
    /// </summary>
    [Fact]
    public void ISqlResult_ShouldHave_ExactlyRequiredProperties()
    {
        // Act
        var interfaceType = typeof(ISqlResult);
        var properties = interfaceType.GetProperties();
        var propertyNames = properties.Select(p => p.Name).OrderBy(n => n).ToArray();
        
        // Assert - Only these 4 properties should exist
        var expectedProperties = new[] { "AffectedRows", "Columns", "Rows", "StatementType" };
        Array.Sort(expectedProperties);
        
        Assert.Equal(expectedProperties, propertyNames);
        Assert.Equal(4, properties.Length);
    }
    
    /// <summary>
    /// Test that ISqlResult properties have exactly the right types.
    /// </summary>
    [Fact]
    public void ISqlResult_Properties_ShouldHaveCorrectTypes()
    {
        // Act
        var interfaceType = typeof(ISqlResult);
        
        var statementTypeProperty = interfaceType.GetProperty("StatementType");
        var columnsProperty = interfaceType.GetProperty("Columns");
        var rowsProperty = interfaceType.GetProperty("Rows");
        var affectedRowsProperty = interfaceType.GetProperty("AffectedRows");
        
        // Assert
        Assert.NotNull(statementTypeProperty);
        Assert.Equal(typeof(SqlStatementType), statementTypeProperty.PropertyType);
        
        Assert.NotNull(columnsProperty);
        Assert.Equal(typeof(IReadOnlyList<SqlColumnInfo>), columnsProperty.PropertyType);
        
        Assert.NotNull(rowsProperty);
        Assert.Equal(typeof(IReadOnlyList<object[]>), rowsProperty.PropertyType);
        
        Assert.NotNull(affectedRowsProperty);
        Assert.Equal(typeof(int), affectedRowsProperty.PropertyType);
    }
}