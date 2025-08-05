using System.Collections.Concurrent;
using TxtDb.Storage.Interfaces;
using TxtDb.Storage.Models;
using TxtDb.Storage.Services;
using Xunit;

namespace TxtDb.Storage.Tests.Core;

/// <summary>
/// CRITICAL: COMPREHENSIVE RENAME NAMESPACE TESTING - NO MOCKING
/// Tests all aspects of namespace renaming with concurrent operations, metadata updates, and error scenarios
/// ALL tests use real file I/O, real concurrent transactions, real serialization
/// </summary>
public class RenameNamespaceTests : IDisposable
{
    private readonly string _testRootPath;
    private readonly IStorageSubsystem _storage;

    public RenameNamespaceTests()
    {
        _testRootPath = Path.Combine(Path.GetTempPath(), $"txtdb_rename_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testRootPath);
        
        _storage = new StorageSubsystem();
        _storage.Initialize(_testRootPath, new StorageConfig { Format = SerializationFormat.Json });
    }

    [Fact]
    public void RenameNamespace_WithActivePages_ShouldUpdateAllMetadataReferences()
    {
        // Arrange - Create namespace with multiple pages
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "old.namespace.test";
        var newNamespace = "new.namespace.test";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        
        // Create multiple pages with data
        var page1Id = _storage.InsertObject(setupTxn, oldNamespace, new { Id = 1, Data = "Page1" });
        var page2Id = _storage.InsertObject(setupTxn, oldNamespace, new { Id = 2, Data = "Page2" });
        var page3Id = _storage.InsertObject(setupTxn, oldNamespace, new { Id = 3, Data = "Page3" });
        
        _storage.CommitTransaction(setupTxn);

        // Act - Rename namespace
        var renameTxn = _storage.BeginTransaction();
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);

        // Assert - Verify all data accessible under new namespace
        var verifyTxn = _storage.BeginTransaction();
        var page1Data = _storage.ReadPage(verifyTxn, newNamespace, page1Id);
        var page2Data = _storage.ReadPage(verifyTxn, newNamespace, page2Id);
        var page3Data = _storage.ReadPage(verifyTxn, newNamespace, page3Id);
        
        Assert.NotEmpty(page1Data);
        Assert.NotEmpty(page2Data);
        Assert.NotEmpty(page3Data);
        
        // Verify old namespace is gone
        var oldPages = _storage.GetMatchingObjects(verifyTxn, oldNamespace, "*");
        Assert.Empty(oldPages);
        
        _storage.CommitTransaction(verifyTxn);
        
        // Verify directory structure changed
        var oldPath = Path.Combine(_testRootPath, "old", "namespace", "test");
        var newPath = Path.Combine(_testRootPath, "new", "namespace", "test");
        
        Assert.False(Directory.Exists(oldPath));
        Assert.True(Directory.Exists(newPath));
    }

    [Fact]
    public void RenameNamespace_NonExistentNamespace_ShouldThrowArgumentException()
    {
        // Arrange
        var txnId = _storage.BeginTransaction();
        var nonExistentNamespace = "does.not.exist";
        var targetNamespace = "target.namespace";

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(txnId, nonExistentNamespace, targetNamespace));
        
        Assert.Contains("does not exist", exception.Message);
        
        _storage.RollbackTransaction(txnId);
    }

    [Fact]
    public void RenameNamespace_TargetAlreadyExists_ShouldThrowArgumentException()
    {
        // Arrange - Create both source and target namespaces
        var setupTxn = _storage.BeginTransaction();
        var sourceNamespace = "source.namespace";
        var targetNamespace = "target.namespace";
        
        _storage.CreateNamespace(setupTxn, sourceNamespace);
        _storage.CreateNamespace(setupTxn, targetNamespace);
        _storage.CommitTransaction(setupTxn);

        var renameTxn = _storage.BeginTransaction();

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(renameTxn, sourceNamespace, targetNamespace));
        
        Assert.Contains("already exists", exception.Message);
        
        _storage.RollbackTransaction(renameTxn);
    }

    [Fact]
    public void RenameNamespace_WithDeepNestedStructure_ShouldCreateCorrectPaths()
    {
        // Arrange - Create deeply nested namespace
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "level1.level2.level3.level4.deep";
        var newNamespace = "newlevel1.newlevel2.newlevel3.newlevel4.newdeep";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        _storage.InsertObject(setupTxn, oldNamespace, new { Data = "Deep nested data" });
        _storage.CommitTransaction(setupTxn);

        // Act
        var renameTxn = _storage.BeginTransaction();
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);

        // Assert - Verify deep directory structure created
        var expectedPath = Path.Combine(_testRootPath, "newlevel1", "newlevel2", "newlevel3", "newlevel4", "newdeep");
        Assert.True(Directory.Exists(expectedPath));
        
        // Verify data accessible
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, newNamespace, "*");
        Assert.NotEmpty(allData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void RenameNamespace_WithActiveTransactionReading_ShouldCompleteSuccessfully()
    {
        // Arrange - Create namespace with data
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "active.read.test";
        var newNamespace = "renamed.read.test";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        var pageId = _storage.InsertObject(setupTxn, oldNamespace, new { Data = "Test data" });
        _storage.CommitTransaction(setupTxn);

        // Start a transaction that reads from the namespace
        var readTxn = _storage.BeginTransaction();
        var initialData = _storage.ReadPage(readTxn, oldNamespace, pageId);
        Assert.NotEmpty(initialData);

        // Act - Rename in different transaction (should wait for operations to complete)
        var renameTxn = _storage.BeginTransaction();
        
        // This should succeed since read operations don't block structural changes
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);

        // Assert - Original transaction should still work with old namespace reference
        // until it commits (snapshot isolation)
        _storage.CommitTransaction(readTxn);
        
        // New transaction should see renamed namespace
        var verifyTxn = _storage.BeginTransaction();
        var renamedData = _storage.ReadPage(verifyTxn, newNamespace, pageId);
        Assert.NotEmpty(renamedData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void RenameNamespace_WithMultipleVersions_ShouldPreserveAllVersions()
    {
        // Arrange - Create namespace with multiple versions
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "versioned.namespace";
        var newNamespace = "renamed.versioned.namespace";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        var pageId = _storage.InsertObject(setupTxn, oldNamespace, new { Version = 1, Data = "Initial" });
        _storage.CommitTransaction(setupTxn);

        // Create version 2
        var updateTxn1 = _storage.BeginTransaction();
        var data1 = _storage.ReadPage(updateTxn1, oldNamespace, pageId);
        _storage.UpdatePage(updateTxn1, oldNamespace, pageId, new object[] { new { Version = 2, Data = "Updated" } });
        _storage.CommitTransaction(updateTxn1);

        // Create version 3
        var updateTxn2 = _storage.BeginTransaction();
        var data2 = _storage.ReadPage(updateTxn2, oldNamespace, pageId);
        _storage.UpdatePage(updateTxn2, oldNamespace, pageId, new object[] { new { Version = 3, Data = "Final" } });
        _storage.CommitTransaction(updateTxn2);

        // Act - Rename namespace
        var renameTxn = _storage.BeginTransaction();
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);

        // Assert - Verify all version files were moved
        var newNamespacePath = Path.Combine(_testRootPath, "renamed", "versioned", "namespace");
        var versionFiles = Directory.GetFiles(newNamespacePath, "*.v*");
        
        // Should have at least 3 version files (v1, v2, v3)
        Assert.True(versionFiles.Length >= 3, $"Expected at least 3 version files, found {versionFiles.Length}");
        
        // Verify data accessible
        var verifyTxn = _storage.BeginTransaction();
        var finalData = _storage.ReadPage(verifyTxn, newNamespace, pageId);
        Assert.NotEmpty(finalData);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void RenameNamespace_ConcurrentRenames_ShouldHandleCorrectly()
    {
        // Arrange - Create multiple namespaces
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "concurrent.test1");
        _storage.CreateNamespace(setupTxn, "concurrent.test2");
        _storage.InsertObject(setupTxn, "concurrent.test1", new { Data = "Test1" });
        _storage.InsertObject(setupTxn, "concurrent.test2", new { Data = "Test2" });
        _storage.CommitTransaction(setupTxn);

        var exceptions = new ConcurrentBag<Exception>();
        var completedRenames = new ConcurrentBag<string>();

        // Act - Attempt concurrent renames
        var tasks = new[]
        {
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    _storage.RenameNamespace(txn, "concurrent.test1", "renamed.test1");
                    _storage.CommitTransaction(txn);
                    completedRenames.Add("test1");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }),
            Task.Run(() =>
            {
                try
                {
                    var txn = _storage.BeginTransaction();
                    _storage.RenameNamespace(txn, "concurrent.test2", "renamed.test2");
                    _storage.CommitTransaction(txn);
                    completedRenames.Add("test2");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
        };

        Task.WaitAll(tasks);

        // Assert - Both renames should succeed since they operate on different namespaces
        Assert.Equal(2, completedRenames.Count);
        Assert.Empty(exceptions);
        
        // Verify both renames worked
        var verifyTxn = _storage.BeginTransaction();
        var data1 = _storage.GetMatchingObjects(verifyTxn, "renamed.test1", "*");
        var data2 = _storage.GetMatchingObjects(verifyTxn, "renamed.test2", "*");
        Assert.NotEmpty(data1);
        Assert.NotEmpty(data2);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void RenameNamespace_InvalidCharacters_ShouldThrowArgumentException()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "valid.namespace");
        _storage.CommitTransaction(setupTxn);

        var invalidNames = new[]
        {
            "namespace/with/slashes",
            "namespace\\with\\backslashes", 
            "namespace:with:colons",
            "namespace*with*wildcards",
            "namespace?with?questions",
            "namespace<with>brackets",
            "namespace|with|pipes"
        };

        // Act & Assert
        foreach (var invalidName in invalidNames)
        {
            var txn = _storage.BeginTransaction();
            
            var exception = Assert.Throws<ArgumentException>(() =>
                _storage.RenameNamespace(txn, "valid.namespace", invalidName));
            
            Assert.Contains("invalid characters", exception.Message);
            _storage.RollbackTransaction(txn);
        }
    }

    [Fact]
    public void RenameNamespace_EmptyOrNullNames_ShouldThrowArgumentException()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "valid.namespace");
        _storage.CommitTransaction(setupTxn);

        var invalidNames = new string[] { null!, "", "   " };

        // Act & Assert
        foreach (var invalidName in invalidNames)
        {
            var txn = _storage.BeginTransaction();
            
            Assert.Throws<ArgumentException>(() =>
                _storage.RenameNamespace(txn, "valid.namespace", invalidName));
            
            Assert.Throws<ArgumentException>(() =>
                _storage.RenameNamespace(txn, invalidName, "target.namespace"));
            
            _storage.RollbackTransaction(txn);
        }
    }

    [Fact]
    public void RenameNamespace_WithLargeAmountOfData_ShouldCompleteSuccessfully()
    {
        // Arrange - Create namespace with many pages
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "large.data.test";
        var newNamespace = "renamed.large.data.test";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        
        // Insert 50 objects to create multiple pages
        var pageIds = new List<string>();
        for (int i = 1; i <= 50; i++)
        {
            var largeData = new string('X', 500); // 500 characters each
            var pageId = _storage.InsertObject(setupTxn, oldNamespace, 
                new { Id = i, Data = largeData, Timestamp = DateTime.UtcNow });
            pageIds.Add(pageId);
        }
        _storage.CommitTransaction(setupTxn);

        // Act - Rename namespace with large dataset
        var start = DateTime.UtcNow;
        var renameTxn = _storage.BeginTransaction();
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);
        var duration = DateTime.UtcNow - start;

        // Assert - All data should be accessible and rename should be reasonably fast
        Assert.True(duration.TotalSeconds < 30, $"Rename took {duration.TotalSeconds} seconds, should be < 30");
        
        var verifyTxn = _storage.BeginTransaction();
        var allData = _storage.GetMatchingObjects(verifyTxn, newNamespace, "*");
        var totalObjects = allData.Values.Sum(pages => pages.Length);
        Assert.Equal(50, totalObjects);
        _storage.CommitTransaction(verifyTxn);
    }

    [Fact]
    public void RenameNamespace_PartialFailure_ShouldRollbackCorrectly()
    {
        // Arrange - Create namespace
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "rollback.test";
        var newNamespace = "renamed.rollback.test";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        _storage.InsertObject(setupTxn, oldNamespace, new { Data = "Test data" });
        _storage.CommitTransaction(setupTxn);

        // Create the target directory manually to force a failure
        var targetPath = Path.Combine(_testRootPath, "renamed", "rollback", "test");
        Directory.CreateDirectory(targetPath);
        File.WriteAllText(Path.Combine(targetPath, "blocker.txt"), "blocking file");

        // Act & Assert
        var renameTxn = _storage.BeginTransaction();
        
        Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace));
        
        _storage.RollbackTransaction(renameTxn);

        // Verify original namespace still exists and is functional
        var verifyTxn = _storage.BeginTransaction();
        var originalData = _storage.GetMatchingObjects(verifyTxn, oldNamespace, "*");
        Assert.NotEmpty(originalData);
        _storage.CommitTransaction(verifyTxn);
        
        // Cleanup
        Directory.Delete(targetPath, true);
    }

    [Fact]
    public void RenameNamespace_InvalidTransactionId_ShouldThrowArgumentException()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        _storage.CreateNamespace(setupTxn, "valid.namespace");
        _storage.CommitTransaction(setupTxn);

        // Act & Assert - Test with invalid transaction IDs
        Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(0, "valid.namespace", "new.namespace"));
        
        Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(-1, "valid.namespace", "new.namespace"));
        
        Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(99999, "valid.namespace", "new.namespace"));
    }

    [Fact]
    public void RenameNamespace_SameSourceAndTarget_ShouldThrowArgumentException()
    {
        // Arrange
        var setupTxn = _storage.BeginTransaction();
        var namespaceName = "same.namespace.test";
        _storage.CreateNamespace(setupTxn, namespaceName);
        _storage.CommitTransaction(setupTxn);

        // Act & Assert
        var renameTxn = _storage.BeginTransaction();
        
        var exception = Assert.Throws<ArgumentException>(() =>
            _storage.RenameNamespace(renameTxn, namespaceName, namespaceName));
        
        // Should indicate that source and target are the same
        _storage.RollbackTransaction(renameTxn);
    }

    [Fact]
    public void RenameNamespace_PreservesNamespaceMarkerFile_WithUpdatedContent()
    {
        // Arrange - Create namespace with marker file
        var setupTxn = _storage.BeginTransaction();
        var oldNamespace = "marker.test";
        var newNamespace = "renamed.marker.test";
        
        _storage.CreateNamespace(setupTxn, oldNamespace);
        _storage.CommitTransaction(setupTxn);

        // Act - Rename namespace
        var renameTxn = _storage.BeginTransaction();
        _storage.RenameNamespace(renameTxn, oldNamespace, newNamespace);
        _storage.CommitTransaction(renameTxn);

        // Assert - Verify marker file exists in new location
        var newNamespacePath = Path.Combine(_testRootPath, "renamed", "marker", "test");
        var markerFile = Path.Combine(newNamespacePath, ".namespace.json");
        
        Assert.True(File.Exists(markerFile), "Namespace marker file should exist after rename");
        
        // Verify marker file content is valid
        var markerContent = File.ReadAllText(markerFile);
        Assert.Contains("marker.test", markerContent); // Should contain original namespace info
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testRootPath))
            {
                Directory.Delete(_testRootPath, recursive: true);
            }
        }
        catch
        {
            // Cleanup failed - not critical for tests
        }
    }
}