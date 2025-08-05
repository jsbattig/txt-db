using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace TxtDb.Storage.Tests.MVCC
{
    /// <summary>
    /// Test data models for multi-process E2E testing
    /// 
    /// These models are designed to validate:
    /// - Multi-page object consistency
    /// - Referential integrity across processes
    /// - Concurrency control and conflict detection
    /// - Sequential numbering and counting validation
    /// </summary>

    /// <summary>
    /// Interface for consistent object naming across test entities
    /// </summary>
    public interface ITestEntity
    {
        string GetObjectId();
    }

    /// <summary>
    /// Multi-page object with relationships - Master record
    /// </summary>
    public class Order : ITestEntity
    {
        [Key]
        public long OrderId { get; set; }
        
        public long CustomerId { get; set; }
        
        public decimal TotalAmount { get; set; }
        
        public int ItemCount { get; set; }
        
        public DateTime Created { get; set; }
        
        public DateTime LastModified { get; set; }
        
        public string Status { get; set; } = "Created";
        
        /// <summary>
        /// Version field for optimistic concurrency control
        /// </summary>
        public long Version { get; set; }
        
        /// <summary>
        /// Process ID that last modified this record
        /// </summary>
        public string LastModifiedBy { get; set; } = "";

        /// <summary>
        /// Consistent object ID for storage and search operations
        /// </summary>
        public string GetObjectId() => $"Order_{OrderId}";
    }

    /// <summary>
    /// Multi-page object with relationships - Detail record
    /// Must maintain referential integrity with Order
    /// </summary>
    public class OrderItem : ITestEntity
    {
        [Key]
        public long OrderItemId { get; set; }
        
        /// <summary>
        /// Must reference valid Order.OrderId
        /// </summary>
        public long OrderId { get; set; }
        
        public string Product { get; set; } = "";
        
        public decimal Amount { get; set; }
        
        /// <summary>
        /// Sequential numbering within order - must be consistent
        /// </summary>
        public int Sequence { get; set; }
        
        public int Quantity { get; set; }
        
        public DateTime Created { get; set; }
        
        /// <summary>
        /// Version field for optimistic concurrency control
        /// </summary>
        public long Version { get; set; }
        
        /// <summary>
        /// Process ID that created/modified this record
        /// </summary>
        public string ProcessId { get; set; } = "";

        /// <summary>
        /// Consistent object ID for storage and search operations
        /// </summary>
        public string GetObjectId() => $"OrderItem_{OrderItemId}";
    }

    /// <summary>
    /// Shared counter for concurrency testing
    /// Tests atomic increment operations across processes
    /// </summary>
    public class SharedCounter : ITestEntity
    {
        [Key]
        public string CounterId { get; set; } = "";
        
        /// <summary>
        /// Current counter value - must be consistent
        /// </summary>
        public long Value { get; set; }
        
        /// <summary>
        /// Number of updates by processes
        /// </summary>
        public int ProcessUpdates { get; set; }
        
        /// <summary>
        /// List of process IDs that have updated this counter
        /// </summary>
        public List<string> UpdatingProcesses { get; set; } = new();
        
        public DateTime LastUpdated { get; set; }
        
        /// <summary>
        /// Expected final value for validation
        /// </summary>
        public long ExpectedValue { get; set; }
        
        /// <summary>
        /// Version field for optimistic concurrency control
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        /// Consistent object ID for storage and search operations
        /// </summary>
        public string GetObjectId() => $"SharedCounter_{CounterId}";
    }

    /// <summary>
    /// Customer record for cross-reference validation
    /// </summary>
    public class Customer : ITestEntity
    {
        [Key]
        public long CustomerId { get; set; }
        
        public string Name { get; set; } = "";
        
        public string Email { get; set; } = "";
        
        public DateTime Created { get; set; }
        
        /// <summary>
        /// Number of orders for this customer
        /// </summary>
        public int OrderCount { get; set; }
        
        /// <summary>
        /// Total amount across all orders
        /// </summary>
        public decimal TotalSpent { get; set; }
        
        /// <summary>
        /// Version field for optimistic concurrency control
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        /// Consistent object ID for storage and search operations
        /// </summary>
        public string GetObjectId() => $"Customer_{CustomerId}";
    }

    /// <summary>
    /// Process test result data for multi-process validation
    /// </summary>
    public class ProcessTestData
    {
        public string ProcessId { get; set; } = "";
        
        public int OperationsCompleted { get; set; }
        
        public int ConflictsEncountered { get; set; }
        
        public int SuccessfulUpdates { get; set; }
        
        public List<string> ErrorMessages { get; set; } = new();
        
        public DateTime StartTime { get; set; }
        
        public DateTime EndTime { get; set; }
        
        public TimeSpan Duration => EndTime - StartTime;
        
        /// <summary>
        /// List of object IDs modified by this process
        /// </summary>
        public List<long> ModifiedObjectIds { get; set; } = new();
        
        /// <summary>
        /// Validation results for consistency checks
        /// </summary>
        public Dictionary<string, bool> ValidationResults { get; set; } = new();
    }

    /// <summary>
    /// Configuration for multi-process test scenarios
    /// </summary>
    public class MultiProcessTestConfig
    {
        public int ProcessCount { get; set; } = 2;
        
        public int OperationsPerProcess { get; set; } = 100;
        
        public TimeSpan ProcessTimeout { get; set; } = TimeSpan.FromMinutes(5);
        
        public TimeSpan BarrierTimeout { get; set; } = TimeSpan.FromMinutes(1);
        
        public string StorageDirectory { get; set; } = "";
        
        public bool EnableLogging { get; set; } = true;
        
        public bool ValidateConsistency { get; set; } = true;
        
        /// <summary>
        /// Whether to deliberately create conflict scenarios
        /// </summary>
        public bool CreateConflicts { get; set; } = false;
        
        /// <summary>
        /// Delay between operations to control timing
        /// </summary>
        public TimeSpan OperationDelay { get; set; } = TimeSpan.FromMilliseconds(10);
    }

    /// <summary>
    /// Consistency validation result
    /// </summary>
    public class ConsistencyValidationResult
    {
        public bool IsValid { get; set; }
        
        public List<string> ValidationErrors { get; set; } = new();
        
        public Dictionary<string, int> ObjectCounts { get; set; } = new();
        
        public Dictionary<string, long> CounterValues { get; set; } = new();
        
        public List<string> OrphanedReferences { get; set; } = new();
        
        public List<string> SequenceErrors { get; set; } = new();
        
        public DateTime ValidationTime { get; set; }
        
        public TimeSpan ValidationDuration { get; set; }
    }
}