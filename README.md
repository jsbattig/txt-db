# TxtDb.Storage

A high-performance, ACID-compliant storage engine that persists objects as human-readable text files with Multi-Version Concurrency Control (MVCC) transaction support.

## Features

### Core Storage Engine
- **Generic Object Persistence**: Store any serializable object without schema constraints
- **MVCC Transaction Management**: Full ACID compliance with snapshot isolation
- **Multiple Serialization Formats**: JSON, XML, and YAML support
- **Human-Readable Storage**: All data stored as readable text files
- **Namespace Organization**: Hierarchical organization using dot notation
- **Immediate Durability**: All operations persisted to disk immediately

### Async/Await Support
- **High-Performance Async Operations**: Full async/await pattern implementation
- **Batch Flushing**: Configurable batch operations for improved throughput
- **Cancellation Support**: Proper CancellationToken handling throughout
- **Resource Management**: Efficient memory and file handle management

### Advanced Concurrency
- **Snapshot Isolation**: Read-committed isolation level with consistent snapshots
- **Cross-Process Coordination**: Multi-process MVCC support with distributed coordination
- **Lock-Free Operations**: High concurrency without repository-wide locking
- **Transaction Lease Management**: Automatic cleanup of abandoned transactions

### Performance & Monitoring
- **Storage Metrics**: Built-in performance monitoring and metrics collection
- **Configurable Page Sizes**: Tunable storage organization (default 8KB pages)
- **Version Cleanup**: Automatic cleanup of old versions
- **Performance Validation**: Built-in performance regression testing

## Requirements

- .NET 8.0 or later
- Dependencies: Newtonsoft.Json 13.0.3, YamlDotNet 13.7.1

## Quick Start

### Basic Usage

```csharp
using TxtDb.Storage.Services;
using TxtDb.Storage.Models;

// Initialize storage
var storage = new StorageSubsystem();
storage.Initialize("/path/to/storage", new StorageConfig 
{
    Format = SerializationFormat.Json,
    MaxPageSizeKB = 8
});

// Start transaction
var txId = storage.BeginTransaction();

try 
{
    // Create namespace and insert data
    storage.CreateNamespace(txId, "users");
    var pageId = storage.InsertObject(txId, "users", new { Name = "John", Age = 30 });
    
    // Read data back
    var data = storage.ReadPage(txId, "users", pageId);
    
    // Commit transaction
    storage.CommitTransaction(txId);
}
catch 
{
    storage.RollbackTransaction(txId);
    throw;
}
```

### Async Usage

```csharp
using TxtDb.Storage.Services.Async;

var asyncStorage = new AsyncStorageSubsystem();
await asyncStorage.InitializeAsync("/path/to/storage");

var txId = await asyncStorage.BeginTransactionAsync();
try 
{
    await asyncStorage.CreateNamespaceAsync(txId, "products");
    var pageId = await asyncStorage.InsertObjectAsync(txId, "products", 
        new { Name = "Widget", Price = 9.99m });
    
    await asyncStorage.CommitTransactionAsync(txId);
}
catch 
{
    await asyncStorage.RollbackTransactionAsync(txId);
    throw;
}
```

## Configuration

```csharp
var config = new StorageConfig
{
    Format = SerializationFormat.Json,           // JSON, XML, or YAML
    MaxPageSizeKB = 8,                          // Page size limit
    VersionCleanupIntervalMinutes = 15,         // Cleanup frequency
    ForceOneObjectPerPage = false,              // Object isolation level
    EnableBatchFlushing = true,                 // Performance optimization
    BatchFlushConfig = new BatchFlushConfig     // Batch configuration
    {
        MaxBatchSize = 50,
        FlushIntervalMs = 100
    }
};
```

## Storage Organization

TxtDb organizes data using a file-per-page approach:

```
/storage-root/
├── config.json                    # Storage configuration
├── metadata.json                  # Version metadata
└── users/                         # Namespace folder
    ├── page_1_v3.json             # Data page with version
    ├── page_2_v1.json
    └── page_3_v2.json
```

## Transaction Model

- **Isolation Level**: Read Committed with snapshot isolation
- **Durability**: All operations immediately persisted to disk
- **Consistency**: MVCC ensures consistent reads across transactions
- **Atomicity**: Transaction-level rollback support

## Testing

The project includes comprehensive test coverage:

- **Unit Tests**: Core functionality validation
- **Integration Tests**: Cross-component interaction testing  
- **Concurrency Tests**: Multi-threaded operation validation
- **Performance Tests**: Throughput and latency benchmarking
- **Stress Tests**: High-load scenario validation
- **Cross-Process Tests**: Multi-process coordination testing

Run tests:
```bash
dotnet test TxtDb.Storage.Tests/
```

## Architecture

### Core Components

- **StorageSubsystem**: Main storage engine implementation
- **AsyncStorageSubsystem**: High-performance async implementation
- **Format Adapters**: JSON, XML, YAML serialization handlers
- **MVCC Components**: Transaction and version management
- **Lock Management**: Concurrency control mechanisms

### Key Interfaces

- `IStorageSubsystem`: Synchronous storage operations
- `IAsyncStorageSubsystem`: Asynchronous storage operations  
- `IFormatAdapter`: Pluggable serialization interface

## Performance Characteristics

- **Throughput**: 200+ operations/second (async implementation)
- **Concurrency**: Lock-free read operations
- **Scalability**: Linear scaling with concurrent transactions
- **Storage Overhead**: Minimal metadata per version

## License

MIT License - see LICENSE file for details.

## Contributing

This project follows standard .NET development practices with comprehensive test coverage requirements. All changes must include appropriate unit and integration tests.