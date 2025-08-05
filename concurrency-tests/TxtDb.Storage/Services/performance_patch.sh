#\!/bin/bash

# 1. Replace PersistMetadata calls with MarkMetadataDirty in critical methods
sed -i 's/PersistMetadata();/MarkMetadataDirty();/g' StorageSubsystem.cs

# 2. Add performance fields after line 12
sed -i '12a\    private Timer? _metadataPersistTimer;\n    private volatile bool _metadataDirty = false;\n    private readonly object _dirtyFlagLock = new object();\n    private readonly ConcurrentDictionary<string, int> _namespaceOperationsCache = new();' StorageSubsystem.cs

# 3. Update constructor
sed -i '/public StorageSubsystem()/,/^    }$/{
    s/Constructor - cleanup timer will be initialized in StartVersionCleanup/Initialize metadata persistence timer for batch updates/
    /Constructor/a\        _metadataPersistTimer = new Timer(PersistMetadataIfDirty, null,\n            TimeSpan.FromMilliseconds(100),\n            TimeSpan.FromMilliseconds(100));
}' StorageSubsystem.cs

# 4. Fix StartVersionCleanup to handle intervalMinutes=0
sed -i '/public void StartVersionCleanup/,/^    }$/{
    /TimeSpan.FromMinutes(1)/i\        // Handle immediate cleanup request\n        if (intervalMinutes == 0)\n        {\n            // Run cleanup once immediately\n            Task.Run(() => RunVersionCleanup(null));\n            return;\n        }\n        \n        // Stop existing timer if any\n        _cleanupTimer?.Dispose();\n        
}' StorageSubsystem.cs

# 5. Remove FlushToDisk calls for performance (except critical ones)
sed -i '/File.WriteAllText(versionFile, serializedContent);/{n;/FlushToDisk/d;}' StorageSubsystem.cs
sed -i '/File.WriteAllText(markerFile, markerContent);/{n;/FlushToDisk/d;}' StorageSubsystem.cs

# 6. Update namespace operations to use cache
sed -i '/private void IncrementNamespaceOperations/,/^    }$/{
    /lock (_metadataLock)/,/PersistMetadata();/{
        s/lock (_metadataLock)/_namespaceOperationsCache.AddOrUpdate(@namespace, 1, (_, count) => count + 1);/
        /_metadata.NamespaceOperations\[@namespace\]/d
        /_metadata.NamespaceOperations.GetValueOrDefault/d
        /PersistMetadata();/d
        /}/d
    }
}' StorageSubsystem.cs

sed -i '/private void DecrementNamespaceOperations/,/^    }$/{
    /lock (_metadataLock)/,/PersistMetadata();/{
        s/lock (_metadataLock)/_namespaceOperationsCache.AddOrUpdate(@namespace, 0, (_, count) => Math.Max(0, count - 1));/
        /var currentCount/d
        /if (currentCount > 0)/d
        /_metadata.NamespaceOperations\[@namespace\]/d
        /else/d
        /_metadata.NamespaceOperations.Remove(@namespace);/d
        /PersistMetadata();/d
        /}/d
    }
}' StorageSubsystem.cs
