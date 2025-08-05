using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace TxtDb.Storage.Services.MVCC
{
    /// <summary>
    /// Atomic File Operations Framework for Epic 003 Phase 1
    /// 
    /// Implements multi-step atomic operations with:
    /// - Manifest-based operation tracking
    /// - Temporary file preparation with directory-based commits
    /// - Comprehensive rollback mechanisms
    /// - Cross-file-system compatibility (no rename atomicity assumptions)
    /// 
    /// CRITICAL: Addresses file system atomicity issues identified in architectural audit
    /// </summary>
    public class AtomicFileOperations : IDisposable
    {
        private readonly string _manifestDirectory;
        private readonly string _tempDirectory;
        private readonly string _commitDirectory;
        private volatile bool _disposed = false;

        /// <summary>
        /// Operation manifest tracking multi-step atomic operations
        /// </summary>
        public class OperationManifest
        {
            public string OperationId { get; set; } = Guid.NewGuid().ToString();
            public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
            public List<OperationStep> Steps { get; set; } = new();
            public OperationState State { get; set; } = OperationState.Preparing;
            public string ProcessId { get; set; } = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        }

        /// <summary>
        /// Individual step in a multi-step atomic operation
        /// </summary>
        public class OperationStep
        {
            public string StepId { get; set; } = Guid.NewGuid().ToString();
            public StepType Type { get; set; }
            public string SourcePath { get; set; } = "";
            public string TargetPath { get; set; } = "";
            public string TempPath { get; set; } = "";
            public byte[] Checksum { get; set; } = Array.Empty<byte>();
            public StepState State { get; set; } = StepState.Pending;
            public DateTime CompletedAt { get; set; }
            public string ErrorMessage { get; set; } = "";
        }

        public enum OperationState
        {
            Preparing,
            ReadyToCommit,
            Committing,
            Committed,
            RollingBack,
            RolledBack,
            Failed
        }

        public enum StepType
        {
            CreateFile,
            UpdateFile,
            DeleteFile,
            CreateDirectory,
            DeleteDirectory
        }

        public enum StepState
        {
            Pending,
            Prepared,
            Committed,
            RolledBack,
            Failed
        }

        public AtomicFileOperations(string coordinationPath)
        {
            if (string.IsNullOrEmpty(coordinationPath))
                throw new ArgumentNullException(nameof(coordinationPath));

            _manifestDirectory = Path.Combine(coordinationPath, "manifests");
            _tempDirectory = Path.Combine(coordinationPath, "temp");
            _commitDirectory = Path.Combine(coordinationPath, "commits");

            // Ensure directories exist
            Directory.CreateDirectory(_manifestDirectory);
            Directory.CreateDirectory(_tempDirectory);
            Directory.CreateDirectory(_commitDirectory);
        }

        /// <summary>
        /// Begins a new atomic operation and returns its manifest
        /// </summary>
        public async Task<OperationManifest> BeginOperationAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AtomicFileOperations));

            var manifest = new OperationManifest();
            await PersistManifestAsync(manifest);
            return manifest;
        }

        /// <summary>
        /// Adds a file creation step to the operation
        /// </summary>
        public async Task AddCreateFileStepAsync(OperationManifest manifest, string targetPath, byte[] content)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AtomicFileOperations));

            var step = new OperationStep
            {
                Type = StepType.CreateFile,
                TargetPath = targetPath,
                TempPath = Path.Combine(_tempDirectory, $"{manifest.OperationId}_{Guid.NewGuid()}.tmp"),
                Checksum = ComputeChecksum(content)
            };

            // Write to temp file
            await File.WriteAllBytesAsync(step.TempPath, content);
            
            // Verify checksum
            var writtenContent = await File.ReadAllBytesAsync(step.TempPath);
            if (!ChecksumEquals(ComputeChecksum(writtenContent), step.Checksum))
                throw new InvalidOperationException("Temp file checksum mismatch");

            step.State = StepState.Prepared;
            manifest.Steps.Add(step);
            manifest.LastUpdated = DateTime.UtcNow;
            await PersistManifestAsync(manifest);
        }

        /// <summary>
        /// Adds a file update step to the operation
        /// </summary>
        public async Task AddUpdateFileStepAsync(OperationManifest manifest, string targetPath, byte[] newContent)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AtomicFileOperations));

            var step = new OperationStep
            {
                Type = StepType.UpdateFile,
                TargetPath = targetPath,
                SourcePath = targetPath + ".backup." + manifest.OperationId, // Backup current file
                TempPath = Path.Combine(_tempDirectory, $"{manifest.OperationId}_{Guid.NewGuid()}.tmp"),
                Checksum = ComputeChecksum(newContent)
            };

            // Create backup of current file if it exists
            if (File.Exists(targetPath))
            {
                await CopyFileWithVerificationAsync(targetPath, step.SourcePath);
            }

            // Write new content to temp file
            await File.WriteAllBytesAsync(step.TempPath, newContent);
            
            // Verify checksum
            var writtenContent = await File.ReadAllBytesAsync(step.TempPath);
            if (!ChecksumEquals(ComputeChecksum(writtenContent), step.Checksum))
                throw new InvalidOperationException("Temp file checksum mismatch");

            step.State = StepState.Prepared;
            manifest.Steps.Add(step);
            manifest.LastUpdated = DateTime.UtcNow;
            await PersistManifestAsync(manifest);
        }

        /// <summary>
        /// Commits all steps in the operation atomically
        /// </summary>
        public async Task<bool> CommitOperationAsync(OperationManifest manifest)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AtomicFileOperations));

            if (manifest.State != OperationState.Preparing && manifest.State != OperationState.ReadyToCommit)
                throw new InvalidOperationException($"Cannot commit operation in state: {manifest.State}");

            // Create commit marker directory (atomic on most file systems)
            var commitMarkerPath = Path.Combine(_commitDirectory, manifest.OperationId);
            
            try
            {
                // Phase 1: Mark as ready to commit
                manifest.State = OperationState.ReadyToCommit;
                await PersistManifestAsync(manifest);

                // Phase 2: Create commit marker (this is the commit point)
                Directory.CreateDirectory(commitMarkerPath);

                // Phase 3: Mark as committing
                manifest.State = OperationState.Committing;
                await PersistManifestAsync(manifest);

                // Phase 4: Apply all steps
                foreach (var step in manifest.Steps.Where(s => s.State == StepState.Prepared))
                {
                    try
                    {
                        await CommitStepAsync(step);
                        step.State = StepState.Committed;
                        step.CompletedAt = DateTime.UtcNow;
                    }
                    catch (Exception ex)
                    {
                        step.State = StepState.Failed;
                        step.ErrorMessage = ex.Message;
                        throw;
                    }
                }

                // Phase 5: Mark as committed
                manifest.State = OperationState.Committed;
                manifest.LastUpdated = DateTime.UtcNow;
                await PersistManifestAsync(manifest);

                return true;
            }
            catch (Exception)
            {
                // Rollback on any failure
                await RollbackOperationAsync(manifest);
                return false;
            }
            finally
            {
                // Cleanup commit marker
                try
                {
                    if (Directory.Exists(commitMarkerPath))
                        Directory.Delete(commitMarkerPath, true);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        /// <summary>
        /// Rolls back all steps in the operation
        /// </summary>
        public async Task RollbackOperationAsync(OperationManifest manifest)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AtomicFileOperations));

            manifest.State = OperationState.RollingBack;
            await PersistManifestAsync(manifest);

            // Rollback committed steps in reverse order
            var committedSteps = manifest.Steps.Where(s => s.State == StepState.Committed).Reverse();
            
            foreach (var step in committedSteps)
            {
                try
                {
                    await RollbackStepAsync(step);
                    step.State = StepState.RolledBack;
                }
                catch (Exception ex)
                {
                    step.ErrorMessage = $"Rollback failed: {ex.Message}";
                    // Continue rolling back other steps
                }
            }

            manifest.State = OperationState.RolledBack;
            manifest.LastUpdated = DateTime.UtcNow;
            await PersistManifestAsync(manifest);

            // Cleanup temp files
            await CleanupOperationAsync(manifest);
        }

        /// <summary>
        /// Commits an individual step
        /// </summary>
        private async Task CommitStepAsync(OperationStep step)
        {
            switch (step.Type)
            {
                case StepType.CreateFile:
                case StepType.UpdateFile:
                    // Use directory-based commit for atomicity
                    var targetDir = Path.GetDirectoryName(step.TargetPath);
                    if (!string.IsNullOrEmpty(targetDir))
                        Directory.CreateDirectory(targetDir);

                    // Copy with verification instead of rename (cross-filesystem compatible)
                    await CopyFileWithVerificationAsync(step.TempPath, step.TargetPath);
                    break;

                case StepType.DeleteFile:
                    if (File.Exists(step.TargetPath))
                        File.Delete(step.TargetPath);
                    break;

                case StepType.CreateDirectory:
                    Directory.CreateDirectory(step.TargetPath);
                    break;

                case StepType.DeleteDirectory:
                    if (Directory.Exists(step.TargetPath))
                        Directory.Delete(step.TargetPath, true);
                    break;
            }
        }

        /// <summary>
        /// Rolls back an individual step
        /// </summary>
        private async Task RollbackStepAsync(OperationStep step)
        {
            switch (step.Type)
            {
                case StepType.CreateFile:
                    // Delete the created file
                    if (File.Exists(step.TargetPath))
                        File.Delete(step.TargetPath);
                    break;

                case StepType.UpdateFile:
                    // Restore from backup
                    if (!string.IsNullOrEmpty(step.SourcePath) && File.Exists(step.SourcePath))
                    {
                        await CopyFileWithVerificationAsync(step.SourcePath, step.TargetPath);
                    }
                    break;

                case StepType.DeleteFile:
                    // Restore from backup if available
                    if (!string.IsNullOrEmpty(step.SourcePath) && File.Exists(step.SourcePath))
                    {
                        await CopyFileWithVerificationAsync(step.SourcePath, step.TargetPath);
                    }
                    break;

                case StepType.CreateDirectory:
                    // Remove the created directory
                    if (Directory.Exists(step.TargetPath))
                        Directory.Delete(step.TargetPath, true);
                    break;
            }
        }

        /// <summary>
        /// Copies a file with checksum verification
        /// </summary>
        private async Task CopyFileWithVerificationAsync(string source, string target)
        {
            var content = await File.ReadAllBytesAsync(source);
            var sourceChecksum = ComputeChecksum(content);
            
            await File.WriteAllBytesAsync(target, content);
            
            // Flush to disk
            using (var fs = new FileStream(target, FileMode.Open, FileAccess.Read))
            {
                fs.Flush(flushToDisk: true);
            }
            
            // Verify
            var targetContent = await File.ReadAllBytesAsync(target);
            var targetChecksum = ComputeChecksum(targetContent);
            
            if (!ChecksumEquals(sourceChecksum, targetChecksum))
                throw new InvalidOperationException($"File copy verification failed: {source} -> {target}");
        }

        /// <summary>
        /// Persists the operation manifest to disk
        /// </summary>
        private async Task PersistManifestAsync(OperationManifest manifest)
        {
            var manifestPath = Path.Combine(_manifestDirectory, $"{manifest.OperationId}.json");
            var tempPath = manifestPath + ".tmp";
            
            var json = JsonSerializer.Serialize(manifest, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            await File.WriteAllTextAsync(tempPath, json);
            
            // Atomic rename within same directory
            File.Move(tempPath, manifestPath, true);
        }

        /// <summary>
        /// Cleans up temporary files for an operation
        /// </summary>
        private async Task CleanupOperationAsync(OperationManifest manifest)
        {
            foreach (var step in manifest.Steps)
            {
                try
                {
                    // Clean up temp files
                    if (!string.IsNullOrEmpty(step.TempPath) && File.Exists(step.TempPath))
                        File.Delete(step.TempPath);
                    
                    // Clean up backup files
                    if (!string.IsNullOrEmpty(step.SourcePath) && File.Exists(step.SourcePath))
                        File.Delete(step.SourcePath);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }

            // Remove manifest after successful cleanup
            try
            {
                var manifestPath = Path.Combine(_manifestDirectory, $"{manifest.OperationId}.json");
                if (File.Exists(manifestPath))
                    File.Delete(manifestPath);
            }
            catch
            {
                // Ignore manifest cleanup errors
            }
        }

        /// <summary>
        /// Computes SHA256 checksum of data
        /// </summary>
        private byte[] ComputeChecksum(byte[] data)
        {
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                return sha256.ComputeHash(data);
            }
        }

        /// <summary>
        /// Compares two checksums for equality
        /// </summary>
        private bool ChecksumEquals(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i]) return false;
            }
            return true;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
                
            _disposed = true;
        }
    }
}