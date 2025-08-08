# SQL Transaction Model Validation and Isolation Level Mapping Specification

## Executive Summary

This specification defines the comprehensive transaction model for mapping SQL transaction semantics to TxtDb's MVCC system. It addresses the critical architectural challenge of bridging SQL's lock-based isolation model with TxtDb's optimistic MVCC approach while maintaining ACID guarantees and SQL-standard compliance.

### Key Architectural Decisions

1. **Hybrid Isolation Model**: Combines MVCC's optimistic concurrency with pessimistic locking for SERIALIZABLE level
2. **Lazy Lock Escalation**: Starts with MVCC, escalates to locks only when conflicts detected
3. **Graph-Based Deadlock Detection**: Leverages existing WaitForGraphDetector with SQL-specific enhancements
4. **State Machine Transaction Management**: Strict state transitions with validation at each boundary
5. **Snapshot-Based Read Consistency**: All isolation levels built on MVCC snapshot foundation

## Architecture Overview

### Component Hierarchy

```
┌─────────────────────────────────────────┐
│         SQL Transaction Manager         │
│   (Transaction lifecycle orchestration)  │
├─────────────────────────────────────────┤
│      Isolation Level Enforcer           │
│  (Maps SQL isolation to MVCC behavior)  │
├─────────────────────────────────────────┤
│     Transaction State Machine            │
│    (Validates and tracks transitions)   │
├─────────────────────────────────────────┤
│      Deadlock Detection Engine          │
│   (SQL-aware cycle detection/resolution)│
├─────────────────────────────────────────┤
│        Lock Escalation Manager          │
│    (MVCC to pessimistic lock upgrade)   │
├─────────────────────────────────────────┤
│    MVCC Transaction Coordinator         │
│     (Existing TxtDb MVCC subsystem)     │
└─────────────────────────────────────────┘
```

## Isolation Level Mapping

### SQL to MVCC Mapping Strategy

| SQL Isolation Level | TxtDb Implementation | Key Behaviors |
|-------------------|---------------------|--------------|
| READ UNCOMMITTED | Snapshot (TSN-1) | Read latest committed version, ignore active transactions |
| READ COMMITTED | Snapshot (per-statement) | New snapshot for each statement within transaction |
| REPEATABLE READ | Snapshot (per-transaction) | Single snapshot for entire transaction duration |
| SERIALIZABLE | Snapshot + Predicate Locks | MVCC with conflict detection and lock escalation |

### Detailed Isolation Semantics

#### READ UNCOMMITTED Implementation
```pseudo-code
ALGORITHM: ReadUncommittedIsolation
INPUT: transactionId, operation
OUTPUT: executionResult

BEGIN
    // Always read latest committed data, skip active transaction checks
    snapshot = GetLatestCommittedSnapshot()
    
    IF operation.type == READ THEN
        // Direct read without lock acquisition
        data = ReadAtSnapshot(operation.resource, snapshot)
        RETURN data
    
    ELSE IF operation.type == WRITE THEN
        // Writes still require transaction context
        version = CreateNewVersion(operation.resource, transactionId)
        RETURN version
    END IF
END
```

#### READ COMMITTED Implementation
```pseudo-code
ALGORITHM: ReadCommittedIsolation
INPUT: transactionId, operation
OUTPUT: executionResult

BEGIN
    // Each statement gets fresh snapshot
    IF operation.isNewStatement THEN
        snapshot = GetCurrentCommittedSnapshot()
        UpdateTransactionSnapshot(transactionId, snapshot)
    ELSE
        snapshot = GetTransactionSnapshot(transactionId)
    END IF
    
    IF operation.type == READ THEN
        data = ReadAtSnapshot(operation.resource, snapshot)
        // No read locks needed
        RETURN data
    
    ELSE IF operation.type == WRITE THEN
        // Acquire write lock
        lock = AcquireWriteLock(operation.resource, transactionId)
        IF lock.acquired THEN
            version = CreateNewVersion(operation.resource, transactionId)
            RETURN version
        ELSE
            RETURN CONFLICT_ERROR
        END IF
    END IF
END
```

#### REPEATABLE READ Implementation
```pseudo-code
ALGORITHM: RepeatableReadIsolation
INPUT: transactionId, operation
OUTPUT: executionResult

BEGIN
    // Single snapshot for entire transaction
    snapshot = GetTransactionSnapshot(transactionId)
    
    IF operation.type == READ THEN
        // Track read set for phantom prevention
        AddToReadSet(transactionId, operation.resource)
        data = ReadAtSnapshot(operation.resource, snapshot)
        RETURN data
    
    ELSE IF operation.type == WRITE THEN
        // Check for read-write conflicts
        IF HasReadWriteConflict(operation.resource, snapshot) THEN
            RETURN CONFLICT_ERROR
        END IF
        
        lock = AcquireWriteLock(operation.resource, transactionId)
        IF lock.acquired THEN
            version = CreateNewVersion(operation.resource, transactionId)
            RETURN version
        ELSE
            RETURN CONFLICT_ERROR
        END IF
    END IF
END
```

#### SERIALIZABLE Implementation
```pseudo-code
ALGORITHM: SerializableIsolation
INPUT: transactionId, operation
OUTPUT: executionResult

BEGIN
    snapshot = GetTransactionSnapshot(transactionId)
    
    IF operation.type == READ THEN
        // Acquire predicate locks for range queries
        IF operation.isRangeQuery THEN
            predicateLock = AcquirePredicateLock(operation.predicate, transactionId)
            IF NOT predicateLock.acquired THEN
                RETURN CONFLICT_ERROR
            END IF
        END IF
        
        // Track all reads for conflict detection
        AddToReadSet(transactionId, operation.resource)
        data = ReadAtSnapshot(operation.resource, snapshot)
        
        // Validate no concurrent modifications
        IF HasConcurrentModification(operation.resource, snapshot) THEN
            RETURN SERIALIZATION_ERROR
        END IF
        
        RETURN data
    
    ELSE IF operation.type == WRITE THEN
        // Check predicate locks
        IF ViolatesPredicateLock(operation.resource, transactionId) THEN
            RETURN SERIALIZATION_ERROR
        END IF
        
        // Escalate to exclusive lock
        lock = EscalateToExclusiveLock(operation.resource, transactionId)
        IF lock.acquired THEN
            version = CreateNewVersion(operation.resource, transactionId)
            RETURN version
        ELSE
            RETURN SERIALIZATION_ERROR
        END IF
    END IF
END
```

## Transaction State Machine

### State Definitions

```pseudo-code
ENUM: TransactionState
VALUES:
    NOT_STARTED      // Transaction object created but not begun
    ACTIVE           // Transaction has begun, can perform operations
    PREPARING        // Two-phase commit preparation (future)
    PREPARED         // Ready to commit in 2PC (future)
    COMMITTING       // Commit in progress
    COMMITTED        // Successfully committed
    ROLLING_BACK     // Rollback in progress
    ROLLED_BACK      // Successfully rolled back
    ABORTED          // Failed due to error or deadlock
    TIMED_OUT        // Exceeded timeout threshold

STATE_TRANSITIONS:
    NOT_STARTED -> ACTIVE: BEGIN command
    ACTIVE -> COMMITTING: COMMIT command
    ACTIVE -> ROLLING_BACK: ROLLBACK command
    ACTIVE -> ABORTED: Deadlock or fatal error
    ACTIVE -> TIMED_OUT: Timeout exceeded
    COMMITTING -> COMMITTED: Commit success
    COMMITTING -> ABORTED: Commit failure
    ROLLING_BACK -> ROLLED_BACK: Rollback success
    ROLLING_BACK -> ABORTED: Rollback failure
    ABORTED -> (terminal)
    COMMITTED -> (terminal)
    ROLLED_BACK -> (terminal)
    TIMED_OUT -> ROLLED_BACK: Automatic cleanup
```

### Transaction Lifecycle Management

```pseudo-code
ALGORITHM: TransactionLifecycleManager
INPUT: command, transactionContext
OUTPUT: result

BEGIN
    currentState = transactionContext.state
    
    SWITCH command.type:
        CASE BEGIN:
            IF currentState != NOT_STARTED THEN
                RETURN ERROR("Transaction already started")
            END IF
            
            transactionId = GenerateTransactionId()
            snapshot = CreateSnapshot()
            lease = CreateTransactionLease(transactionId, snapshot)
            
            transactionContext.id = transactionId
            transactionContext.snapshot = snapshot
            transactionContext.lease = lease
            transactionContext.state = ACTIVE
            transactionContext.startTime = NOW()
            transactionContext.isolationLevel = command.isolationLevel
            
            StartHeartbeat(transactionContext)
            RegisterWithCoordinator(transactionContext)
            
            RETURN SUCCESS
        
        CASE COMMIT:
            IF currentState != ACTIVE THEN
                RETURN ERROR("Cannot commit non-active transaction")
            END IF
            
            transactionContext.state = COMMITTING
            
            // Validate all constraints
            IF NOT ValidateConstraints(transactionContext) THEN
                transactionContext.state = ROLLING_BACK
                PerformRollback(transactionContext)
                RETURN ERROR("Constraint violation")
            END IF
            
            // Check for conflicts based on isolation level
            conflicts = DetectConflicts(transactionContext)
            IF conflicts.exist THEN
                IF transactionContext.isolationLevel == SERIALIZABLE THEN
                    transactionContext.state = ROLLING_BACK
                    PerformRollback(transactionContext)
                    RETURN ERROR("Serialization failure")
                END IF
            END IF
            
            // Perform actual commit
            TRY
                FlushChanges(transactionContext)
                ReleaseLocks(transactionContext)
                UpdateTransactionLog(transactionContext, COMMITTED)
                transactionContext.state = COMMITTED
                RETURN SUCCESS
            CATCH error:
                transactionContext.state = ABORTED
                PerformEmergencyCleanup(transactionContext)
                RETURN ERROR("Commit failed: " + error)
            END TRY
        
        CASE ROLLBACK:
            IF currentState != ACTIVE THEN
                RETURN ERROR("Cannot rollback non-active transaction")
            END IF
            
            transactionContext.state = ROLLING_BACK
            PerformRollback(transactionContext)
            transactionContext.state = ROLLED_BACK
            RETURN SUCCESS
        
        CASE SAVEPOINT:
            IF currentState != ACTIVE THEN
                RETURN ERROR("Cannot create savepoint in non-active transaction")
            END IF
            
            savepoint = CreateSavepoint(transactionContext, command.savepointName)
            transactionContext.savepoints.add(savepoint)
            RETURN SUCCESS
        
        CASE ROLLBACK_TO_SAVEPOINT:
            IF currentState != ACTIVE THEN
                RETURN ERROR("Cannot rollback to savepoint in non-active transaction")
            END IF
            
            savepoint = transactionContext.savepoints.get(command.savepointName)
            IF savepoint == NULL THEN
                RETURN ERROR("Savepoint not found")
            END IF
            
            RollbackToSavepoint(transactionContext, savepoint)
            RETURN SUCCESS
    END SWITCH
END
```

## Deadlock Detection and Resolution

### Enhanced Wait-For Graph for SQL

```pseudo-code
ALGORITHM: SqlDeadlockDetector
EXTENDS: WaitForGraphDetector

STRUCTURE: SqlWaitNode
    transactionId: long
    waitingFor: Set<ResourceLock>
    holdingLocks: Set<ResourceLock>
    isolationLevel: SqlIsolationLevel
    startTime: DateTime
    lastActivity: DateTime
    statementCount: int
    priority: int  // For victim selection

STRUCTURE: ResourceLock
    resourceId: string
    lockType: LockType  // SHARED, EXCLUSIVE, PREDICATE
    grantedAt: DateTime
    escalated: boolean  // True if escalated from MVCC

ALGORITHM: DetectAndResolveDeadlock
INPUT: transactionId, requestedResource, lockType
OUTPUT: deadlockResolved: boolean

BEGIN
    // Add wait edge to graph
    holders = GetLockHolders(requestedResource, lockType)
    
    FOR EACH holder IN holders DO
        AddWaitEdge(transactionId, holder.transactionId)
    END FOR
    
    // Detect cycles using enhanced DFS
    cycles = FindCyclesInGraph()
    
    IF cycles.isEmpty() THEN
        RETURN FALSE  // No deadlock
    END IF
    
    // Select victim using SQL-specific heuristics
    victim = SelectDeadlockVictim(cycles)
    
    // Abort victim transaction
    AbortTransaction(victim.transactionId, DEADLOCK_REASON)
    
    // Remove victim from graph
    RemoveTransactionFromGraph(victim.transactionId)
    
    // Notify waiting transactions
    NotifyWaiters(victim.holdingLocks)
    
    RETURN TRUE
END

ALGORITHM: SelectDeadlockVictim
INPUT: cycles: List<TransactionCycle>
OUTPUT: victim: SqlWaitNode

BEGIN
    candidates = ExtractUniqueTransactions(cycles)
    
    // Victim selection criteria (in priority order):
    // 1. Transaction with lowest priority
    // 2. Youngest transaction (latest start time)
    // 3. Transaction with least work done (fewer statements)
    // 4. Transaction holding fewer locks
    
    victim = candidates[0]
    
    FOR EACH candidate IN candidates DO
        IF candidate.priority < victim.priority THEN
            victim = candidate
        ELSE IF candidate.priority == victim.priority THEN
            IF candidate.startTime > victim.startTime THEN
                victim = candidate
            ELSE IF candidate.startTime == victim.startTime THEN
                IF candidate.statementCount < victim.statementCount THEN
                    victim = candidate
                ELSE IF candidate.statementCount == victim.statementCount THEN
                    IF candidate.holdingLocks.size() < victim.holdingLocks.size() THEN
                        victim = candidate
                    END IF
                END IF
            END IF
        END IF
    END FOR
    
    LogDeadlockVictimSelection(victim, cycles)
    RETURN victim
END
```

### Deadlock Prevention Strategies

```pseudo-code
ALGORITHM: DeadlockPrevention
INPUT: transaction, resourceRequest
OUTPUT: preventionAction

BEGIN
    // Wait-Die scheme for young transactions
    IF UseWaitDieScheme() THEN
        IF transaction.timestamp > resourceHolder.timestamp THEN
            // Younger transaction waits
            RETURN WAIT
        ELSE
            // Older transaction forces abort
            RETURN ABORT_HOLDER
        END IF
    END IF
    
    // Wound-Wait scheme for old transactions
    IF UseWoundWaitScheme() THEN
        IF transaction.timestamp < resourceHolder.timestamp THEN
            // Older transaction wounds younger
            RETURN WOUND_HOLDER
        ELSE
            // Younger transaction aborts itself
            RETURN ABORT_SELF
        END IF
    END IF
    
    // Timeout-based prevention
    IF transaction.waitTime > MAX_WAIT_TIME THEN
        RETURN TIMEOUT_ABORT
    END IF
    
    // Resource ordering prevention
    IF ViolatesResourceOrder(transaction, resourceRequest) THEN
        RETURN REORDER_REQUEST
    END IF
    
    RETURN PROCEED
END
```

## Lock Escalation Strategy

### MVCC to Lock Escalation

```pseudo-code
ALGORITHM: LockEscalationManager
INPUT: transaction, conflictInfo
OUTPUT: escalationResult

STRUCTURE: EscalationThresholds
    conflictCount: int = 3        // Conflicts before escalation
    retryCount: int = 5           // Retries before escalation
    contentionRatio: float = 0.3  // Resource contention threshold
    timeWindow: Duration = 10s    // Window for measuring conflicts

ALGORITHM: EscalateLockIfNeeded
INPUT: transaction, resource, operation
OUTPUT: escalated: boolean

BEGIN
    // Check if escalation is needed
    metrics = GetTransactionMetrics(transaction.id)
    
    IF metrics.conflictCount >= thresholds.conflictCount THEN
        RETURN PerformEscalation(transaction, resource, CONFLICT_THRESHOLD)
    END IF
    
    IF metrics.retryCount >= thresholds.retryCount THEN
        RETURN PerformEscalation(transaction, resource, RETRY_THRESHOLD)
    END IF
    
    contentionLevel = MeasureResourceContention(resource)
    IF contentionLevel >= thresholds.contentionRatio THEN
        RETURN PerformEscalation(transaction, resource, CONTENTION_THRESHOLD)
    END IF
    
    // Check isolation level requirements
    IF transaction.isolationLevel == SERIALIZABLE THEN
        IF operation.type == RANGE_SCAN THEN
            RETURN PerformEscalation(transaction, resource, SERIALIZABLE_SCAN)
        END IF
    END IF
    
    RETURN FALSE
END

ALGORITHM: PerformEscalation
INPUT: transaction, resource, reason
OUTPUT: success: boolean

BEGIN
    // Determine target lock type
    targetLockType = DetermineLockType(transaction, resource, reason)
    
    // Attempt to acquire pessimistic lock
    TRY
        // Release MVCC lease
        ReleaseOptimisticLock(transaction.id, resource)
        
        // Acquire pessimistic lock
        lock = AcquirePessimisticLock(transaction.id, resource, targetLockType)
        
        // Update transaction metadata
        transaction.escalatedLocks.add(lock)
        transaction.escalationCount++
        
        LogEscalation(transaction.id, resource, reason, targetLockType)
        
        RETURN TRUE
        
    CATCH error:
        // Rollback to MVCC if escalation fails
        ReacquireOptimisticLock(transaction.id, resource)
        LogEscalationFailure(transaction.id, resource, error)
        RETURN FALSE
    END TRY
END

ALGORITHM: DetermineLockType
INPUT: transaction, resource, reason
OUTPUT: lockType

BEGIN
    IF transaction.isolationLevel == READ_UNCOMMITTED THEN
        RETURN NO_LOCK  // Continue with MVCC only
    END IF
    
    IF transaction.isolationLevel == READ_COMMITTED THEN
        IF reason == WRITE_CONFLICT THEN
            RETURN EXCLUSIVE_LOCK
        ELSE
            RETURN SHARED_LOCK
        END IF
    END IF
    
    IF transaction.isolationLevel == REPEATABLE_READ THEN
        IF resource.type == ROW THEN
            RETURN SHARED_LOCK
        ELSE
            RETURN RANGE_SHARED_LOCK
        END IF
    END IF
    
    IF transaction.isolationLevel == SERIALIZABLE THEN
        IF resource.type == PREDICATE THEN
            RETURN PREDICATE_LOCK
        ELSE IF resource.type == TABLE THEN
            RETURN TABLE_LOCK
        ELSE
            RETURN EXCLUSIVE_LOCK
        END IF
    END IF
END
```

## Concurrent Transaction Coordination

### Multi-Transaction Coordinator

```pseudo-code
ALGORITHM: ConcurrentTransactionCoordinator
INPUT: activeTransactions
OUTPUT: coordinationStatus

STRUCTURE: TransactionRegistry
    activeTransactions: Map<TransactionId, TransactionContext>
    waitQueues: Map<ResourceId, Queue<TransactionId>>
    conflictMatrix: Map<TransactionPair, ConflictInfo>
    dependencyGraph: Graph<TransactionId>

ALGORITHM: CoordinateTransactions
INPUT: none
OUTPUT: none

BEGIN
    WHILE coordinator.isRunning DO
        // Update transaction states
        FOR EACH transaction IN activeTransactions DO
            UpdateTransactionState(transaction)
            CheckTransactionTimeout(transaction)
            UpdateHeartbeat(transaction)
        END FOR
        
        // Process wait queues
        FOR EACH queue IN waitQueues DO
            ProcessWaitQueue(queue)
        END FOR
        
        // Detect and resolve conflicts
        conflicts = DetectConflicts()
        FOR EACH conflict IN conflicts DO
            ResolveConflict(conflict)
        END FOR
        
        // Perform deadlock detection
        IF DeadlockDetectionInterval.elapsed() THEN
            deadlocks = DetectDeadlocks()
            FOR EACH deadlock IN deadlocks DO
                ResolveDeadlock(deadlock)
            END FOR
        END IF
        
        // Clean up completed transactions
        CleanupCompletedTransactions()
        
        SLEEP(CoordinationInterval)
    END WHILE
END

ALGORITHM: ResolveConflict
INPUT: conflict
OUTPUT: resolution

BEGIN
    transaction1 = conflict.transaction1
    transaction2 = conflict.transaction2
    
    // Determine resolution based on isolation levels
    IF transaction1.isolationLevel > transaction2.isolationLevel THEN
        winner = transaction1
        loser = transaction2
    ELSE IF transaction2.isolationLevel > transaction1.isolationLevel THEN
        winner = transaction2
        loser = transaction1
    ELSE
        // Same isolation level - use timestamp ordering
        IF transaction1.startTime < transaction2.startTime THEN
            winner = transaction1
            loser = transaction2
        ELSE
            winner = transaction2
            loser = transaction1
        END IF
    END IF
    
    // Apply resolution
    IF conflict.type == WRITE_WRITE THEN
        AbortTransaction(loser)
        RetryTransaction(loser)
    ELSE IF conflict.type == READ_WRITE THEN
        IF loser.isolationLevel >= REPEATABLE_READ THEN
            AbortTransaction(loser)
            RetryTransaction(loser)
        ELSE
            // Allow read to proceed with old value
            AllowStaleRead(loser)
        END IF
    END IF
    
    RETURN resolution
END
```

## Read Consistency Implementation

### Consistent Read Management

```pseudo-code
ALGORITHM: ReadConsistencyManager
INPUT: transaction, readRequest
OUTPUT: consistentData

STRUCTURE: ReadView
    transactionId: long
    snapshotTSN: long
    isolationLevel: SqlIsolationLevel
    activeTransactions: Set<long>
    readTimestamp: DateTime
    statementId: long  // For READ COMMITTED

ALGORITHM: GetConsistentRead
INPUT: transaction, resource
OUTPUT: data

BEGIN
    readView = GetOrCreateReadView(transaction)
    
    // Get all versions of the resource
    versions = GetResourceVersions(resource)
    
    // Find visible version based on isolation level
    visibleVersion = NULL
    
    FOR EACH version IN versions (ORDER BY tsn DESC) DO
        IF IsVersionVisible(version, readView) THEN
            visibleVersion = version
            BREAK
        END IF
    END FOR
    
    IF visibleVersion == NULL THEN
        // Resource doesn't exist in this read view
        RETURN NOT_FOUND
    END IF
    
    // Validate consistency based on isolation level
    IF NOT ValidateReadConsistency(transaction, visibleVersion) THEN
        RETURN CONSISTENCY_ERROR
    END IF
    
    // Track read for validation at commit time
    IF transaction.isolationLevel >= REPEATABLE_READ THEN
        transaction.readSet.add(resource, visibleVersion.tsn)
    END IF
    
    RETURN visibleVersion.data
END

ALGORITHM: IsVersionVisible
INPUT: version, readView
OUTPUT: visible: boolean

BEGIN
    // Version must be committed before snapshot
    IF version.tsn > readView.snapshotTSN THEN
        RETURN FALSE
    END IF
    
    // Check if version was created by active transaction
    IF version.transactionId IN readView.activeTransactions THEN
        // Only visible if it's our own transaction
        RETURN version.transactionId == readView.transactionId
    END IF
    
    // Check if version was deleted
    IF version.deletedAt != NULL THEN
        IF version.deletedAt <= readView.snapshotTSN THEN
            // Was deleted before our snapshot
            IF version.deletedBy NOT IN readView.activeTransactions THEN
                RETURN FALSE  // Deletion is visible
            END IF
        END IF
    END IF
    
    RETURN TRUE
END

ALGORITHM: ValidateReadConsistency
INPUT: transaction, version
OUTPUT: valid: boolean

BEGIN
    SWITCH transaction.isolationLevel:
        CASE READ_UNCOMMITTED:
            // Always valid - we read whatever is there
            RETURN TRUE
            
        CASE READ_COMMITTED:
            // Check if version is still the latest committed
            latestCommitted = GetLatestCommittedVersion(version.resource)
            RETURN version.tsn == latestCommitted.tsn
            
        CASE REPEATABLE_READ:
            // Check if we've read this before
            IF transaction.readSet.contains(version.resource) THEN
                previousTsn = transaction.readSet.get(version.resource)
                RETURN version.tsn == previousTsn
            END IF
            RETURN TRUE
            
        CASE SERIALIZABLE:
            // Validate no concurrent modifications
            concurrentMods = GetConcurrentModifications(
                version.resource,
                transaction.startTime,
                NOW()
            )
            RETURN concurrentMods.isEmpty()
    END SWITCH
END
```

## Transaction Timeout and Recovery

### Timeout Management

```pseudo-code
ALGORITHM: TransactionTimeoutManager
INPUT: configuration
OUTPUT: none

STRUCTURE: TimeoutConfiguration
    defaultTimeout: Duration = 30s
    longRunningTimeout: Duration = 5m
    deadlockTimeout: Duration = 10s
    idleTimeout: Duration = 1m

ALGORITHM: MonitorTransactionTimeouts
INPUT: none
OUTPUT: none

BEGIN
    WHILE monitor.isRunning DO
        currentTime = NOW()
        
        FOR EACH transaction IN activeTransactions DO
            // Check different timeout conditions
            elapsedTime = currentTime - transaction.startTime
            idleTime = currentTime - transaction.lastActivity
            
            timeoutAction = NONE
            
            IF transaction.isLongRunning THEN
                IF elapsedTime > config.longRunningTimeout THEN
                    timeoutAction = TIMEOUT_ABORT
                END IF
            ELSE
                IF elapsedTime > config.defaultTimeout THEN
                    timeoutAction = TIMEOUT_ABORT
                END IF
            END IF
            
            IF idleTime > config.idleTimeout THEN
                timeoutAction = IDLE_ABORT
            END IF
            
            IF transaction.waitTime > config.deadlockTimeout THEN
                // Possible undetected deadlock
                timeoutAction = DEADLOCK_TIMEOUT_ABORT
            END IF
            
            IF timeoutAction != NONE THEN
                HandleTransactionTimeout(transaction, timeoutAction)
            END IF
        END FOR
        
        SLEEP(TimeoutCheckInterval)
    END WHILE
END

ALGORITHM: HandleTransactionTimeout
INPUT: transaction, timeoutAction
OUTPUT: none

BEGIN
    LogTimeout(transaction, timeoutAction)
    
    // Mark transaction as timed out
    transaction.state = TIMED_OUT
    
    // Perform cleanup based on timeout type
    SWITCH timeoutAction:
        CASE TIMEOUT_ABORT:
            // Normal timeout - clean rollback
            PerformRollback(transaction)
            NotifyClient(transaction, "Transaction timed out")
            
        CASE IDLE_ABORT:
            // Idle timeout - save state if possible
            SaveTransactionState(transaction)
            PerformRollback(transaction)
            NotifyClient(transaction, "Transaction idle timeout")
            
        CASE DEADLOCK_TIMEOUT_ABORT:
            // Potential deadlock - aggressive cleanup
            ForceReleaseAllLocks(transaction)
            PerformRollback(transaction)
            NotifyClient(transaction, "Transaction aborted due to potential deadlock")
    END SWITCH
    
    // Remove from active transactions
    RemoveActiveTransaction(transaction)
    
    // Clean up resources
    ReleaseTransactionResources(transaction)
END
```

## Interface Specifications

### Core Transaction Management Interfaces

```pseudo-code
INTERFACE: ISqlTransactionManager
PURPOSE: Orchestrates SQL transaction lifecycle
METHODS:
    BeginTransaction(isolationLevel, options) -> ISqlTransaction
    GetTransaction(transactionId) -> ISqlTransaction
    GetActiveTransactions() -> List<ISqlTransaction>
    ValidateTransaction(transactionId) -> ValidationResult
    SetDefaultIsolationLevel(level) -> void
    ConfigureTimeouts(timeoutConfig) -> void

INTERFACE: ISqlTransaction
EXTENDS: IDatabaseTransaction
PURPOSE: SQL-specific transaction operations
PROPERTIES:
    IsolationLevel -> SqlIsolationLevel
    SavepointStack -> Stack<Savepoint>
    ReadView -> ReadView
    ConflictCount -> int
    RetryCount -> int
    EscalatedLocks -> Set<Lock>
METHODS:
    SetIsolationLevel(level) -> void
    CreateSavepoint(name) -> Savepoint
    RollbackToSavepoint(name) -> void
    ReleaseSavepoint(name) -> void
    GetStatistics() -> TransactionStatistics
    SetPriority(priority) -> void

INTERFACE: IIsolationLevelEnforcer
PURPOSE: Enforces SQL isolation semantics
METHODS:
    EnforceReadIsolation(transaction, resource) -> ReadResult
    EnforceWriteIsolation(transaction, resource, value) -> WriteResult
    ValidateSerializability(transaction) -> bool
    DetectPhantoms(transaction, predicate) -> PhantomResult
    CheckWriteSkew(transaction) -> WriteSkewResult

INTERFACE: IDeadlockDetector
PURPOSE: SQL-aware deadlock detection
METHODS:
    AddLockRequest(transaction, resource, lockType) -> void
    RemoveLockRequest(transaction, resource) -> void
    DetectDeadlock() -> DeadlockInfo
    SelectVictim(deadlockInfo) -> TransactionId
    GetWaitGraph() -> Graph<TransactionId>
    SetDetectionInterval(interval) -> void

INTERFACE: ILockEscalationManager
PURPOSE: Manages MVCC to lock escalation
METHODS:
    ShouldEscalate(transaction, metrics) -> bool
    EscalateLock(transaction, resource, targetType) -> Lock
    DowngradeLock(transaction, resource) -> void
    GetEscalationStatistics() -> EscalationStats
    ConfigureThresholds(thresholds) -> void

INTERFACE: IReadConsistencyManager
PURPOSE: Ensures consistent reads per isolation level
METHODS:
    CreateReadView(transaction) -> ReadView
    GetConsistentVersion(resource, readView) -> Version
    ValidateReadSet(transaction) -> bool
    DetectLostUpdate(transaction, resource) -> bool
    RefreshReadView(transaction) -> ReadView
```

### Supporting Data Structures

```pseudo-code
STRUCTURE: SqlIsolationLevel
    level: ENUM {READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE}
    mvccMode: ENUM {SNAPSHOT, STATEMENT_SNAPSHOT, NO_SNAPSHOT}
    lockingMode: ENUM {NO_LOCKS, WRITE_LOCKS, READ_WRITE_LOCKS, PREDICATE_LOCKS}
    conflictDetection: ENUM {NONE, WRITE_CONFLICTS, ALL_CONFLICTS}

STRUCTURE: TransactionStatistics
    startTime: DateTime
    elapsedTime: Duration
    statementCount: int
    readCount: int
    writeCount: int
    conflictCount: int
    retryCount: int
    escalationCount: int
    locksHeld: int
    waitTime: Duration
    cpuTime: Duration
    ioTime: Duration

STRUCTURE: ConflictInfo
    type: ConflictType
    transaction1: TransactionId
    transaction2: TransactionId
    resource: ResourceId
    timestamp: DateTime
    resolution: ResolutionType

STRUCTURE: DeadlockInfo
    cycles: List<TransactionCycle>
    involvedTransactions: Set<TransactionId>
    involvedResources: Set<ResourceId>
    detectedAt: DateTime
    victim: TransactionId
```

## Integration Points

### Integration with Existing Systems

```pseudo-code
ALGORITHM: IntegrateWithStorageLayer
INPUT: sqlTransaction
OUTPUT: storageTransaction

BEGIN
    // Map SQL transaction to storage transaction
    storageConfig = MapIsolationToStorageConfig(sqlTransaction.isolationLevel)
    
    storageTransaction = storageSubsystem.BeginTransaction(
        storageConfig.isolationMode,
        storageConfig.flushPriority
    )
    
    // Wrap with SQL semantics
    sqlTransaction.storageTransaction = storageTransaction
    sqlTransaction.transactionId = storageTransaction.id
    
    // Register with coordinators
    mvccCoordinator.RegisterTransaction(storageTransaction)
    sqlCoordinator.RegisterTransaction(sqlTransaction)
    
    // Setup monitoring
    transactionMonitor.StartMonitoring(sqlTransaction)
    
    RETURN storageTransaction
END

ALGORITHM: IntegrateWithDatabaseLayer
INPUT: sqlTransaction, databaseContext
OUTPUT: databaseTransaction

BEGIN
    // Create database transaction with SQL semantics
    databaseTransaction = databaseContext.CreateTransaction(
        sqlTransaction.transactionId,
        sqlTransaction.isolationLevel
    )
    
    // Configure based on SQL requirements
    IF sqlTransaction.isolationLevel == SERIALIZABLE THEN
        databaseTransaction.EnablePredicateLocking()
        databaseTransaction.EnableConflictDetection()
    END IF
    
    // Setup callbacks for state changes
    databaseTransaction.OnCommit = sqlTransaction.HandleCommit
    databaseTransaction.OnRollback = sqlTransaction.HandleRollback
    databaseTransaction.OnConflict = sqlTransaction.HandleConflict
    
    RETURN databaseTransaction
END
```

## Performance Considerations

### Optimization Strategies

1. **Read-Only Transaction Optimization**
   - Skip write lock acquisition
   - Use lightweight snapshots
   - Bypass conflict detection

2. **Batch Lock Acquisition**
   - Group lock requests to reduce overhead
   - Pre-acquire locks for known access patterns

3. **Adaptive Isolation Level**
   - Dynamically adjust based on contention
   - Downgrade when possible, upgrade when necessary

4. **Connection Pooling Integration**
   - Reuse transaction contexts
   - Cache prepared statements with isolation requirements

5. **Monitoring and Metrics**
   - Track escalation frequency
   - Monitor deadlock patterns
   - Measure isolation level impact on throughput

## Error Handling and Recovery

### Error Categories and Responses

```pseudo-code
ENUM: TransactionErrorType
    DEADLOCK_DETECTED
    SERIALIZATION_FAILURE  
    LOCK_TIMEOUT
    CONSTRAINT_VIOLATION
    SNAPSHOT_TOO_OLD
    TRANSACTION_ABORTED
    ISOLATION_VIOLATION
    RESOURCE_UNAVAILABLE

ALGORITHM: HandleTransactionError
INPUT: error, transaction
OUTPUT: recoveryAction

BEGIN
    SWITCH error.type:
        CASE DEADLOCK_DETECTED:
            // Automatic retry with backoff
            IF transaction.retryCount < MAX_RETRIES THEN
                RETURN RETRY_WITH_BACKOFF
            ELSE
                RETURN ABORT_AND_NOTIFY
            END IF
            
        CASE SERIALIZATION_FAILURE:
            // Retry at lower isolation if possible
            IF CanDowngradeIsolation(transaction) THEN
                RETURN RETRY_WITH_LOWER_ISOLATION
            ELSE
                RETURN RETRY_WITH_BACKOFF
            END IF
            
        CASE LOCK_TIMEOUT:
            // Check if deadlock detection missed something
            IF PossibleUndetectedDeadlock(transaction) THEN
                RETURN FORCE_DEADLOCK_CHECK
            ELSE
                RETURN INCREASE_TIMEOUT_AND_RETRY
            END IF
            
        CASE CONSTRAINT_VIOLATION:
            // No retry - application error
            RETURN ABORT_AND_NOTIFY
            
        CASE SNAPSHOT_TOO_OLD:
            // Refresh snapshot if READ COMMITTED
            IF transaction.isolationLevel == READ_COMMITTED THEN
                RETURN REFRESH_SNAPSHOT
            ELSE
                RETURN ABORT_AND_NOTIFY
            END IF
            
        DEFAULT:
            RETURN ABORT_AND_NOTIFY
    END SWITCH
END
```

## Testing Strategy

### Comprehensive Test Scenarios

1. **Isolation Level Verification**
   - Dirty read prevention tests
   - Non-repeatable read tests  
   - Phantom read tests
   - Write skew tests
   - Serialization anomaly tests

2. **Deadlock Scenarios**
   - Simple cycle detection
   - Complex multi-resource deadlocks
   - Distributed deadlocks (future)
   - Priority inversion scenarios

3. **Escalation Testing**
   - Threshold trigger verification
   - Escalation under contention
   - De-escalation scenarios
   - Mixed MVCC/lock operations

4. **Timeout Testing**
   - Transaction timeout
   - Statement timeout
   - Lock acquisition timeout
   - Idle transaction cleanup

5. **Concurrency Testing**
   - High contention workloads
   - Mixed isolation levels
   - Long-running vs short transactions
   - Savepoint interaction

## Future Enhancements

### Planned Improvements

1. **Distributed Transaction Support**
   - Two-phase commit protocol
   - Distributed deadlock detection
   - Cross-node isolation guarantees

2. **Optimistic Concurrency Control**
   - Validation-based concurrency
   - Timestamp ordering enhancements
   - Multi-version timestamp ordering

3. **Advanced Isolation Levels**
   - Snapshot Isolation (SI)
   - Serializable Snapshot Isolation (SSI)
   - Read Committed Lock Free

4. **Performance Optimizations**
   - Lock-free data structures
   - NUMA-aware locking
   - Adaptive concurrency control

5. **Monitoring and Diagnostics**
   - Transaction profiling
   - Deadlock visualization
   - Isolation level recommendations

## Summary

This specification provides a comprehensive blueprint for implementing SQL transaction semantics on top of TxtDb's MVCC system. The hybrid approach leverages MVCC's strengths while providing SQL-standard isolation guarantees through selective lock escalation and sophisticated conflict detection.

Key achievements:
- Full SQL isolation level compliance
- Efficient deadlock detection and resolution
- Graceful escalation from optimistic to pessimistic concurrency
- Robust state management and error recovery
- Seamless integration with existing TxtDb architecture

The design prioritizes correctness while maintaining TxtDb's performance characteristics, providing a solid foundation for the SQL layer implementation.