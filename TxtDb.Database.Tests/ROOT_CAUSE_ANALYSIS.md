# Transaction Isolation Issue - Root Cause Analysis

## Problem Statement
Epic 004 Story 1 is blocked by a critical transaction isolation issue where data inserted via Table.InsertAsync is not visible to Table.GetAsync, even within the same transaction and table instance. This affects 82% of database tests (18/22 failing).

## Investigation Summary

### Key Findings

1. **Table Instance Behavior**
   - Table instances are NOT cached (Database.GetTableAsync creates new instances each time)
   - Each Table instance maintains its own in-memory primary key index (_primaryKeyIndex)
   - The index is crucial for GetAsync operations to locate data

2. **Index Persistence Issue**
   - Index persistence was commented out as "TODO" in the original code
   - When enabled, index persistence failed due to MVCC read-before-write requirements
   - Fixed by reading existing index page before updating, or inserting if new

3. **Core Problem: LoadPrimaryKeyIndexIfNeeded**
   - This method is called by BOTH InsertAsync and GetAsync
   - Original implementation ALWAYS cleared and reloaded the index from storage
   - This caused inserted data to "disappear" because:
     - Insert adds to in-memory index
     - Get calls LoadPrimaryKeyIndexIfNeeded
     - LoadPrimaryKeyIndexIfNeeded clears the index and reloads from storage
     - Uncommitted data is not in storage yet, so it's lost

4. **Attempted Fixes**
   - Added _indexLoaded flag check to prevent reloading
   - Enabled index persistence
   - Fixed MVCC read-before-write issue
   - Added comprehensive debugging

5. **Current Status**
   - Despite fixes, data is STILL not visible even within same transaction
   - Same table instance (verified by hash code)
   - This suggests a deeper issue with the index update mechanism

## Root Cause Hypothesis

The primary issue appears to be that the LoadPrimaryKeyIndexIfNeeded method is fundamentally incompatible with the non-cached table architecture. Since each GetTableAsync returns a new instance:

1. For cross-transaction visibility: Index persistence is required
2. For same-transaction visibility: The index should not be reloaded if already loaded

However, even with these fixes, the issue persists, suggesting there may be:
- An issue with how the primary key is extracted or compared
- A threading/concurrency issue with the ConcurrentDictionary
- A problem with the transaction propagation to storage layer

## Recommended Solution

1. **Short-term**: Disable LoadPrimaryKeyIndexIfNeeded reload behavior for loaded indexes
2. **Long-term**: Either:
   - Implement table instance caching at the Database level
   - OR ensure index persistence works correctly with proper MVCC semantics
   - OR redesign the index loading strategy to support non-cached tables

## Next Steps

1. Add more detailed logging to understand why inserted keys are not found in the index
2. Verify primary key extraction is working correctly
3. Consider implementing table-level caching as a more robust solution
4. Review the storage layer's transaction isolation implementation