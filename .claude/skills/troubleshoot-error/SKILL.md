---
name: troubleshoot-error
description: Troubleshoot Hudi errors from stack traces or error messages. Use when pasting exceptions or describing error symptoms.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [error message or exception class e.g. "HoodieWriteConflictException", "Cannot resolve conflicts"]
---

# Troubleshoot Hudi Error

Error: **$ARGUMENTS**

## Instructions

### Step 1: Identify the exception
Search for the exception class or error message in the codebase:
- Exception classes: `hudi-io/src/main/java/org/apache/hudi/exception/` and `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/exception/`
- Error messages: Grep for the exact message text across the codebase

### Step 2: Trace the throw site
Find where this exception is thrown. Read the surrounding code to understand:
- What condition triggers it
- What state the system is in when it occurs
- What the code was trying to do

### Step 3: Map to common scenarios

**Exception -> Common cause mapping:**

| Exception | Common Cause |
|-----------|-------------|
| `HoodieWriteConflictException` | Concurrent writers modifying same file group without proper locking |
| `HoodieEarlyConflictDetectionException` | Overlapping writes detected via markers |
| `HoodieLockException` | Lock acquisition timeout or lock provider misconfiguration |
| `HoodieCompactionException` | Compaction plan references files that no longer exist (cleaned) |
| `HoodieClusteringException` | Clustering conflicts or resource issues |
| `HoodieMetadataException` | Metadata table out of sync with data table |
| `HoodieValidationException` | Pre-write/pre-commit validation failed |
| `HoodieSchemaEvolutionConflictException` | Incompatible schema change |
| `CorruptedLogFileException` | Log file corrupted (partial write, disk issue) |
| `HoodieRollbackException` | Rollback failed (missing files, permissions) |
| `HoodieCommitException` | Commit failed (timeline conflict, storage issue) |
| `HoodieUpsertException` | Record-level failure during upsert |
| `InvalidTableException` | Table metadata corrupted or incompatible version |
| `SchemaCompatibilityException` | Writer schema incompatible with table schema |

### Step 4: Provide resolution with safety markers

For each error provide:
1. **Why it happened** - root cause explanation
2. **Immediate fix** - commands to resolve the current state, marked as:
   - `[SAFE]` — read-only diagnostic
   - `[REVERSIBLE]` — can be undone (e.g., rollback)
   - `[CAUTION]` — mutating, verify before running
   - `[DANGEROUS]` — potential data loss, needs approval
3. **Config changes** - settings to prevent recurrence
4. **If data loss is possible** - clearly state it and suggest verification steps
5. **When to escalate** - if the error indicates corruption, version mismatch, or repeated occurrence

### Common resolution commands:
```sql
-- [REVERSIBLE] Rollback stuck instant
CALL rollback_to_instant_time(table => '<name>', instant_time => '<time>');

-- [REVERSIBLE] Rollback all inflight table services
CALL run_rollback_inflight_table_service(table => '<name>');

-- [CAUTION] Rebuild metadata table
CALL delete_metadata_table(table => '<name>');

-- [SAFE] Verify table state after fix
CALL show_timeline(path => '<path>', limit => 10);
CALL validate_metadata_table_files(table => '<name>');
```

### Step 5: Check for known patterns
Search for the error in:
- Test files (how is this scenario tested?)
- Other exception handlers that catch and rethrow
- Config validation code that tries to prevent this state
