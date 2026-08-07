---
name: hudi-repair
description: Repair a broken Hudi table. Use when metadata is corrupt, files are dangling, operations are stuck, or partitions have issues.
user-invocable: true
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [issue e.g. "metadata table corrupt", "dangling files", "stuck compaction", "partition metadata missing"]
---

# Hudi Table Repair Guide

Issue: **$ARGUMENTS**

## Instructions

**CAUTION**: Repair operations can be destructive. Always recommend dry-run first.

### Key repair tools:
- `hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieRepairTool.java` - File-level repair (3 modes: dry_run, repair, undo)
- `hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieMetadataTableValidator.java` - Metadata validation
- `hudi-cli/src/main/java/org/apache/hudi/cli/commands/RepairsCommand.java` - CLI repair commands
- Spark procedures for repair operations

### Repair procedures available:
```sql
-- Remove dangling files from corrupted clean metadata
CALL repair_corrupted_clean_files(table => '<name>');

-- Add missing partition metadata
CALL repair_add_partition_meta(table => '<name>', dry_run => true);

-- Deduplicate a partition
CALL repair_deduplicate(table => '<name>', partition_path => '<path>', dry_run => true);

-- Migrate partition metadata format
CALL repair_migrate_partition_meta(table => '<name>', dry_run => true);

-- Overwrite table properties
CALL repair_overwrite_hoodie_props(table => '<name>', new_props_file_path => '<props_path>');
```

### Common repair scenarios:

**1. Metadata table out of sync:**
```sql
-- Validate first
CALL validate_metadata_table_files(table => '<name>');
-- If corrupt, delete and let it rebuild
CALL delete_metadata_table(table => '<name>');
-- Next write will rebuild metadata table automatically
```

**2. Stuck INFLIGHT operations:**
```sql
-- Rollback specific instant
CALL rollback_to_instant_time(table => '<name>', instant_time => '<time>');
-- Or rollback all inflight table services
CALL run_rollback_inflight_table_service(table => '<name>');
```

**3. Dangling files (files not referenced by timeline):**
- Use HoodieRepairTool:
  ```bash
  spark-submit --class org.apache.hudi.utilities.HoodieRepairTool \
    --base-path <table_path> --mode dry_run
  ```
- Review output, then run with `--mode repair` if safe

**4. Savepoint and restore:**
```sql
-- Create savepoint before risky operations
CALL create_savepoint(table => '<name>', commit_time => '<instant>');
-- Restore to savepoint if needed
CALL rollback_to_savepoint(table => '<name>', instant_time => '<savepoint_instant>');
```

**5. Invalid Parquet files:**
```sql
CALL show_invalid_parquet(path => '<table_path>');
```

### Pre-repair checklist:
1. **Stop all writers** — concurrent writes during repair can cause corruption
2. **Backup `.hoodie/` directory**: `aws s3 cp --recursive s3://my-bucket/my_table/.hoodie s3://my-bucket/backups/my_table/.hoodie.bak`
3. **Run diagnostic queries first** (show_timeline, validate_metadata) — understand the damage scope
4. **Create savepoint if table is readable**: `CALL create_savepoint(table => '<name>', commit_time => '<latest_good_instant>');`
5. **Use dry_run mode** before actual repair — every repair command that supports it
6. **Verify after repair** with validation queries

### Safety classification of repair commands:
- `[SAFE]` validate_metadata_table_files, show_invalid_parquet, repair_add_partition_meta(dry_run=true)
- `[CAUTION]` delete_metadata_table (rebuilds on next write), rollback_to_instant_time, repair_add_partition_meta(dry_run=false)
- `[DANGEROUS]` repair_overwrite_hoodie_props (can break table if wrong props), rollback_to_savepoint (drops all commits after savepoint), HoodieRepairTool --mode repair (deletes unreferenced files)

### When to escalate:
- Timeline has missing COMPLETED instants with no matching rollback
- Multiple instant files have inconsistent state transitions
- hoodie.properties is corrupted or has wrong table version
- Data files reference instants that don't exist on timeline
- Repair tool dry_run shows files that should NOT be orphans

### Output:
1. **Diagnosis** - what's wrong, severity, blast radius
2. **Repair plan** - ordered steps with dry-run first, each marked `[SAFE]`/`[CAUTION]`/`[DANGEROUS]`
3. **Verification** - commands to confirm repair worked
4. **Prevention** - configs to avoid recurrence
5. **Escalation guidance** - when this indicates a deeper issue
