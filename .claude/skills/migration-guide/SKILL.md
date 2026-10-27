---
name: migration-guide
description: Guide Hudi version upgrades, downgrades, or table type migrations. Use when planning upgrades to Hudi 1.0/1.1 or migrating CoW to MoR.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [migration e.g. "upgrade to 1.0", "CoW to MoR", "downgrade table version"]
---

# Hudi Migration Guide

Migration: **$ARGUMENTS**

## Instructions

### Key source files:
- Upgrade/downgrade handlers: search for `UpgradeHandler` and `DowngradeHandler` in `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/upgrade/`
- Table version: `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java`
- Table config: `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableConfig.java`

### For version upgrades:
1. Find the relevant `UpgradeHandler` class for the target version
2. Read what changes it makes (timeline format, metadata, file naming, etc.)
3. Identify:
   - **Automatic migrations**: What Hudi handles automatically on first write
   - **Manual steps**: What the user must do before/after upgrade
   - **Breaking changes**: What's incompatible and requires data migration
   - **Rollback plan**: Can you downgrade back? What's the `DowngradeHandler`?

### For table type migration (CoW <-> MoR):
1. This is generally NOT supported as an in-place operation
2. Options:
   - Create new table with desired type and backfill data
   - Use `CALL copy_to_table()` procedure
3. Explain the tradeoffs of each table type:
   - CoW: Better read performance, higher write amplification
   - MoR: Better write performance, read merges needed, requires compaction

### Spark SQL procedures:
```sql
-- Upgrade table to latest version
CALL upgrade_table(table => '<name>');
-- Downgrade table to specific version
CALL downgrade_table(table => '<name>', to_version => '<version>');
```

### Pre-migration checklist:
1. Backup table metadata (`.hoodie/` directory)
2. Ensure no writers are active
3. Complete all pending compactions/clusterings
4. Run `CALL validate_metadata_table_files()` to verify consistency
5. Take a savepoint: `CALL create_savepoint(table => '<name>', commit_time => '<instant>')`

### Output:
1. **Migration path** - step-by-step instructions
2. **Breaking changes** - what will break and how to handle
3. **Rollback plan** - how to revert if something goes wrong
4. **Validation** - how to verify the migration succeeded
5. **Timeline** - estimated impact/downtime
