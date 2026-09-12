---
name: diagnose-table
description: Health check a Hudi table. Use when a table is slow, has stuck operations, small files, or needs a diagnostic.
user-invocable: true
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [table-path or symptoms e.g. "/data/my_table" or "compaction stuck"]
---

# Diagnose Hudi Table

User's input: **$ARGUMENTS**

## Instructions

You are a Hudi production operations expert. Help diagnose the table's health.

### If a table path is provided:
Run these diagnostic checks using Spark SQL CALL procedures or direct filesystem inspection:

#### 1. Timeline Health
```sql
-- Show recent commits
CALL show_commits(path => '<table_path>', limit => 20);
-- Show timeline with all action types
CALL show_timeline(path => '<table_path>', limit => 50);
```
Look for:
- Gaps in commit times (indicates failed writes)
- INFLIGHT instants that never completed (stuck operations)
- REQUESTED compactions/clusterings that were never executed
- Ratio of delta_commits to compactions (for MoR tables)

#### 2. Pending Operations
```sql
-- Pending compactions
CALL show_compaction(path => '<table_path>', limit => 50);
-- Pending clustering
CALL show_clustering(path => '<table_path>', limit => 50);
```
Flag: More than 5 pending compactions means compaction is falling behind writes.

#### 3. File System Health
```sql
-- File sizes per partition
CALL stats_file_size(table => '<table_name>');
-- Write amplification
CALL stats_write_amplification(table => '<table_name>');
-- Invalid parquet files
CALL show_invalid_parquet(path => '<table_path>');
```

#### 4. Metadata Table Health
```sql
-- Metadata table stats
CALL show_metadata_table_stats(table => '<table_name>');
-- Validate metadata consistency
CALL validate_metadata_table_files(table => '<table_name>');
```

#### 5. Clean & Archive Status
```sql
CALL show_cleans(path => '<table_path>', limit => 10);
```
Check: Is cleaning keeping up? Are old file versions accumulating?

#### 6. Filesystem-level checks (when Spark SQL is unavailable, adapt for gs://, az://, or hdfs://)
```bash
# Table properties
aws s3 cp s3://my-bucket/my_table/.hoodie/hoodie.properties -

# Recent timeline instants
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep -E '\.(commit|deltacommit|compaction|clean)' | sort | tail -20

# Stuck INFLIGHT operations
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep '.inflight'

# Heartbeat files (stuck writers)
aws s3 ls s3://my-bucket/my_table/.hoodie/.heartbeat/

# Marker files (incomplete writes)
aws s3 ls s3://my-bucket/my_table/.hoodie/.temp/

# Timeline directory size
aws s3 ls --summarize --recursive s3://my-bucket/my_table/.hoodie/ | tail -2
```

### If symptoms are described:
Map symptoms to likely causes:

| Symptom | Likely Cause | Check | Urgency |
|---------|-------------|-------|---------|
| Slow reads | Too many small files, missing compaction | File sizes, pending compactions | P2 |
| Slow writes | Lock contention, too many inline services | Lock config, inline service configs | P1 |
| OOM during compaction | Large log files, wrong memory config | Log file sizes, `hoodie.memory.merge.max.size` | P1 |
| Stuck INFLIGHT | Writer crashed mid-operation | Heartbeat files, rollback needed | P1 |
| Growing .hoodie dir | Archival not keeping up | Archive config, `hoodie.keep.max.commits` | P3 |
| Query returns stale data | Sync lag, metadata stale | Metadata table health, sync status | P2 |
| Missing records | Rollback/failed commit/schema issue | show_rollbacks, schema history | P1 |
| Spark job OOM | Record/file sizes, merge memory | Executor memory, log file sizes | P1 |

### Output format:
1. **Table State Summary** - Key metrics at a glance
2. **Issues Found** - Ordered by severity (P1 critical → P3 low)
3. **Recommended Actions** - Specific commands with safety markers: `[SAFE]` read-only, `[CAUTION]` mutating, `[DANGEROUS]` potential data loss
4. **Verification** - Commands to confirm the fix worked
5. **Preventive Configs** - Settings to prevent recurrence
