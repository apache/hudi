---
name: oncall-triage
description: Triage Hudi production incidents. Use when oncall, paged, or facing a production issue with symptoms like writes failing, queries slow, OOM, storage growing, or data missing.
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [symptom e.g. "writes failing", "queries slow", "OOM in compaction", "storage growing", "missing records"]
---

# Hudi Oncall Triage

Symptom: **$ARGUMENTS**

## Instructions

You are helping an oncall engineer triage a Hudi production incident. Be direct, prioritize speed-to-resolution, and clearly mark which commands are safe vs dangerous.

### Step 1: Classify severity and identify scenario

Map the reported symptom to a scenario and severity:

| Symptom | Scenario | Severity | Immediate action |
|---------|----------|----------|-----------------|
| Writes failing / HoodieWriteConflictException | Write conflict | P1 - data pipeline stopped | Check lock config, identify conflicting writers |
| Writes failing / HoodieCommitException | Commit failure | P1 - data pipeline stopped | Check timeline for stuck INFLIGHT, disk space |
| Writes failing / OOM | Writer resource issue | P1 | Check executor memory, record sizes, batch size |
| Queries slow (>2x normal) | Read degradation | P2 - service degraded | Check small files, pending compactions, metadata table |
| Queries returning stale data | Stale reads | P2 | Check metadata table sync, timeline lag, HMS sync |
| Spark job OOM | Resource exhaustion | P1-P2 | Check compaction/clustering memory, log file sizes |
| Storage growing rapidly | Storage runaway | P3 - cost impact | Check cleaning, archival, file sizing |
| Data missing / wrong counts | Data quality | P1 - data correctness | Check rollbacks, failed commits, schema evolution |
| .hoodie directory huge | Timeline bloat | P3 | Check archival config, manual archive |
| Table unreadable / InvalidTableException | Table corruption | P1 | Check hoodie.properties, timeline consistency |

### Step 2: Quick triage commands

**These are ALL read-only and safe to run anytime:**

#### Without Spark (S3 CLI — adapt for gs://, az://, or hdfs://):
```bash
# Read table properties
aws s3 cp s3://my-bucket/my_table/.hoodie/hoodie.properties -

# Check active timeline (list recent instants)
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep -E '\.(commit|deltacommit|compaction|clean|rollback|replacecommit)' | sort | tail -20

# Check for stuck INFLIGHT operations
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep '.inflight'

# Check for REQUESTED operations waiting to execute
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep '.requested'

# Check heartbeat files (stuck writers leave these)
aws s3 ls s3://my-bucket/my_table/.hoodie/.heartbeat/

# Check marker files (incomplete writes leave these)
aws s3 ls s3://my-bucket/my_table/.hoodie/.temp/

# Check .hoodie directory size (timeline bloat)
aws s3 ls --summarize --recursive s3://my-bucket/my_table/.hoodie/ | tail -2

# Check archived timeline size
aws s3 ls --summarize --recursive s3://my-bucket/my_table/.hoodie/archived/ | tail -2

# Check partition sizes (top-level)
aws s3 ls s3://my-bucket/my_table/ --recursive --summarize
# Or per-partition file count
aws s3 ls s3://my-bucket/my_table/<partition_path>/ | wc -l
aws s3 ls s3://my-bucket/my_table/<partition_path>/ | awk '{print $3}' | sort -n
```

#### With Spark SQL (more detailed):
```sql
-- SAFE: Timeline overview
CALL show_timeline(path => '<path>', limit => 50);

-- SAFE: Recent commits with write stats
CALL show_commits(path => '<path>', limit => 20);

-- SAFE: Pending compactions (MoR tables)
CALL show_compaction(path => '<path>', limit => 50);

-- SAFE: Pending clustering
CALL show_clustering(path => '<path>', limit => 50);

-- SAFE: Recent cleans
CALL show_cleans(path => '<path>', limit => 10);

-- SAFE: File size distribution
CALL stats_file_size(table => '<table_name>');

-- SAFE: Metadata table health
CALL validate_metadata_table_files(table => '<table_name>');
CALL show_metadata_table_stats(table => '<table_name>');

-- SAFE: Check for corrupted files
CALL show_invalid_parquet(path => '<path>');

-- SAFE: Recent rollbacks (indicates failed operations)
CALL show_rollbacks(path => '<path>', limit => 10);
```

### Step 3: Scenario-specific runbooks

#### Writes Failing
1. Check for stuck INFLIGHT instants → rollback if heartbeat expired
2. Check lock provider config if multi-writer
3. Check disk space on driver/executors
4. Check if table version matches Hudi library version
5. **Mitigation**: Rollback stuck instant (see below), restart writer

#### Queries Slow
1. Count pending compactions — if >5, compaction is behind
2. Check file size distribution — many files <100MB = small file problem
3. Check if metadata table is enabled and in sync
4. Check column stats if data skipping is expected
5. **Mitigation**: Trigger manual compaction/clustering, rebuild metadata table

#### OOM in Compaction/Clustering
1. Check log file sizes (`aws s3 ls s3://my-bucket/my_table/<partition>/` — look for .log files)
2. Check `hoodie.memory.merge.max.size` (default 1GB)
3. Check executor memory allocation
4. **Mitigation**: Use BoundedIOCompactionStrategy, increase memory, reduce parallelism

#### Storage Growing
1. Check if cleaning is running (`show_cleans`)
2. Check `hoodie.cleaner.commits.retained` (default: 10) — higher = more storage
3. Check if archival is running — `hoodie.keep.max.commits` (default: 30)
4. Check for dangling files not referenced by timeline
5. **Mitigation**: Trigger clean, reduce retained commits, archive old instants

#### Data Missing / Wrong Counts
1. Check for recent rollbacks (`show_rollbacks`)
2. Check for recent `replacecommit` (clustering/insert_overwrite can drop data if buggy)
3. Verify schema — column renames/drops can cause apparent data loss
4. Check if queries are hitting correct snapshot (time-travel, incremental)
5. **Mitigation**: DO NOT write until root cause identified. Compare data at different instants.

#### Table Unreadable
1. Read `hoodie.properties` — check table version, type, name
2. Check for missing instant files (COMPLETED without matching plan)
3. Validate metadata table
4. **Mitigation**: Repair metadata, restore from savepoint if available

### Step 4: Mitigation commands

**CAUTION markers:**
- `[SAFE]` = read-only, no side effects
- `[REVERSIBLE]` = makes changes but can be undone
- `[CAUTION]` = makes changes, verify carefully before running
- `[DANGEROUS]` = potential data loss, require explicit approval

```sql
-- [REVERSIBLE] Rollback a stuck INFLIGHT instant
CALL rollback_to_instant_time(table => '<name>', instant_time => '<stuck_instant>');

-- [REVERSIBLE] Rollback all stuck inflight table services
CALL run_rollback_inflight_table_service(table => '<name>');

-- [CAUTION] Trigger compaction to catch up
CALL run_compaction(op => 'scheduleandexecute', path => '<path>');

-- [CAUTION] Trigger clustering
CALL run_clustering(op => 'scheduleandexecute', path => '<path>');

-- [CAUTION] Trigger cleaning
CALL run_clean(table => '<name>');

-- [CAUTION] Archive old commits
CALL archive_commits(table => '<name>');

-- [CAUTION] Rebuild metadata table (deletes and recreates)
CALL delete_metadata_table(table => '<name>');
-- Next write automatically rebuilds it

-- [REVERSIBLE] Create savepoint before risky operations
CALL create_savepoint(table => '<name>', commit_time => '<instant>');

-- [DANGEROUS] Restore to savepoint (rolls back all commits after savepoint)
CALL rollback_to_savepoint(table => '<name>', instant_time => '<savepoint_instant>');
```

### Step 5: Verify fix

After applying mitigation:
```sql
-- Confirm timeline is clean (no stuck INFLIGHT)
CALL show_timeline(path => '<path>', limit => 20);

-- Confirm writes succeed (check new commits appear)
CALL show_commits(path => '<path>', limit => 5);

-- Confirm query works and returns expected data
SELECT count(*) FROM <table>;

-- Confirm metadata table is consistent
CALL validate_metadata_table_files(table => '<name>');
```

### Step 6: When to escalate

Escalate to the Hudi team or senior oncall if:
- **Data corruption confirmed** — records missing that shouldn't be
- **Timeline corruption** — missing instant files, inconsistent state transitions
- **Repeated OOM** after memory tuning — may indicate a bug
- **Savepoint restore needed** — involves potential data loss
- **Table version mismatch** — requires upgrade/downgrade procedure
- **Unknown exceptions** not in the common exception table

### Output format
1. **Severity**: P1/P2/P3
2. **Scenario**: What's happening
3. **Root cause**: Why it's happening
4. **Immediate mitigation**: Exact commands with safety markers
5. **Verification**: How to confirm the fix
6. **Follow-up**: Configs to prevent recurrence, monitoring to add
