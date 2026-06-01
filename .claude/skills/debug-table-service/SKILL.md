---
name: debug-table-service
description: Debug compaction, clustering, or cleaning issues. Use when table services are stuck, slow, failing, or producing bad results.
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [symptom e.g. "compaction stuck", "clustering OOM", "too many small files after clustering"]
---

# Debug Hudi Table Service

Problem: **$ARGUMENTS**

## Instructions

Identify which table service is involved (compaction, clustering, or cleaning) and debug it.

### Key source locations:
- Compaction: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/`
- Clustering: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/`
- Cleaning: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/clean/`
- Configs: `HoodieCompactionConfig.java`, `HoodieClusteringConfig.java`, `HoodieCleanConfig.java`

### Diagnostic Steps

#### 1. Check service state
```sql
CALL show_compaction(path => '<path>', limit => 20);
CALL show_clustering(path => '<path>', limit => 20);
CALL show_cleans(path => '<path>', limit => 10);
CALL show_timeline(path => '<path>', limit => 50);
```

#### 2. Common compaction issues (MoR only)
- **Not scheduling**: Check `hoodie.compact.inline` and `hoodie.compact.inline.max.delta.commits` (default: 5)
- **Stuck INFLIGHT**: Writer crashed. Rollback: `CALL rollback_to_instant_time(table => '<name>', instant_time => '<time>');`
- **OOM**: Large log files. Tune `hoodie.memory.merge.max.size`, consider `BoundedIOCompactionStrategy`
- **Too many pending**: Writes outpacing compaction. Increase parallelism or move to async
- **Small files after**: Check `hoodie.parquet.max.file.size` (default: 120MB)
- **Log compaction**: `hoodie.compact.inline.log.compact` for intermediate log merging

#### 3. Common clustering issues
- **Not scheduling**: Check `hoodie.clustering.inline` / `hoodie.clustering.async.enabled` and trigger threshold
- **Not improving queries**: Verify `hoodie.clustering.plan.strategy.sort.columns` aligns with query filters
- **OOM**: Reduce `max.bytes.per.group` and `max.num.groups`
- **Conflicts**: Clustering creates REPLACE_COMMIT; check concurrent write conflicts

#### 4. Common cleaning issues
- **Not running**: Check `hoodie.clean.automatic` (default: true)
- **Not freeing space**: Old file versions retained by `hoodie.cleaner.commits.retained` (default: 10)
- **Cleaning blocked**: Pending compaction/clustering blocks cleaning of involved file groups

#### 5. Filesystem-level checks (no Spark needed, adapt for gs://, az://, or hdfs://)
```bash
# Count pending compactions/clustering
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep -c '.compaction.requested'
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep -c '.compaction.inflight'
aws s3 ls s3://my-bucket/my_table/.hoodie/ | grep -c '.replacecommit.requested'

# Check log file sizes in a partition (MoR)
aws s3 ls s3://my-bucket/my_table/<partition>/ | grep '\.log'

# Check heartbeat for stuck writers
aws s3 ls s3://my-bucket/my_table/.hoodie/.heartbeat/
```

### Output
1. **Root cause** with evidence
2. **Fix** — specific commands with safety markers (`[SAFE]`/`[CAUTION]`/`[DANGEROUS]`)
3. **Verification** — commands to confirm the fix worked
4. **Prevention** — configs to avoid recurrence
5. **Escalation** — flag if issue indicates deeper problems (data corruption, version bugs)
