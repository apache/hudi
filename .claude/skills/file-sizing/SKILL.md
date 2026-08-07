---
name: file-sizing
description: Fix Hudi file sizing and write performance. Use when dealing with small files, large files, write amplification, or file size optimization.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [scenario e.g. "too many small files", "write amplification high", "large log files"]
---

# Hudi File Sizing & Performance Tuning

Scenario: **$ARGUMENTS**

## Instructions

### Key concepts (read source code for details):
- **File group**: A set of files (1 base + N log files) sharing a file ID in a partition
- **File slice**: A specific version of a file group at a point in time
- **Small file**: Base file smaller than `hoodie.parquet.small.file.limit` (default: 104857600 = 100MB)
- **Target file size**: `hoodie.parquet.max.file.size` (default: 125829120 = 120MB)

### File sizing configs to analyze:
Read `HoodieStorageConfig.java` and `HoodieWriteConfig.java` for these:
- `hoodie.parquet.max.file.size` - Target base file size
- `hoodie.parquet.small.file.limit` - Below this, file is "small" and gets more inserts routed to it
- `hoodie.record.size.estimation.threshold` - Min records to estimate average record size
- `hoodie.copy.on.write.record.size.estimate` - Fallback record size estimate
- `hoodie.logfile.max.size` - Max log file size before rolling (default: 1GB)
- `hoodie.logfile.data.block.max.size` - Max data block size in log file

### For small file problems:
1. **Diagnosis**: Check file size distribution with `CALL stats_file_size(table => '<name>');`
2. **Root causes**:
   - Too many partitions relative to data volume (partition explosion)
   - Writers with too much parallelism creating many small files
   - Frequent small batch writes
   - Compaction producing small files when data is delete-heavy
3. **Solutions by table type**:
   - **CoW**: Hudi's small file handling routes inserts to existing small files. Tune `small.file.limit` and `max.file.size`
   - **MoR**: Small base files grow via log files. Compaction creates properly-sized new base files
4. **Clustering as fix**: After-the-fact reorganization to merge small files

### For write amplification:
1. Check with `CALL stats_write_amplification(table => '<name>');`
2. High WA causes:
   - CoW table with many updates (every update rewrites entire base file)
   - Switch to MoR for update-heavy workloads
   - Reduce `hoodie.parquet.max.file.size` to limit rewrite cost
3. MoR WA is mainly from compaction - tune compaction frequency

### For large log files:
1. Too many delta commits between compactions
2. Reduce `hoodie.compact.inline.max.delta.commits`
3. Consider log compaction as intermediate step
4. Increase compaction throughput (more resources)

### Output:
1. **Current state assessment**
2. **Root cause** of the sizing issue
3. **Recommended config changes** with specific values
4. **Expected improvement** and tradeoffs
5. **Monitoring** - how to verify the fix is working
