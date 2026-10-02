---
name: trace-path
description: Trace Hudi write or read path end-to-end through source code. Use when debugging writes/reads or learning how upsert, insert, snapshot query, or incremental query works internally.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [operation e.g. "upsert", "snapshot query", "incremental read", "bulk_insert"]
---

# Trace Hudi Code Path

Operation: **$ARGUMENTS**

## Instructions

Trace the actual code path by reading source code at each step. Do NOT rely on memory.

### Write path entry points:
1. **Spark SQL**: `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/`
2. **DataFrame API**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala`
3. **Write client**: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java`
4. **Base client**: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java`

### Write phases to trace:
1. Config resolution → 2. Schema resolution → 3. Deduplication → 4. Index lookup → 5. Partition routing / small file handling → 6. File writing → 7. Commit → 8. Table services → 9. Post-commit (archive, metadata, sync)

### Read path entry points:
1. **Spark catalog**: `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/`
2. **DataSource V2**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/`
3. **File index**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`
4. **Merging**: `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java`

### Read phases to trace:
1. Query type resolution → 2. Timeline resolution → 3. Partition pruning → 4. File pruning (data skipping) → 5. File group/slice selection → 6. Base file reading → 7. Log merging (MoR) → 8. Schema evolution → 9. Record merging

### For each step document:
- **Class.method()** with file:line reference
- **What it does** in 1-2 sentences
- **CoW vs MoR divergence** if applicable
