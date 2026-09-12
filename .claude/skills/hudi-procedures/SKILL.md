---
name: hudi-procedures
description: Find and explain Hudi Spark SQL CALL procedures. Use when asking about available procedures or how to run table operations via SQL.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [procedure name or operation e.g. "show_commits", "run compaction", "repair"]
---

# Hudi Spark SQL Procedures

Query: **$ARGUMENTS**

## Instructions

All procedures are registered in:
`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/HoodieProcedures.scala`

Individual procedure implementations are in:
`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/`

### If asking about a specific procedure:
1. Find the procedure class in the procedures directory
2. Read the `parameters()` method to get all input parameters (name, type, required/optional)
3. Read the `outputType()` method to understand what columns are returned
4. Read the `call()` method to understand what it does
5. Provide:
   - **Syntax**: `CALL <procedure_name>(param1 => value1, param2 => value2)`
   - **Parameters**: table with name, type, required/optional, description
   - **Output columns**: what the result set contains
   - **Example**: concrete usage example
   - **When to use**: practical scenarios

### If asking about an operation (e.g. "repair"):
1. Search for related procedures by topic
2. List all relevant procedures with brief descriptions
3. Show the recommended workflow (which to run in what order)

### Procedure categories for reference:
- **Timeline inspection**: show_commits, show_archived_commits, show_commit_files, show_commit_partitions, show_commit_write_stats, show_commit_extra_metadata, show_timeline
- **Compaction**: run_compaction (schedule/execute), show_compaction
- **Clustering**: run_clustering, show_clustering
- **Cleaning**: run_clean, show_cleans, show_cleans_plan
- **Rollback/Savepoint**: rollback_to_instant_time, rollback_to_savepoint, show_rollbacks, create_savepoint, delete_savepoint, show_savepoints, run_rollback_inflight_table_service
- **Metadata table**: create_metadata_table, init_metadata_table, delete_metadata_table, show_metadata_table_files, show_metadata_table_partitions, show_metadata_table_column_stats, show_metadata_table_stats, validate_metadata_table_files
- **File system**: show_file_system_view, show_file_status, show_fs_path_detail, show_hudi_log_file_metadata, show_hudi_log_file_records, show_invalid_parquet, show_column_stats_overlap
- **Statistics**: stats_file_size, stats_write_amplification
- **Repair**: repair_add_partition_meta, repair_deduplicate, repair_migrate_partition_meta, repair_corrupted_clean_files, repair_overwrite_hoodie_props
- **Bootstrap**: run_bootstrap, show_bootstrap_mapping, show_bootstrap_partitions
- **Maintenance**: upgrade_table, downgrade_table, archive_commits, run_ttl, truncate_table, drop_partition
- **Sync**: hive_sync, validate_hudi_sync
- **Data operations**: copy_to_table, copy_to_temp_view, export_instants, delete_marker
- **Locking**: set_audit_lock, show_audit_lock_status, validate_audit_lock, cleanup_audit_lock

### Usage pattern:
```sql
-- Named parameters (recommended)
CALL hudi.default.show_commits(path => '/data/my_table', limit => 10);

-- Positional parameters
CALL hudi.default.run_compaction('schedule', '/data/my_table');
```
