---
name: hudi-metadata-table
description: Explain or troubleshoot the Hudi metadata table. Use when metadata is slow, corrupt, column stats not working, or data skipping is ineffective.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [question e.g. "enable metadata table", "column stats not working", "metadata table rebuild"]
---

# Hudi Metadata Table

Question: **$ARGUMENTS**

## Instructions

### Key source files:
- Config: `hudi-common/src/main/java/org/apache/hudi/common/config/HoodieMetadataConfig.java`
- Metadata table implementation: search for `HoodieBackedTableMetadata` and `HoodieMetadataPayload`
- Metadata partitions: search for `MetadataPartitionType` enum
- File listing: search for `HoodieMetadataFileSystemView`

### Metadata table partitions:
1. **FILES** - File listing per partition (avoids cloud storage LIST calls)
2. **COLUMN_STATS** - Per-file column min/max/null stats (enables data skipping)
3. **BLOOM_FILTERS** - Per-file bloom filters (accelerates index lookups)
4. **RECORD_INDEX** - Record-to-file mapping (point lookup index)
5. **SECONDARY_INDEX** - Secondary index data
6. **PARTITION_STATS** - Per-partition aggregate stats
7. **EXPRESSION_INDEX** - Expression-based index data

### Key configs:
- `hoodie.metadata.enable` (default: true) - Master switch
- `hoodie.metadata.index.column.stats.enable` - Enable column stats partition
- `hoodie.metadata.index.bloom.filter.enable` - Enable bloom filter partition
- `hoodie.metadata.record.index.enable` - Enable record-level index
- `hoodie.metadata.index.column.stats.column.list` - Which columns to index
- `hoodie.metadata.compact.max.delta.commits` - Compaction frequency for metadata table

### Common issues:

**Metadata table out of sync:**
- Symptoms: queries return stale data, file listing mismatches
- Diagnosis: `CALL validate_metadata_table_files(table => '<name>');`
- Fix: Delete and rebuild: `CALL delete_metadata_table(table => '<name>');`

**Metadata table slow:**
- Too many delta commits between compactions
- Tune `hoodie.metadata.compact.max.delta.commits` (default: 10)
- Check metadata table size with `CALL show_metadata_table_stats(table => '<name>');`

**Column stats not working (queries still slow):**
- Verify column stats partition is enabled
- Check the column list includes query filter columns
- Verify `hoodie.metadata.index.column.stats.column.list` is set
- Check `CALL show_metadata_table_column_stats(table => '<name>', column_name => '<col>');`

**Data skipping not kicking in:**
- Column stats must exist for the filter column
- Predicate must be pushdown-compatible (=, <, >, IN, etc.)
- Check column stats overlap: high overlap means poor data skipping
- `CALL show_column_stats_overlap(table => '<name>', columns => '<col>');`

### Output:
1. **Explanation** or **diagnosis** depending on the question
2. **Config recommendations** for the user's use case
3. **Verification steps** to confirm metadata table is working correctly
