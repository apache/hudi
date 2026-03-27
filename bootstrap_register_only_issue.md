### Feature Description

**What the feature achieves:**
Adds a `REGISTER_ONLY` bootstrap mode that allows Hudi to register existing partitions and their file listings without reading file contents or creating skeleton files. At query time, Hudi natively reads these partitions as plain Parquet, ensuring `SELECT * FROM table` returns complete results across all tiers — no view wrappers or query changes needed. This enables a three-tier bootstrap strategy for onboarding large Hive tables where historical data resides in cold storage (e.g., S3 Glacier, Azure Archive).

**Why this feature is needed:**
Problem: Organizations migrating large Hive tables to Hudi often have a tiered storage layout:
- Recent data (e.g., last 30 days) in hot/standard storage — should be fully rewritten into Hudi
- Warm data (e.g., 30 days to 1 year) in standard storage — suitable for METADATA_ONLY bootstrap
- Cold data (e.g., older than 1 year) in archival/cold storage (S3 Glacier, etc.) — cannot be read without expensive retrieval

Current gaps:
- Bootstrap requires every discovered partition to be either `FULL_RECORD` or `METADATA_ONLY` (enforced by `checkArgument` in `SparkBootstrapCommitActionExecutor.java:292-293`)
- Both modes require reading file contents: `FULL_RECORD` rewrites all data, `METADATA_ONLY` reads every record to extract record keys
- For cold storage, reading file contents triggers data retrieval (e.g., restore from Glacier), which is expensive, slow, and often impractical for terabytes of archival data
- If users bootstrap only recent partitions and skip cold ones entirely, Hudi queries that span into the cold date range silently return incomplete results — **silent data loss**
- The bootstrap epic ([#14665](https://github.com/apache/hudi/issues/14665)) describes "Onboard for new partitions alone" but there is no implementation that safely handles query completeness for skipped partitions

Real scenario:
- A Hive table has 3 years of daily partitions (~1,095 partitions)
- Only the last year of data is in standard storage; older data is in S3 Glacier
- User wants to onboard to Hudi but cannot afford to restore 2+ years of Glacier data just to extract record keys
- With today's bootstrap, the user must either: (a) pay the Glacier retrieval cost for all cold data, or (b) skip old partitions and risk silent data loss on queries

### User Experience

**How users will use this feature:**

Configuration:
```properties
# Use the date-based 3-tier selector
hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.DateBasedBootstrapModeSelector

# Partitions newer than 30 days → FULL_RECORD (full rewrite into Hudi)
hoodie.bootstrap.mode.selector.days.full_record=30

# Partitions between 30 and 365 days → METADATA_ONLY (skeleton files, read warm storage for record keys)
hoodie.bootstrap.mode.selector.days.metadata_only=365

# Partitions older than 365 days → REGISTER_ONLY (no file content reading at all)
# (implicit: anything older than metadata_only threshold)

# Partition date format (to parse partition paths like datestr=2024-01-15)
hoodie.bootstrap.mode.selector.partition.date.format=yyyy-MM-dd
hoodie.bootstrap.mode.selector.partition.date.field=datestr
```

Usage Example — Spark bootstrap:
```scala
spark.emptyDataFrame.write
  .format("hudi")
  .option("hoodie.bootstrap.base.path", "/data/hive_table")
  .option("hoodie.table.name", "my_hudi_table")
  .option("hoodie.datasource.write.operation", "bootstrap")
  .option("hoodie.bootstrap.mode.selector",
    "org.apache.hudi.client.bootstrap.selector.DateBasedBootstrapModeSelector")
  .option("hoodie.bootstrap.mode.selector.days.full_record", "30")
  .option("hoodie.bootstrap.mode.selector.days.metadata_only", "365")
  .option("hoodie.bootstrap.mode.selector.partition.date.format", "yyyy-MM-dd")
  .option("hoodie.bootstrap.mode.selector.partition.date.field", "datestr")
  .mode(SaveMode.Overwrite)
  .save("/data/my_hudi_table")
```

Query behavior — **no query changes needed**:
```sql
-- This returns ALL data: hot (FULL_RECORD) + warm (METADATA_ONLY) + cold (REGISTER_ONLY)
-- No views, no UNION ALL, no special syntax
SELECT * FROM my_hudi_table;

-- Partition filtering works as expected
SELECT * FROM my_hudi_table WHERE datestr >= '2024-01-01';

-- Cold partition queries work, just read as plain Parquet (may have different performance)
SELECT * FROM my_hudi_table WHERE datestr = '2022-06-15';
```

Performance characteristics by tier:

| Tier | Bootstrap cost | Query performance | Hudi meta columns |
|------|---------------|-------------------|-------------------|
| FULL_RECORD (hot) | Full rewrite | Best — native Hudi file | All populated |
| METADATA_ONLY (warm) | Read for record keys | Moderate — skeleton stitching at read time | All populated |
| REGISTER_ONLY (cold) | File listing only (no content read) | Same as plain Parquet | Returned as null |

Write guardrails:
- Upserts/deletes targeting REGISTER_ONLY partitions fail fast with a clear error message
- This is expected: without record keys, Hudi cannot index or merge records in these partitions
- If cold data is later restored to warm/hot storage, partitions can be "promoted" via re-bootstrap

API Changes:

New public APIs:
```java
// New enum value in BootstrapMode
public enum BootstrapMode {
  FULL_RECORD,
  METADATA_ONLY,
  REGISTER_ONLY  // NEW: register partition file listing without reading contents
}

// New selector
public class DateBasedBootstrapModeSelector extends BootstrapModeSelector {
  @Override
  public Map<BootstrapMode, List<String>> select(
      List<Pair<String, List<HoodieFileStatus>>> partitions);
}
```

### Hudi RFC Requirements

**RFC PR link:** (if applicable)

Why RFC is needed:
- Does this change public interfaces/APIs? **Yes**
  - New `REGISTER_ONLY` value in `BootstrapMode` enum
  - New `DateBasedBootstrapModeSelector` class
  - Read path changes for bootstrapped tables to natively serve plain Parquet files without Hudi meta columns
  - New table property to indicate presence of REGISTER_ONLY partitions

- Does this change storage format? **Minor**
  - Bootstrap commit metadata will include REGISTER_ONLY partition entries (file listings without skeleton files)
  - No new file formats; REGISTER_ONLY partitions reference original source Parquet files as-is
  - Backward compatible: tables without REGISTER_ONLY partitions are unaffected

Justification:
- Extends the bootstrap mode enum (affects selectors, executor, read path)
- Read path changes require handling files without Hudi meta columns within a Hudi table
- Needs design review for query completeness and schema merging guarantees
- Affects how Hudi defines table boundaries (a Hudi table now includes "unmanaged" partitions)

### Task Breakdown

**Phase 1: Core Bootstrap Changes (write path)**
- Add `REGISTER_ONLY` to `BootstrapMode` enum (`BootstrapMode.java`)
- Create `DateBasedBootstrapModeSelector` with configurable date thresholds and partition date parsing
- Add config properties to `HoodieBootstrapConfig.java` for date-based tier boundaries
- Modify `SparkBootstrapCommitActionExecutor` to handle 3 modes:
  - Relax `checkArgument` validation (line 292-293) to accept `REGISTER_ONLY` partitions
  - For `REGISTER_ONLY`: record partition file listings in commit metadata without reading file contents or creating skeleton files
  - Skip bootstrap index entries for `REGISTER_ONLY` partitions (`HFileBootstrapIndex`)
- Add table property `hoodie.bootstrap.has.register.only.partitions`
- Unit tests for selector and executor changes

**Phase 2: Read Path — Native Query Completeness (critical)**
- Modify `HoodieBootstrapRelation` (Spark) to handle REGISTER_ONLY partitions:
  - When a base file has no bootstrap index entry AND its schema has no Hudi meta columns → read as plain Parquet
  - Return null for Hudi meta columns (`_hoodie_record_key`, `_hoodie_commit_time`, `_hoodie_partition_path`, `_hoodie_file_name`, `_hoodie_commit_seqno`)
- Handle schema merging: queries spanning multiple tiers must produce a unified schema where Hudi meta columns are nullable
- Ensure partition pruning works correctly for REGISTER_ONLY partitions
- Integration tests verifying:
  - `SELECT *` across all 3 tiers returns complete results
  - `SELECT * WHERE partition_col = <cold_partition>` returns correct data
  - `SELECT * WHERE partition_col = <hot_partition>` performance is unaffected
  - Hudi meta columns are null for REGISTER_ONLY rows, populated for others

**Phase 3: Write Path Guardrails**
- Fail fast when upsert/delete targets a REGISTER_ONLY partition with a clear error message:
  `"Cannot upsert/delete in REGISTER_ONLY bootstrap partition [datestr=2022-06-15]. Re-bootstrap with FULL_RECORD or METADATA_ONLY mode to enable writes."`
- Allow insert-overwrite to "promote" a REGISTER_ONLY partition to a regular Hudi partition (optional, future enhancement)

**Phase 4: Tooling & Documentation** (optional, future)
- CLI command to list partitions by bootstrap mode
- CLI command to "promote" REGISTER_ONLY partitions to METADATA_ONLY or FULL_RECORD (when data is restored from cold storage)
- Documentation and migration guide updates

### Related Issues
- [#14665](https://github.com/apache/hudi/issues/14665) — Efficient bootstrap and migration of existing non-Hudi dataset (parent epic)
- [#15974](https://github.com/apache/hudi/issues/15974) — Treat full bootstrap table as regular table
- [#15856](https://github.com/apache/hudi/issues/15856) — Precombine field is not required for metadata only bootstrap
