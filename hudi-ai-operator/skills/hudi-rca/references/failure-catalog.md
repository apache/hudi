<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Hudi failure catalog

38 failure patterns, each with the signature that identifies it, the mechanism behind it, and an
ordered mitigation ladder. Used by the `hudi-rca` skill to classify a pasted failure; readable on its
own as a troubleshooting reference.

## How to read a row

| Field | Meaning |
|---|---|
| **ID** | Stable identifier. Never renumber; retire instead. |
| **Category** | `input/config` · `sizing/scale` · `environment` · `defect` · `not-a-failure` |
| **Discoverability** | `self-evident` (the message says what to do) · `needs-expertise` (the message is generic, misleading, or points at the wrong layer) · `silent` (no exception at all) |
| **Signature** | Exception classes, message regexes, and the Hudi frames that identify the pattern. |
| **Symptom** | What the operator sees, in their terms. |
| **Root cause** | The mechanism, in one or two sentences. |
| **Evidence required** | Which rungs of the evidence ladder are needed to reach a confident call. |
| **Disambiguation** | Present only where a signature maps to more than one pattern. The explicit discriminator. |
| **Mitigation ladder** | Ordered, cheapest and most likely first. Counter-intuitive steps are marked and justified. |
| **Governing configs** | Exact `hoodie.*` / `spark.*` keys. |
| **`fixedInVersion`** | The Hudi release that fixes the defect, where one exists. |
| **`preflightGuardExists`** | Whether a current Hudi release fails fast on the condition instead of corrupting silently. |
| **`alsoAppliesWhenHealthy`** | `true` when the same advice is an optimization on a merely-slow job, not only a fix for a broken one. |
| **Provenance** | Public `apache/hudi` issue numbers. |

## Three rules that govern every row

**1. Match the innermost `Caused by`, never the wrapper.** In the public tracker the wrapper classes
dominate the counts — bare `HoodieException` 498 occurrences, `HoodieUpsertException` 189 — while the
classes that actually identify the fault barely appear: `SchemaCompatabilityException` 1,
`HoodieAvroSchemaException` 2, `HoodieKeyGeneratorException` 3. Users paste the wrapper. Every regex
below is written against the innermost cause plus the first Hudi frame, because a classifier keyed on
the top-level exception class will mostly see wrappers and learn nothing.

**2. The stacktrace is the weakest signal, not the backbone.** For a large share of these the terminal
exception is generic and the real cause is table state or config sizing several steps removed. A
`413 Payload Too Large` on timeline init is really a forgotten savepoint blocking archival. A stalled
ingestion is really a rollback that never finished and still holds the lock. Where a row says the
evidence requires table state, the honest move is to run `HoodieTableHealthChecker` rather than parse
the trace harder.

**3. Ask the Hudi version early.** The heavy issue clusters are 0.9 through 0.15, and several
high-traffic patterns here are fixed outright in 1.0 or 1.2. For this population "upgrade" is a
genuinely good first-line remedy in a way it usually is not. Rows carry `fixedInVersion` where known.

## Disambiguation index

Four signatures map to more than one pattern. Resolve them here before attributing.

| Signature | Candidates | Discriminator |
|---|---|---|
| `ConcurrentModificationException` / `HoodieWriteConflictException` | HUDI-RCA-006 healthy OCC retry **vs** HUDI-RCA-006 no lock provider | **Writer config, not the trace** — the stacktrace is identical. If `hoodie.write.lock.provider` is set and two writers are intended, this is a by-design rejection: retry the application attempt. If no lock provider is set and two writers exist, the table is being corrupted silently and the conflict exception is the lucky case. |
| `HoodieRollbackException` | HUDI-RCA-027 rollback wedge · HUDI-RCA-026 metadata-table divergence · HUDI-RCA-025 dangling files | **The frame below `rollbackFailedWrites`.** `AsyncCleanerService.waitForCompletion` → cleaner/metadata-table path (026). A lock-provider frame (`LockManager.lock`, `*LockProvider.acquireLock`) → lock problem first (006). `HoodieMergeHandle.getLatestBaseFile` with `FileID ... does not exist` → 025. |
| `OutOfMemoryError` | HUDI-RCA-015 merge-side · HUDI-RCA-015 archival-side · HUDI-RCA-014 bloom index · HUDI-RCA-021 driver/timeline-server | **The Hudi frame.** `HoodieMergedLogRecordScanner` / `ExternalSpillableMap` → merge side (015a). `HoodieTimelineArchiveLog.writeToFile` / `HoodieAvroDataBlock.serializeRecords` → **archival side (015b), where adding executor memory is the wrong answer entirely** — the fix is to shrink commit metadata. Kryo / `ExternalSorter` frames under a shuffle read with no Hudi frame → bloom index (014). `Requested array size exceeds VM limit` in a driver Jetty thread → 021, a structural ceiling that more heap cannot move. |
| `FileNotFoundException` | HUDI-RCA-031 markers · HUDI-RCA-033 archiver race · HUDI-RCA-025 stale plan · HUDI-RCA-036 source file | **The path.** `MARKERS*` or `.hoodie/.temp/` → 031. A `.commit` / `.deltacommit` under `.hoodie/` → 033. A data or log file named in a compaction plan → 025. A path under the **source** bucket, not under the table → 036. A path under the table ending `.log.<n>` → treat as a compaction race, see 025's note. |

## Cross-cutting trap: MOR defers the failure

On Merge-on-Read an incompatible schema write **succeeds**, and compaction fails hours later in a
*different job* whose stacktrace names `HoodieCompactor` rather than the write. The table then cannot
compact and every later write fails. Cause and symptom are separated by both time and process.

**Rule: a compaction frame plus a schema exception means the bad write happened earlier, in another
job. Investigate the writes from that window, not the job that just failed.** This applies to
HUDI-RCA-001, HUDI-RCA-003 and HUDI-RCA-004.

---

# Group A — input/config

The user asked Hudi to do something invalid, or configured it inconsistently. The remedy is to fix
data or config; nothing is wrong with Hudi or the cluster.

---

## HUDI-RCA-001 — Schema evolution the writer rejects

- **Category:** `input/config` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.avro\.AvroTypeException: Cannot encode decimal with precision (\d+) as max precision (\d+)`
  - `org\.apache\.parquet\.io\.InvalidRecordException: Parquet/Avro schema mismatch: Avro field '([^']+)' not found`
  - Hudi frames: `HoodieAvroUtils.rewriteRecordWithNewSchema`, `HoodieAvroUtils.rewritePrimaryTypeWithDiffSchemaType`, `HoodieMergeHelper.runMerge`, `AbstractHoodieLogRecordReader.scanInternal`, `HoodieCompactor.compact`
- **Symptom:** A write that worked yesterday fails after an upstream column was retyped or a decimal
  widened. On MOR the write succeeds and **compaction** fails hours later, permanently, for every
  subsequent write.
- **Root cause:** The incoming Avro schema is not a backward-compatible evolution of the table schema
  under Hudi's rules. Full schema-on-read evolution is off by default, and decimal precision widening
  was rejected outright until the `SchemaChangeUtils.isTypeUpdateAllow` relaxation.
- **Evidence required:** stacktrace · writer props · `hoodie.properties` (table schema) · for the MOR
  variant, table state to show which file slices span the change.
- **Disambiguation:** a compaction frame here means the offending write already committed — see the
  MOR deferred-failure rule above. Distinguish from HUDI-RCA-003 by *direction*: a field the table has
  and the batch lacks is 003; a field whose **type** changed is 001.
- **Mitigation ladder:**
  1. Upgrade if the signature is decimal precision — fixed in 1.0.x, and no config works around it on
     0.14/0.15.
  2. Set `hoodie.datasource.write.reconcile.schema=true`, with `hoodie.schema.on.read.enable=true`.
  3. Stop dropping or retyping the column in the writer: keep it and write typed nulls.
  4. If MOR and already wedged, the table cannot compact until the pending compaction is unscheduled
     or the affected file groups are rewritten. Do not hand-delete the pending instant files.
- **Governing configs:** `hoodie.datasource.write.reconcile.schema`, `hoodie.schema.on.read.enable`,
  `hoodie.avro.schema.validate`, `hoodie.datasource.write.schema.allow.auto.evolution.column.drop`
- **`fixedInVersion`:** 1.0.0 for decimal precision widening · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #8040, #5452, #11335, #8020, #2919

---

## HUDI-RCA-002 — Record key or precombine field missing, null, or wrong case

- **Category:** `input/config` · **Discoverability:** `self-evident` for the key case, `needs-expertise` for precombine
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieKeyException: recordKey value: "null" for field: "([^"]+)" cannot be null or empty`
  - `org\.apache\.hudi\.exception\.HoodieKeyException: Record key has to be non-null!`
  - `org\.apache\.hudi\.exception\.HoodieException: \w+\(Part -\w+\) field not found in record\. Acceptable fields were :\[`
  - Hudi frames: `HoodieAvroUtils.getNestedFieldVal`, `ComplexKeyGenerator`, often wrapped by
    `SparkException: Failed to execute user defined function` so the top frame is Spark, not Hudi.
- **Symptom:** The write fails on the first batch, or only when one composite key field is null.
- **Root cause:** Three sub-cases sharing a remedy family: the configured field does not exist or is
  null in some rows; the key config is **case-sensitive** and the user wrote `ID` for a column named
  `id`; or HoodieStreamer defaulted `--source-ordering-field` to `ts`, producing a phantom `ts`
  requirement that then persisted into `hoodie.properties`.
- **Evidence required:** stacktrace · writer props · `hoodie.properties`
- **Disambiguation:** if the named field is one the user never configured (classically `ts`), this is
  the HoodieStreamer default, not a missing column. The message names a field the user has never heard
  of, which is the whole confusion — say so explicitly rather than asking them to add a `ts` column.
- **Mitigation ladder:**
  1. Check the exact field name **and case** against the dataframe schema. Hudi does not normalize case.
  2. Filter or default nulls in the key columns before writing.
  3. For HoodieStreamer with no ordering field: set `--source-ordering-field` to a real column, or use
     `--op INSERT` / `bulk_insert`, which skip precombine entirely.
  4. Multi-field keys with `bulk_insert` had a genuine bug before 0.13.1 — upgrade, or switch to
     `upsert` to confirm.
- **Governing configs:** `hoodie.datasource.write.recordkey.field`,
  `hoodie.datasource.write.precombine.field`, `hoodie.datasource.write.keygenerator.class`,
  `hoodie.table.precombine.field`, `--source-ordering-field`
- **`fixedInVersion`:** 0.13.1 for the bulk_insert multi-field key bug · **`preflightGuardExists`:** partial — the key check fires at write time, not at config time
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #3759, #9799, #11776, #10233

---

## HUDI-RCA-003 — Source drops a column the table still has

- **Category:** `input/config` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.parquet\.io\.InvalidRecordException: Parquet/Avro schema mismatch: Avro field '([^']+)' not found`
  - Frames, in order: `AvroRecordConverter.getAvroField` → `ParquetReaderIterator.hasNext` →
    `HoodieMergeHelper.runMerge`
  - Wrapped by `HoodieUpsertException: Error upserting bucketType UPDATE for partition :\d+`
- **Symptom:** Every sync fails after an upstream schema change. The exception arrives on the **merge**
  path while *reading existing Parquet*, so operators look at their table instead of their batch.
- **Root cause:** The incoming batch no longer carries a column the table's base files have. Hudi reads
  existing records using the latest (incoming) schema, and the Parquet-to-Avro converter cannot find
  the field.
- **Evidence required:** stacktrace · writer props · last commit metadata (`extraMetadata.schema`) for
  the field's declared type
- **Disambiguation:** same exception class as HUDI-RCA-001's field-not-found variant. The
  `HoodieMergeHelper.runMerge` frame and the fact that the field exists in the **table** but not the
  **batch** is what makes it 003. Recover the field's declared type including its Avro `logicalType`
  before re-adding it, or the re-add fails differently.
- **Mitigation ladder:**
  1. Re-add the column to the incoming batch as a **typed** null via a transformer — `cast(null as
     string)`, or `cast(null as date)` where the logical type demands it. Cheapest and reversible.
  2. Set `hoodie.avro.schema.validate=false` only as a stopgap, not as the fix.
  3. **Config-layering trap:** if your config system overrides rather than merges list-valued settings,
     adding a transformer silently drops the transformer that was already configured. Re-list all of
     them, and verify the resolved config rather than the file you edited.
- **Governing configs:** `hoodie.avro.schema.validate`,
  `hoodie.datasource.write.schema.allow.auto.evolution.column.drop`,
  `hoodie.datasource.write.reconcile.schema`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #8040

---

## HUDI-RCA-004 — Unvalidated source schema: records do not parse against the inferred type

- **Category:** `input/config` · **Discoverability:** `self-evident` for the message, `needs-expertise` for the fix
- **Signature** (innermost cause):
  - `org\.apache\.spark\.sql\.catalyst\.util\.BadRecordException`
  - `java\.lang\.RuntimeException: Failed to parse a value for data type struct<.*> \(current token: \w+\)`
- **Symptom:** Records fail to parse and the whole job fails, rather than the bad rows being isolated.
- **Root cause:** A semi-structured source (CDC, JSON, document-store feed) emits a field whose actual
  shape differs from what was inferred from earlier records — most often an array where a struct was
  inferred.
- **Evidence required:** stacktrace · writer props (schema-provider config) · a raw source sample
- **Mitigation ladder:**
  1. Set an explicit **target schema** instead of relying on inference, and enable validation with a
     quarantine destination so bad records are isolated rather than failing the commit.
  2. For CDC sources, make sure the target schema declares the envelope fields the transformer adds
     (operation type, transaction id, LSN, event timestamps) — otherwise validation rejects every record.
  3. Loosen validation only as a last resort. It converts a loud failure into a silent one.
- **Governing configs:** `hoodie.streamer.schemaprovider.target.schema.file`,
  `hoodie.streamer.schemaprovider.source.schema.file`, `hoodie.avro.schema.validate`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public — the mechanism is config hygiene and rarely filed

---

## HUDI-RCA-005 — Timestamp key generator cannot parse the partition field

- **Category:** `input/config` · **Discoverability:** `self-evident`
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieKeyGeneratorException: Unable to parse input partition field: (.+)`
  - `java\.lang\.IllegalArgumentException: Invalid format: "(.+)" is malformed at "(.+)"`
  - Hudi frame: `TimestampBasedAvroKeyGenerator.getPartitionPath`
- **Symptom:** Every write fails immediately, naming the exact offending value.
- **Root cause:** The configured **input** date format does not match the actual partition-field format.
  Typical triggers: microsecond precision, a trailing `Z`, or a switch from epoch to ISO-8601 upstream.
- **Evidence required:** stacktrace alone suffices · writer props to confirm the configured format
- **Mitigation ladder:**
  1. Update the key generator's **input** format config to match the value printed in the exception.
  2. Verify the **output** partition format is unchanged. Changing the output format alters partition
     paths and effectively forks the table — a far worse outcome than the parse failure.
  3. If the source format is now inconsistent across records, normalize it in a transformer before the
     key generator sees it.
- **Governing configs:** `hoodie.keygen.timebased.input.dateformat`,
  `hoodie.keygen.timebased.output.dateformat`, `hoodie.keygen.timebased.timestamp.type`,
  `hoodie.keygen.timebased.input.timezone`, `hoodie.keygen.timebased.output.timezone`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public with this exact signature; included because it is trivially fixable and
  almost certainly under-reported

---

## HUDI-RCA-006 — Multi-writer without, or with a misconfigured, lock provider

- **Category:** `input/config` · **Discoverability:** `needs-expertise`, and **`silent`** in the no-lock-at-all case
- **Signature** (innermost cause):
  - `java\.util\.ConcurrentModificationException: Cannot resolve conflicts for overlapping writes`,
    wrapped in `HoodieWriteConflictException`; frames
    `SimpleConcurrentFileWritesConflictResolutionStrategy.resolveConflict`,
    `TransactionUtils.resolveWriteConflictIfAny`, `SparkRDDWriteClient.preCommit`
  - `org\.apache\.hudi\.exception\.HoodieLockException: Unable to acquire lock, lock object (null|LockResponse\(lockid:\d+, state:WAITING\))`;
    frames `LockManager.lock`, `TransactionManager.beginTransaction`
  - `java\.lang\.IllegalArgumentException: ALREADY_ACQUIRED` at `HiveMetastoreBasedLockProvider.acquireLock`
  - `org\.apache\.hadoop\.fs\.FileAlreadyExistsException: File already exists: .*\.hoodie/\d+\.commit\.requested`
    at `HoodieActiveTimeline.createImmutableFileInPath`
- **Symptom:** Intermittent failures that scale with writer count. With **no** lock provider there may
  be no exception at all — just overwritten timeline state and lost data.
- **Root cause:** Two writers against one table without OCC properly configured, or OCC configured and
  genuinely conflicting. The semantics users most often get wrong: **the OCC lock is table-level, not
  partition-level**, and it serializes only the commit-validation phase. Writers on disjoint partitions
  still serialize on the lock.
- **Evidence required:** stacktrace · **writer props — mandatory** · `hoodie.properties`
- **Disambiguation (the most important row in this catalog):** the conflict exception and the no-lock
  corruption produce the **same stacktrace**. Only the writer config splits them.
  - `hoodie.write.lock.provider` set, concurrency intended → by-design rejection of the later committer.
    Expected. Add orchestrator-level retry of the whole application attempt. Severity: routine.
  - No lock provider set, but two writers demonstrably exist → the table is being corrupted and this
    exception is the *lucky* case where you found out. Severity: urgent.
  - Treat "is a second writer intended?" as a question to ask the user, not something to infer.
- **Mitigation ladder:**
  1. If two writers exist and no lock provider is set, set one. Non-negotiable; absence is silent
     corruption, not a performance choice.
  2. **Lower** `hoodie.write.lock.wait_time_ms` to around 5000. Counter-intuitive: a long wait converts
     a fast, retryable rejection into a long stall that looks like a hang, and the later writer has to
     retry regardless.
  3. For DynamoDB locks, set the endpoint URL explicitly and use on-demand billing; stale lock rows
     occasionally need manual removal.
  4. Add retry of the whole application attempt at the orchestrator. The later committer is rejected by
     design and must retry.
  5. Where lock serialization is itself the bottleneck on MOR, consider 1.x non-blocking concurrency
     control, which pairs best with a bucket index.
- **Governing configs:** `hoodie.write.concurrency.mode`, `hoodie.write.lock.provider`,
  `hoodie.write.lock.wait_time_ms`, `hoodie.write.lock.num_retries`, `hoodie.write.lock.dynamodb.*`,
  `hoodie.write.lock.zookeeper.*`, `hoodie.cleaner.policy.failed.writes`
- **`fixedInVersion`:** n/a — NBCC added in 1.x as an alternative mode, not a fix
- **`preflightGuardExists`:** no — Hudi does not detect "two writers, no lock" at write time
- **`alsoAppliesWhenHealthy`:** true — lock wait tuning is an optimization on a merely-slow multi-writer setup
- **Provenance:** #7653, #3731, #4456, #6226, #9213

---

## HUDI-RCA-007 — Hive or Glue catalog sync failure: table written but invisible

- **Category:** `input/config` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.RuntimeException: Unable to instantiate org\.apache\.hadoop\.hive\.ql\.metadata\.SessionHiveMetaStoreClient`;
    frames `HMSDDLExecutor.<init>`, `HiveSyncTool.initClient`
  - `NoSuchObjectException: .* table not found`
  - Wrapped in `HoodieHiveSyncException: Got runtime exception when hive syncing` or
    `Could not sync using the meta sync class`
  - Log giveaway: `Trying to connect to metastore with URI thrift://localhost:9083` when the user
    believes they configured Glue
- **Symptom:** Data lands on storage correctly but the query engine cannot see it, or the write fails
  only in the post-commit sync step.
- **Root cause:** Sync mode and catalog wiring mismatch, Hive metastore version incompatibility, or
  table-name case sensitivity. The `localhost:9083` line is the tell that the intended catalog config
  never took effect.
- **Evidence required:** stacktrace · writer props · driver log (for the metastore URI line)
- **Mitigation ladder:**
  1. Lowercase the table and database names everywhere. An uppercase table name causes a rename failure.
  2. Set `hoodie.datasource.hive_sync.mode` explicitly — `hms` for Glue and HMS.
  3. For Glue, set the AWS Glue sync tool class explicitly rather than relying on inference.
  4. Verify the Hive metastore version is 2.3 or newer for Spark 3 bundles.
  5. Confirm `hoodie.datasource.hive_sync.partition_extractor_class` matches the actual partitioning —
     the non-partitioned extractor for unpartitioned tables, the multi-part extractor for multi-level.
- **Governing configs:** `hoodie.datasource.hive_sync.enable`, `hoodie.datasource.hive_sync.mode`,
  `hoodie.datasource.hive_sync.database`, `hoodie.datasource.hive_sync.table`,
  `hoodie.datasource.hive_sync.partition_extractor_class`, `hoodie.datasource.hive_sync.partition_fields`,
  `hoodie.meta.sync.client.tool.class`, `hoodie.datasource.write.hive_style_partitioning`
- **`fixedInVersion`:** 0.11.1 for the Glue client-factory regression · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #5484, #8368, #2409, #954

---

## HUDI-RCA-008 — Config changed between runs against an existing table

- **Category:** `input/config` · **Discoverability:** `self-evident` when the guard fires, **`silent`** when it does not
- **Signature:**
  - `HoodieException: Config conflict \(key, current value, existing value\): ` — when Hudi's guard
    catches it
  - When the guard does not catch it: **no exception**. Duplicates, no-op deletes, or an unqueryable
    table.
- **Symptom:** A pipeline that ran for months starts failing, or silently producing duplicates, after
  someone edited the writer config or upgraded Hudi.
- **Root cause:** Several table-level properties are written into `hoodie.properties` on first write and
  are effectively **immutable**: record key field, partition path field, key generator class, table
  type, precombine field, hive-style partitioning. Changing them later either trips the conflict guard
  or silently reinterprets existing records.
- **Evidence required:** `hoodie.properties` · writer props. **This pattern is definitionally a diff
  between the two** — no stacktrace substitutes for it, and this is why the skill requests
  `hoodie.properties` by default.
- **Disambiguation:** overlaps HUDI-RCA-013 (silent duplicates). If the properties diff shows a changed
  key generator or record-key field, the duplicates are 008's mechanism and 013's symptom. Report 008 —
  it names the cause.
- **Mitigation ladder:**
  1. Read `.hoodie/hoodie.properties` and diff it against the current writer config. This is step one of
     nearly any Hudi triage, not just this pattern.
  2. Revert the changed property to match the table.
  3. If the change is genuinely required, rewrite the table into a new path with `bulk_insert` rather
     than fighting the guard. Editing `hoodie.properties` by hand is not a migration.
  4. For upgrade-induced key-encoding changes, pin the old version or rewrite. A single-field record key
     under `ComplexKeyGenerator` was encoded differently from 0.14.1 onward, which silently created
     duplicates and silently no-op'd deletes.
- **Governing configs:** `hoodie.table.recordkey.fields`, `hoodie.table.partition.fields`,
  `hoodie.table.keygenerator.class`, `hoodie.table.type`, `hoodie.table.precombine.field`,
  `hoodie.table.version`, `hoodie.datasource.write.hive_style_partitioning`
- **`fixedInVersion`:** no — the 0.14.1 key-encoding change shipped without a migration path
- **`preflightGuardExists`:** partial — the conflict guard covers some keys, not all
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #10508, #10587, #10233

---

## HUDI-RCA-009 — Wrong query type or engine wiring makes data look missing or stale

- **Category:** `input/config` · **Discoverability:** **`silent`**
- **Signature:** **No exception.** Recognised by symptom, not by trace: an `INSERT OVERWRITE` wrote
  files but `SELECT` returns old data; only a `_ro` table exists where the user expected `_rt`;
  timestamps render as implausible far-future dates in the query engine.
- **Symptom:** Data is demonstrably on storage but the query returns stale, empty, or garbled results.
- **Root cause:** Query-side wiring, not write-side. Reading the read-optimized (`_ro`) view of a MOR
  table and seeing pre-compaction state; missing `spark.sql.hive.convertMetastoreParquet=false`, so
  Spark uses its own Parquet reader and bypasses Hudi's file-slice resolution; expecting two Hive tables
  for a Copy-on-Write table, which only ever has one; or a timestamp type the catalog declares
  differently from how it was written.
- **Evidence required:** `hoodie.properties` (table type) · the query and the engine · writer props
- **Proactive detection:** compare `COUNT(*)` on the snapshot view against the `_ro` view. A gap that
  closes after compaction is this pattern, by design, not data loss.
- **Mitigation ladder:**
  1. Set `spark.sql.hive.convertMetastoreParquet=false`. Note that whitespace around `=` in a SQL `set`
     has been enough to make this silently not apply — verify it took effect.
  2. Query the snapshot view, not `_ro`; set `hoodie.datasource.query.type=snapshot`.
  3. For MOR, confirm compaction has actually run if you need read-optimized freshness.
  4. For timestamp rendering, enable `hoodie.datasource.hive_sync.support_timestamp`.
- **Governing configs:** `hoodie.datasource.query.type`,
  `hoodie.datasource.hive_sync.support_timestamp`, `spark.sql.hive.convertMetastoreParquet`,
  `hoodie.datasource.write.drop.partition.columns`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #4154, #2409, #2123, #2338

---

## HUDI-RCA-010 — HoodieStreamer source or schema-provider misconfiguration

- **Category:** `input/config` · **Discoverability:** `needs-expertise`
- **Signature:**
  - `org\.apache\.hudi\.exception\.HoodieException: Commit \d+ failed and rolled-back !` at
    `DeltaSync.writeToSink` / `StreamSync.writeToSink` — **this message carries no diagnostic content
    whatsoever**
  - `java\.lang\.NullPointerException` inside `AvroDeserializer` with no Hudi frame
  - Structurally wrong data: a whole CDC envelope ingested instead of the unwrapped row
- **Symptom:** The Streamer fails with a generic "commit failed and rolled-back", or ingests
  structurally wrong data.
- **Root cause:** Schema provider mismatched to source. A provider returning a null target schema while
  a transformer is configured; a generic Avro source used where a CDC-specific source and payload class
  are required to unwrap the envelope; Confluent wire-format Avro needing registry-aware deserialization.
- **Evidence required:** stacktrace · **Streamer CLI args** · driver log — the real exception is in the
  driver log *below* the "failed and rolled-back" line, not in the thrown one
- **Disambiguation:** `Commit ... failed and rolled-back` on its own is a **content-free wrapper**. Do
  not attribute from it. Ask for the driver log and classify from the exception beneath it. If the user
  can only supply this line, say so and request the driver log — that is the correct outcome, not a guess.
- **Mitigation ladder:**
  1. Read the driver log below the "failed and rolled-back" line and classify the real exception.
  2. For CDC sources, use the database-specific source class with its matching payload class, not a
     generic Avro source.
  3. For Confluent Avro, let the Kafka Avro source fetch from the registry rather than hand-rolling a
     schema provider.
  4. If using a transformer, do not return null from the target schema provider. Set a real schema, or
     omit the provider so Hudi infers from the dataframe.
- **Governing configs:** `--source-class`, `--schemaprovider-class`, `--payload-class`,
  `--transformer-class`, `--source-ordering-field`, `hoodie.streamer.schemaprovider.registry.url`,
  `hoodie.streamer.source.kafka.topic`
- **`fixedInVersion`:** no · **`preflightGuardExists`:** no — and the uninformative message is itself
  worth an upstream report
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #2515, #2149, #2589, #8519, #10233, #5540

---

## HUDI-RCA-011 — Source initialization fails: credentials or schema registry

- **Category:** `input/config` · **Discoverability:** `self-evident` for credentials, `needs-expertise` for the registry case
- **Signature:** Failures during writer startup, **before any batch is read**:
  - `org\.apache\.kafka\.common\.errors\.\w+Exception`, SSL or keystore load failures,
    connection-refused against brokers
  - `org\.apache\.hudi\.utilities\.exception\.HoodieSchemaProviderException`
- **Symptom:** The job never ingests anything. No data lands, so there is no partial state to reason
  about — which is itself the diagnostic.
- **Root cause:** Source credentials or TLS material are wrong or unreadable, or the source requires a
  schema registry that was never configured. A common origin: a stream configuration **cloned** from an
  older one created before registry support existed, carrying an empty registry reference that only
  fails at writer init.
- **Evidence required:** stacktrace · writer props **as resolved, not as written** · the age of the
  failure signal
- **Disambiguation:** distinguish an **init** failure from a **sync** failure. An init-failure counter
  that never decrements can reflect a long-past startup failure on a driver that has since recovered.
  Confirm the failure is recent before investigating it.
- **Mitigation ladder:**
  1. Fix the credential or registry configuration and restart.
  2. **Do not create a stream by cloning another** when the source type has gained required fields since
     the original was created. Recreate from scratch — the clone silently carries empty required fields.
  3. Validate required source fields at creation time rather than discovering them at writer init.
- **Governing configs:** `hoodie.streamer.schemaprovider.registry.url`,
  `hoodie.streamer.source.kafka.topic`, Kafka client security properties passed through
  `hoodie.streamer.kafka.*`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public — onboarding-time failures are rarely filed

---

## HUDI-RCA-012 — Checkpoint ahead of the source: offsets aged out or topic recreated

- **Category:** `input/config` (persistent case) / `environment` (transient case) · **Discoverability:** **`silent`** in its most common form
- **Signature:**
  - `org\.apache\.kafka\.clients\.consumer\.OffsetOutOfRangeException: Fetch position \S+ is out of range for partition`
  - `Some data may have been lost because they are not available in Kafka any more`
  - Or **no exception at all** — a healthy-looking job ingesting nothing
- **Symptom:** The stream looks perfectly healthy and ingests nothing.
- **Root cause — two causes with opposite correct responses:**
  - **Persistent:** the topic was deleted and recreated, or retention aged past the committed
    checkpoint. The checkpoint is permanently beyond the high watermark and will never recover.
  - **Transient:** the broker's latest offset briefly moved backward (unclean leader election, replica
    truncation, rebalance). Self-heals on the next produce.
- **Evidence required:** writer checkpoint from commit metadata · source topic offsets and metadata ·
  stacktrace where present
- **Disambiguation:** compare the committed checkpoint against current end offsets, then **watch the
  drift for one interval**. Transient regressions close within minutes; persistent ones do not move at
  all. A topic creation timestamp later than your last successful commit means recreation. Retention
  settings alone do not delete a topic — if the topic is gone, something deleted it.
- **Mitigation ladder:**
  1. **Transient: do nothing.** Resetting the checkpoint here loses data for no reason. This is the
     counter-intuitive step and the most common mistake on this pattern.
  2. **Persistent:** reset the checkpoint to the earliest available offset or a deliberately chosen
     position, accepting that the gap is unrecoverable. Write down what was skipped.
  3. Confirm with the source owner before resetting anything.
  4. Prevent: set retention comfortably above your worst-case ingestion outage, and never delete a topic
     a live stream is checkpointed against.
- **Governing configs:** `hoodie.streamer.source.kafka.topic`,
  `auto.offset.reset` (the native Kafka consumer property, passed through — not a `hoodie.*` key),
  `hoodie.streamer.checkpoint.provider.path`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public with this signature

---

## HUDI-RCA-013 — Wrong operation or index choice produces silent duplicates

- **Category:** `input/config` · **Discoverability:** **`silent`** — the highest-value row in the catalog
- **Signature:** **No exception.** Found only by running
  `SELECT key, COUNT(*) FROM t GROUP BY key HAVING COUNT(*) > 1`, or noticed as unexplained table growth.
- **Symptom:** A primary key appears more than once. Often accompanied by table size growing far faster
  than ingested volume.
- **Root cause:** A ladder of distinct mechanisms the user cannot tell apart without help:
  `hoodie.datasource.write.operation` set to `insert` or `bulk_insert`, which **do not deduplicate
  against existing data by design**; a non-global index with a record that moved partitions, so the key
  legitimately exists twice; a global index on MOR where read-optimized queries show duplicates until
  compaction runs; querying through Hive without `convertMetastoreParquet=false`; or a key-generator
  encoding change across versions.
- **Evidence required:** `hoodie.properties` · writer props · the dedup query result · table type
- **Proactive detection:** `SELECT COUNT(1), COUNT(DISTINCT <record_key>)` on the **snapshot** view.
  Run it after any index or key-generator change.
- **Disambiguation:** before calling this a defect, establish which mechanism applies. Cross-partition
  duplicates with a non-global index are **working as configured**. Duplicates on a MOR `_ro` view that
  disappear after compaction are **transient by design**. Only duplicates on the snapshot view with
  `upsert` and a correct index are a real fault — and then check HUDI-RCA-008 for a key-encoding change.
- **Mitigation ladder:**
  1. Confirm `hoodie.datasource.write.operation=upsert`. This alone explains most reports.
  2. Determine whether the duplicates are cross-partition. If so, move to a global index and set
     `hoodie.bloom.index.update.partition.path=true`.
  3. Re-run the dedup check against the **snapshot** view, not `_ro`, with
     `convertMetastoreParquet=false`.
  4. If MOR, force a compaction and re-check — read-optimized duplicates may be transient by design.
  5. If the table crossed 0.14.1 with a single-field record key under `ComplexKeyGenerator`, compare
     `_hoodie_record_key` encoding before and after: this is the known encoding regression, and the
     remedy is to pin the old version or rewrite the table.
- **Governing configs:** `hoodie.datasource.write.operation`, `hoodie.index.type`,
  `hoodie.bloom.index.update.partition.path`, `hoodie.datasource.write.keygenerator.class`,
  `hoodie.combine.before.insert`, `hoodie.datasource.query.type`
- **`fixedInVersion`:** no · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #2338, #2255, #5777, #10508

---

# Group B — sizing/scale

The config was right at 1,000 file groups and wrong at 500,000. Nobody's bug: the remedy is to re-tune
and add a guardrail. This is the category where a generic LLM most often gives the exactly wrong
advice, because several ladders here run *downward*.

---

## HUDI-RCA-014 — Bloom index OOM from low parallelism or key skew

- **Category:** `sizing/scale` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.OutOfMemoryError: Java heap space`
  - Frames are Kryo and shuffle internals with **nothing naming Hudi's index**:
    `com\.esotericsoftware\.kryo\.io\.Input\.readString`, `Tuple2Serializer.read`,
    `KryoDeserializationStream.readObject`, `ExternalSorter.insertAll`, `BlockStoreShuffleReader.read`
- **Symptom:** A heap OOM in a shuffle-read stage during index lookup. Nothing in the trace says "index".
- **Root cause:** The index tagging stage either has too few partitions for the key volume, or the keys
  are skewed so a few tasks receive disproportionate work.
- **Evidence required:** stacktrace · writer props · **Spark event log** for stage task counts and the
  p75-versus-max shuffle-read spread
- **Disambiguation:** distinguish from HUDI-RCA-015 by the frames. Kryo and `ExternalSorter` under a
  shuffle read with no Hudi frame is the index stage (014); `HoodieMergedLogRecordScanner` or
  `ExternalSpillableMap` is the merge side (015). Diagnose skew versus low parallelism before acting: a
  wide gap between p75 and max task input is skew; a small task count with uniformly large tasks is low
  parallelism. They have different fixes.
- **Mitigation ladder:**
  1. **Low parallelism:** raise input partitioning at the source first, since the index stage inherits
     it. For Kafka that is `hoodie.streamer.source.kafka.minPartitions`. Only then set
     `hoodie.bloom.index.parallelism` explicitly.
  2. **Lower `hoodie.bloom.index.keys.per.bucket`.** Counter-intuitive — you are shrinking the per-task
     batch to produce more, smaller units of work. Raising it makes the OOM worse.
  3. **Skew with random keys:** set `hoodie.bloom.index.fileid.key.sorting.enable=true` **and**
     `hoodie.bloom.index.bucketized.checking=false`. Both are required: bucketized checking stays on
     unless explicitly disabled, so setting only the first changes nothing.
  4. If the OOM frame is `ExternalSorter`, lower
     `spark.shuffle.spill.numElementsForceSpillThreshold` (e.g. to 100000) to force earlier spilling.
  5. Only then change index type or add executor memory.
- **Governing configs:** `hoodie.bloom.index.parallelism`, `hoodie.bloom.index.keys.per.bucket`,
  `hoodie.bloom.index.fileid.key.sorting.enable`, `hoodie.bloom.index.bucketized.checking`,
  `hoodie.index.type`, `spark.shuffle.spill.numElementsForceSpillThreshold`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — the same parallelism and skew analysis is the first move on an
  index stage that merely runs slowly
- **Provenance:** #1491, #11960, #12116

---

## HUDI-RCA-015 — Executor OOM during upsert or bulk_insert at scale

- **Category:** `sizing/scale` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.OutOfMemoryError: (Java heap space|GC overhead limit exceeded|Requested array size exceeds VM limit)`
  - `ExecutorLostFailure`, `Container killed by YARN`, `exit code 52`
  - **(a) merge side** — Hudi frames `ExternalSpillableMap`, `HoodieMergedLogRecordScanner`,
    `BoundedInMemoryExecutor`
  - **(b) archival side** — frames `ByteArrayOutputStream.hugeCapacity` →
    `HoodieAvroDataBlock.serializeRecords` → `HoodieTimelineArchiveLog.writeToFile` →
    `HoodieTimelineArchiveLog.archiveIfRequired`; or
    `java\.lang\.NullPointerException: null of string of map of union in field extraMetadata of org\.apache\.hudi\.avro\.model\.HoodieCommitMetadata`
    at the same frame
- **Symptom:** The job runs fine at small volume, then OOMs persistently as the key space or log-file
  volume grows — often **independent of input batch size**, which is the tell that it is *state* that
  grew, not input. In the archival variant the frames are **in archival, nowhere near the user's
  upsert**, which operators reasonably but wrongly read as an upsert sizing problem.
- **Root cause:** **(a)** The in-memory map used for MOR log merge exceeds what the executor heap allows,
  because merge spill thresholds are sized wrongly relative to heap, or because genuine key-space scale
  has outgrown the index choice. **(b)** Commit metadata files grew enormous — most often because cleaner
  retention was set so aggressively that each commit records a huge number of file operations — and
  archival then loads them in batches and exhausts the heap.
- **Evidence required:** stacktrace · writer props · **Spark event log** · executor logs
- **Disambiguation:** before tuning anything, classify the OOM — see ladder **L2** in
  `mitigation-ladders.md`. Exit 137 with a healthy Java heap is native memory, not heap. A
  `FetchFailedException` is **not memory at all**; it is ephemeral local disk, and it does not show up
  in the Spark UI. Reacting to a FetchFailed by adding heap wastes a tuning cycle. Then split (a) from
  (b) on the Hudi frame: a `HoodieTimelineArchiveLog` frame means the archival variant, where **adding
  executor memory does not help at all** and the fix is to shrink commit metadata.
- **Mitigation ladder:**
  1. Classify the OOM first (L2), then split merge-side from archival-side on the frame. The remedies do
     not overlap.
  2. **(a) merge side:** raise `hoodie.memory.merge.max.size` and `spark.executor.memoryOverhead` —
     cheapest, and correct for a genuine heap-side merge OOM.
  3. Raise `hoodie.upsert.shuffle.parallelism` / `hoodie.bulkinsert.shuffle.parallelism`. **Note these
     default to `0`, not 200** — `0` means "inherit the input RDD's partition count". Any advice built
     on "it is 200 unless you change it" is wrong on every current release.
  4. **Lower `spark.memory.fraction` and `spark.memory.storageFraction`** if the symptom is GC pressure
     rather than hard OOM. Counter-intuitive: giving Spark's managed regions *less* leaves more heap for
     user objects.
  5. **(b) archival side:** lower `hoodie.commits.archival.batch` so fewer commit files are deserialized
     at once, then fix whatever made commit metadata huge — usually an over-aggressive
     `hoodie.cleaner.commits.retained`. Counter-intuitive: retaining *more* commits can produce *smaller*
     per-commit metadata, because each clean then touches fewer files. Driver heap buys time, not a fix.
  6. At genuine scale, switch index to record-level or bucket index, enable the metadata table, and
     partition the table.
- **Governing configs:** `hoodie.memory.merge.max.size`, `hoodie.memory.compaction.max.size`,
  `hoodie.upsert.shuffle.parallelism`, `hoodie.bulkinsert.shuffle.parallelism`, `hoodie.index.type`,
  `hoodie.metadata.enable`, `hoodie.commits.archival.batch`, `hoodie.cleaner.commits.retained`,
  `spark.executor.memoryOverhead`, `spark.memory.fraction`, `spark.memory.storageFraction`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — the same parallelism and spill analysis applies to a slow but
  succeeding upsert
- **Provenance:** #1491, #11960, #12116, #8332, #2408, #2515

---

## HUDI-RCA-016 — Write-profiling shuffle produces an unfetchable broadcast

- **Category:** `sizing/scale` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.spark\.shuffle\.MetadataFetchFailedException: Unable to deserialize broadcasted map statuses for shuffle`
  - `\[INTERNAL_ERROR_BROADCAST\] Failed to get broadcast_\d+_piece\d+ of broadcast_\d+`
  - Hudi frame: `BaseSparkCommitActionExecutor.buildProfile`, failing in a `countByKey` stage; wrapped
    in `HoodieUpsertException: Failed to upsert`
- **Symptom:** A Spark broadcast-fetch error with nothing naming Hudi. The instinctive fix — more
  parallelism — makes it worse.
- **Root cause:** Write profiling and simple-index tagging fan out with **file-group count**. The
  shuffle's map-status broadcast grows with the number of shuffle partitions until Spark cannot
  reliably fetch and deserialize its pieces.
- **Evidence required:** stacktrace · writer props · file-group count (`HoodieTableLayoutAnalyzer`) ·
  Spark event log
- **Mitigation ladder:**
  1. **Reduce `hoodie.simple.index.parallelism`.** Counter-intuitive and the whole point of this row:
     fewer shuffle partitions means a smaller map-status structure to broadcast. **Raising parallelism
     makes this failure worse**, which is exactly what an operator reaching for the obvious lever will do.
  2. Address file-group growth itself — tune clustering and compaction so file groups consolidate
     rather than accumulate without bound.
  3. Consider an index whose tagging cost does not scale with file-group count (record-level or bucket).
- **Governing configs:** `hoodie.simple.index.parallelism`, `hoodie.index.type`,
  `hoodie.clustering.inline`, `hoodie.compact.inline`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — file-group consolidation is an optimization long before it is a fix
- **Provenance:** none public with this exact broadcast signature

---

## HUDI-RCA-017 — Record-size estimate blowup on wide payloads

- **Category:** `sizing/scale` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.IllegalArgumentException: You cannot call toBytes\(\) more than once without calling reset\(\)`
  - Frames: `RunLengthBitPackingHybridEncoder.toBytes` → `ColumnWriteStoreBase.sizeCheck` →
    `HoodieBaseParquetWriter.write` → `HoodieCreateHandle.doWrite`
  - Often preceded by `ERROR HoodieCreateHandle : Error writing record HoodieRecord{key=...}`
- **Symptom:** A Parquet encoder assertion with nothing suggesting that the records are too wide.
- **Root cause:** Hudi's dynamic record-size estimation drives Parquet row-group sizing. Very wide
  values (large string or JSON columns) break the estimate badly enough that the encoder is asked to
  finalize a page twice.
- **Evidence required:** stacktrace · table schema · writer props (batch size)
- **Disambiguation:** driven by **value width, not table size** — a small table with one fat JSON column
  hits this. If the job also stalls at "Building Workload Profile" first, that corroborates.
- **Mitigation ladder:**
  1. Halve the per-sync batch size and observe.
  2. If that does not clear it, **pin the estimate instead of computing it**:
     `hoodie.copyonwrite.record.size.estimate` set well below the 1024 default (e.g. 200). This bypasses
     dynamic estimation entirely — counter-intuitive, because the configured value is a *lower* number
     than the real record size, and that is the point.
  3. Longer term, keep genuinely large blobs out of wide columns, or reduce row-group size at the source.
- **Governing configs:** `hoodie.copyonwrite.record.size.estimate`,
  `hoodie.parquet.small.file.limit`, `hoodie.parquet.max.file.size`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** true — pinning the estimate also stabilizes file sizing on a healthy wide-schema table
- **Provenance:** none public with this signature

---

## HUDI-RCA-018 — Table-service backlog: compaction, cleaning, or archival falling behind

- **Category:** `sizing/scale` · **Discoverability:** **`silent`** early, `needs-expertise` once terminal
- **Signature:** Starts silent — growing log files, growing `.hoodie`, lengthening write times — then
  surfaces as:
  - `INFO No Instants to archive`
  - `Not archiving as there is no compaction yet on the metadata table`
  - OOM inside `HoodieTimelineArchiveLog.archive` (see HUDI-RCA-015, archival variant)
- **Symptom:** Writes get progressively slower, `.hoodie` accumulates thousands of instants, object-store
  list costs climb, and eventually something fails hard.
- **Root cause:** A pending or stuck instant blocks archival; the timeline cannot archive past an
  incomplete table-service instant, and the metadata table's timeline cannot archive until the data
  table's does.
- **Evidence required:** **health-checker output** — this is the pattern the checker exists for ·
  `hoodie.properties` · writer props
- **Disambiguation:** diagnose the chain in order using ladder **L4**: archival → clean → compaction →
  blocking inflight instant. Each link gates the next, and fixing the wrong link accomplishes nothing.
  Note that **archival silently overrides `hoodie.keep.min.commits`** when the cleaner's
  `earliestCommitToRetain` demands more retention. "I set min commits to 20 but have 400 instants" is
  usually this — **fix the cleaning config, not the archival one.**
- **Mitigation ladder:**
  1. Run `HoodieTableHealthChecker --checks archival,cleaner,compaction,mdt-compaction --output JSON`
     and read the chain rather than guessing which service is behind.
  2. Finish or roll back pending table-service instants on the data table first; metadata-table archival
     unblocks behind it.
  3. Fix the **cleaner** configuration if `earliestCommitToRetain` is what pins the timeline. Tuning
     archival while the cleaner holds the line does nothing.
  4. For MOR, ensure compaction is actually scheduled and running — inline with
     `hoodie.compact.inline.max.delta.commits`, or a dedicated offline compactor.
  5. **Do not hand-delete `.requested` or `.inflight` files.** Use the CLI's unschedule path. Deleting
     timeline state by hand is how a recoverable backlog becomes an unrecoverable table.
- **Governing configs:** `hoodie.keep.min.commits`, `hoodie.keep.max.commits`,
  `hoodie.commits.archival.batch`, `hoodie.cleaner.commits.retained`, `hoodie.clean.automatic`,
  `hoodie.archive.automatic`, `hoodie.compact.inline`, `hoodie.compact.inline.max.delta.commits`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** yes — `HoodieTableHealthChecker` detects this
  class before it becomes terminal
- **`alsoAppliesWhenHealthy`:** **true** — service cadence tuning is the archetypal healthy-table optimization
- **Provenance:** #9478, #2515, #2020

---

## HUDI-RCA-019 — Streaming source throttled by the provider

- **Category:** `environment` / `sizing/scale` · **Discoverability:** `needs-expertise` — the provider
  exception is clear, but the correct tuning direction is the opposite of the instinct
- **Signature** (innermost cause):
  - A provider throughput-exceeded exception from the source read path — on shard-based streams,
    `ProvisionedThroughputExceededException`
  - `org\.apache\.hudi\.exception\.HoodieIOException: Failed to list shards for \S+` during shard discovery
  - Often lands in an "unknown" failure bucket because the exception is provider-specific rather than Hudi's
- **Symptom:** Ingestion throughput collapses or fails intermittently against a shard-based streaming
  source. Adding capacity appears not to help.
- **Root cause:** The job is requesting records faster than the per-shard quota allows. Two things
  routinely hide this: **closed shards still appear in listings but contribute zero read capacity**, so a
  scale-up that lands on closed shards changes nothing; and every component reading those shards consumes
  the same per-shard request quota, including background lag and monitoring probes, not just the
  ingestion loop.
- **Evidence required:** stacktrace · writer props **as resolved, not as written** · shard and throughput
  metrics (count of **open** shards, actual average record size)
- **Disambiguation:** before tuning, measure actual average record size and the count of **open** shards.
  A throttling exception on a stream that was just scaled up usually means the new shards are closed.
- **Mitigation ladder:**
  1. Slow the poll: raise the get-records interval (e.g. to 1000 ms; provider defaults are often far
     lower). Counter-intuitive, because the instinct under throttling is to retry harder.
  2. **Reduce** max records per request step-wise (e.g. 10000 → 5000 → 2000) until stable. Derive the
     target from per-shard byte limit ÷ average record size ÷ polls per second. **Do not copy a number
     from another deployment** — the right value is payload-specific.
  3. Add open shards, verifying they are actually open and not closed-but-listed.
  4. Reduce the frequency of auxiliary probes competing for the same shard quota.
  5. **Verify the key actually took effect** — see ladder **L12**. A misspelled Hudi config key produces
     no error at all; the library default simply applies, and the intended interval is silently ignored.
- **Governing configs:** the source-specific throughput keys under `hoodie.streamer.source.*` for your
  connector (records per request, poll interval) — resolve the exact key for your source and release
  with the config catalog rather than assuming, and see L12 on verifying it took effect
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — poll pacing against shard quota is an ordinary throughput
  optimization on a healthy stream
- **Provenance:** none public with this signature

---

## HUDI-RCA-020 — Timeline bloat from a savepoint that blocks archival

- **Category:** `input/config` (the stray savepoint) with a `defect` component (the archival gap) ·
  **Discoverability:** **`silent`** until terminal
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieRemoteException: Failed to initialise timeline remotely in server, status code: 413`
  - `Payload Too Large`
  - Frame: `RemoteHoodieTableFileSystemView.initialiseTimelineInRemoteView`
  - Archival log line: `Setting Earliest Commit to Not Archive same as Earliest commit to retain`
- **Symptom:** The timeline grows invisibly for weeks, then ingestion breaks outright with a 413 on
  timeline initialization.
- **Root cause:** **Archival stops at the earliest savepoint, not the latest.** One forgotten savepoint
  pins the entire timeline after it. The active timeline grows without bound until the serialized
  timeline exceeds what the embedded timeline server will accept.
- **Evidence required:** stacktrace · **health-checker output** (`--checks savepoint,archival`) ·
  `.hoodie` instant count
- **Disambiguation:** a 413 on timeline init is almost never a network or payload-size problem. Check
  for a savepoint **first** — it is the single most commonly missed check on this signature.
- **Mitigation ladder:**
  1. Run `HoodieTableHealthChecker --checks savepoint,archival --output JSON`. The savepoint check
     exists precisely for this.
  2. **Immediate unblock:** disable remote timeline initialization / the embedded timeline server so the
     oversized request is never made. Minutes, and buys room to do the real fix.
  3. **Remove the stray savepoint.** This is the actual fix; archival resumes and the timeline shrinks on
     its own.
  4. **Do not reach for aggressive archival scheduling first.** Counter-intuitive but load-bearing: with
     the savepoint still in place, a more frequent archival job prunes nothing and costs object-store
     requests. It is pointless until the savepoint is gone.
  5. Re-enable remote timeline init once the instant count is back to a few thousand.
  6. Tune archival so it is not trying to prune the whole backlog in one round.
- **Governing configs:** `hoodie.embed.timeline.server`,
  `hoodie.filesystem.remote.backup.view.enable`, `hoodie.keep.min.commits`, `hoodie.keep.max.commits`,
  `hoodie.archive.beyond.savepoint`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** yes — the `savepoint` health check
- **`alsoAppliesWhenHealthy`:** **true** — a forgotten savepoint is worth finding long before it is terminal
- **Provenance:** #9478

---

## HUDI-RCA-021 — Non-partitioned MOR table: rollback wedge and whole-table listings

- **Category:** `sizing/scale` (a design shape, not a tuning value) · **Discoverability:** `needs-expertise`, severely
- **Signature:** A cluster of symptoms rather than one exception:
  - Tasks blocked off-CPU in `HoodieAppendHandle.doAppend` → `HoodieLogFile.rollOver` →
    `FSUtils.computeNextLogVersion` → `getAllLogFiles` → `listStatus`
  - A `.rollback.inflight` plan file of **megabytes** that never completes and **grows** across attempts
  - Driver timeline server: `OutOfMemoryError: Java heap space` and
    `OutOfMemoryError: Requested array size exceeds VM limit` in Jetty threads under
    `objectMapper.writeValueAsString`
  - Downstream consequences: executor `SocketTimeoutException: Read timed out`, marker-create failures
    (HUDI-RCA-031), writers queued behind the stuck rollback reporting lock acquisition failures
- **Symptom:** A large non-partitioned MOR table that progressively wedges, firing several unrelated-looking
  alerts at once.
- **Root cause:** Two mechanisms compounding. MOR failed-write rollback uses the **listing** strategy,
  enumerating every file in the partition — and on a non-partitioned table the "partition" is the whole
  table, so rollback cost scales with total file-group count rather than with the failed instant.
  Separately, the timeline server serializes the entire file-system view into a single JSON string,
  which on a whole-table view hits the JVM's roughly 2 GB single-array ceiling. That is a **structural**
  limit, not a heap-size one.
- **Evidence required:** stacktrace · `hoodie.properties` (table type, partitioning) · layout analysis
  (file-group count) · `.rollback.inflight` plan sizes · driver log · Spark event log
- **Disambiguation:** note that **rolling back a completed instant ignores `hoodie.rollback.using.markers`
  entirely** — the config is ANDed with "instant is not completed", so restores and savepoint rollbacks
  always take the listing path regardless of how that config is set. This is the mechanism behind "my
  rollback took longer than the write", and it is why setting the marker config does not help here.
- **Mitigation ladder:**
  1. **Do not restart the driver while a rollback is in flight.** Each restart abandons it and re-requests
     a larger one. The plan file growing across attempts is the proof.
  2. **Do not disable compaction to dodge a write conflict.** It stops the conflict and lets log files
     pile up per file group without bound, making both the per-append listing and every future rollback
     slower. This is the single most damaging available move.
  3. Move the file-system view off heap: `hoodie.filesystem.view.type=SPILLABLE_DISK`, with
     `hoodie.filesystem.view.spillable.mem` sized around 1 GB and
     `hoodie.common.spillable.diskmap.type=ROCKS_DB`.
  4. Raise `hoodie.markers.timeline_server_based.batch.num_threads` so marker traffic is not the next
     bottleneck.
  5. Size compaction for the workload: `hoodie.compaction.target.io` must be large enough that one round
     sweeps most candidate file groups. If only half fit per round, a tail accumulates forever. Raise it
     substantially rather than nudging it.
  6. Give the driver more heap only after 3 through 5 — for the JSON serialization path this is a ceiling
     problem, not a heap problem.
  7. Prefer partitioning the table. A non-partitioned MOR table is pathological for both the listing-based
     rollback path and the whole-table file-system-view path.
- **Governing configs:** `hoodie.filesystem.view.type`, `hoodie.filesystem.view.spillable.mem`,
  `hoodie.common.spillable.diskmap.type`, `hoodie.markers.timeline_server_based.batch.num_threads`,
  `hoodie.compaction.target.io`, `hoodie.rollback.using.markers`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** partial — layout analysis surfaces the shape
- **`alsoAppliesWhenHealthy`:** **true** — the view and compaction sizing are optimizations well before the wedge
- **Provenance:** none public with this combination

---

## HUDI-RCA-022 — Unbounded incremental clustering plan wedges everything behind it

- **Category:** `sizing/scale` with a `defect` component · **Discoverability:** `needs-expertise` — several
  alerts fire, none of which names clustering
- **Signature:**
  - A `.replacecommit.requested` whose **plan file is tens to hundreds of MB**, `.inflight` for many hours
  - Driver `java\.lang\.OutOfMemoryError: GC overhead limit exceeded`, with a heap dump showing many
    duplicated copies of the same large plan byte array across worker threads
  - Metadata-table compaction stops scheduling entirely; MDT log blocks accumulate on the `files` partition
  - Marker CREATE timeouts, connection-pool shutdown, container OOM-kills where the Java heap looks healthy
- **Symptom:** A multi-day outage on a heavily-partitioned append-only table, announced by half a dozen
  symptoms that each look like a different problem.
- **Root cause:** The incremental clustering planner caps group count and total bytes but **not input
  file-slice count**. On a heavily-partitioned append-only table where clustering has fallen behind, it
  accumulates pending partitions and tries to catch up all at once. The plan is then re-read and fully
  deserialized **on every timeline refresh**, just to answer "is this a clustering instant?", so each
  worker thread pins its own copy.
- **Evidence required:** table state (`.replacecommit.requested` age and plan size, decoded plan) ·
  heap dump · Spark event log
- **Disambiguation:** the giveaway is plan **file size**. Over a few MB is already a warning. If ingestion
  writes roughly one file group per commit and the pending plan proposes hundreds of thousands of file
  slices, the plan is wildly disproportionate and is the cause, not a victim.
- **Mitigation ladder:**
  1. **Capture heap dumps from the driver and a few executors before restarting anything.** Restarting
     destroys the only evidence and does not clear the wedge.
  2. **Disable clustering entirely** and let ingestion and metadata-table compaction drain. This is the
     only reliable break. Counter-intuitive: throttling clustering while the giant plan is still pending
     does not help, because the cost is in deserializing the existing plan, not in making new ones.
  3. Re-enable with a reduced per-round group count and a cap on input file groups per plan where your
     release supports it.
  4. If executors are container-OOM-killed with a healthy Java heap, suspect **native** memory from
     compression codecs: raise `spark.executor.memoryOverheadFactor`.
  5. Keep clustering cadence ahead of partition creation so a backlog never forms.
- **Governing configs:** `hoodie.clustering.inline`, `hoodie.clustering.async.enabled`,
  `hoodie.clustering.plan.strategy.max.num.groups`,
  `hoodie.clustering.plan.strategy.max.bytes.per.group`, `spark.executor.memoryOverheadFactor`
- **`fixedInVersion`:** n/a — the input-slice-count cap is the missing guard
- **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — clustering cadence versus partition creation rate is a healthy-table check
- **Provenance:** none public with this mechanism

---

## HUDI-RCA-023 — Long-lived driver leaks spillable file-system-view resources

- **Category:** `defect` surfacing as `sizing/scale` · **Discoverability:** **`silent`**
- **Signature (a heap dump, not a stacktrace):**
  - Leak suspect: `java\.lang\.ApplicationShutdownHooks` retaining the bulk of heap via tens of thousands
    of shutdown-hook threads
  - `BitCaskDiskMap` and `HoodieFileGroupId` dominating the object histogram
  - Externally: a busy-thread dump where nearly all top-CPU threads are GC threads
- **Symptom:** No exception. The driver stays "Ready", ingests in bursts, and degrades. Several different
  alerts fire, none naming the cause.
- **Root cause:** With `SPILLABLE_DISK` file-system views, each spillable map registers a JVM shutdown
  hook. Table-service cycles rebuild the view on every full refresh, and replaced maps are abandoned
  without being closed, so the hooks are never deregistered and pin the maps for the life of the JVM.
  Leak volume scales with tables multiplied by table-service cycles.
- **Evidence required:** **heap dump — a stacktrace is useless here** · writer props (view type) ·
  driver uptime
- **Disambiguation:** note the direct tension with HUDI-RCA-021, which prescribes `SPILLABLE_DISK` for a
  different reason. Apply the view type **per table**, not globally: a table with a very large view needs
  spillable; a long-lived driver hosting many small tables does not.
- **Mitigation ladder:**
  1. Set `hoodie.filesystem.view.type=MEMORY` for the dominant table — this is Hudi's own default and
     removes the leaking path.
  2. Restart the driver to clear accumulated hooks — **after** capturing dumps, not before.
  3. Shorten driver lifetime or reduce tables per driver as a stopgap.
  4. Upgrade to a release where the table object is closed per cycle.
- **Governing configs:** `hoodie.filesystem.view.type`, `hoodie.filesystem.view.spillable.mem`,
  `hoodie.common.spillable.diskmap.type`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public — requires heap-dump forensics on a long-running multi-table driver

---

## HUDI-RCA-024 — Object-store connection pool exhausted

- **Category:** `sizing/scale` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause — **both parts required**):
  - An SDK client exception, **and** the text `Timeout waiting for connection from pool`
  - Wrapped in `org\.apache\.hudi\.exception\.HoodieIOException: getFileStatus on `
  - Frame: the filesystem connector's exception-translation utility
- **Symptom:** Intermittent to persistent IO failures under high file-listing or write concurrency,
  reading like a storage outage.
- **Root cause:** The filesystem client's HTTP connection pool is smaller than the concurrency the job
  drives. More executors and more threads make it worse.
- **Evidence required:** stacktrace · writer props and Hadoop config · executor count
- **Disambiguation:** the match requires **both** the SDK client exception **and** the pool-timeout text.
  Other SDK errors from the same connector are a different problem entirely — do not match on the SDK
  exception class alone.
- **Mitigation ladder:**
  1. Double the connection-pool maximum (`fs.s3a.connection.maximum` on the S3A connector) and restart.
     Repeat once if needed.
  2. Above roughly 2000, **stop doubling and think.** Per-pod connection counts compound across executors,
     and you can exhaust the host or the endpoint instead of fixing anything.
  3. **Reduce job-side concurrency** — fewer executors, lower listing parallelism. Counter-intuitive, but
     it attacks the demand side rather than the supply side, and the demand side is the one that scales.
  4. Reduce the number of file-listing operations outright: enable the metadata table, partition better.
- **Governing configs:** `fs.s3a.connection.maximum` (or the equivalent for your connector),
  `hoodie.metadata.enable`, `hoodie.file.listing.parallelism`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** — listing reduction is an optimization on any healthy job at scale
- **Provenance:** none public with this exact match rule

---

# Group C — defects and table state

Hudi reached a state it should not have. Remedies are to pin or upgrade a version, rebuild derived
state, or — in one case only — run a supervised repair. **Attribution matters here:** these are not the
user's mistake, and saying so plainly is part of the job.

---

## HUDI-RCA-025 — Dangling data files from an incomplete rollback — EXPLAIN ONLY

> **⚠️ EXPLAIN-ONLY ROW — HARD CONSTRAINT FOR THE AGENT AND FOR FUTURE EDITORS ⚠️**
>
> The agent **identifies** this pattern, **names it as a known defect class rather than the user's
> mistake**, points at `HoodieTableHealthChecker`, and **states that repair requires the safe-deletion
> protocol (ladder L3) with a human driving it.**
>
> **The agent must never recommend, script, enumerate, or sequence the file deletion.** Not as a
> suggestion, not as an example, not "for reference", not even when the user asks directly. The correct
> response to "just tell me which files to delete" is to restate that the deletion premise needs two
> independent verifications and a savepoint, and to hand over L3 for a human to execute.
>
> This row is the template for any future pattern whose only remedy is destructive. If you are adding
> such a pattern, copy this block.

- **Category:** `defect` · **Discoverability:** `needs-expertise` — and actively misleading
- **Signature** (innermost cause):
  - `java\.util\.NoSuchElementException: FileID \S+ of partition path \S+ does not exist`
  - Frames: `HoodieMergeHandle.getLatestBaseFile` → `BaseSparkCommitActionExecutor.getUpdateHandle`
  - In the driver log beforehand: `BaseRollbackActionExecutor : Rollback of Commits [<instant>] is complete`
- **Symptom:** Ingestion fails on **every** attempt after a prior write failure, deterministically, on
  the same file ID across restarts.
- **Root cause:** A commit failed and was rolled back, but its data files and markers were never
  physically deleted. A later operation resolves a file slice to an untracked file that has no timeline
  entry. **The message says a file "does not exist", but the actual condition is an *extra*, untracked
  file** — which is why it reads like user error or data loss when it is neither.
- **Evidence required:** stacktrace · **health-checker output** · table state (partition listing,
  `.hoodie`, `.hoodie/.temp/`) · driver log for the rollback-complete line
- **Disambiguation:** shares `FileNotFoundException`-family symptoms with several rows — see the
  disambiguation index. A missing path under the **source** bucket is HUDI-RCA-036. A path under the
  table ending `.log.<n>` indicates a deltacommit that raced an unfinished compaction, which is a
  different mechanism with a different (also destructive, also human-driven) remedy. Confirm all of:
  the file exists on storage, its instant is **not** in the active timeline, and its instant **is**
  named by a later `.rollback`.
- **How this is explained to the user:**
  1. Name it: this is a known timeline-consistency defect class, not a configuration mistake. The error
     message is misleading by design-accident.
  2. Point at `HoodieTableHealthChecker` to characterize the table's current state.
  3. State that repair means removing untracked files, that this is destructive and irreversible, and
     that it must run inside the **safe-deletion protocol (L3)** — stakeholder notice, writer paused,
     savepoint taken, **two independent verifications of the deletion premise**, backup copy, then delete.
  4. State that a **human must drive it**, and that if any verification is ambiguous the correct action
     is to stop: deleting a non-orphan corrupts the table silently.
  5. Where the release supports it, note that `rollback` or `restore` to the last clean commit is an
     alternative to hand-deletion — it rewrites the timeline, so it is also expert-only and also L3.
  6. Upgrade: the deltacommit-removed-while-requested-remained trigger is fixed in later releases, so
     establishing the version is worthwhile before anything else.
- **Governing configs:** `hoodie.cleaner.policy.failed.writes`, `hoodie.rollback.using.markers`,
  `hoodie.filesystem.view.incr.timeline.sync.enable` (experimental; if it is on, that alone can produce
  plans naming wrong filenames — turn it off)
- **`fixedInVersion`:** 1.0.x for the requested/inflight-orphan trigger · **`preflightGuardExists`:** partial
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #11202, #2020, #19290

---

## HUDI-RCA-026 — Metadata table disagrees with the filesystem

- **Category:** `defect` (data correctness) · **Discoverability:** **`silent`** — reads can return wrong or
  missing files with no write failure at all
- **Signature:** Validator output from `HoodieMetadataTableValidator`:
  - `Validation of latest file slices for partition \S+ failed`
  - `File system-based listing: \d+ & MDT-based listing: \d+`
  - `Validation of record index content failed`
  - May instead surface as `HoodieMetadataException: Failed to retrieve files in partition`, or as
    `Invalid number of file groups for partition:column_stats`
- **Symptom:** Queries return wrong or missing files, or an offline operation cannot list partitions.
  Often no write failure at all.
- **Root cause:** The metadata table diverged from the data table. Known triggers: enabling column-stats
  or bloom-filter indexes mid-life on an existing table; duplicate concurrent writers caused by
  `spark.speculation=true`, which could trigger a record-index purge; a deltacommit deleted while its
  requested/inflight state remained.
- **Evidence required:** validator output · **both timelines, active and archived** · writer props
  (`hoodie.cleaner.policy.failed.writes`, `hoodie.write.concurrency.mode`,
  `hoodie.write.lock.provider` — these gate which rollback path can fire)
- **Disambiguation — classify the direction first, per table, before touching anything:**

  | Direction | Meaning | Response |
  |---|---|---|
  | **MDT behind** | Storage has slices the metadata table lacks | Rebuild the metadata table |
  | **MDT ahead** | The metadata table lists slices storage no longer has | **A rebuild does not fix this** — it re-derives from a data table whose storage still holds orphans, so the divergence returns |
  | **Per-slice** | Same count, but a file group resolves to competing base files differing only in write token | HUDI-RCA-028 |

  One validator run can cover several tables diverging in **opposite** directions from the same defect.
  Interpreting the timeline evidence: a `.clean` naming the file means the cleaner removed it logically
  while the object survived. A data-side `.rollback` whose rolled-back instant **is** the divergent base
  instant, with no filename hit, means the commit was rolled back and the objects are orphan task-attempt
  files — **the metadata table is correct, do not rebuild.** A `.replacecommit` means clustering replaced
  the slice, and replaced files leave only when a later clean deletes them.
- **Mitigation ladder:**
  1. **Archive the evidence first.** Both branches overwrite the diverged state; once you rebuild, the
     evidence is gone.
  2. Confirm the signal is real before investigating at all — see ladder **L6**. Several tables failing
     the same check within minutes usually means the *check* changed, not the data.
  3. **Branch A, metadata table is wrong:** rebuild. Set `hoodie.metadata.enable=false`, let one write
     proceed, re-enable so the `files` partition rebuilds from scratch, then re-run the validator.
  4. **Branch B, metadata table is right and storage has orphans:** rebuilding will not fix it. The
     orphans must be removed under the safe-deletion protocol (L3), human-driven — see HUDI-RCA-025.
  5. **If neither branch matches positively, do not mitigate. Escalate.** A wrong branch either leaves the
     divergence in place or deletes files on a false premise.
  6. Set `spark.speculation=false` — Hudi 1.2.0 fails fast on this for good reason.
  7. If the failure is index-specific, disabling that specific index is a confirmed workaround.
- **Governing configs:** `hoodie.metadata.enable`, `hoodie.metadata.index.column.stats.enable`,
  `hoodie.metadata.index.bloom.filter.enable`, `hoodie.metadata.record.index.enable`,
  `hoodie.metadata.compact.max.delta.commits`, `spark.speculation`
- **`fixedInVersion`:** 0.14.x for the mid-life index-enable trigger; 1.2.0 guards the speculation trigger
- **`preflightGuardExists`:** yes in 1.2.0 for `spark.speculation`; also the `mdt-sync` health check
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #7657, #11567, #11202, #9478

---

## HUDI-RCA-027 — Rollback failure wedging the writer

- **Category:** `defect` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieRollbackException: Failed to rollback \S+ commits \d+`
  - Frames: `BaseHoodieWriteClient.rollback` ← `rollbackFailedWrites` ← `CleanerUtils.rollbackFailedWrites`
    ← `BaseHoodieWriteClient.startCommitWithTime`
- **Symptom:** Every subsequent run fails at startup while trying to clean up a previous failed write.
  Self-perpetuating — the job can never get past commit start.
- **Root cause:** A previous write died leaving an inflight instant whose rollback cannot complete,
  usually because the files it references are already gone, a lock could not be acquired to perform the
  rollback, or markers are missing.
- **Evidence required:** stacktrace · writer props (lock config) · **health-checker output** · timeline state
- **Disambiguation — `HoodieRollbackException` maps to three patterns. Read the frame below
  `rollbackFailedWrites`:**
  - `AsyncCleanerService.waitForCompletion` → metadata-table or cleaner path → HUDI-RCA-026
  - A lock-provider frame (`LockManager.lock`, `*LockProvider.acquireLock`) → lock problem first →
    HUDI-RCA-006
  - `HoodieMergeHandle.getLatestBaseFile` with `FileID ... does not exist` → HUDI-RCA-025
  - None of these, and the rollback simply never finishes on a large table → HUDI-RCA-021
- **Mitigation ladder:**
  1. Check for and fix lock-provider problems first. Rollback happens inside the lock, so a lock problem
     presents as a rollback problem.
  2. Set `hoodie.cleaner.policy.failed.writes=LAZY` so a single writer does not eagerly roll back another
     writer's in-flight commit.
  3. Inspect the timeline and roll back the specific instant through the CLI rather than deleting files.
  4. Last resort: savepoint, then restore to a known-good instant. This rewrites the timeline — expert-only.
  5. If the rollback is not failing but merely never finishing, stop and read HUDI-RCA-021 before
     restarting anything.
- **Governing configs:** `hoodie.cleaner.policy.failed.writes`, `hoodie.write.lock.provider`,
  `hoodie.rollback.using.markers`, `hoodie.rollback.parallelism`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #9213, #7657, #5765

---

## HUDI-RCA-028 — Two base files for one file slice (duplicate write token)

- **Category:** `defect` · **Discoverability:** **`silent`** — the primary symptom is wrong query results,
  with no exception anywhere
- **Signature:** Two base files sharing the same file-group id **and** the same instant time, differing
  only in write token:
  - `<fileId>_0-\d+-\d+_<instant>\.parquet` alongside `<fileId>_1-\d+-\d+_<instant>\.parquet`
  - Downstream: `COUNT(1) > COUNT(DISTINCT <record_key>)`
- **Symptom:** Duplicate rows for one primary key, with no failure signal at all.
- **Root cause:** A Spark task retry combined with cached-RDD invalidation produced two output files
  where commit metadata records one. With the metadata table enabled it should guard against serving the
  untracked one; without it, file-slice resolution can pick either, non-deterministically.
- **Evidence required:** per-file-group listing · commit metadata · compaction plan if compaction has
  run · a duplicate-count query
- **Proactive detection:** `SELECT COUNT(1), COUNT(DISTINCT <record_key>)`, then for each file group list
  base files sharing one instant time. More than one is the signature.
- **Mitigation ladder — strictly ordered, and the most delicate procedure in this catalog:**
  1. Notify stakeholders, **pause the writer**, **take a savepoint**. Everything below is inside the
     safe-deletion protocol (L3) and requires a human.
  2. **Latest file slice, compaction has not run:** if there are no log files on the untracked base file,
     the untracked base file can be removed under L3. If log files exist only on the *tracked* slice, the
     same holds. If log files are on the *untracked* slice, trigger or wait for compaction on that file
     group first, then re-assess.
  3. **Compaction already ran and more updates landed:** compaction considered only one of the two base
     files — read the compaction plan to learn which. Diff the missed base file against the
     post-compaction base file, excluding records a later commit updated (they may now live in a different
     file group), and re-ingest whatever is genuinely missing. **There is no shortcut here.**
  4. Validate, resume, let a few commits land, then pause, delete the savepoint, resume.
  5. If the writer failed during metadata-table initialization because of these files, disable the
     metadata table so the stream progresses, do the cleanup in a quiet window, then re-enable.
- **Governing configs:** `hoodie.metadata.enable`, `spark.speculation`, `hoodie.cleaner.policy.failed.writes`
- **`fixedInVersion`:** later releases made file-slice resolution deterministic; the cleanup procedure is
  the durable value · **`preflightGuardExists`:** partial
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #2338 (duplicate symptom reports); the two-base-file mechanism is not separately filed

---

## HUDI-RCA-029 — Record-level index: duplicate key while writing the index HFile

- **Category:** `defect` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieDuplicateKeyException: Duplicate key found for insert statement`
  - `Duplicate recordKey \S+ found while writing to HFile`
  - Frames: `HoodieAvroHFileWriter.writeAvro` → `HoodieCreateHandle.doWrite`
  - **Tell-tale:** `partitionPath=record_index` and a fileId shaped `record-index-\d+-0` — this is the
    metadata table's record-index partition, **not** the user's data
- **Symptom:** Ingestion fails with a duplicate-key error naming internals the user has never seen.
- **Root cause:** Two entries for the same record key reach the same record-index file group in one write.
  HFiles require strictly increasing keys, so the writer aborts.
- **Evidence required:** stacktrace · metadata-table timeline · writer props (index config)
- **Disambiguation:** the `record_index` partition path is what makes this a metadata-table defect rather
  than a duplicate in the user's data. Do not send the user looking for duplicates in their source.
- **Mitigation ladder:**
  1. **Archive evidence first** — copy `.hoodie`, and the whole table if it is small, to a backup location.
     This is corruption evidence and the mitigation destroys it.
  2. Disable the record-level index and fall back to a global index so ingestion proceeds:
     `hoodie.metadata.record.index.enable=false`, `hoodie.index.type=GLOBAL_SIMPLE`.
  3. Resume. Re-enable the record index only after the defect is understood — re-enabling rebuilds it
     from scratch, which is expensive and erases the evidence.
  4. Note that the record index silently falls back to a simple index when it is uninitialized, logging
     only a warning. A table can therefore start doing full base-file key scans with no error at all —
     check for that warning if performance regressed rather than failed.
- **Governing configs:** `hoodie.metadata.record.index.enable`, `hoodie.index.type`,
  `hoodie.metadata.enable`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** yes — the `record-index` health check sizes the index
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public with this signature

---

## HUDI-RCA-030 — A zero-column schema gets committed and poisons the table

- **Category:** `defect` with an `input/config` trigger · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.NullPointerException` at `AvroSchemaEvolutionUtils.reconcileSchema`
  - Preceded in the log by `Seeing new schema. Source: \[null\], Target: \{.*"fields": \[\]` and, on each
    retry, an empty-batch marker from the source
- **Symptom:** Every sync fails identically. The checkpoint never advances, so a restart re-reads the same
  checkpoint and reproduces it exactly — a closed loop. **Restarting is provably useless.**
- **Root cause:** A brand-new table's first batch produced zero rows and zero columns, so the deduced
  writer schema was an empty record and was committed. Afterwards, schema reconciliation returns the null
  source schema untouched whenever the table schema has zero fields, and that null reaches a dereference
  guarded only against the Avro `NULL` *type*, never against a Java null reference.
- **Evidence required:** stacktrace · last commit metadata (`extraMetadata.schema`) · writer props
  (schema provider)
- **Disambiguation:** the poison is visible: read `extraMetadata.schema` from the latest `.commit` and
  look for `"fields": []`. Established tables are not exposed to this — confirm the table is new.
- **Mitigation ladder:**
  1. **Do not restart the job.** The checkpoint lives in commit metadata, so restarting re-throws the same
     exception. This is the counter-intuitive step, and it is the first thing most operators try.
  2. Apply a release with a null guard in schema reconciliation. There is no safe config-only escape.
  3. Without a patched release, the only alternative is hand-advancing the checkpoint past the empty
     batch — which risks skipping un-ingested data. Weigh that explicitly and in writing before doing it.
  4. Prevent recurrence by pinning an explicit source schema, so a zero-row first batch cannot produce a
     zero-column schema.
- **Governing configs:** `hoodie.streamer.schemaprovider.source.schema.file`,
  `hoodie.streamer.schemaprovider.target.schema.file`, `hoodie.avro.schema.validate`
- **`fixedInVersion`:** n/a — the null guard is the fix · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public — the trigger is narrow (a brand-new table whose very first batch is empty)

---

## HUDI-RCA-031 — Timeline-server or marker creation failure

- **Category:** `environment`, sometimes `defect` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieRemoteException: Failed to create marker file \S+\.marker\.(CREATE|MERGE|APPEND)`
  - `java\.net\.SocketTimeoutException: Read timed out` at
    `TimelineServerBasedWriteMarkers.executeRequestToTimelineServer`
  - `HoodieRemoteException: .*status code: 500`
  - `Failed to read MARKERS file`
  - `WARN PriorityBasedFileSystemView: Got error running preferred function\. Trying secondary`
  - Frames: `TimelineServerBasedWriteMarkers.create`, `HoodieWriteHandle.createMarkerFile`,
    `LazyIterableIterator.next`
- **Symptom:** Writes fail mid-stage with a connection or timeout error against an ephemeral
  driver-hosted HTTP endpoint. Intermittent, worse on busy drivers.
- **Root cause:** Hudi's embedded timeline server runs on the Spark driver, and executors call it over
  HTTP for marker creation and file-system views. A saturated driver — large active timeline, huge
  file-system view, GC pressure — times those calls out. A socket timeout reads as a network problem; the
  cause is usually **driver saturation**.
- **Evidence required:** stacktrace · `.hoodie` instant count · driver log and GC behaviour ·
  health-checker output (is archival running? is a savepoint pinning it?)
- **Disambiguation:** this is frequently a **consequence**, not a root cause. If the same table also shows
  a large instant count or a stuck rollback, fix HUDI-RCA-020 or HUDI-RCA-021 first — marker timeouts
  will clear on their own. Treat a marker timeout on an otherwise-healthy small table as the primary
  fault; on a big or wedged table, treat it as a symptom.
- **Mitigation ladder:**
  1. **Immediate unblock:** `hoodie.write.markers.type=DIRECT`. Removes the driver round-trip entirely at
     the cost of more object-store writes. Almost always works.
  2. Raise `hoodie.markers.timeline_server_based.batch.num_threads` to stay on timeline-server markers on
     a busy table.
  3. Set `hoodie.embed.timeline.server=false` if you need to be unblocked and can accept the performance
     cost.
  4. Fix the underlying driver pressure: get archival running, move the file-system view off heap, or give
     the driver more memory.
  5. Upgrade — several of these were genuine server-side defects fixed in 0.12 and later.
- **Governing configs:** `hoodie.write.markers.type`,
  `hoodie.markers.timeline_server_based.batch.num_threads`, `hoodie.embed.timeline.server`,
  `hoodie.filesystem.view.type`, `hoodie.filesystem.remote.backup.view.enable`
- **`fixedInVersion`:** 0.12.0 for several server-side defects · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** true — `DIRECT` markers are a reasonable standing choice on busy drivers
- **Provenance:** #4230, #5460, #6900, #7689

---

## HUDI-RCA-032 — Missing `hoodie.properties` or instant files

- **Category:** `environment` / `defect` · **Discoverability:** `self-evident` that something is wrong,
  `needs-expertise` for recovery
- **Signature:** The writer or a reader cannot open the table at all. `hoodie.properties` absent from
  `.hoodie/`, or timeline instant files missing.
- **Symptom:** Total failure to open the table, rather than a failure during a write.
- **Root cause:** The table's config file or timeline entries were removed — an errant storage lifecycle
  rule, an interrupted copy or migration, or bad cleanup.
- **Evidence required:** `.hoodie` listing for **both** the data table and the metadata table · writer props
- **Mitigation ladder:**
  1. **Back up the whole `.hoodie` directory before any repair.**
  2. If `hoodie.properties` is missing, look for `hoodie.properties.backup` alongside it — the CLI's
     config-recovery path restores from it.
  3. With no backup, reconstruct from the writer's known configuration: table type, key generator, record
     key and partition fields, payload class, table version. **Get expert eyes on this** — a wrong value
     silently changes how every future read resolves records, which is worse than the outage.
  4. If the **metadata table** is the affected one, do not hand-repair it. Disable the metadata table, let
     the data table proceed, then re-enable to rebuild from scratch.
- **Governing configs:** `hoodie.metadata.enable`, and every immutable key listed under HUDI-RCA-008
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** no canonical public issue — the phrase "hoodie.properties not found" recurs across
  many tracker threads without a single representative report

---

## HUDI-RCA-033 — Reading commit files races the inline archiver

- **Category:** `defect` (concurrency) · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.io\.FileNotFoundException` reading a `\.deltacommit` or `\.commit` from `.hoodie/`, in a code
    path **walking the timeline** — typically a catalog sync or an inspection tool, not the writer
  - **Giveaway: an alternating pass/fail pattern across successive cycles.**
- **Symptom:** A `FileNotFoundException` for a file that genuinely existed moments earlier. The
  alternating rhythm is the only reliable clue.
- **Root cause:** Hudi's inline archiver writes a commit into `.hoodie/archived/` and **deletes** it from
  `.hoodie/` as part of the commit. Anything holding a cached timeline listing and then reading individual
  commit files can find them gone. Walking **oldest to newest** maximizes the odds, because the oldest
  instant is precisely the one about to be archived.
- **Evidence required:** stacktrace · timing correlation with archival · the reader's iteration order
- **Disambiguation:** against HUDI-RCA-018 and HUDI-RCA-025, the discriminators are that the failing
  reader is **not the writer**, and that the failures alternate rather than persist. A persistent
  failure on the same file is not this pattern.
- **Mitigation ladder:**
  1. In any code reading individual commit files from a cached timeline, iterate **newest to oldest** and
     tolerate `FileNotFound` for an entry since archived. Newest instants are very unlikely to be
     archived mid-read.
  2. Refresh the timeline immediately before reading, rather than trusting a cached listing.
  3. If you cannot change the reader, move archival off the inline commit path so the deletion window is
     not inside every commit.
  4. **General rule worth remembering:** any optimization that replaces a bulk listing with targeted
     commit-file reads newly exposes you to concurrent archival. That trade-off is the pattern.
- **Governing configs:** `hoodie.archive.automatic`, `hoodie.archive.async`, `hoodie.keep.min.commits`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public

---

## HUDI-RCA-034 — Metadata-table clean cannot create its requested instant

- **Category:** `defect` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieIOException: Failed to create file \S+/\.hoodie/metadata/\.hoodie/\d+\.clean\.requested`
  - Frames: `HoodieWrapperFileSystem.createImmutableFileInPath` →
    `HoodieActiveTimeline.saveToCleanRequested` → `HoodieBackedTableMetadataWriter.cleanIfNecessary`
- **Symptom:** Repeated clean failures on the metadata table, surfacing as an IO exception.
- **Root cause:** The metadata table's cleaner tries to create a `.clean.requested` instant that already
  exists, so the immutable-file create fails. It is a state and idempotency problem wearing an IO
  exception's clothes.
- **Evidence required:** stacktrace · metadata-table timeline listing · writer props (cleaner config)
- **Disambiguation:** **the path is under `.hoodie/metadata/.hoodie/`** — the *metadata table's* timeline,
  not the data table's. An otherwise-identical message against the data table's `.hoodie/` is a different
  problem.
- **Mitigation ladder:**
  1. Confirm a stale `.clean.requested` or `.clean.inflight` exists on the metadata table's timeline.
  2. Shift the clean schedule so the next attempt lands on a different instant — lengthen cleaner
     frequency, let two commits succeed, then revert. Counter-intuitive, because the fix is a timing
     change rather than a state repair.
  3. If it persists on a current release, treat it as a regression — this signature was fixed upstream and
     should not recur.
  4. Last resort: disable the metadata table, let the data table progress, re-enable to rebuild.
- **Governing configs:** `hoodie.metadata.enable`,
  `hoodie.metadata.derive.from.datatable.clean.policy`, `hoodie.metadata.compact.max.delta.commits`,
  `hoodie.clean.automatic`, `hoodie.cleaner.commits.retained`
- **`fixedInVersion`:** fixed upstream; recurrence on a current release indicates a regression
- **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public with this signature

---

# Group D — environment

Hudi is fine. The cluster, the cloud, or a dependency failed.

---

## HUDI-RCA-035 — Classpath or bundle version mismatch

- **Category:** `environment` · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.lang\.NoSuchMethodError: scala\.\S+`
  - `java\.lang\.NoSuchMethodError: org\.apache\.hadoop\.\S+`
  - `java\.lang\.NoClassDefFoundError: \S+`
  - First Hudi frame is typically `HoodieSparkSqlWriter$.write` at a **suspiciously early line number**
- **Symptom:** Fails immediately on the first write, before any Hudi logic runs.
- **Root cause:** The Scala binary version, Spark minor version, or Hive metastore version does not match
  the `hudi-spark{X}-bundle` in use. Hudi bundles are compiled per Spark-and-Scala combination.
- **Evidence required:** stacktrace · the environment: Spark version, Scala version, bundle artifact,
  Hive version
- **Disambiguation:** a `NoSuchMethodError` naming a Scala class is a Scala binary mismatch; one naming a
  Hadoop or Parquet class is a dependency-version mismatch; `NoClassDefFoundError` for a cloud SDK class
  is a missing bundle rather than a wrong one. All three resolve to "match the bundle", but naming which
  one saves a cycle.
- **Mitigation ladder:**
  1. Match the bundle exactly — `hudi-spark3.4-bundle_2.12` for Spark 3.4 with Scala 2.12. Check the
     `spark-avro` Scala suffix too.
  2. Remove duplicate or conflicting jars. Do not mix `--packages` and `--jars` for the same artifact.
  3. On managed Spark services, prefer the platform's own Hudi integration flag over hand-managed jars,
     and set `spark.serializer` to the Kryo serializer.
  4. On vendor Hive older than 2.3, upgrade Hive or move to a Spark version whose bundle matches.
- **Governing configs:** not Hudi configs — `spark.serializer`,
  `spark.sql.hive.convertMetastoreParquet`, and bundle selection
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** #1977, #8368, #5765, #6297

---

## HUDI-RCA-036 — Source file deleted after the incremental source tracked it

- **Category:** `environment` (upstream) · **Discoverability:** `needs-expertise`
- **Signature** (innermost cause):
  - `java\.io\.FileNotFoundException: No such file or directory: \S+`, where the missing path is under the
    **source** location
  - Wrapped in `HoodieUpsertException: Failed to upsert for commit time \d+`
- **Symptom:** Ingestion fails on every sync after upstream deleted or lifecycle-expired a file the source
  had already registered.
- **Root cause:** A cloud-object incremental source records the files it intends to consume, and Hudi
  expects every tracked file to exist. Upstream deletion between registration and consumption breaks that
  contract.
- **Evidence required:** stacktrace · writer props (source config) · the source bucket's lifecycle config
- **Disambiguation — critical, because the sibling pattern has nearly opposite mitigations:** the missing
  path is under the **source**, not under the table's `.hoodie/`. If the path is under the table and ends
  in `.log.<n>`, it is a deltacommit that raced an unfinished compaction, which is destructive to repair
  and requires L3 with a human — see HUDI-RCA-025's disambiguation note. **Do not apply the file-skip flag
  below to a missing file under the table**; skipping table files hides real corruption.
- **Mitigation ladder:**
  1. Enable the file-existence bypass so missing **source** files are skipped rather than fatal:
     `hoodie.streamer.source.s3incr.check.file.exists=true`.
  2. If it still fails on a prefix you know is gone, add a relative-path skip through
     `hoodie.streamer.source.cloud.data.ignore.relpath.substring`.
  3. Fix the real problem upstream: lifecycle rules or cleanup jobs deleting source objects faster than
     ingestion consumes them.
- **Governing configs:** `hoodie.streamer.source.s3incr.check.file.exists`,
  `hoodie.streamer.source.cloud.data.ignore.relpath.substring`
- **`fixedInVersion`:** not a defect · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** false
- **Provenance:** none public with this signature

---

# Group E — not a failure

The right answer is to do nothing. These rows exist because the natural reaction — change a config,
restart, reset a checkpoint — is actively harmful, and because nobody files a public issue about
something that turned out to be correct. Expect these to be **under-represented everywhere except here.**

---

## HUDI-RCA-037 — Cleaner appears not to run (usually correct behaviour)

- **Category:** `not-a-failure` · **Discoverability:** **`silent`** — there is no error, just an absence
- **Signature:** **No exception.** An absence of recent `.clean` instants in `.hoodie/`, possibly
  alongside a growing active timeline.
- **Symptom:** "The cleaner isn't running." Usually it is, and it is finding nothing to do.
- **Root cause:** Most commonly, there is genuinely nothing to clean. A write that creates a **new file
  group** leaves no older version to remove, and Hudi also retains one prior version as a buffer, so a
  file group with two base files still has nothing to clean.
- **Evidence required:** **health-checker output** (`--checks cleaner,archival`) · commit metadata ·
  writer props (cleaner config)
- **Disambiguation — the decisive, cheap check:** in recent commit metadata compare `numWrites` and
  `numInserts` per file. **Equal means that write created a new file group**, so there is no older version
  to clean and the cleaner is working correctly. Different means the file group has multiple versions —
  but the one-version buffer still applies. Also read `earliestCommitToRetain` from the newest `.clean`:
  archival will not archive anything after that timestamp, so a stale clean fully explains a growing
  timeline without the cleaner being broken.
- **Mitigation ladder:**
  1. **Confirm it is benign before changing anything.** Most of these are non-incidents, and changing
     cleaner retention in response is how a non-incident becomes an incident.
  2. Recognize the legitimate causes: append-only or immutable data (no updates means no new versions); a
     recent retention change, which produces a quiet window equal to the difference; clustering having
     just consolidated many file groups into few; or a MOR table whose file groups writers only append to
     — the cleaner cannot clean a group that compaction has not yet given a new version.
  3. If clean genuinely is not running **and** a compaction or rollback is stuck, that is the real problem —
     work ladder **L4**, not the cleaner config.
  4. Note that **incremental clean can permanently miss partitions** that received no new ingests, leaving
     old file versions forever. Periodically run a **full** clean rather than incremental.
- **Governing configs:** `hoodie.cleaner.policy`, `hoodie.cleaner.commits.retained`,
  `hoodie.cleaner.hours.retained`, `hoodie.clean.max.commits`, `hoodie.clean.automatic`,
  `hoodie.clean.trigger.strategy`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** yes — the `cleaner` health check distinguishes
  "behind" from "nothing to do"
- **`alsoAppliesWhenHealthy`:** **true** by definition — this row *is* the healthy case
- **Provenance:** no canonical public issue — consistent with a pattern whose correct resolution is
  "nothing is wrong", which nobody files

---

## HUDI-RCA-038 — Source has no new data

- **Category:** `not-a-failure` · **Discoverability:** **`silent`** — nothing is wrong and nothing is
  happening, which is the hardest state to reason about
- **Signature:** Repeated log lines reporting **0 messages**, with latest and committed offsets equal, and
  **no ERROR-level lines in the same window.**
- **Symptom:** A stream appears stuck. No exceptions, no restarts, no progress.
- **Root cause — two cases, and they must be distinguished before acting:**
  - The source genuinely has no new data. Nothing is wrong.
  - The stream is wedged in a bootstrap or initialization state and will never consume even when data
    arrives.
- **Evidence required:** driver log **at INFO level, not just ERROR** · source offsets · the stream's
  lifecycle state
- **Disambiguation:** confirm there are **no ERROR lines** in the window first — if there are, that is the
  real problem and this row does not apply. Then compare latest against committed offsets: equal and both
  advancing over time is healthy; equal and **frozen** while the source is known to be receiving data is a
  wedge. **The "0 messages" log alone is not proof of a wedge.**
- **Mitigation ladder:**
  1. Healthy case: **do nothing.**
  2. Wedged case: restart the driver. The stream resumes from the last committed offset checkpointed in
     `.hoodie/`, so no data is lost; the in-flight micro-batch is dropped and re-read idempotently. Expect
     a short ingestion gap.
  3. If restarting does not clear it, the stream never left bootstrap — recreate the flow rather than
     restarting repeatedly.
- **Governing configs:** `hoodie.streamer.source.kafka.topic`, source-specific offset configs
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** no
- **`alsoAppliesWhenHealthy`:** **true** by definition
- **Provenance:** none public — nobody files "my job did nothing and that was correct"

---

## HUDI-RCA-039 — Spurious data files detected at commit

- **Category:** `not-a-failure` (a transient `defect` whose correct response is to wait) ·
  **Discoverability:** `needs-expertise` — a frightening message that is usually benign
- **Signature** (innermost cause):
  - `org\.apache\.hudi\.exception\.HoodieException: Failing commit \d+ due to presence of spurious data files in addition to markers`
- **Symptom:** A clustering or write commit aborts. The next run usually succeeds.
- **Root cause:** Marker-based reconciliation at commit found more data files on storage than the markers
  account for — typically output from a retried task attempt. The guard is doing its job.
- **Evidence required:** stacktrace · whether the **next run** succeeded
- **Disambiguation:** this is benign **only if it does not recur**. If it fires on three or more
  consecutive runs, stop treating it as transient and investigate the file groups involved as
  HUDI-RCA-028 (two base files for one file slice), which is a genuine correctness problem.
- **Mitigation ladder:**
  1. **Do nothing.** Verify that the next run succeeds. This is the whole recommendation and it is correct
     far more often than it feels.
  2. Escalate only if it recurs across three or more consecutive runs.
  3. If it does recur, treat it as HUDI-RCA-028 and investigate the file groups involved.
- **Governing configs:** `hoodie.write.markers.type`, `spark.speculation`
- **`fixedInVersion`:** n/a · **`preflightGuardExists`:** yes — this message *is* the guard firing
- **`alsoAppliesWhenHealthy`:** **true** by definition
- **Provenance:** none public — nobody files a bug for something that fixed itself

---

## Coverage notes

**Flink.** These recipes come from users running Spark, because that is where the evidence came from. The
core and table-layer patterns — schema (001, 003, 004, 030), keys (002, 005), timeline and table services
(018, 020, 025, 027, 033, 034, 037), and the metadata table (026, 029) — describe table-layer mechanisms
that apply to Flink deployments too, though the surrounding frames and the engine-side ladder steps will
differ. Flink-specific failure modes (checkpoint tolerance, bucket fileID collisions on parallelism
change, autoscaling interactions) are **not** covered here and will deepen over time.

**Not covered, deliberately.** Deeply managed-platform-specific failures with no open-source analogue;
Spark-side tuning that belongs in Spark's own documentation rather than Hudi's; and patterns whose
diagnosis requires forensic access an open-source user cannot reproduce.

**Adding a row.** Give it the next free ID; never reuse or renumber. Write the signature against the
innermost `Caused by`. If the signature collides with an existing row, add a line to the disambiguation
index rather than relying on prose inside the row. If the only remedy is destructive, copy the
explain-only block from HUDI-RCA-025 verbatim.
