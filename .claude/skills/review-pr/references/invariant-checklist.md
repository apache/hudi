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

# Hudi invariant checklist

Check every changed code path against these. A **BLOCKING** violation must be fixed before
merge; an **IMPORTANT** violation is a finding unless the surrounding code accepts the
tradeoff deliberately and says so.

Loaded by `review-pr` Step 3. Companion:
[`reviewer-priorities.md`](reviewer-priorities.md) for what reviewers weight and why.

## Breaking changes — BLOCKING

- `@PublicAPIClass` constructor or name changes, `@PublicAPIMethod` signature changes →
  breaking; needs an RFC / migration plan.
- Config key renames or removals without a `withAlternatives(...)` deprecation path.
- Config semantics silently changed — one config's change altering whether another takes
  effect.
- Storage-format changes without versioning or backward compatibility:
  - Avro schemas for timeline metadata (LSM, archived, active)
  - Metadata table Avro schema
  - Parquet footer metadata (bloom filters, key ranges) — must handle old **and** new encoding
  - Log format header/footer additions — old versions must handle new keys gracefully
  - Log block content serialization — the log block format must be versioned
  - Base / log file naming conventions
  - `.hoodie` metadata directory structure or paths
  - Metadata table partition names — needs migration steps
  - Record-to-file-group sharding within MDT partitions
  - Record key / metafield encoding

## Concurrency and data correctness — BLOCKING

- Timeline operations (request + complete) happen **within** a lock, and the lock is
  released on every path.
- The timeline is refreshed **inside** the lock's critical section before any check, so
  concurrent completions since lock acquisition are visible.
- The timeline is refreshed before requesting new actions (outside the lock).
- No check-then-act on distributed storage (HDFS, S3, GCS) — those operations are not
  atomic.
- Shared mutable state has correct `synchronized` / `volatile` / atomic access.
- Spark partition pruning, predicate pushdown, data skipping, index pruning, and schema
  evolution paths must not silently drop or corrupt data.

## Resource leaks — BLOCKING

- `SparkSession`, `JavaSparkContext`, `HoodieTableMetaClient`, `FileSystem`,
  `FSDataInputStream` / `FSDataOutputStream`, `HoodieTable*`, and record-stream iterators
  are closed via try-with-resources or `finally`.
- Spark RDD/Dataset references are not held beyond their intended scope (prevents GC).

## Metadata consistency — BLOCKING

- Data-path modifications have corresponding Metadata Table updates.
- Commits between the data table and MDT are atomic — a partial update leaves the table
  inconsistent.
- Changes to MDT partitions or record layout stay consistent with what the data path
  expects.

## Concurrency control — BLOCKING

- A new lock does not create deadlock potential against existing lock ordering — check
  multi-writer scenarios with concurrent ingestion, compaction, and clustering.
- Converting a lock-free structure to locked (or the reverse) requires analyzing every
  caller.
- Multi-writer conflict resolution handles all timeline action types, not just commits.

## File layout compatibility — BLOCKING

- Changes to `HoodieFileGroup` or `FileSlice` logic stay backward compatible with existing
  `HoodieTableFileSystemView` implementations.
- File-group assignment, file-slice ordering, and log-file association remain consistent
  with what existing readers expect.

## Schema evolution compatibility — BLOCKING

- Changes to `Schema.Field` handling preserve read compatibility for older Parquet/Avro
  base files.
- New fields have defaults; removed fields are handled gracefully on the read path.
- Schema reconciliation does not break data written with an older schema.

## Layer violations — IMPORTANT

- `hudi-common` must not depend on Spark or any engine-specific runtime.
- Engine-agnostic code must not import engine-specific classes.
- Changes under `packaging` need extra scrutiny for cross-version breakage.

## Silent failure — IMPORTANT

- No empty catch blocks, and no catch-and-log-only, in write / commit / storage paths —
  rethrow.
- No swallowed `IOException` in storage operations.
- No returning null or an empty `Option` where callers assume non-null.

## Memory management — IMPORTANT

- Memory-intensive operations (external sort, spill-to-disk, bloom filters) respect
  `maxMemoryPerPartition` and the related memory-budget configs.
- `DirectByteBuffer` off-heap buffers are explicitly released.
- Prefer iterators and streaming over materializing everything — flag `collect()`,
  `toList()`, or accumulation into a `List<HoodieRecord>` where an iterator-based approach
  is feasible.

## Completeness

- Works for both COW **and** MOR.
- Works across the relevant query types (snapshot, incremental, read-optimized).
- New public methods and behavior changes have tests.
- New configs: decide whether they should be marked advanced (power-user tuning).
