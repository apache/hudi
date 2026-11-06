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

# What Hudi reviewers actually scrutinize

Distilled from 30,530 review comments across 3,945 Apache Hudi PRs by five senior
reviewers: vinothchandar (project creator), nsivabalan, yihua, bvaradar, and danny0405
(Flink integration lead, 6,729 comments). Where a rule is attributed, that reviewer
enforces it repeatedly.

Loaded by `review-pr` Step 3. Companion:
[`invariant-checklist.md`](invariant-checklist.md) for the blocking gates.

## Project shape

- **Key modules**: `hudi-client` (write path), `hudi-common` (shared data structures,
  timeline, metadata), `hudi-spark-datasource` (Spark integration), `hudi-flink` (Flink
  integration, dedicated maintainer), `hudi-utilities` (HoodieStreamer ingestion).
- **Languages**: Java core, Scala for the Spark datasource / SQL integration.
- **Storage formats**: Parquet base files, Avro log files, custom log blocks.
- **Engine parity**: Spark and Flink are both first-class. Flink has its own write path,
  compaction operators, CDC/changelog support, and streaming semantics. Config
  declarations belong in `hudi-common`, not duplicated across the Spark and Flink modules.

## Review priorities, in order

1. **Thread safety and concurrency** — the most heavily scrutinized area. Reviewers
   construct thread-interleaving diagrams to prove race conditions. Scrutinize shared
   mutable state, lock ordering, heartbeat interactions, and timeline reload sequences.
   Flink adds checkpoint threads and compaction operators racing with commits.
2. **Metadata table (MDT) correctness** — valid instant filtering, rollback propagation
   from data table to MDT, compaction blocking rules, delta-commit consistency. Changes to
   `HoodieBackedTableMetadataWriter` / `HoodieBackedTableMetadata` deserve deep inspection.
   danny0405 flags where MDT compaction restrictions can be relaxed, since the MDT reader
   already filters invalid instants.
3. **Schema evolution and backwards compatibility** — Avro schema changes must preserve
   reader compatibility. Field additions need defaults; payload deserialization must handle
   older formats; check `InternalSchema` handling and Spark/Flink-to-Avro type mappings. In
   Flink, primary-key nullability differs from Spark.
4. **Error handling and failure modes** — "what happens if this fails mid-way?" Partial
   failures must leave the table recoverable via rollback. For Flink streaming jobs,
   checkpoint failure recovery paths are critical.
5. **Test coverage** — every new code path needs tests, covering both COW and MOR, edge
   cases (empty inputs, schema mismatches, concurrent ops), and parameterized tests for
   combinatorial scenarios. nsivabalan enumerates exact test matrices.
6. **Performance at scale** — Hudi runs on PB-scale tables with 10M+ files. Flag full-table
   scans, N+1 patterns, unbounded in-memory collections, redundant timeline reloads,
   missing `.persist()` on reused RDDs, and filesystem listing pressure (danny0405: read
   the schema directly from files, not via `TableSchemaResolver`).
7. **Config management** — new properties use the `ConfigProperty` builder with `.key()`,
   `.defaultValue()`, `.withDocumentation()`, `.sinceVersion()`; `.withAlternatives()` for
   renames. Consolidate duplicate declarations across Spark/Flink into `hudi-common`. Flink
   has split configs (separate scheduling vs execution for inline compaction) that must
   stay consistent with the Spark equivalents.
8. **API stability and design** — public API changes require deprecating the old method
   first. Separate transformation logic from container classes. Contracts must be
   consistent (e.g. RDD persist/unpersist ownership in the write client). bvaradar
   emphasizes clean separation of responsibilities in class hierarchies.

## Must-check patterns

Always flag these when they appear in a diff:

- **Shared mutable state without synchronization** — look for `volatile`, `synchronized`,
  `AtomicReference`, or an explicit lock on shared fields.
- **Missing null checks** on user configs, schema providers, filesystem calls, timeline
  instant details. (Only where a realistic null path exists.)
- **Timeline operations without reload** — stale timelines produce incorrect rollback and
  compaction decisions.
- **Avro `Schema` captured in a closure** — `org.apache.avro.Schema` is not serializable.
  Flag any lambda / RDD / Flink operator capturing it; pass strings and reconstruct.
- **Unclosed resources** — `HoodieFileReader`, `HoodieLogFileReader`, input streams.
- **Missing test for a new code path** — new branches, methods, or configs.
- **Config property without `sinceVersion` or documentation.**
- **Public API change without deprecation** — `BaseHoodieWriteClient`, `HoodieWriteConfig`,
  datasource options.
- **Hardcoded magic numbers** — thresholds, timeouts, batch sizes should be configurable.
- **MOR log/base file interactions** — verify ordering-field semantics and delete handling.
- **Flink rollback instant ordering** — rollback instants can carry timestamps greater than
  pending commits, causing timeline-service sync issues.
- **Flink primary-key nullability** — Flink and Spark differ; flag code assuming uniform
  nullability across engines.

## Anti-patterns

1. **Double-pass processing** — iterating the same collection twice where one pass suffices
   (parsing then re-parsing a file name).
2. **Redundant timeline reloads** — repeated `metaClient.reloadActiveTimeline()` in one
   operation path.
3. **Unsafe concurrent collection usage** — `HashMap` where `ConcurrentHashMap` is needed;
   iterating a collection another thread mutates.
4. **Catching generic `Exception`/`Throwable`** — hides real failures.
5. **Reassigning method return values** — one variable holding both the original and the
   transformed result.
6. **Duplicate code across COW/MOR or Spark/Flink** — logic that belongs in a shared base
   class, copy-pasted into engine- or table-type-specific implementations.
7. **Missing `.withAlternatives()` on a renamed config.**
8. **Ignoring table-version differences** — code assuming one version, when layouts,
   timeline formats, and MDT schemas differ across `HoodieTableVersion`.
9. **Filesystem listing where a metadata read suffices** — `TableSchemaResolver` or full
   listing when the schema can be read from the file or MDT.

## Code style

- **Imports**: grouped by package (java, javax, scala, org.apache.hudi, third-party). No
  wildcard imports in Java.
- **Naming**: `Hoodie` prefix for core abstractions. Config keys follow
  `hoodie.<module>.<feature>.<property>`. Vague names like `process()` or `handle()` get
  rejected. Names must reflect actual semantics — danny0405's example: `DeleteKey` is not
  really a key.
- **Logging**: SLF4J `LOG`, with context (instant time, partition path, file ID). No
  WARN/ERROR in expected paths; no noisy warnings from reflection-based constructor
  resolution.
- **Comments**: non-obvious concurrency invariants need a comment explaining the reasoning.
  TODOs must reference a JIRA ticket.
- **Simplification**: avoid multiple return points; prefer a single result variable.
  Reviewers here routinely post complete rewritten blocks rather than describing the change.

## Domain glossary a reviewer reasons with

**Timeline** — source of truth for table state. Instants are actions (commit, deltacommit,
compaction, clean, rollback, restore, savepoint) at a timestamp, moving requested →
inflight → completed. Active in `.hoodie/`, older instants archived. Rollback instants can
be newer than the instant they roll back — a known source of timeline-service sync bugs,
especially in Flink.

**Table types** — COW rewrites whole Parquet files on update. MOR appends to Avro log files
and compacts periodically. Flink MOR with CDC/changelog mode persists all change flags per
record for stateful streaming.

**Write operations** — `upsert`, `insert`, `bulk_insert`, `delete`, `insert_overwrite`,
`insert_overwrite_table`; each with distinct partitioning and file-sizing behavior.

**Table services** — compaction (MOR: logs → base files), clustering (layout), cleaning
(old versions), archival (old instants). Inline or async. Flink splits inline compaction
into separate scheduling and execution configs.

**Metadata table (MDT)** — file listings, column stats, bloom filters, RLI, secondary
indexes. Must stay in sync with the data-table timeline. MDT compaction must account for
pending data-table instants; the MDT reader filters invalid instants.

**File group model** — a file group holds a base file (Parquet) plus log files (Avro); a
file slice is one version of a file group. At table version 8, delta commits are not tied
to base-file commit time, which changes savepoint/restore semantics.

**Precombine / ordering field** — decides which record wins among duplicates; higher value
takes precedence. Must be passed consistently from the engine API down to I/O.

**Multi-writer concurrency** — optimistic concurrency with lock providers (ZooKeeper,
DynamoDB, HMS). Heartbeats detect failed transactions. MDT delta commits for table services
are deferred until execution, to avoid premature rollback.

**Flink streaming semantics** — continuous reads of COW/MOR, CDC changelog mode,
checkpoint-based commits, async compaction/clustering operators. The Flink writer handles
`RowKind` tags (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) for CDC sources; deletes are
emitted as empty-payload records when changelog mode is off.
