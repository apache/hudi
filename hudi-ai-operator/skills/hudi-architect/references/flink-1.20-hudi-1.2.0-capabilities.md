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
# Flink 1.20 and Hudi 1.2.0 capability baseline

This reference is the only verified baseline for the initial Flink-native Architect path:

- Apache Hudi 1.2.0.
- Apache Flink 1.20.
- Flink 1.20.1 for build and test fixtures.
- Hudi source revision `f05c83f2b97732de7a558ff9b26959e1139c05f5`.
- Flink SQL with the Hudi DynamicTable connector as the executable API.
- No XTable integration.

An explicit version outside this baseline is `REVIEW_REQUIRED`. Do not treat current `master`, a
newer release, or an older supported connector as equivalent without its own verified reference.

## Immutable validation input

`flink-1.20-hudi-1.2.0-capabilities.toml` is the machine-readable validation input. It records the
full Hudi release commit, pinned Maven fixture artifacts, source paths and hashes, the option
allowlist, the bounded design, reconciliation, and catalog-composition contracts, and acceptance
evidence.

Normal validation must read that checked-in manifest. It must not discover accepted Flink options
by scanning the enclosing checkout: the checkout may be `master` or another release and can contain
options introduced after Hudi 1.2.0. `validate_flink_capabilities.py --verify-source` is an explicit
maintainer check that reads each source through `git show <pinned-revision>:<path>` and compares its
SHA-256 with the manifest. It never substitutes the working-tree copy.

Before producing a Flink safety assessment, run:

```bash
python3 validate_flink_capabilities.py --emit-evidence
```

Copy the returned baseline ID, manifest and design-contract schemas, Hudi source revision, and
Flink fixture version into the validation evidence. If the manifest cannot be loaded or validated, add
`FLINK_BASELINE_EVIDENCE_INVALID` with a `BLOCKED` contribution and make no baseline capability
claim. Continue collecting independent workload facts before selecting the final status.

The manifest allowlist covers the initial Flink SQL sink path; it is not an enumeration of every
option in Hudi 1.2.0. An option absent from the allowlist is unverified for generated output even if
the current checkout contains it.

## Current capability matrix

| Capability | Current behavior |
|---|---|
| Select Flink after the shared tier gate | Supported routing |
| Hudi 1.2.0 / Flink 1.20 baseline disclosure | Supported |
| Immutable capability manifest and source revision evidence | Supported |
| Explicit version mismatch | `REVIEW_REQUIRED` |
| New-versus-existing detection | Supported |
| Existing-table exact-match reconciliation | Supported as a non-executable PR5 assessment |
| Existing-table SQL generation or automatic mutation | Not supported |
| Existing-table schema evolution or version upgrade | `REVIEW_REQUIRED`; no commands emitted |
| Separate-writer or cross-engine takeover | `REVIEW_REQUIRED` |
| Confirmed single-writer detection | Supported |
| Multi-writer or unknown writer model | `REVIEW_REQUIRED`; deferred to PR7 |
| External-catalog requirement detection | Supported |
| Five direct SQL / DFS Catalog / HMS Catalog profiles | Supported for new tables through PR6 |
| Direct SQL or DFS Catalog with sink-owned HMS sync | Supported |
| HMS Catalog | Supported only for external, application-owned tables with HMS discovery |
| DFS Catalog lifecycle | Catalog-owned; explicit acceptance required |
| Hive catalog freshness | Post-commit eventual; no atomicity or SLA claim |
| AWS Glue, embedded HMS, or managed HMS | `REVIEW_REQUIRED`; no executable output |
| Existing-table catalog registration or mutation | `REVIEW_REQUIRED`; assessment-only |
| Physical-schema availability detection | Supported |
| Bounded scalar physical-schema validation | Supported |
| Binary payload columns | Supported |
| `BYTES`, `BINARY`, or `VARBINARY` record-key and partition fields | `BLOCKED`; deterministic routing encoding is not available |
| Avro-compatible physical field names | Required; incompatible names are `BLOCKED` |
| Hudi fixed metadata field names in the target schema | `BLOCKED`; six exact names are reserved case-insensitively |
| `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ` precision 0 through 6 | Supported |
| Temporal precision above 6 | `BLOCKED` by the pinned connector limit |
| Other nested, computed, metadata, and watermark columns | `REVIEW_REQUIRED` |
| Append-only record-key posture detection | Supported |
| Auto-generated-key durability warning | Supported |
| Replay-idempotence classification | Supported |
| First bounded mutable COW upsert/delete path | Supported after schema-2 validation |
| First bounded mutable MOR upsert/delete path | Supported after schema-3 validation |
| Stable non-null mutable record key | Required; auto-key is `BLOCKED` |
| Mutable ordering | One non-null `BIGINT` or `TIMESTAMP(p<=6)` event-time field |
| Mutable source changelog | Normalized UPSERT (`I`, `UA`, `D`, no `UB`) |
| Mutable delete payload | Full projected row required when deletes are emitted |
| Partition changes for an existing key | `BLOCKED`; immutable partition values required |
| Mutable index | Global `FLINK_STATE`, TTL 0, bootstrap enabled |
| Alternative indexes, expiring state, custom mergers, or retract normalization | `BLOCKED`; deferred |
| Full changelog history / Hudi CDC query | Not claimed; output is latest-state behavior |
| COW/MOR derivation | Workload-derived for mutable paths; append-only remains COW |
| MOR compaction ownership | Same Flink writer only; standalone or unknown ownership withholds SQL |
| MOR compaction | Async schedule and execution, `num_commits`, five delta commits |
| MOR snapshot freshness | Base files plus log state at a visible completed instant |
| MOR read-optimized freshness | Compacted base-file state; current only after compaction |
| Detailed compaction tuning or clustering | `BLOCKED`; deferred to operations/separate work |
| New-table, single-writer, append-only COW SQL | Supported after static validation |
| Stable-key DDL | `PRIMARY KEY (...) NOT ENFORCED` |
| Explicitly accepted auto-generated keys | Supported with durable warning |
| Streaming checkpoint contract | Required; interval must be at least 1000 ms |
| Declared source schema and `INSERT_ONLY` changelog | Required |
| Flink SQL DDL and connector options | Validator-rendered for eligible requests |
| Executable sink-side `INSERT INTO` | Validator-rendered for eligible requests |
| Hudi 1.2.0 `HoodieTableFactory` fixture | Supported through pinned release bundle |
| Flink 1.20.1 planner fixture | Supported with a local declared source contract |
| Spark and HoodieStreamer behavior | Must remain unchanged |

## Implemented PR6 catalog composition contract

PR6 adds catalog contract schema 1 alongside design schemas 1, 2, and 3. It supports exactly five
profiles: direct SQL without discovery, direct SQL plus Hive Metastore discovery, Hudi DFS Catalog
without discovery, Hudi DFS Catalog plus Hive Metastore discovery, and Hudi HMS Catalog plus Hive
Metastore discovery. The legacy no-block, `external_catalog=false` path remains direct SQL and
retains its existing SQL byte-for-byte.

DFS Catalog requires `catalog.path`, derives `${catalog.path}/${database}/${table.name}`, and owns
the data lifecycle; its generated table DDL omits `path`. HMS Catalog requires non-embedded Hive
configuration, renders the desired table path, and is limited to `table.external=true` so the
application owns data. Direct and DFS Hive discovery uses sink-owned `hive_sync.*` options; HMS
Catalog supplies those values from catalog context and the validator checks their effective
configuration without duplicating them.

Sink-owned metastore URIs are limited to credential-free `thrift://host:port` endpoints with no
path, query, or fragment. Catalog-managed streaming sources use a three-part identifier and cannot
resolve to the same complete object as the new sink target. Missing lifecycle choices remain
unresolved; explicit requests for DFS application ownership or managed HMS tables are conflicts.

MOR registration derives `RT`, `RO`, or `ALL` from the declared query modes. With the pinned
defaults (`skip_ro_suffix=false` and snapshot-with-base-name true), ALL registers `base_ro`,
`base_rt`, and the base snapshot name. Catalog freshness is always recorded as
`post_commit_eventual`: Hudi invokes Hive sync asynchronously after a successful checkpoint
commit, so static validation does not claim atomic external visibility.

The released-bundle Java fixture selects DFS and HMS catalog implementations, rejects unknown and
embedded modes, checks DFS path derivation and external HMS posture, exercises DFS Catalog DDL and
an insert plan, and verifies Hive sync/naming defaults. It does not start HMS or Glue. Live
connectivity and visibility are deployment checks rather than static status findings.

## Implemented PR5 existing-table reconciliation contract

PR5 reuses the explicit schema-1, schema-2, or schema-3 desired design and changes only the table
lifecycle to `existing`. It accepts normalized, provenance-bearing evidence for persisted table
properties, the observed table path, the physical Hudi schema, writer inventory, and timeline state. Every evidence item
has an observation time and SHA-256 source digest; contradictory sources remain visible rather
than being silently merged.

The desired table path is also the authority boundary. Evidence scoped to another table remains in
the report with its provenance and a review finding, but its facts are excluded before target-table
observations and comparisons are built.

Persisted `HoodieTableConfig` facts are authoritative for table name, type, version, timeline
layout, file format, record key, ordering, merge mode, partitioning, and key-generator type.
The table path has separate identity evidence; it is not a persisted table-config key. Datetime
partition formats also require effective writer-configuration evidence. The latest Hudi
table schema is authoritative for physical columns. A Flink DDL is only registration evidence.
This mirrors the pinned `HoodieTableFactory` behavior: existing table config can supply record-key
and ordering options, and its table type overrides a conflicting Flink table option.

The first bounded compatible result requires current and initial table version 9, timeline layout
2, PARQUET base files, an exact durable-property match, and an exact physical-schema match. A version difference,
schema reorder, additional field, pending instant, or evidence disagreement is unverified rather
than guessed. A durable property or incompatible schema mismatch is a conflict. Partition-field
type suffixes in Hudi table config are normalized before comparison.

The manifest also pins the required and conditional fact names, every comparison field, comparison
and reconciliation vocabularies, finding-to-status mapping, and the four change dispositions. A
custom key generator outside the pinned built-in mappings is unverified rather than treated as a
proven conflict. A recoverable writer-state-only mismatch yields
`DEPLOYMENT_CONFIGURATION_CHANGE`; it does not imply that table migration is required. Timestamp
partition-setting differences instead remain `UNVERIFIED`, because changing the next writer cannot
reconcile physical partition paths already produced by another format or timezone.

Writer transition is explicit. Same-job restore requires recoverable checkpoint or savepoint
evidence plus matching design schema, operation, mutable index state, and MOR compaction ownership.
Cold start requires a stopped previous Flink writer and no concurrent writer; mutable cold start
also emits an index-bootstrap-before-first-checkpoint deployment check. A distinct Flink writer
takeover and cross-engine takeover remain review-required. Writer inventory and
timeline state must be rechecked immediately before deployment because their evidence is
time-sensitive.

The reconciliation result is `COMPATIBLE`, `CONFLICT`, or `UNVERIFIED`, independently of the
existing final-status precedence. Even `CONFIG_VALIDATED` plus `COMPATIBLE` has
`executable_eligible=false`: PR5 emits comparisons, provenance, findings, a bounded change
disposition, and deployment checks, but no SQL, mutation, upgrade, or migration commands.

## Implemented PR2 acceptance contract

The first executable path cannot claim `CONFIG_VALIDATED` using only a successful sink-factory
construction. The stable identifiers and their Python and Java evidence are recorded in the
machine-readable manifest.

### `FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED`

The pinned Hudi 1.2.0 `RowDataKeyGen` converts Flink binary values through Java array
`toString()`, producing object-identity strings such as `[B@...`. Equal byte sequences can
therefore produce different record keys or partition directories. The design validator blocks
`BYTES`, `BINARY`, and `VARBINARY` only when used in either routing role; those types remain
available for payload columns. Python tests cover all three types in both roles, and the pinned
bundle fixture preserves the object-identity behavior as regression evidence.

### `FLINK_RECORD_KEY_FIELD_MISSING`

For append mode, `HoodieTableFactory.sanityCheck` skips `checkRecordKey`. The Architect validator
therefore rejects an explicit record-key field absent from the physical schema before factory
validation. The negative fixture is bound to this manifest and its pinned release artifacts.

### `FLINK_APPEND_MODE_CLUSTERING_ENABLED`

On Hudi 1.2.0, a COW insert is append mode only when the effective value of
`write.insert.cluster` is `false`. The design validator requires and renders that value and rejects
an incompatible `true` override. Checking only `write.operation = insert` is insufficient.

### `FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY`

The generated `INSERT INTO` is consumed by a Flink 1.20.1 planner fixture with a declared physical
source schema and `ChangelogMode.insertOnly()`. The Python validator independently rejects a
non-`INSERT_ONLY` source contract. The fixture is local and requires no external source service.

### `FLINK_TEMPORAL_PRECISION_UNSUPPORTED`

The pinned `HoodieSchemaConverter` maps `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ` only through
precision 6. The design validator blocks higher precision before SQL emission. Python tests cover
the 6, 7, and 9 boundaries for all three types, and the pinned planner fixture reproduces the
connector rejection at 7 and 9 for both timestamp forms. A direct pinned-converter test covers
the same boundaries for `TIME`, closing the adjacent path governed by the same connector limit.

### `FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED`

The Hudi sink builds an Avro-backed physical schema, so a Flink-quoted name such as `user-id` is
not sufficient. The design validator requires every source and target field name to match
`^[A-Za-z_][A-Za-z0-9_]*$` before rendering. Python tests cover valid and invalid boundaries, and
the pinned planner fixture preserves the connector failure as regression evidence. The validator
does not silently sanitize names because doing so would alter schema, key, and partition semantics.

### `FLINK_HUDI_METADATA_FIELD_CONFLICT`

The pinned Hudi write path prepends the six names in
`HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION`. The design validator therefore rejects those
exact target names case-insensitively before SQL emission, while allowing other `_hoodie_`-prefixed
names. Python tests cover all six names, case-insensitive matching, and the non-reserved prefix
boundary. The pinned writer-path fixture reproduces Flink's duplicate-field failure for all six
names and anchors the canonical reserved-name set.

### `FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED`

Flink 1.20.1 rejects checkpoint intervals below its 10 ms runtime minimum. The bounded Architect
path deliberately applies a stricter 1000 ms safety floor and blocks lower values instead of
silently increasing them. The pinned runtime test preserves the 9/10 ms dependency boundary, and
the Python validator tests preserve the 999/1000 ms Architect boundary.

## Implemented PR3 mutable COW contract

The schema-2 path is deliberately narrower than the full Hudi upsert surface. It accepts only a
new table, one writer, streaming checkpoints, stable non-null identity, one
event-time ordering field, normalized UPSERT input, immutable partitions, and full-row deletes when
present. All three resolved replay answers are eligible; only an unresolved replay posture blocks
validation. The exact ordering-field value `no_precombine` is rejected because Hudi 1.2.0 resolves
that sentinel to no ordering field even if the physical schema contains a same-named column.
The renderer fixes `COPY_ON_WRITE`, `upsert`, `EVENT_TIME_ORDERING`, global `FLINK_STATE`, state
TTL `0`, index bootstrap, and `changelog.enabled=false`; it accepts no pass-through override.

The pinned Flink planner fixture consumes the exact SQL golden with an `I/UA/D` source and proves
that the generated graph includes changelog normalization, the Hudi stream writer, and
`index_bootstrap`. Direct pinned-option fixtures preserve the ordering, index, TTL, bootstrap, and
changelog values. Python tests cover the successful golden and fail-closed deviations.

Bootstrap closes the cold-start gap for a table created and operated under this contract: state
loss must not make existing keys look new. It does not expand the path into arbitrary
existing-table takeover; that still needs the separate PR5 evidence and compatibility flow.

## Implemented PR4 mutable MOR contract

Schema 3 reuses every schema-2 mutable safety invariant and changes only the table type plus an
explicit read/table-service contract. The skill derives MOR from sustained mutable writes and
reader needs; it does not ask the user to select a Hudi table type without context. Low or
occasional updates, simplest-operation priority, and strict read-optimized freshness derive COW.
Append-only MOR is rejected because Hudi 1.2.0 treats MOR insert as append mode and does not enter
the verified mutable compaction schedule path.

The canonical MOR sink pins `MERGE_ON_READ` upsert, event-time ordering, global `FLINK_STATE` with
TTL 0 and bootstrap, and `changelog.enabled=false`. The same Flink writer owns compaction;
`compaction.schedule.enabled`, `compaction.async.enabled`, and
`compaction.operation.execute.async.enabled` are true, with `num_commits` and five delta commits.
Both clustering controls remain false. Five is the Hudi 1.2.0 effective default selected as a fixed
bounded contract, not a workload-tuned SLA.

Snapshot queries read base files plus log state at a visible completed instant. Read-optimized
queries read compacted base files and can lag delta commits until compaction. The reader requirement
is recorded in the design contract, but reader-side query options are not emitted into sink DDL.
A standalone compactor is treated as an independent writer and remains review-required; unknown
ownership also fails closed. Compaction parallelism, time triggers, target IO, clustering,
concurrency configuration, and live service execution are not claimed.

The pinned Flink planner fixture consumes the exact schema-3 golden SQL and verifies the mutable
writer, index bootstrap, `compact_plan_generate`, `compact_task`, and `compact_commit` operators.
Focused release-bundle fixtures retain effective MOR, compaction, clustering, and query-mode
semantics without claiming that a live job ran.

## Status vocabulary

- `INCOMPLETE` — a required workload fact, schema, or lifecycle fact is missing.
- `BLOCKED` — the requested path is known to be outside the currently implemented Flink scope.
- `REVIEW_REQUIRED` — compatibility, writer topology, catalog behavior, or another operational
  risk requires human confirmation.
- `CONFIG_VALIDATED` — every applicable gate and load-bearing value passed pinned static
  validation. PR2, PR3, and PR4 also emit canonical SQL; PR5 instead requires a `COMPATIBLE`
  assessment and always remains non-executable.

No status claims that storage permissions, JAR deployment, source availability, catalog
connectivity, active writers, checkpoint behavior, or live table state were verified.
