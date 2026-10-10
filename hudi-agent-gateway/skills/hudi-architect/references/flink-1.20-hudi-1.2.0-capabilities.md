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
allowlist, the bounded design contract, and executable-path acceptance evidence.

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
| Existing-table design | `BLOCKED`; deferred to PR5 |
| Confirmed single-writer detection | Supported |
| Multi-writer or unknown writer model | `REVIEW_REQUIRED`; deferred to PR7 |
| External-catalog requirement detection | Supported |
| Catalog or metastore composition | `REVIEW_REQUIRED`; deferred to PR6 |
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
| Stable non-null mutable record key | Required; auto-key is `BLOCKED` |
| Mutable ordering | One non-null `BIGINT` or `TIMESTAMP(p<=6)` event-time field |
| Mutable source changelog | Normalized UPSERT (`I`, `UA`, `D`, no `UB`) |
| Mutable delete payload | Full projected row required when deletes are emitted |
| Partition changes for an existing key | `BLOCKED`; immutable partition values required |
| Mutable index | Global `FLINK_STATE`, TTL 0, bootstrap enabled |
| Alternative indexes, expiring state, custom mergers, or retract normalization | `BLOCKED`; deferred |
| Full changelog history / Hudi CDC query | Not claimed; output is latest-state behavior |
| MOR and compaction ownership | Deferred to PR4 |
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
new table, one writer, no external catalog, streaming checkpoints, stable non-null identity, one
event-time ordering field, normalized UPSERT input, immutable partitions, and full-row deletes when present.
The renderer fixes `COPY_ON_WRITE`, `upsert`, `EVENT_TIME_ORDERING`, global `FLINK_STATE`, state
TTL `0`, index bootstrap, and `changelog.enabled=false`; it accepts no pass-through override.

The pinned Flink planner fixture consumes the exact SQL golden with an `I/UA/D` source and proves
that the generated graph includes changelog normalization, the Hudi stream writer, and
`index_bootstrap`. Direct pinned-option fixtures preserve the ordering, index, TTL, bootstrap, and
changelog values. Python tests cover the successful golden and fail-closed deviations.

Bootstrap closes the cold-start gap for a table created and operated under this contract: state
loss must not make existing keys look new. It does not expand the path into arbitrary
existing-table takeover; that still needs the separate PR5 evidence and compatibility flow.

## Status vocabulary

- `INCOMPLETE` — a required workload fact, schema, or lifecycle fact is missing.
- `BLOCKED` — the requested path is known to be outside the currently implemented Flink scope.
- `REVIEW_REQUIRED` — compatibility, writer topology, catalog behavior, or another operational
  risk requires human confirmation.
- `CONFIG_VALIDATED` — every applicable gate and all load-bearing PR2 or PR3 design values passed the
  pinned static validator. Canonical SQL was emitted without unresolved placeholders.

No status claims that storage permissions, JAR deployment, source availability, catalog
connectivity, active writers, checkpoint behavior, or live table state were verified.
