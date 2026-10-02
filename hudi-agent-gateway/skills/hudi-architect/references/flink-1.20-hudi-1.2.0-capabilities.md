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
- Flink SQL with the Hudi DynamicTable connector as the future executable API.
- No XTable integration.

An explicit version outside this baseline is `REVIEW_REQUIRED`. Do not treat current `master`, a
newer release, or an older supported connector as equivalent without its own verified reference.

## Immutable validation input

`flink-1.20-hudi-1.2.0-capabilities.toml` is the machine-readable validation input. It records the
full Hudi release commit, Flink fixture version, source paths and hashes, the option allowlist for
the initial sink path, and later executable-path acceptance checks.

Normal validation must read that checked-in manifest. It must not discover accepted Flink options
by scanning the enclosing checkout: the checkout may be `master` or another release and can contain
options introduced after Hudi 1.2.0. `validate_flink_capabilities.py --verify-source` is an explicit
maintainer check that reads each source through `git show <pinned-revision>:<path>` and compares its
SHA-256 with the manifest. It never substitutes the working-tree copy.

Before producing a Flink safety assessment, run:

```bash
python3 validate_flink_capabilities.py --emit-evidence
```

Copy the returned baseline ID, manifest schema, Hudi source revision, and Flink fixture version into
the validation evidence. If the manifest cannot be loaded or validated, add
`FLINK_BASELINE_EVIDENCE_INVALID` with a `BLOCKED` contribution and make no baseline capability
claim. Continue collecting independent workload facts before selecting the final status.

The manifest allowlist covers the initial Flink SQL sink path; it is not an enumeration of every
option in Hudi 1.2.0. An option absent from the allowlist is unverified for generated output even if
the current checkout contains it. PR1 still does not generate an executable configuration.

## PR1 capability matrix

| Capability | PR1 behavior |
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
| Physical-schema parsing and DDL validation | Deferred to PR2 |
| Append-only record-key posture detection | Supported |
| Auto-generated-key durability warning | Supported |
| Replay-idempotence classification | Supported |
| Mutable COW, upsert, and deletes | `BLOCKED`; deferred to PR3 |
| MOR and compaction ownership | Deferred to PR4 |
| Flink SQL DDL and connector options | Not generated in PR1 |
| Executable sink-side example | Not generated in PR1 |
| `HoodieTableFactory` fixture validation | Deferred to PR2 |
| Spark and HoodieStreamer behavior | Must remain unchanged |

## Required PR2 acceptance contract

The following checks are recorded now so that the first executable path cannot claim
`CONFIG_VALIDATED` using only a successful sink-factory construction. Their stable identifiers and
requirements are also present in the machine-readable manifest.

### `FLINK_RECORD_KEY_FIELD_MISSING`

For append mode, `HoodieTableFactory.sanityCheck` skips `checkRecordKey`. PR2 must therefore add an
Architect-validator fixture that explicitly supplies a record-key field absent from the physical
schema and rejects it before factory validation. The fixture must use the pinned Hudi revision and
Flink 1.20.1.

### `FLINK_APPEND_MODE_CLUSTERING_ENABLED`

On Hudi 1.2.0, a COW insert is append mode only when the effective value of
`write.insert.cluster` is `false`. PR2 must pin or validate that value and reject an incompatible
`true` override. Checking only `write.operation = insert` is insufficient.

### `FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY`

PR2 must validate the generated `INSERT INTO` with a planner fixture that declares the source
schema and changelog contract. A successful `HoodieTableFactory` construction proves only that the
sink can be constructed; it does not prove that the source-to-sink statement is append-only. The
planner fixture remains local and requires no external source service.

## Status vocabulary

- `INCOMPLETE` — a required workload fact, schema, or lifecycle fact is missing.
- `BLOCKED` — the requested path is known to be outside the currently implemented Flink scope.
- `REVIEW_REQUIRED` — compatibility, writer topology, catalog behavior, or another operational
  risk requires human confirmation.
- `CONFIG_VALIDATED` — reserved for a later executable path whose load-bearing values have passed
  static validation. This status is unreachable in PR1.

No status claims that storage permissions, JAR deployment, source availability, catalog
connectivity, active writers, checkpoint behavior, or live table state were verified.
