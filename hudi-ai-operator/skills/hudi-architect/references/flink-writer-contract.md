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
# Flink multi-writer Writer Contract

Use this reference only after F2 confirms an independent writer or separately deployed table
service. The contract is orthogonal to design schemas 1, 2, and 3. It records the writer topology
and assesses pinned Hudi 1.2.0 constraints; it never selects OCC, NBCC, a lock provider, an index,
or a recovery strategy, and it never makes multi-writer SQL executable.

## Contract shape

Set `writer_contract.contract_schema` to `1`. Record one unique `primary_writer_id` for the Flink
design and at least one `additional_writers` entry. Each additional writer requires:

- a unique `writer_id`;
- role: `INGESTION`, `BACKFILL`, `CLEANUP`, `STANDALONE_COMPACTION`,
  `STANDALONE_CLUSTERING`, or `OTHER`;
- relationship: `INDEPENDENT_CONCURRENT` or `MAY_OVERLAP`;
- engine: `FLINK`, `SPARK`, `OTHER`, or `UNKNOWN`;
- concrete engine and Hudi version strings, using the literal `UNKNOWN` only when the fact has
  explicitly been established as unknown; and
- a canonical Hudi 1.2.0 `WriteOperationType` name, or `UNKNOWN`.

Do not duplicate primary engine, engine version, Hudi version, or operation in the input block.
The validator derives those facts from the existing Flink design contract.

`ownership.schema_evolution_owner_id` is required and must reference a declared writer,
`EXTERNAL_CONTROL_PLANE`, or `UNKNOWN`. `table_configuration_owner_id` is optional with the same
reference rule. `UNKNOWN` ownership produces `FLINK_WRITER_OWNERSHIP_UNRESOLVED`.

Express a standalone compactor or clustering service as an additional writer with the matching
role. Same-job asynchronous compaction remains part of design schema 3 and is not an independent
writer.

## Observed concurrency

`observed_concurrency` is optional and observation-only. When supplied, it requires:

- `mode`: `SINGLE_WRITER`, `OPTIMISTIC_CONCURRENCY_CONTROL`,
  `NON_BLOCKING_CONCURRENCY_CONTROL`, or `UNKNOWN`;
- the observed lock-provider class name; and
- `lock_configuration_consistency`: `CONSISTENT`, `CONFLICTING`, or `UNKNOWN`.

Never ask the user to choose a mode or provider to make validation pass. Do not collect provider
credentials, timeout/retry tuning, namespaces, conflict rates, keyspace assumptions, or file-group
overlap guesses.

The pinned source recognizes OCC and NBCC as multi-writer modes and requires an effective LAZY
failed-writes policy. PR7 still reports OCC as `UNVERIFIED`: its fixtures do not run concurrent
writers, a lock service, or recovery. NBCC requires MOR with simple bucket index (or the metadata
table). The existing schema-1/2 COW paths and schema-3 MOR path with `FLINK_STATE` therefore return
`UNSUPPORTED` and `FLINK_WRITER_COMBINATION_UNSUPPORTED`. Never switch the index automatically.

## Existing-table evidence

PR5 evidence may optionally add `active_writers` and `schema_evolution_owner_id` to
`writer_inventory`, and `write_concurrency_mode`, `lock_provider_class`, and
`lock_configuration_consistency` to `flink_writer_config`. Every item retains the existing
evidence ID, timestamp, digest, and exact table-path scope.

Only in-scope, non-conflicting observations contribute. Evidence disagreement or a mismatch with
the explicit Writer Contract produces `FLINK_EXISTING_EVIDENCE_CONFLICT` and/or
`FLINK_WRITER_CONTRACT_CONFLICT`. Keep the Writer Contract `CONFLICT`, reconciliation
`UNVERIFIED`, change disposition `UNVERIFIED`, and executable eligibility `false`; do not infer a
migration or silently replace either fact.

Assess supplied concurrency evidence even when `observed_concurrency` is absent from the explicit
contract. Preserve that evidence and its provenance as an unverified comparison; do not copy it
into `normalized_writer_contract` or treat missing optional contract facts as inferred values.
Apply the same independent capability assessment when the Writer Contract is missing or invalid:
the contract remains `INCOMPLETE`, while bounded NBCC evidence still returns `UNSUPPORTED`.
Reject concurrency modes and lock-consistency values outside the enumerations above.

Treat `active_writers` as a time-sensitive snapshot, not as the complete declared topology. Every
observed active writer must be declared, but a declared writer need not be active in that snapshot.
Multiple active writers cannot validate a single-writer design. Report the combination of multiple
active writers and `concurrent_writers: false` as internally inconsistent only when both facts come
from the same evidence source; do not combine separate snapshots into a fabricated contradiction.

## Result semantics

Return `writer_contract_result` as `NOT_APPLICABLE`, `COMPLETE`, `INCOMPLETE`, or `CONFLICT` and
`multi_writer_capability` as `NOT_APPLICABLE`, `UNVERIFIED`, or `UNSUPPORTED`. Include the
normalized contract, capability notes, and deployment checks. Never return `SUPPORTED`.

Deployment checks remind the operator to verify writer inventory, uniform concurrency mode, lock
configuration, LAZY failed-write handling, recovery procedure, and schema ownership. They are not
evidence that a deployment or service was inspected. The general existing-table checks include
`confirm_single_writer` only for a single-writer design; a multi-writer design uses the bounded
Writer Contract deployment checks instead.
