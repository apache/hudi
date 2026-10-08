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
# Flink warnings — routing and first executable path

Load these findings and advisories only for the Flink route. Each code has a deterministic trigger
and action. Surface it when the triggering answer lands rather than batching messages at the end.

## FLINK_BASELINE_EVIDENCE_INVALID

**Trigger:** The checked-in capability manifest cannot be loaded or validated, or its required
baseline evidence is unavailable.

**Message:** The Hudi 1.2.0 / Flink 1.20 capability claim cannot be reproduced from the pinned
manifest. The current checkout is not a substitute for that release input.

**Action:** `BLOCKED`; withhold every executable or version-specific capability claim while
continuing to collect independent workload facts.

## FLINK_VERSION_UNVERIFIED

**Trigger:** The user explicitly supplies a Hudi version other than 1.2.0 or a Flink version other
than 1.20.x.

**Message:** The verified baseline is Hudi 1.2.0 with Flink 1.20 (1.20.1 fixtures). This request
needs a version-specific capability review; the baseline will not be silently reused.

**Action:** `REVIEW_REQUIRED`; withhold executable output.

## FLINK_VERSION_REQUIRED

**Trigger:** The supplied Hudi or Flink version remains ambiguous after clarification.

**Message:** The deployed version is required before the checked-in Hudi 1.2.0 / Flink 1.20
baseline can be reused or rejected.

**Action:** `INCOMPLETE`; withhold version-specific capability claims and executable output.

## FLINK_EXISTING_TABLE_DEFERRED

**Trigger:** The target is an existing table.

**Message:** Existing table facts must be treated as authoritative evidence and cannot be replaced
with a new-table recommendation. Evidence comparison is deferred to PR5.

**Action:** `BLOCKED`; request only non-secret evidence if the user wants it recorded for later.

## FLINK_TABLE_LIFECYCLE_REQUIRED

**Trigger:** The user cannot confirm whether the target is a new or existing Hudi table.

**Message:** New-table design must not overwrite or substitute for facts from an existing table.
The table lifecycle must be known before choosing a path.

**Action:** `INCOMPLETE`; do not route through the new-table flow.

## FLINK_MULTI_WRITER_REVIEW

**Trigger:** Another pipeline, backfill, cleanup writer, cross-engine writer, standalone compactor,
or standalone clustering job can commit to the table.

**Message:** The single-writer assumptions are invalid. This path does not claim an OCC, NBCC,
lock-provider, index, or table-service combination is safe.

**Action:** `REVIEW_REQUIRED`; withhold concurrency-sensitive configuration.

## FLINK_WRITER_MODEL_UNRESOLVED

**Trigger:** The user cannot confirm whether another writer or standalone table service can commit
to the table.

**Message:** The single-writer assumption is unresolved, so concurrency-sensitive design cannot
be treated as safe.

**Action:** `REVIEW_REQUIRED`; do not infer `SINGLE_WRITER` or recommend concurrency settings.

## FLINK_EXTERNAL_CATALOG_REVIEW

**Trigger:** An external engine or BI tool must discover the table through a catalog or metastore.

**Message:** Flink table resolution and external metastore synchronization are separate
responsibilities that must be composed together. That composition is deferred to PR6.

**Action:** `REVIEW_REQUIRED`; generate no catalog or sync configuration.

## FLINK_CATALOG_REQUIREMENT_UNRESOLVED

**Trigger:** The user cannot determine whether anything outside the Flink application requires
catalog or metastore visibility.

**Message:** External discovery requirements are unresolved and cannot be replaced with an
assumption that path-based access is sufficient.

**Action:** `REVIEW_REQUIRED`; generate no catalog or sync configuration.

## FLINK_PHYSICAL_SCHEMA_REQUIRED

**Trigger:** Authoritative field names and types are missing, partial, guessed, or available only
through a schema location whose contents cannot be read safely in the session.

**Message:** A schema URI, catalog name, registry subject, or object-store path does not by itself
make the physical schema available. Concrete field names and types are required so record-key,
ordering, and partition references can eventually be checked.

**Action:** `INCOMPLETE`; withhold DDL.

## FLINK_MUTABLE_COW_DEFERRED

**Trigger:** Existing logical records can be updated or deleted.

**Message:** Mutable COW requires identity, ordering, and an upsert-capable path that is deferred
to PR3.

**Action:** `BLOCKED`; collect independent facts but generate no executable configuration.

## FLINK_MUTABILITY_REQUIRED

**Trigger:** The user cannot confirm whether logical records are append-only or mutable.

**Message:** Mutation behavior is required before the record-key and write path can be assessed.

**Action:** `INCOMPLETE`; do not infer append-only behavior.

## FLINK_RECORD_KEY_POSTURE_REQUIRED

**Trigger:** An append-only workload cannot confirm whether a stable business key exists.

**Message:** Record-key posture must be known before the skill can assess replay behavior and the
auto-generated-key trade-off.

**Action:** `INCOMPLETE`; withhold the record-key path.

## FLINK_AUTO_KEY_DURABILITY

**Trigger:** An append-only workload has no stable business key and replay behavior permits the
auto-generated-key path to be considered.

**Message:** Reprocessing the same business record can create another Hudi record with a different
generated key. A later move to upsert requires stable identity and may require migration or table
recreation.

**Action:** Explain the trade-off before asking for explicit acceptance. Record it as a durable
decision only after acceptance. This advisory alone does not change status.

## FLINK_AUTO_KEY_ACCEPTANCE_REQUIRED

**Trigger:** An eligible append-only workload has no stable business key, but auto-key acceptance
is not decided, is unanswered, or the session ends before acceptance.

**Message:** Auto-generated record keys require an explicit durability decision. Eligibility for
that path must not be implied while the decision is pending.

**Action:** `INCOMPLETE`; retain this finding and withhold executable output.

## FLINK_AUTO_KEY_DECLINED

**Trigger:** An eligible append-only workload has no stable business key and the user explicitly
declines auto-generated record keys.

**Message:** The request has neither a stable business key nor an accepted auto-generated-key
posture, so no record-key path is available.

**Action:** `BLOCKED`; retain this finding and withhold executable output.

## FLINK_STABLE_KEY_NOT_IDEMPOTENT

**Trigger:** An append-only workload has a stable business key.

**Message:** The key preserves business identity and future upsert compatibility, but it does not
make `write.operation = insert` deduplicate independent retries, replays, or backfills.

**Action:** Ask the separate replay-idempotence question. This warning alone does not change
status.

## FLINK_REPLAY_IDEMPOTENCE_DEFERRED

**Trigger:** Replayed or backfilled copies can occur and must collapse into one table record.

**Message:** This requires stable record identity and an upsert-capable design. The append-only
insert path cannot be presented as deduplicating.

**Action:** `BLOCKED` until the mutable COW path is implemented.

## FLINK_REPLAY_BEHAVIOR_UNRESOLVED

**Trigger:** The user cannot determine whether retries, replays, or backfills can repeat logical
records or whether repeated copies must collapse.

**Message:** Replay safety is unresolved, so the append-only insert path cannot be presented as
idempotent.

**Action:** `REVIEW_REQUIRED`; retain the uncertainty and withhold executable output.

## FLINK_RECORD_KEY_FIELD_MISSING

**Trigger:** A stable key or explicit record-key option names a field absent from the physical
schema, including when `write.operation=insert`.

**Message:** Hudi 1.2.0 skips its record-key field check in append mode, so the Architect validator
must reject this mismatch before factory validation.

**Action:** `BLOCKED`; emit no DDL or `INSERT INTO`.

## FLINK_RECORD_KEY_NULLABLE

**Trigger:** A stable record-key field is nullable.

**Message:** The canonical Flink primary key requires non-null identity fields.

**Action:** `BLOCKED`; require an authoritative non-null schema or a different stable key.

## FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT

**Trigger:** PRIMARY KEY syntax and a record-key option are both supplied, or the auto-key path
contains either form.

**Message:** The canonical PR2 output uses one identity representation. It does not rely on the
factory warning that PRIMARY KEY syntax takes precedence.

**Action:** `BLOCKED`; resolve the identity contract before rendering SQL.

## FLINK_PARTITION_FIELD_MISSING

**Trigger:** `PARTITIONED BY` references a field absent from the physical schema.

**Message:** Partition fields must resolve without guessing before table creation.

**Action:** `BLOCKED`; correct the schema or partition decision.

## FLINK_SCHEMA_TYPE_UNVERIFIED

**Trigger:** A physical field uses a complex, computed, metadata, watermark, or other type outside
the pinned PR2 scalar validation surface.

**Message:** The type may be valid Flink SQL, but it is not covered by this executable baseline.

**Action:** `REVIEW_REQUIRED`; withhold executable output instead of passing through unchecked DDL.

## FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED

**Trigger:** A physical source or target field name does not match
`^[A-Za-z_][A-Za-z0-9_]*$`.

**Message:** Flink can quote the identifier, but the pinned Hudi connector cannot represent it in
the Avro-backed physical schema.

**Action:** `BLOCKED`; require an authoritative compatible field name. Do not silently rename it.

## FLINK_TEMPORAL_PRECISION_UNSUPPORTED

**Trigger:** A `TIME`, `TIMESTAMP`, or `TIMESTAMP_LTZ` field declares precision outside 0 through
6.

**Message:** The pinned Hudi 1.2.0 schema converter supports temporal precision no greater than 6.

**Action:** `BLOCKED`; require an authoritative compatible type. Do not silently reduce precision.

## FLINK_SOURCE_CONTRACT_REQUIRED

**Trigger:** The source table, expected physical schema, or changelog semantics are unavailable.

**Message:** A sink-side example must identify the existing source table and its expected contract.

**Action:** `INCOMPLETE`; do not invent or generate a source connector.

## FLINK_SOURCE_SCHEMA_MISMATCH

**Trigger:** An explicitly projected source field is missing or differs in type or nullability from
the target field.

**Message:** PR2 does not infer casts, aliases, or schema reconciliation.

**Action:** `BLOCKED`; require an explicit compatible source contract.

## FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY

**Trigger:** The declared source changelog contains updates or deletes.

**Message:** Sink-factory construction does not prove the complete source-to-sink statement is
append-only. Mutable change semantics are deferred to PR3.

**Action:** `BLOCKED`; do not generate executable append SQL.

## FLINK_APPEND_MODE_CLUSTERING_ENABLED

**Trigger:** The effective value of `write.insert.cluster` is not `false`.

**Message:** In Hudi 1.2.0, COW insert is append mode only when insert clustering is disabled.
Checking only `write.operation=insert` is insufficient.

**Action:** `BLOCKED`; reject an incompatible override rather than silently changing it.

## FLINK_PR2_WRITE_PATH_UNSUPPORTED

**Trigger:** Table type, write operation, or execution mode is outside streaming COW insert.

**Message:** PR2 implements only the first bounded append-only SQL sink path.

**Action:** `BLOCKED`; route mutable COW to PR3 and MOR to PR4.

## FLINK_CHECKPOINTING_REQUIRED

**Trigger:** Checkpointing is explicitly disabled for the streaming sink.

**Message:** The streaming Hudi commit lifecycle depends on completed Flink checkpoints.

**Action:** `BLOCKED`; do not claim a normal commit cadence.

## FLINK_CHECKPOINT_INTERVAL_REQUIRED

**Trigger:** The checkpoint interval is absent, zero, negative, or unresolved.

**Message:** The executable runtime contract needs a concrete positive checkpoint interval.

**Action:** `INCOMPLETE`; withhold runtime SQL.

## FLINK_LOAD_BEARING_VALUE_REQUIRED

**Trigger:** The target table, target path, source table, or another load-bearing value is missing,
a placeholder, or contains credentials that must be redacted.

**Message:** `CONFIG_VALIDATED` cannot contain a value that still needs substitution.

**Action:** `INCOMPLETE`; request a concrete non-secret value.

## FLINK_OPTION_NOT_VERIFIED

**Trigger:** The design supplies a connector option absent from the pinned manifest allowlist.

**Message:** An option from the enclosing checkout or a later release is not evidence that the
Hudi 1.2.0 path supports it.

**Action:** `REVIEW_REQUIRED`; withhold executable output.

## FLINK_DESIGN_CONTRACT_INVALID

**Trigger:** The machine-readable design contract is malformed, omits a required structural field,
or contains a value the canonical renderer would silently ignore.

**Message:** Static validation cannot reproduce the intended design from the supplied contract.

**Action:** `INCOMPLETE`; correct the contract without inferring architecture decisions.

## FLINK_CHECKPOINT_SMALL_FILE_RISK

**Trigger:** Known rate, checkpoint cadence, and active-partition evidence indicates very little
data per checkpoint and partition.

**Message:** The requested freshness may create excessive small files or timeline pressure.

**Action:** Advisory only. Record the evidence and revisit cadence; do not enable clustering or
invent a tuning value in PR2.

## FLINK_SECRET_REDACTED

**Trigger:** Supplied evidence contains a credential assignment, authorization header,
credential-bearing URI, sensitive query parameter, or private-key block.

**Message:** Credential material was removed before the evidence was quoted. Provide only sanitized
configuration or secret references; never paste live credentials into the design session.

**Action:** Continue with redacted evidence. If redaction makes a load-bearing fact unreadable, mark
that fact `INCOMPLETE` without asking for the secret value.
