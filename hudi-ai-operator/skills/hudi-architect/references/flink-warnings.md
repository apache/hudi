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

**Trigger:** An existing-table contract is sent to the new-table design validator or SQL renderer.

**Message:** Existing table facts must be treated as authoritative evidence and cannot be replaced
with a new-table recommendation. Route the request through the PR5 reconciler.

**Action:** `BLOCKED`; render no SQL. Collect non-secret evidence through the PR5 contract.

## FLINK_EXISTING_EVIDENCE_REQUIRED

**Trigger:** A required existing-table evidence kind or load-bearing fact is absent.

**Message:** Compatibility cannot be established without provenance-bearing table identity, config,
physical schema, writer inventory, and timeline evidence.

**Action:** `INCOMPLETE`; name the missing facts and emit no SQL.

## FLINK_EXISTING_EVIDENCE_INVALID

**Trigger:** An evidence item is malformed, has unknown fields, lacks a valid observation time or
digest, or contains a credential-shaped fact.

**Message:** The evidence cannot be used deterministically or safely in the comparison.

**Action:** `INCOMPLETE`; reject the malformed item without executing or echoing its content.

## FLINK_EXISTING_EVIDENCE_CONFLICT

**Trigger:** Two observations disagree, an evidence envelope identifies another table path, an
in-scope envelope's identity fact names another path, or a Flink DDL disagrees with authoritative
Hudi table config or physical schema.

**Message:** Contradictory evidence cannot be silently merged. Persisted table config and the Hudi
table schema remain authoritative, but the discrepancy needs review.

**Action:** `REVIEW_REQUIRED`; retain provenance for every conflicting source.

## FLINK_EXISTING_TABLE_VERSION_UNVERIFIED

**Trigger:** The table version or timeline layout differs from the pinned Hudi 1.2.0 values.

**Message:** PR5 does not infer upgrade or downgrade compatibility from a different table version.

**Action:** `REVIEW_REQUIRED`; do not authorize a table upgrade.

## FLINK_EXISTING_TABLE_PROPERTY_CONFLICT

**Trigger:** Table identity, type, file format, record key, ordering, merge mode, partitioning, or key-generator
facts conflict with the explicit desired contract.

**Message:** The desired writer cannot safely reinterpret a durable existing-table property.

**Action:** `BLOCKED`; report the conflicting comparison fields and emit no mutation commands.

## FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED

**Trigger:** An existing table property, such as a custom key generator, is outside the pinned
built-in semantic mappings.

**Message:** The property may be valid, but PR5 cannot prove that it matches the desired contract.

**Action:** `REVIEW_REQUIRED`; preserve the observed value and its provenance without calling it a
known conflict.

## FLINK_EXISTING_SCHEMA_CONFLICT

**Trigger:** A required field is absent or its type or nullability conflicts with the physical
Hudi table schema.

**Message:** The existing physical schema cannot accept the bounded desired contract as supplied.

**Action:** `BLOCKED`; do not infer casts or apply schema evolution.

## FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED

**Trigger:** The schemas differ only by order or additional fields, or otherwise need an evolution
decision outside the exact-match path.

**Message:** PR5 does not turn a plausible schema evolution into a compatibility claim.

**Action:** `REVIEW_REQUIRED`; record the difference and emit no evolution commands.

## FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED

**Trigger:** Writer transition is unknown, a separate Flink writer will take over, the prior writer
is not confirmed stopped, or same-job recovery state is unavailable.

**Message:** Table compatibility alone does not prove that writer state can be handed over safely.

**Action:** `REVIEW_REQUIRED`; resolve ownership and recovery evidence before deployment.

## FLINK_EXISTING_WRITER_CONFIG_CONFLICT

**Trigger:** State restored by the same Flink job differs from the bounded desired writer
configuration.

**Message:** The durable table may still be compatible, but the writer configuration cannot be
reused as supplied.

**Action:** `REVIEW_REQUIRED`; return `DEPLOYMENT_CONFIGURATION_CHANGE` and emit no commands.

## FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED

**Trigger:** The previous writer's timestamp partition type, input unit, output format, or output
timezone differs from the bounded desired settings.

**Message:** The same logical partition may map to a different physical directory. Changing only
the next writer's configuration does not reconcile paths already written by the previous writer.

**Action:** `REVIEW_REQUIRED`; retain `UNVERIFIED`, require layout or migration review, and emit no
commands.

## FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED

**Trigger:** A writer from another engine will hand over the table, or the previous engine is not
confirmed as Flink for a bounded Flink restore/cold start.

**Message:** Cross-engine writer-state and operational handoff are outside PR5.

**Action:** `REVIEW_REQUIRED`; do not invent a takeover procedure.

## FLINK_EXISTING_TIMELINE_UNRESOLVED

**Trigger:** The observed table timeline contains requested or inflight instants.

**Message:** A timestamped timeline snapshot with pending work cannot prove a clean handoff.

**Action:** `REVIEW_REQUIRED`; resolve the pending work and recheck immediately before deployment.

## FLINK_TABLE_LIFECYCLE_REQUIRED

**Trigger:** The user cannot confirm whether the target is a new or existing Hudi table.

**Message:** New-table design must not overwrite or substitute for facts from an existing table.
The table lifecycle must be known before choosing a path.

**Action:** `INCOMPLETE`; do not route through the new-table flow.

## FLINK_MULTI_WRITER_REVIEW

**Trigger:** Another pipeline, backfill, cleanup writer, cross-engine writer, standalone compactor,
or standalone clustering job can commit to the table.

**Message:** The single-writer assumptions are invalid. PR7 can record a Writer Contract, but it
does not claim an OCC, NBCC, lock-provider, index, or table-service combination is safe.

**Action:** `REVIEW_REQUIRED`; collect the Writer Contract and withhold concurrency-sensitive
configuration and executable output.

## FLINK_WRITER_MODEL_UNRESOLVED

**Trigger:** The user cannot confirm whether another writer or standalone table service can commit
to the table.

**Message:** The single-writer assumption is unresolved, so concurrency-sensitive design cannot
be treated as safe.

**Action:** `REVIEW_REQUIRED`; do not infer `SINGLE_WRITER` or recommend concurrency settings.

## FLINK_WRITER_CONTRACT_REQUIRED

**Trigger:** The writer model is confirmed multi-writer but no version-1 Writer Contract exists.

**Message:** The writer inventory and ownership facts are incomplete.

**Action:** `INCOMPLETE`; collect the bounded PR7 contract without selecting concurrency settings.

## FLINK_WRITER_CONTRACT_INVALID

**Trigger:** A Writer Contract has an invalid type, unknown field, duplicate writer ID, invalid
enum, undeclared owner, or another structural error.

**Message:** The validator cannot normalize the writer topology without guessing.

**Action:** `INCOMPLETE`; correct the explicit facts and emit no executable output.

## FLINK_WRITER_OWNERSHIP_UNRESOLVED

**Trigger:** Schema-evolution or supplied table-configuration ownership is explicitly `UNKNOWN`.

**Message:** Concurrent schema or configuration changes have no confirmed owner.

**Action:** `REVIEW_REQUIRED`; establish ownership before deployment.

## FLINK_WRITER_CONTRACT_CONFLICT

**Trigger:** The contract contradicts the writer model or scoped existing-table evidence, or the
observed lock configuration is conflicting.

**Message:** PR7 preserves both facts and does not select one as authoritative.

**Action:** `REVIEW_REQUIRED`; return `CONFLICT`, withhold SQL, and reconcile the evidence.

## FLINK_WRITER_COMBINATION_UNSUPPORTED

**Trigger:** NBCC is observed for design schema 1 or 2 COW, or schema 3 MOR with `FLINK_STATE`.

**Message:** Pinned Hudi 1.2.0 requires NBCC on MOR with simple bucket index (or the metadata
table), which none of the existing executable schemas uses.

**Action:** `BLOCKED`; do not switch the table type or index automatically.

## FLINK_EXTERNAL_CATALOG_REVIEW

**Trigger:** An external engine or BI tool must discover the table through a catalog or metastore.

**Message:** Flink table resolution and external metastore synchronization are separate
responsibilities. The outcome gate must be resolved into one explicit PR6 profile before SQL is
eligible.

**Action:** `REVIEW_REQUIRED` at the outcome gate; collect the PR6 contract rather than guessing a
catalog or sync owner.

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

**Trigger:** Existing logical records can be updated or deleted, but the request is still using the
version-1 append contract.

**Message:** Mutable input requires the explicit version-2 COW or version-3 MOR identity, ordering,
changelog, delete, partition, and index safety contract after table-type derivation.

**Action:** `BLOCKED`; qualify the request for schema 2 or 3 or generate no executable configuration.

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

**Trigger:** On the append contract, replayed or backfilled copies can occur and must collapse into
one table record.

**Message:** This requires stable record identity and an upsert-capable design. The append-only
insert path cannot be presented as deduplicating.

**Action:** `BLOCKED`; use a mutable schema-2 or schema-3 contract only if all gates are satisfied.

## FLINK_REPLAY_BEHAVIOR_UNRESOLVED

**Trigger:** The user cannot determine whether retries, replays, or backfills can repeat logical
records or whether repeated copies must collapse.

**Message:** Replay safety is unresolved, so an executable append-only or mutable path cannot be
validated.

**Action:** `REVIEW_REQUIRED`; retain the uncertainty and withhold executable output.

## FLINK_RECORD_KEY_FIELD_MISSING

**Trigger:** A stable key or explicit record-key option names a field absent from the physical
schema, including when `write.operation=insert`.

**Message:** Hudi 1.2.0 skips its record-key field check in append mode, so the Architect validator
must reject this mismatch before factory validation.

**Action:** `BLOCKED`; emit no DDL or `INSERT INTO`.

## FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED

**Trigger:** A `BYTES`, `BINARY`, or `VARBINARY` field is used as a stable record key or
partition field.

**Message:** The pinned Hudi 1.2.0 routing path converts the backing Java byte array with
`toString()`. Equal byte sequences can therefore produce different `[B@...` record keys or
partition directories. Binary payload columns are not affected.

**Action:** `BLOCKED`; choose a deterministically encoded routing field or wait until a verified
binary encoding is supported. Do not silently encode or cast the supplied value.

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

## FLINK_HUDI_METADATA_FIELD_CONFLICT

**Trigger:** A target physical field name case-insensitively matches one of Hudi's six fixed
metadata fields.

**Message:** The Hudi write path prepends these metadata fields, so the supplied target name would
produce a duplicate physical field. Other names beginning with `_hoodie_` are not rejected by this
rule.

**Action:** `BLOCKED`; require an authoritative non-conflicting target field name. Do not silently
rename it.

## FLINK_TEMPORAL_PRECISION_UNSUPPORTED

**Trigger:** A `TIME`, `TIMESTAMP`, or `TIMESTAMP_LTZ` field declares precision outside 0 through
6.

**Message:** The pinned Hudi 1.2.0 schema converter supports temporal precision no greater than 6.

**Action:** `BLOCKED`; require an authoritative compatible type. Do not silently reduce precision.

## FLINK_SOURCE_CONTRACT_REQUIRED

**Trigger:** The source table, expected physical schema, or changelog semantics are unavailable,
or a catalog-managed profile supplies a source identifier that is not fully qualified.

**Message:** A sink-side example must identify the existing source table and its expected contract.

**Action:** `INCOMPLETE`; do not invent or generate a source connector.

## FLINK_SOURCE_SCHEMA_MISMATCH

**Trigger:** An explicitly projected source field is missing or differs in type or nullability from
the target field.

**Message:** The bounded executable paths do not infer casts, aliases, or schema reconciliation.

**Action:** `BLOCKED`; require an explicit compatible source contract.

## FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY

**Trigger:** The declared source changelog contains updates or deletes.

**Message:** Sink-factory construction does not prove the complete source-to-sink statement is
append-only. An eligible mutable request must use a schema-2 COW or schema-3 MOR contract.

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

## FLINK_MUTABLE_AUTO_KEY_UNSUPPORTED

**Trigger:** A mutable contract has no stable business key.

**Message:** Upsert and delete semantics require durable identity across events and restarts.

**Action:** `BLOCKED`; require a stable, non-null source and target primary key.

## FLINK_MUTABLE_SOURCE_CHANGELOG_UNSUPPORTED

**Trigger:** The mutable source is not normalized UPSERT (`I`, `UA`, `D`, without `UB`).

**Message:** The first mutable sink path does not normalize retract streams or infer changelog.

**Action:** `BLOCKED`; normalize upstream or defer the path.

## FLINK_MUTABLE_ORDERING_FIELD_REQUIRED

**Trigger:** Event-time ordering or its single ordering field is missing.

**Message:** The validator cannot select conflict-resolution semantics or invent a field.

**Action:** `INCOMPLETE`; obtain one explicit ordering field.

## FLINK_MUTABLE_ORDERING_FIELD_INVALID

**Trigger:** The ordering field is absent, nullable, not BIGINT/TIMESTAMP(p<=6), or is the exact
Hudi `no_precombine` sentinel.

**Message:** The bounded event-time merger surface does not cover nullable or other orderings, and
`no_precombine` disables ordering instead of selecting a same-named physical field.

**Action:** `BLOCKED`; correct the source contract without casting silently.

## FLINK_MUTABLE_DELETE_PAYLOAD_UNSUPPORTED

**Trigger:** Deletes omit projected key, ordering, partition, or row values.

**Message:** The first path accepts only full-row deletes; key-only delete normalization is not
implemented.

**Action:** `BLOCKED`; normalize upstream or defer.

## FLINK_MUTABLE_PARTITION_EVOLUTION_UNSUPPORTED

**Trigger:** An existing key can change its partition value.

**Message:** Cross-partition movement is deliberately outside this first mutable path.

**Action:** `BLOCKED`; use immutable partition fields or an unpartitioned design.

## FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED

**Trigger:** Index type, scope, or TTL differs from global FLINK_STATE with TTL 0.

**Message:** Alternative indexes and expiring state need separate correctness evidence.

**Action:** `BLOCKED`; do not weaken the canonical index contract.

## FLINK_MUTABLE_INDEX_BOOTSTRAP_REQUIRED

**Trigger:** Index bootstrap is not explicitly enabled.

**Message:** A cold start with empty state could otherwise miss keys already present in the table.

**Action:** `BLOCKED`; enable bootstrap. This does not authorize arbitrary existing-table takeover.

## FLINK_PR3_WRITE_PATH_UNSUPPORTED

**Trigger:** Mutable table type, operation, merge mode, changelog setting, or runtime leaves the
bounded COW upsert path.

**Message:** PR3 implements one conservative latest-state mutable route, not the complete Hudi
mutable configuration surface.

**Action:** `BLOCKED`; retain the request as a later capability rather than guessing settings.

## FLINK_MOR_APPEND_ONLY_UNSUPPORTED

**Trigger:** An append-only contract requests a MOR table.

**Message:** The bounded append route remains COW. Hudi 1.2.0 treats MOR insert as append mode and
does not schedule the mutable MOR compaction path that PR4 verifies.

**Action:** `BLOCKED`; use schema-1 COW or reassess whether the workload is actually mutable.

## FLINK_MOR_COMPACTION_OWNER_REQUIRED

**Trigger:** A schema-3 MOR contract omits compaction ownership.

**Message:** The validator cannot assume that compaction runs inside the only confirmed writer.

**Action:** `INCOMPLETE`; identify the owner before emitting SQL.

## FLINK_MOR_COMPACTION_CONFIGURATION_UNSUPPORTED

**Trigger:** MOR schedule, async execution, operation execution, mode, trigger strategy, or delta
commit threshold differs from the bounded schema-3 contract.

**Message:** PR4 verifies same-writer asynchronous compaction using `num_commits` and five delta
commits; it is not a general compaction planner.

**Action:** `BLOCKED`; retain the fixed contract or defer to operational design.

## FLINK_MOR_COMPACTION_TUNING_UNSUPPORTED

**Trigger:** A request supplies compaction task count, seconds, target IO, timeout, or other
detailed tuning.

**Message:** These values depend on observed workload behavior and are outside design-time static
validation.

**Action:** `BLOCKED`; defer tuning to operations rather than passing it through silently.

## FLINK_MOR_QUERY_REQUIREMENT_UNRESOLVED

**Trigger:** Snapshot/read-optimized usage or the acceptable read-optimized freshness is missing
or unresolved.

**Message:** MOR suitability depends on whether consumers read logs or can accept compaction lag.

**Action:** `INCOMPLETE`; obtain the reader contract before selecting schema 3.

## FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED

**Trigger:** Read-optimized consumers require every completed delta commit immediately.

**Message:** Read-optimized queries expose compacted base files; the five-delta-commit trigger is
not an immediate or time-based freshness guarantee.

**Action:** `BLOCKED`; derive COW or explicitly accept freshness only after compaction.

## FLINK_MOR_CLUSTERING_UNSUPPORTED

**Trigger:** Clustering schedule or async execution is enabled for schema 3.

**Message:** PR4 owns only the minimum MOR compaction service and does not compose clustering.

**Action:** `BLOCKED`; keep both clustering controls disabled.

## FLINK_PR4_WRITE_PATH_UNSUPPORTED

**Trigger:** Mutable MOR table type, operation, merge mode, changelog, compaction, clustering, or
runtime leaves the bounded schema-3 path.

**Message:** PR4 implements one conservative mutable MOR route, not the complete Hudi table-service
surface.

**Action:** `BLOCKED`; do not guess or pass through unsupported settings.

## FLINK_CHECKPOINTING_REQUIRED

**Trigger:** Checkpointing is explicitly disabled for the streaming sink.

**Message:** The streaming Hudi commit lifecycle depends on completed Flink checkpoints.

**Action:** `BLOCKED`; do not claim a normal commit cadence.

## FLINK_CHECKPOINT_INTERVAL_REQUIRED

**Trigger:** The checkpoint interval is absent, zero, negative, or unresolved.

**Message:** The executable runtime contract needs a concrete positive checkpoint interval.

**Action:** `INCOMPLETE`; withhold runtime SQL.

## FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED

**Trigger:** The checkpoint interval is positive but below the 1000 ms Architect safety floor.

**Message:** Flink 1.20.1 has a 10 ms runtime minimum, while this bounded executable path applies
the stricter 1000 ms minimum before rendering SQL.

**Action:** `BLOCKED`; require an interval of at least 1000 ms. Do not silently increase it.

## FLINK_LOAD_BEARING_VALUE_REQUIRED

**Trigger:** The target table, target path, source table, or another load-bearing value is missing,
a placeholder, or contains credentials that must be redacted. Standard ABFS/ABFSS
`filesystem@account.dfs.core.windows.net` authorities are filesystem identifiers, not userinfo;
Azure SAS `sig` query parameters are credentials and must be rejected.

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

## FLINK_CATALOG_CONTRACT_REQUIRED

**Trigger:** `safety.external_catalog=true` but no versioned catalog block is supplied.

**Message:** The boolean gate does not identify table resolution, discovery, sync ownership, or
data lifecycle.

**Action:** `INCOMPLETE`; collect the PR6 catalog contract and emit no SQL.

## FLINK_CATALOG_CONTRACT_INVALID

**Trigger:** The catalog block is malformed, contains inapplicable fields, or lacks a required
identifier or Hive configuration source.

**Message:** Static validation cannot reproduce the catalog design.

**Action:** `INCOMPLETE`; correct the contract without selecting architecture on the user's behalf.

## FLINK_CATALOG_MODE_UNSUPPORTED

**Trigger:** Resolution, discovery, or sync ownership uses a mode outside the pinned PR6 surface.

**Message:** Hudi 1.2.0 source contains more integration code than this Flink fixture proves.

**Action:** `BLOCKED`; use one of the five supported profiles or add pinned evidence later.

## FLINK_CATALOG_PATH_CONFLICT

**Trigger:** A DFS Catalog desired path differs from
`${catalog.path}/${database}/${table.name}`.

**Message:** Hudi DFS Catalog ignores an explicit table path and derives its own location.

**Action:** `BLOCKED`; reconcile the desired path before rendering catalog SQL.

## FLINK_CATALOG_LIFECYCLE_UNRESOLVED

**Trigger:** DFS catalog-owned deletion semantics or the HMS external-table decision is missing.

**Message:** Catalog operations can otherwise imply a different data lifecycle than the design.

**Action:** `REVIEW_REQUIRED`; collect the missing lifecycle decision and do not emit executable
catalog SQL. An explicit conflicting decision uses `FLINK_CATALOG_COMPOSITION_CONFLICT` instead.

## FLINK_CATALOG_COMPOSITION_CONFLICT

**Trigger:** The legacy boolean gate, resolution/discovery pair, sync owner, lifecycle owner, or
HMS physical-schema partition order contradicts another explicit field; an HMS managed table is
explicitly requested; or the streaming new-table source resolves to the composed sink object.

**Message:** The validator does not arbitrate conflicting architecture decisions.

**Action:** `BLOCKED`; make the contract internally consistent.

## FLINK_GLUE_CATALOG_UNVERIFIED

**Trigger:** AWS Glue is selected for external discovery.

**Message:** The pinned source contains Glue support, but PR6 has no Flink-specific released-bundle
fixture or live deployment evidence for an executable path.

**Action:** `REVIEW_REQUIRED`; emit no Glue configuration or SQL.

## FLINK_MOR_CATALOG_NAMING_UNVERIFIED

**Trigger:** MOR sync strategy disagrees with query modes, or custom suffix/snapshot naming is
requested.

**Message:** PR6 fixes RO, RT, and ALL registrations with RO suffix skipping disabled and snapshot
with base name enabled.

**Action:** `REVIEW_REQUIRED`; retain the requested naming as a later capability.

## FLINK_CATALOG_CREDENTIAL_REJECTED

**Trigger:** Catalog configuration contains a username, password, token, IAM/keytab material, or a
credential-bearing metastore URI.

**Message:** Architect contracts may reference credential-free deployment configuration only.

**Action:** `BLOCKED`; remove the secret and use deployment-managed authentication.

## FLINK_EXISTING_CATALOG_MUTATION_UNVERIFIED

**Trigger:** An existing-table desired contract requests any non-legacy catalog composition.

**Message:** PR6 does not register, alter, rename, delete, or take over existing catalog entries.

**Action:** `REVIEW_REQUIRED`; return an `UNVERIFIED`, non-executable reconciliation with no SQL.
