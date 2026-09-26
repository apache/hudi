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
# Flink warnings — PR1

Load these warnings only for the Flink route. Each warning has a deterministic trigger and action.
Surface it when the triggering answer lands rather than batching warnings at the end.

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

## FLINK_EXISTING_TABLE_DEFERRED

**Trigger:** The target is an existing table.

**Message:** Existing table facts must be treated as authoritative evidence and cannot be replaced
with a new-table recommendation. Evidence comparison is deferred to PR5.

**Action:** `BLOCKED`; request only non-secret evidence if the user wants it recorded for later.

## FLINK_MULTI_WRITER_REVIEW

**Trigger:** Another pipeline, backfill, cleanup writer, cross-engine writer, standalone compactor,
or standalone clustering job can commit to the table.

**Message:** The single-writer assumptions are invalid. PR1 does not claim an OCC, NBCC,
lock-provider, index, or table-service combination is safe.

**Action:** `REVIEW_REQUIRED`; withhold concurrency-sensitive configuration.

## FLINK_EXTERNAL_CATALOG_REVIEW

**Trigger:** An external engine or BI tool must discover the table through a catalog or metastore.

**Message:** Flink table resolution and external metastore synchronization are separate
responsibilities that must be composed together. That composition is deferred to PR6.

**Action:** `REVIEW_REQUIRED`; generate no catalog or sync configuration.

## FLINK_PHYSICAL_SCHEMA_REQUIRED

**Trigger:** Authoritative field names and types are missing, partial, guessed, or available only
through a schema location whose contents cannot be read safely in the session.

**Message:** A schema URI, catalog name, registry subject, or object-store path does not by itself
make the physical schema available. Concrete field names and types are required so record-key,
ordering, and partition references can eventually be checked.

**Action:** `INCOMPLETE`; withhold DDL.

## FLINK_AUTO_KEY_DURABILITY

**Trigger:** An append-only workload has no stable business key and the user accepts an
auto-generated-key posture.

**Message:** Reprocessing the same business record can create another Hudi record with a different
generated key. A later move to upsert requires stable identity and may require migration or table
recreation.

**Action:** Record this as a durable decision. This warning alone does not change status.

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

## FLINK_EXECUTABLE_PATH_DEFERRED

**Trigger:** Every PR1 safety gate passes.

**Message:** The request is eligible for the future new-table, single-writer, append-only COW Flink
SQL path, but PR1 intentionally provides routing and safety assessment only.

**Action:** `BLOCKED`; emit the non-executable ADR envelope and no DDL, connector options, or submit
command.

## FLINK_SECRET_REDACTED

**Trigger:** Supplied evidence contains a credential assignment, authorization header,
credential-bearing URI, sensitive query parameter, or private-key block.

**Message:** Credential material was removed before the evidence was quoted. Provide only sanitized
configuration or secret references; never paste live credentials into the design session.

**Action:** Continue with redacted evidence. If redaction makes a load-bearing fact unreadable, mark
that fact `INCOMPLETE` without asking for the secret value.
