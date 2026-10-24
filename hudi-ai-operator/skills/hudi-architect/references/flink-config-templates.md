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
# Flink output template — bounded executable sinks

Use this file only after the Flink route is selected. The PR1 safety gates still control entry to
the executable path. Never fall back to shared Spark configuration templates.

## Non-executable envelope

Use this envelope whenever a safety-gate or design-contract finding remains:

```text
Flink design status: <INCOMPLETE | BLOCKED | REVIEW_REQUIRED>
Verified baseline: Apache Hudi 1.2.0 / Apache Flink 1.20
Baseline ID: hudi-1.2.0-flink-1.20
Hudi source revision: f05c83f2b97732de7a558ff9b26959e1139c05f5
Fixture version: Apache Flink 1.20.1
Capability manifest schema: 6
Catalog contract schema: 1
Catalog profile: <one pinned profile or unresolved>
Design contract schema: <1 append-only COW | 2 mutable COW | 3 mutable MOR>

Confirmed facts:
- <facts established by explicit answers>

Safety-gate findings:
- <finding code>: <reason and next evidence or capability needed>

Durable decisions already accepted:
- <stable-key or auto-generated-key posture, when known>

Revisit conditions:
- <deferred catalog, writer, or version decision>

Executable output:
- Withheld because at least one status-contributing finding remains.

Executable eligible: false
```

Populate evidence from the validators. List every finding in evaluation order. A lower-precedence
finding remains visible even when another finding determines the summary status. Do not include a
partial DDL, connector option block, or `INSERT INTO` when executable eligibility is false.

## Existing-table reconciliation envelope

Use only the machine result from `validate_flink_existing_table.py`:

```text
Flink design status: <CONFIG_VALIDATED | INCOMPLETE | BLOCKED | REVIEW_REQUIRED>
Reconciliation result: <COMPATIBLE | CONFLICT | UNVERIFIED>
Verified baseline: Apache Hudi 1.2.0 / Apache Flink 1.20
Capability manifest schema: 6
Catalog contract schema: 1
Catalog profile: <direct_sql_none or requested non-legacy profile>
Catalog setup SQL: <always empty for an existing table>
Effective catalog configuration: <validator-produced mapping>
Reconciliation schema: 1
Desired design contract schema: <1 | 2 | 3>
Writer transition: <same_job_restore | cold_start_new_job | flink_writer_takeover |
cross_engine_takeover | unknown>

Evidence:
- <evidence ID, kind, table path, in-scope flag, observation time, and digest; no raw secrets>

Comparisons:
- <field>: <MATCH | CONFLICT | UNVERIFIED | NOT_APPLICABLE> (<source IDs>)

Findings:
- <every finding in evaluation order>

Change disposition: <NO_CHANGE | DEPLOYMENT_CONFIGURATION_CHANGE | MIGRATION_REQUIRED | UNVERIFIED>

Deployment checks:
- Confirm the single-writer inventory immediately before deployment.
- Recheck pending instants immediately before deployment.
- For a mutable cold start, verify index bootstrap before relying on the first checkpoint.
- Retain the validator-produced catalog deployment checks for any non-legacy profile; these are
  observations to perform, not permission to mutate an existing registration.

Executable output:
- Withheld because PR5 is assessment-only, including for COMPATIBLE results.

Executable eligible: false
```

Do not append SQL, table-property edits, upgrade steps, or migration commands. A compatible static
report does not prove that time-sensitive writer or timeline evidence remains current.

## Executable envelope

Only `validate_flink_design.py` may open this envelope:

```text
Flink design status: CONFIG_VALIDATED
Verified baseline: Apache Hudi 1.2.0 / Apache Flink 1.20
Baseline ID: hudi-1.2.0-flink-1.20
Hudi source revision: f05c83f2b97732de7a558ff9b26959e1139c05f5
Fixture version: Apache Flink 1.20.1
Capability manifest schema: 6
Catalog contract schema: 1
Catalog profile: <direct_sql_none | direct_sql_hive_metastore | hudi_dfs_catalog_none |
hudi_dfs_catalog_hive_metastore | hudi_hms_catalog_hive_metastore>
Design contract schema: <1 append-only COW | 2 mutable COW | 3 mutable MOR>

Confirmed facts:
- New table; confirmed single writer; explicit catalog/discovery outcome.
- Workload: <append-only or mutable, with confirmed replay behavior>.
- Identity: <stable key fields | explicitly accepted append-only auto-generated keys>.
- Mutable only: one event-time ordering field; immutable partition values; full-row delete posture.

Source contract:
- Existing table: <source identifier>
- Expected physical schema: <Avro-compatible field names, bounded types, nullability>
- Changelog: <INSERT_ONLY for schema 1 | normalized UPSERT (I/UA/D, no UB) for schemas 2/3>
- The source connector and live source availability were not validated.

Target contract:
- Table: <target identifier>
- Path: <credential-free concrete path>
- Table type: <COPY_ON_WRITE for schemas 1/2 | MERGE_ON_READ for schema 3>
- Operation: <insert for schema 1 | upsert for schemas 2/3>
- Path-specific fixed settings: <insert clustering false | EVENT_TIME_ORDERING plus global
  FLINK_STATE, TTL 0, bootstrap true, and changelog false>
- MOR only: compaction owned by this Flink writer, snapshot/read-optimized freshness recorded,
  fixed async five-delta-commit compaction, and clustering disabled.

Runtime contract:
- Execution: streaming
- Checkpoint interval: <concrete duration of at least 1000 ms>
- A completed checkpoint coordinates a Hudi commit; this is not an end-to-end exactly-once claim.

Validation summary:
- <baseline evidence>
- <design-contract validation evidence>
- <effective catalog configuration and post-commit-eventual freshness when applicable>
- <advisories, including stable-key or auto-key durability>

Catalog deployment checks:
- <validator-produced check codes; these do not claim live connectivity or change static status>

Executable eligible: true
```

Then emit the validator-produced `combined_sql`. Its canonical order is `runtime_sql`, optional
`catalog_setup_sql`, `table_ddl`, and `insert_sql`; the JSON form retains all boundaries. Do not
retype, modify, or execute those artifacts after validation.

## Canonical SQL contract

The runtime block contains concrete streaming and checkpoint settings:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '<milliseconds, minimum 1000> ms';
```

Flink 1.20.1 has a lower runtime boundary of 10 ms, but the bounded Architect path intentionally
requires at least 1000 ms and never raises a supplied value implicitly.

For schema 1, the generated `CREATE TABLE` contains exactly these bounded load-bearing settings:

```sql
WITH (
  'connector' = 'hudi',
  'path' = '<concrete credential-free target path>',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'insert',
  'write.insert.cluster' = 'false'
)
```

The stable-key form uses `PRIMARY KEY (...) NOT ENFORCED`. The accepted auto-key form omits both
primary-key syntax and the record-key option. Partitioning uses `PARTITIONED BY (...)`, not a
duplicate partition-path option. The `INSERT INTO` names every target and source field and never
uses `SELECT *`. Pass-through connector options must be explicitly empty; the validator rejects
even a verified option when the bounded renderer has no canonical place for it.

The angle-bracket values in this reference explain the shape only. They are forbidden in an actual
`CONFIG_VALIDATED` artifact; the validator requires concrete values.

For schema 2 mutable COW, the canonical block is:

```sql
WITH (
  'connector' = 'hudi',
  'path' = '<concrete credential-free target path>',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'upsert',
  'ordering.fields' = '<one non-null BIGINT or TIMESTAMP(p<=6) field>',
  'hoodie.write.record.merge.mode' = 'EVENT_TIME_ORDERING',
  'index.type' = 'FLINK_STATE',
  'index.global.enabled' = 'true',
  'index.state.ttl' = '0',
  'index.bootstrap.enabled' = 'true',
  'changelog.enabled' = 'false'
)
```

The mutable source must declare the same primary key as the target and normalized UPSERT
changelog (`I`, `UA`, `D`, no `UB`). Deletes, when emitted, must be full-row; partition values are immutable.
This is a latest-state sink contract; it is not full changelog-history or CDC-query support.

For schema 3 mutable MOR, preserve every schema-2 identity, ordering, source, index, and runtime
invariant, replace only `table.type`, and append the bounded service settings:

```sql
WITH (
  'connector' = 'hudi',
  'path' = '<concrete credential-free target path>',
  'table.type' = 'MERGE_ON_READ',
  'write.operation' = 'upsert',
  'ordering.fields' = '<one non-null BIGINT or TIMESTAMP(p<=6) field>',
  'hoodie.write.record.merge.mode' = 'EVENT_TIME_ORDERING',
  'index.type' = 'FLINK_STATE',
  'index.global.enabled' = 'true',
  'index.state.ttl' = '0',
  'index.bootstrap.enabled' = 'true',
  'changelog.enabled' = 'false',
  'compaction.schedule.enabled' = 'true',
  'compaction.async.enabled' = 'true',
  'compaction.operation.execute.async.enabled' = 'true',
  'compaction.trigger.strategy' = 'num_commits',
  'compaction.delta_commits' = '5',
  'clustering.schedule.enabled' = 'false',
  'clustering.async.enabled' = 'false'
)
```

Compaction is scheduled and executed by the same Flink writer job. Snapshot readers include log
state at a visible completed instant; read-optimized readers expose compacted base files and are
current only after compaction. Five delta commits is a trigger, not a time SLA. Reader-side
`hoodie.datasource.query.type` is intentionally absent from sink DDL.

For catalog profiles, apply the additional canonical rules in
`flink-catalog-composition.md`. DFS Catalog setup precedes table DDL and the DDL omits `path`;
external HMS Catalog includes the desired table path and does not duplicate its auto-supplied Hive
sync options. Direct/DFS sink-owned discovery renders the five bounded `hive_sync.*` settings.
MOR plus Hive discovery also renders the query-derived `hive_sync.table.strategy`.

## Deployment checks

Always retain these outside the static success claim:

- Deploy `org.apache.hudi:hudi-flink1.20-bundle:1.2.0` with the target Flink 1.20 environment.
- Verify storage permissions and the concrete target path.
- Verify that the declared source table exists and preserves its validated INSERT_ONLY or
  normalized UPSERT contract.
- Configure durable checkpoint storage and a restart strategy appropriate to the environment.
- Confirm the single-writer inventory again immediately before deployment.
- After recovery, verify that completed checkpoints correspond to completed Hudi instants.

## Still forbidden in the bounded paths

- A complete Kafka, CDC, JDBC, or other source connector job.
- A Flink deployment or submit command.
- A Spark fallback.
- OCC, NBCC, lock-provider, alternative index, catalog, append-only MOR, standalone compaction,
  detailed compaction tuning, or enabled clustering.
- Existing-table SQL generation or takeover, multi-writer operation, expiring state index, key-only deletes,
  retract-stream normalization, partition movement, custom mergers, or full CDC history.
- A claim that static validation checked live storage, the deployed JAR, the live source, active
  writers, checkpoint completion, or end-to-end exactly-once behavior.
