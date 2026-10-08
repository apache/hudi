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
# Flink output template — first executable PR2 sink

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
Capability manifest schema: 2
Design contract schema: 1

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

## Executable envelope

Only `validate_flink_design.py` may open this envelope:

```text
Flink design status: CONFIG_VALIDATED
Verified baseline: Apache Hudi 1.2.0 / Apache Flink 1.20
Baseline ID: hudi-1.2.0-flink-1.20
Hudi source revision: f05c83f2b97732de7a558ff9b26959e1139c05f5
Fixture version: Apache Flink 1.20.1
Capability manifest schema: 2
Design contract schema: 1

Confirmed facts:
- New table; confirmed single writer; no current external-catalog requirement.
- Append-only input; replay behavior: <cannot occur | duplicates accepted>.
- Identity: <stable key fields | explicitly accepted auto-generated keys>.

Source contract:
- Existing table: <source identifier>
- Expected physical schema: <Avro-compatible field names, bounded types, nullability>
- Changelog: INSERT_ONLY
- The source connector and live source availability were not validated.

Target contract:
- Table: <target identifier>
- Path: <credential-free concrete path>
- Table type: COPY_ON_WRITE
- Operation: insert
- Insert clustering: false

Runtime contract:
- Execution: streaming
- Checkpoint interval: <positive duration>
- A completed checkpoint coordinates a Hudi commit; this is not an end-to-end exactly-once claim.

Validation summary:
- <baseline evidence>
- <design-contract validation evidence>
- <advisories, including stable-key or auto-key durability>

Executable eligible: true
```

Then emit, in order, the validator-produced `runtime_sql`, `table_ddl`, and `insert_sql`. Do not
retype or modify those artifacts after validation.

## Canonical SQL contract

The runtime block contains concrete streaming and checkpoint settings:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '<positive milliseconds> ms';
```

The generated `CREATE TABLE` contains a Hudi `WITH` block with exactly the bounded load-bearing
settings:

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

## Deployment checks

Always retain these outside the static success claim:

- Deploy `org.apache.hudi:hudi-flink1.20-bundle:1.2.0` with the target Flink 1.20 environment.
- Verify storage permissions and the concrete target path.
- Verify that the declared source table exists and preserves the expected insert-only contract.
- Configure durable checkpoint storage and a restart strategy appropriate to the environment.
- Confirm the single-writer inventory again immediately before deployment.
- After recovery, verify that completed checkpoints correspond to completed Hudi instants.

## Still forbidden in PR2

- A complete Kafka, CDC, JDBC, or other source connector job.
- A Flink deployment or submit command.
- A Spark fallback.
- OCC, NBCC, lock-provider, index, catalog, MOR, compaction, or clustering configuration.
- A claim that static validation checked live storage, the deployed JAR, the live source, active
  writers, checkpoint completion, or end-to-end exactly-once behavior.
