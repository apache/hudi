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
# Flink output template — PR1 safety assessment

PR1 has no executable Flink configuration template. This file defines the output envelope that
must be used instead of falling back to the shared Spark configuration templates.

## Required output

```text
Flink design status: <INCOMPLETE | BLOCKED | REVIEW_REQUIRED>
Verified baseline: Apache Hudi 1.2.0 / Apache Flink 1.20
Baseline ID: hudi-1.2.0-flink-1.20
Hudi source revision: f05c83f2b97732de7a558ff9b26959e1139c05f5
Fixture version: Apache Flink 1.20.1
Capability manifest schema: 1

Confirmed facts:
- <facts established by explicit answers>

Safety-gate findings:
- <finding code>: <reason and next evidence or capability needed>

Durable decisions already accepted:
- <stable-key or auto-generated-key posture, when known>

Revisit conditions:
- <deferred catalog, writer, or version decision>

Executable output:
- Withheld. PR1 provides routing and safety assessment only.

Executable eligible: false
```

Populate the baseline evidence from `validate_flink_capabilities.py --emit-evidence`; do not copy
it from the enclosing checkout. List every safety-gate finding in gate order. A lower-precedence
finding remains present even when another finding determines the single summary status.

The output may use the existing ADR sections where they apply, but it must not imply that a full
table design has been completed.

## Forbidden PR1 output

Do not emit any of the following for a Flink request in PR1:

- `CREATE TABLE` or `INSERT INTO` statements.
- Hudi DynamicTable connector options.
- A Flink submit command.
- A Spark submit command presented as a Flink fallback.
- A runnable or executable label on a configuration draft.
- `CONFIG_VALIDATED`.
- OCC, NBCC, lock-provider, index, catalog, or table-service configuration inferred from an
  unverified combination.

PR2 extends this file with the first executable new-table, single-writer, append-only COW Flink SQL
sink template and its static validation requirements.
