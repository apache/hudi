<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Repeated materialized-view acceptance

Run `materialized_views.py` with the Spark 4.1.1 Connect harness from #20453
and a JDK 17 candidate bundle containing #20492 (catalog reconciliation).
Install the harness's pinned `requirements.txt` in a dedicated virtual environment.

```sh
python hudi-integ-test/spark-pipelines/materialized_views.py \
  --harness-dir /path/to/harness/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar
```

Three Hudi materialized views cover keyed JSON and Hudi sources and a keyless
JSON aggregate. The aggregate declares only `type=cow`, with no primary key,
and groups by `value`, computing `sum(id)` and `count(*)`.

Four real CLI runs use the following exact inputs and aggregate expectations:

| Run | Input `(id, value)` | Aggregate `(value, total, rows)` |
| --- | --- | --- |
| 1 | `(1, one), (2, one), (3, two)` | `(one, 3, 2), (two, 3, 1)` |
| 2 | `(2, one), (4, one), (3, three)` | `(one, 6, 2), (three, 3, 1)` |
| 3 | `(4, four)` | `(four, 4, 1)` |
| 4 | empty | empty |

After every refresh, all three views must match exact expected business rows.
The repeated `one` group changes its aggregate, old groups disappear, and the
last refresh removes every group. This detects stale rows and duplicate aggregate
rows even when Hudi generates record keys. The keyed JSON target remains
partitioned by `value`; the aggregate is unpartitioned.

Checks remain active under
`python -O`, and SIGTERM unwinds the harness for server and temporary-directory cleanup.

This is Phase 2 coverage on today's purge-style truncate. See the
[truncate and full-refresh limitations](ACCEPTANCE.md#truncate-and-full-refresh-limitations)
for history loss, the non-atomic refresh window, and downstream rebuild requirements.
Timeline-preserving truncate and checkpoint-generation isolation are deferred
together beyond this release under #20404; atomic refresh is also out of scope.
Metadata indexing is disabled. The test does not establish streaming-table
chaining or streaming-table full-refresh behavior.

## Schema changes and failed refreshes

The same command also runs an isolated SQL materialized view, `contract_mv`,
through the real CLI. It starts with `id` and `value`, adds an `extra STRING`
column, and reruns the unchanged definition. Each successful nonempty run checks
exact rows, catalog and persisted schemas, target location/type, and persisted
record-key/partition settings.

Full-table truncate resolves the current schema before deleting the timeline and
uses it to initialize the empty table. An evolved schema therefore survives both
the next refresh and repeated truncation of an empty table. This preserves schema,
not history or savepoints; the purge-style contract is unchanged.

The failure cases use real invalid definitions or a data-dependent SQL
`raise_error`, without production fault-injection hooks:

| Case | Boundary checked | Expected state before retry |
| --- | --- | --- |
| Unresolved input column | Resolution before purge | Previous rows and completed timeline unchanged |
| Changed partition columns | Partition validation before purge | Previous rows and completed timeline unchanged |
| Changed primary key | Catalog reconciliation after purge | Empty target, no completed writes, original key/partition identity retained |
| Data-dependent `raise_error` | Result-write execution after purge | Empty target and no completed writes |

Every failure must reach the CLI with its expected diagnostic and leave the
Connect server running. The scenario then restores a valid definition and
reruns the refresh, checking exact recomputation. The result-write failure is
followed by an empty recomputation and then a nonempty refresh. Verification logs
include all timeline filenames, including requested/inflight actions, and the
catalog schema; generated SQL and expected rows are retained with the CLI logs.
Checks remain active under `python -O`.

In particular, rejection of a primary-key change is **not** a guarantee that the
old data survives: SDP truncates before asking the catalog to reconcile that
property. Fix the definition and rerun the complete refresh from a replayable
source. There is no rollback across truncate, reconciliation and writing.

These cases exercise failures before purge, after purge during reconciliation,
and during result writing. They do not inject an I/O failure partway through the
filesystem purge or promise recovery of the old snapshot after deletion begins.
Timeline-preserving truncate and its commit-boundary failure semantics remain
under #20404.

## Manual external-target acceptance

Run the same matrix manually against an explicitly located, pre-created external
target:

```sh
python -O hudi-integ-test/spark-pipelines/materialized_views.py \
  --harness-dir /path/to/harness/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar \
  --external --logs target/mv-external-acceptance-logs
```

The baseline three views still use managed targets. The contract matrix creates
`contract_mv` with `CREATE TABLE ... USING hudi ... LOCATION ...` before running
its materialized-view definition. It verifies `EXTERNAL` catalog type and that
the explicit location remains unchanged after refresh, schema changes and
retries. Without `--external`, it verifies the managed target and warehouse
location. Both modes use COW with metadata indexing disabled; this is not a MOR
or metadata-indexing compatibility matrix. External-target acceptance is opt-in
and adds no CI job.

The harness bounds each client/CLI process and removes its temporary data.
Logs and generated pipeline definitions remain under `target/mv-acceptance-logs`.
