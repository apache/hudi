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

The harness bounds each client/CLI process and removes its temporary data.
Logs and generated pipeline definitions remain under `target/mv-acceptance-logs`.
