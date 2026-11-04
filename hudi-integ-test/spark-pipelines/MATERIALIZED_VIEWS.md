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

Two Hudi materialized views read a JSON source and a Hudi source respectively.
Four real CLI runs replace the source contents with `(1, one), (2, two)`,
then `(2, updated), (3, three)`, then `(4, four)`, then zero rows.
The JSON-backed target is partitioned by `value`. After every refresh, each
view must exactly match its defining query's expected rows. Removing old rows
ensures that accidental append behavior fails. Repeated catalog reconciliation
exercises metadata-column deletes and unchanged properties.

Keyless aggregate materialized views remain deferred under #20405.
Land this scenario after the #20453 harness dependency. Checks remain active under
`python -O`, and SIGTERM unwinds the harness for server and temporary-directory cleanup.

This is Phase 2 coverage on today's purge-style truncate. History retention,
atomic refresh and timeline-preserving truncate are intentionally outside this
scenario and remain Phase 4 work. Metadata indexing is disabled. The test does
not establish streaming-table chaining or full-refresh behavior.

The harness bounds each client/CLI process and removes its temporary data.
Logs and generated pipeline definitions remain under `target/mv-acceptance-logs`.
