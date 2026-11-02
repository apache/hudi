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

# Keyed SDP flow acceptance

Run the real Spark 4.1.1 CLI against Connect using the #20453 harness and a
candidate bundle containing this PR's early partition-field validation.
Use JDK 17 and install the harness's pinned requirements in a dedicated environment.

```sh
python hudi-integ-test/spark-pipelines/keyed_flows.py \
  --harness-dir /path/to/harness/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar
```

The generated SQL defines partitioned COW tables with `primaryKey=id`,
`preCombineField=ts`, and `hoodie.record.merge.mode=EVENT_TIME_ORDERING`.
A boolean `_hoodie_is_deleted` carries deletes. The scenario checks a newly
created target and a pre-existing catalog table, then restarts each through the
same SDP-managed checkpoint. It verifies exact contents after intra-batch
precombine, a newer update, a late lower-order update, deletion, reinsertion and
an unchanged restart. Partition `b` retains its unrelated key throughout.

Negative scenarios reject nonexistent key, ordering and partition fields,
non-boolean delete markers and a change to persisted record-key identity.
Failed new targets must have no completed data commit; the existing valid targets
must retain their exact contents after every rejected scenario.

Generated Python/SQL and logs remain under `target/keyed-acceptance-logs`.
Metadata indexing is disabled. This is delete input carried by a flow, not CDC
propagation of upstream deletes. It does not cover full refresh, table chaining,
fan-in or failure between commit and checkpoint (separate recovery coverage).
