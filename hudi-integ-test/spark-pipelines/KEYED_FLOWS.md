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

Run the real Spark 4.1.1 CLI against Connect using the bundled harness and a
candidate bundle built from the same checkout.
Use JDK 17 and install the harness's pinned requirements in a dedicated environment.

```sh
PYTHONOPTIMIZE=1 python hudi-integ-test/spark-pipelines/keyed_flows.py \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar
```

Pass `--table-type mor` to run the same matrix with MOR; the default is COW.
Run each table type separately with isolated targets and checkpoints.

The generated SQL defines partitioned tables with `primaryKey=id`,
`preCombineField=ts`, and `hoodie.record.merge.mode=EVENT_TIME_ORDERING`.
A boolean `_hoodie_is_deleted` carries deletes. The scenario checks a newly
created target and a pre-existing catalog table containing an unrelated key in
partition `c`. The existing target's flow omits `TBLPROPERTIES`, so its table type,
key, ordering field and merger must come from persisted table settings. Its seed
row must survive every input stage and rejected configuration. Both targets use
the same SDP-managed checkpoints on restart. The scenario verifies exact contents after intra-batch
precombine (higher ordering value arrives first), a newer update, a late lower-order
update, a late delete that must preserve the newer row, deletion, reinsertion and
an unchanged restart. Partition `b` retains its unrelated key throughout.

Negative scenarios reject nonexistent key, ordering and partition fields,
non-boolean delete markers and a change to persisted record-key identity.
Failed new targets must have no completed data commit; the existing valid targets
must retain their exact contents after every rejected scenario.
Their completed data timelines must also remain unchanged after rejection.

After reinsertion, the fixture models interruption between Hudi commit and Spark
checkpoint completion by removing the latest Spark commit-log entry for each
stopped AvailableNow query. It retains both Hudi timelines, source offsets and
checkpoint identities. A retry must recreate Spark's commit-log entries, return
the exact expected rows and add no Hudi data commits. `retry-commits.json` retains
the before/after timelines and the replayed batch paths. The subsequent ordinary
restart must also leave rows and completed commits unchanged. This is a controlled
checkpoint replay test; the real sink failure-injection and resource-cleanup
tests are in `TestStreamingSinkRecovery`.

Generated Python/SQL and logs remain under `target/keyed-acceptance-logs`.
Metadata indexing is enabled, matching the production default. Both targets must
initialize their metadata tables and complete metadata writes. This is delete input carried by a flow, not CDC
propagation of upstream deletes. It does not cover full refresh, table chaining,
fan-in, Auto CDC or SCD1/2.

For MOR, inline and asynchronous compaction are disabled. After the newer update,
the test requires log files and verifies that read-optimized base-file output matches
the exact stage-1 rows while the snapshot matches the updated rows. This proves the
snapshot must merge logs. Subsequent late updates, deletes and reinsertion must also
produce exact snapshots. Both table
types verify that an unchanged restart adds no completed commit, reporting the
before/after commit filenames for each table if the check fails.
The flows rely on the default streaming upsert operation, without a session override.
The catalog regression uses `PARTITIONED BY (PART)` against schema field `part`
to verify case-insensitive normalization. The SDP scenario uses exact spelling
because Spark separately rejects a declared/stored partition spelling mismatch.
Checks remain active under `PYTHONOPTIMIZE=1`, including verification subprocesses;
SIGTERM unwinds harness cleanup.
