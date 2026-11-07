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

# Hudi streaming source recovery

`hoodie.datasource.read.streaming.recovery.strategy` defaults to `FAIL` for
latest-state structured-streaming readers, including SDP. It replaces the
unreleased boolean guard. An unreadable saved offset (archived or cleaned
history), logical overwrite/truncate, or physical recreation produces an
error naming the strategy config and the coordinated rebuild procedure.

| Strategy | Recovery batch | Consequence |
| --- | --- | --- |
| `FAIL` (default) | Stop before constructing an unsafe batch | Rebuild downstream tables and checkpoints together |
| `READ_UPTO_LATEST_COMMIT` | One full snapshot bounded by Spark's recorded batch end, then resume increments | Append targets may duplicate rows; upsert targets may retain keys deleted upstream |
| `READ_LATEST` | Empty batch acknowledging Spark's recorded latest end, then resume increments | Deliberately skip the unread range, accepting data loss |

These are recovery policies, not alternate settings for every healthy batch.
An explicit choice takes precedence over
`hoodie.datasource.read.incr.fallback.fulltablescan.enable`; latest-state
streaming incremental reads disable that legacy silent fallback. The strategy
is captured at source construction. Restart the query to change it. Initial
startup still uses `hoodie.datasource.streaming.startOffset` (earliest, latest,
or an explicit instant); a fresh earliest checkpoint can read the current state.
CDC behavior is unchanged.

For direct readers:

```python
spark.readStream.option(
    "hoodie.datasource.read.streaming.recovery.strategy", "FAIL"
).table("sdp_test.upstream").select("id", "ts", "value")
```

For SDP graph-internal reads, Spark 4.1.1 discards explicit reader options.
Use pipeline configuration or the flow's SQLConf to select recovery:

```yaml
configuration:
  spark.hoodie.datasource.read.streaming.recovery.strategy: READ_UPTO_LATEST_COMMIT
```

Direct reader options take precedence when Spark forwards them. There is no
need to set the configuration merely to obtain the default `FAIL` behavior.

## Recovery boundaries and limits

Stop consumers and rebuild affected downstream tables **and** checkpoints in
upstream-to-downstream order. Merely discarding a checkpoint can duplicate rows
or retain stale rows. Snapshot recovery is explicit permission to accept those
risks; it does not truncate or reconcile the downstream table. Every external
source must retain enough data for a complete coordinated rebuild.

Spark owns each batch end and checkpoint. Recovery never reads a newer snapshot
while acknowledging an older end. Snapshot recovery uses a time-travel snapshot
at that batch's last included write. If that snapshot cannot be reconstructed
(for example, recreation removed it, cleaning expired its files, or completion
ordering cannot be represented by the requested-time snapshot), rebuild tables
and checkpoints together. Concurrent destructive operations must still be
coordinated: validation and execution are not atomic.

Retain sufficient active history for the slowest consumer with
`hoodie.keep.min.commits` / `hoodie.keep.max.commits`. An archived checkpoint
requires recovery even if some data files remain. With
`hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS`, a clean after the checkpoint
that deletes any files requires recovery, even when those files were not needed:
that policy records no earliest retained commit. Choose a policy with a retention
boundary when incremental continuity must be established.

Non-clustering replace commits are logical resets; clustering and compaction
remain readable. This includes the timeline-preserving `TRUNCATE TABLE` proposed
in #20378. Physical recreation is detected at the next resumed batch, not while
an empty source produces no batch. Copying identical timeline history does not
establish a new table identity.

Snapshot recovery is a single unbounded batch. Chunked bootstrap and source
admission control remain follow-ups; size the backlog and snapshot accordingly.
Latest-state chaining does not propagate hard deletes or every intermediate
change. CLI full-refresh acceptance remains separate from this recovery policy.

## Verification

`TestStreamingSource` covers both offset versions and COW/MOR: default failure,
explicit strategy precedence, reset/recreation, cleaner boundaries, clustering,
exact snapshot rows, bounded recovery after a newer write, and incremental
continuation after recovery.

`source_safety_acceptance.py` uses the #20453 Spark 4.1.1 CLI/Connect harness.
It verifies exact two-stage rows, unchanged restarts, recreation rejection before
any downstream commit, and explicit coordinated rebuild of both tables with new
checkpoints. It does not validate the CLI full-refresh path.

```sh
python hudi-integ-test/spark-pipelines/source_safety_acceptance.py \
  --harness-dir /path/to/harness-checkout/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar \
  --table-type cow
# Repeat with --table-type mor.
```
