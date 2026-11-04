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

# Guarded Hudi streaming-table chaining

Enable `hoodie.datasource.read.streaming.fail.on.data.loss=true` on each
latest-state Hudi **reader** whose downstream table must be rebuilt after a
source reset. The default is `false`, preserving existing source behavior.
This option is independent of the sink's write operation and is not a persisted
table-identity property. For an SDP graph-internal source, put
`spark.hoodie.datasource.read.streaming.fail.on.data.loss: "true"` in the pipeline
configuration (or set it in the flow's SQLConf). Spark currently discards explicit
reader options when resolving graph-internal dataset references, so relying only
on `.option(...)` for such a source does not enable the guard. Hudi captures this
flow-scoped setting when it creates the source. For direct or external readers,
explicit reader options take precedence over the session setting:

```python
return (spark.readStream
        .option("hoodie.datasource.read.streaming.fail.on.data.loss", "true")
        .table("sdp_test.upstream")
        .select("id", "ts", "value"))
```

Before constructing each resumed batch, both requested-time and completion-time
source implementations require the saved offset to remain in the active
completed timeline. Missing history, including an archived offset or a physical
table recreation, produces a `Streaming source rebuild required` error. The
check also rejects a cleaner retention boundary beyond the saved offset and
cleaning after that offset which deleted files without recording a retention
boundary (for example file-version retention).

Completed replace commits after the offset are rejected unless their operation
is clustering. This includes table overwrite and partition overwrite/deletion,
and protects a batch even when a reset completed after its captured end offset.
Ordinary clustering and compaction remain readable. An earliest read with a new
checkpoint can read the current snapshot after a reset.

## Recovery and limits

Stop downstream consumers before resetting an upstream table. After a
rebuild-required failure, rebuild affected downstream tables **and** their
checkpoints in dependency order from upstream to downstream. Keeping an old
downstream table while discarding only its checkpoint can duplicate data or keep
stale rows. With all flows stopped, truncate every affected table and restart the graph
with a new pipeline storage/checkpoint location. Ensure every external source
still contains the data needed to reconstruct the graph. Spark
`--full-refresh-all` additionally requires the catalog `DeleteColumn` support
tracked by #20492; it is not established by this source-safety change.

This is a conservative active-timeline contract. Retain enough active timeline
history for the slowest consumer; the guard does not prove safety from archived
metadata or silently fall back to a full-table scan. Physical recreation is
detected before the next resumed batch, not while an empty source produces no
batch. Copying a table together with identical timeline history is not detected
as a new table identity. Concurrent upstream destructive operations must still
be coordinated with consumers; validation and batch execution are not atomic.

These checks apply to latest-state reads, not CDC queries. They do not propagate
hard deletes or every intermediate change. AvailableNow still has no bounded
admission control, so the input backlog must fit the source's current batch
behavior. Setting the option to `false` restores the prior source behavior and
removes these safety checks.

## Verification

`source_safety_acceptance.py` uses the #20453 Spark 4.1.1 CLI/Connect harness.
It enables the guard through pipeline SQLConf, reads through `readStream.table(...)`,
and checks exact business rows,
restarts, physically truncates the upstream table, then checks that the resumed
pipeline fails without any downstream commit. Finally, with all flows stopped, it truncates both tables, assigns a fresh
pipeline checkpoint location, and verifies the reconstructed upstream and
downstream rows. This exercises explicit coordinated recovery rather than the
CLI full-refresh path, which depends on #20492.

```sh
python hudi-integ-test/spark-pipelines/source_safety_acceptance.py \
  --harness-dir /path/to/harness-checkout/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar \
  --table-type cow
# Repeat with --table-type mor.
```

The focused `TestStreamingSource` regressions additionally exercise logical
overwrite, clustering, missing active history, cleaner retention boundaries,
legacy opt-out behavior, and both source offset versions.
