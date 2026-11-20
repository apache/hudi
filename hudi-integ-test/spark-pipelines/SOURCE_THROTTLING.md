<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Streaming source throttling acceptance

User-facing configuration, trigger and retention guidance is maintained in the
website Streaming Reads page ([documentation PR #20751](https://github.com/apache/hudi/pull/20751)).

Regression coverage in `TestStreamingSource` checks five commits drained as
2/2/1 batches for COW and MOR with both offset versions. A sixth commit arriving
during AvailableNow is excluded until restart, and a quiescent restart emits
no batch. Cap-one coverage includes ProcessingTime, CDC, and V1 MOR with
out-of-order compaction completion under BLOCK and USE_TRANSITION_TIME.
Invalid options are rejected at `readStream.load()`.

`TestHoodieSourceAdmissionControl` covers archived writes, skipped MOR compactions,
service offsets, completion-time ties, unbounded/Once behavior, invalid limits,
and an initially empty AvailableNow run.

## Upgrade note for Spark V1 checkpoints

When a Spark V1 stream in `BLOCK` mode resumes an older checkpoint, it uses the
last checkpointed instant's requested time as the lower bound. Older readers
configured with `hoodie.write.table.version` 8 or later used that instant's
completion time as the bound. During the first catch-up after upgrading, the
stream may re-emit records it previously read from commits requested after the
checkpointed instant's requested time and at or before its completion time.
This can occur with asynchronous compaction or concurrent writers, including
when the commit cap is unset.

## Recovery integration

Admission counts archived and active completed writes. Snapshot fallback and
recovery reads are not split into capped reads. The branch includes merged
#20516. COW/MOR regressions for both offset versions pin a capped intermediate
end before the latest write, reject logical reset under `FAIL`, and verify that
snapshot recovery excludes later data. After real cleaning deletes old files,
the recorded snapshot must fail with a retention error instead of silently
reading the latest snapshot. Explicit skip recovery remains empty, and a
caught-up reader resumes normal increments under `FAIL`.

## CLI/Connect acceptance

`source_throttling_acceptance.py` writes three upstream commits before running
the real CLI. A cap of one must produce three downstream commits with exact rows.
An unchanged restart adds no commit; a fourth input commit produces one more.
Both COW/MOR targets use metadata enabled. Run with the candidate bundle:

```sh
PYTHONOPTIMIZE=1 python hudi-integ-test/spark-pipelines/source_throttling_acceptance.py \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar --table-type cow
# Repeat with --table-type mor.
```

For SDP, set `spark.hoodie.datasource.read.streaming.max.commits.per.trigger`
in pipeline configuration or flow SQLConf. Both bare `hoodie.*` and
`spark.hoodie.*` settings are captured when the source is constructed; explicit
reader options take precedence. Restart the source to change its cap. This
avoids relying on explicit reader options that Spark 4.1 drops on graph-internal
reads. Regression tests cover both prefixes, capture outside the SQLConf scope,
explicit-option precedence and invalid configured limits.
