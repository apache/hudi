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

# Streaming source commit throttling

Set `hoodie.datasource.read.streaming.max.commits.per.trigger` on the Hudi
streaming reader to a positive integer. Without the option, reads remain unbounded.
For example, an SDP Python flow can use:

```python
spark.readStream.format("hudi").option(
    "hoodie.datasource.read.streaming.max.commits.per.trigger", "5"
).load(source_path)
```

This limits completed write commits in each incremental micro-batch. It applies
to both Hudi streaming offset versions and COW/MOR tables. Requested-time offsets
retain hollow-commit handling; completion-time offsets are sorted by completion.
Clean and rollback instants do not consume the limit. Commits with the same
completion timestamp share one offset and cannot be split, so a boundary tie can
admit more than the configured count.

`Trigger.AvailableNow` pins its end offset when the query starts and drains that
backlog in capped batches. Writes arriving afterward wait for the next run.
Ordinary checkpoint restart resumes from the last processed offset.
`Trigger.Once` requests all available data and ignores the cap, per Spark's contract.
Use AvailableNow for bounded triggered updates.

A commit can contain arbitrarily many rows or bytes. This option is not a memory,
file-size or row-count bound. It counts the available active write timeline;
archived-history fallback and snapshot recovery are not split into capped reads.
Keep history and data-file retention long enough for consumers to catch up.
Source recovery strategies remain separate from admission control.

Regression coverage checks five commits drained as 2/2/1 batches for COW and MOR
with both offset versions. A sixth commit arriving during the run is excluded until
restart. It also checks a quiescent restart, service offsets, completion-time ties,
unbounded/Once behavior, invalid limits and an initially empty AvailableNow run.
