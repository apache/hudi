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

# HoodieSparkEventLogAnalyzer

Summarises a Spark event log by **Hudi operation and phase**, so a stage report reads in Hudi's own
vocabulary rather than Spark's.

A generic Spark profiler can tell you stage 14 was slow and had 16000 tasks. It cannot tell you that
stage 14 was bloom-index probing inside an upsert, so its advice stays generic. This tool reads the
Spark job description that Hudi publishes for each of its jobs and maps it onto a named operation and
phase. That join is the point of the tool; the Spark metrics around it are the standard ones.

The tool is **read-only**. It never writes to the event log or to any Hudi table, and it needs no
`SparkContext` — it reads a file.

## Running it

```
java -cp "$HUDI_UTILITIES_BUNDLE" \
  org.apache.hudi.utilities.HoodieSparkEventLogAnalyzer \
  --event-log /path/to/eventlog --output TABLE
```

| Flag | Default | Meaning |
|---|---|---|
| `--event-log`, `-e` | *required* | File or directory (rolling logs), optionally `.gz`/`.lz4`/`.snappy`/`.zstd`. |
| `--output`, `-o` | `TABLE` | `TABLE` for humans, `JSON` for machines. |
| `--top-n`, `-n` | `20` | Cap on stage rows in `TABLE` output, longest first. Ignored for JSON. |
| `--min-stage-seconds`, `-m` | `0` | Omit stages shorter than this from `TABLE` output. Ignored for JSON. |
| `--include-tasks` | `false` | Add p25/p75/p99 task percentiles to JSON output. |
| `--output-file`, `-f` | stdout | Write the report to a local file instead of stdout. |
| `--help`, `-h` | | Print usage. |

Prefer `--output-file` when consuming the JSON programmatically. Hudi's bundles and most deployments
send logging to stderr, but some logging configurations write to stdout, and that would interleave
with the document.

Paths are read through `HoodieStorage`, so `s3://`, `gs://` and `hdfs://` work wherever the matching
filesystem implementation is on the classpath.

Exit codes: **0** the log parsed, **2** bad arguments or an unreadable/unparseable log.

Gzip is decoded with the JDK. The other codecs are resolved through Hadoop's
`CompressionCodecFactory` reflectively, so the tool still runs on a classpath without Hadoop and does
not pin a codec library version. If a codec is unavailable the tool says so and exits 2; decompress
the log first in that case.

## The model: operation → phase → stage

The model is two levels, because a phase alone is ambiguous. A write inside an upsert is not the same
thing as the write inside a compaction, and forty repetitions of the rollback phases is one restore
rather than forty rollbacks. The optimizer's advice differs by operation, so both are reported.

### Phases are coarse on purpose

The question worth answering is *where did the time go*, at a granularity someone can act on:
**"60% of the run is index tagging"**, or **"compaction planning dominates execution"**. Precision
beyond that costs complexity and risks being confidently wrong. So there are nine buckets, not
thirty, and the finer label a description resolved to is carried alongside as `subPhase` rather than
fragmenting the list. The verbatim `jobDescription` is always reported too, as the escape hatch.

**A write operation has five buckets:**

| Bucket | What it covers |
|---|---|
| `SOURCE_READ_AND_TRANSFORM` | Source read, user transformation and record creation |
| `DEDUP_AND_INDEX_TAGGING` | Deduplication, index tagging and workload profiling — one job in Hudi |
| `DATA_TABLE_WRITE` | The write itself, plus small-file probing and commit |
| `MARKER_RECONCILIATION` | Marker creation, reconciliation, and deleting unreconciled files |
| `METADATA_TABLE_WRITE` | Writing and committing the metadata table and its index partitions |

**Table services have two**, `PLANNING` and `EXECUTION`. Those names recur across compaction,
clustering, cleaning and rollback, so **the operation is what disambiguates them** — this is the main
reason the operation dimension exists. `ARCHIVAL` is one bucket, and `UNKNOWN` is everything that did
not resolve.

### Operations

`WRITE_OPERATION`, `COMPACTION`, `LOG_COMPACTION`, `CLUSTERING`, `CLEANING`, `ARCHIVAL`, `ROLLBACK`,
`RESTORE`, `SAVEPOINT`, `BOOTSTRAP`, `INDEXING`, `STREAMER_SYNC`, `UNKNOWN`.

`WRITE_OPERATION` deliberately covers bulk_insert, insert, upsert, delete, insert_overwrite,
insert_overwrite_table and delete_partition together. They share the same phase pipeline, and only
the module name occasionally disambiguates them, so splitting them from stages alone would be a guess.

The operation is derived from two signals, in order, and **which one was used is reported** as
`operationSource` so a reader can judge confidence:

1. **`module`** — the module half of the job description often names the operation outright
   (`HoodieCompactor`, `SparkInsertOverwriteTableCommitActionExecutor`, `SavepointActionExecutor`).
   This is the stronger signal.
2. **`sequence`** — the sub-phases and buckets of the job's stages. Used when the module is absent or
   too generic.
3. **`none`** — neither resolved. The operation is `UNKNOWN` and nothing is guessed.

**Restore is detected by counting.** `BaseRestoreActionExecutor` sets no job status of its own: it
loops over the instants to roll back and calls the ordinary rollback path for each, so the child jobs
carry ordinary rollback modules. Three or more rollback job groups in one application are reported as
a `RESTORE`; fewer stay `ROLLBACK`, because a single failed write rolls back once.

### How the phase is derived from the job description

Hudi publishes a job description through `HoodieEngineContext#setJobStatus(activeModule,
activityDescription)`. Two wire encodings exist and **both are supported**, because logs of both are
in circulation:

- **Current** (HUDI-8596 onwards, Hudi 1.0+): `setJobDescription(module + ":" + activity)`, so
  `spark.job.description` reads `HoodieWriteHelper:Tagging: my_table`.
- **Legacy** (before HUDI-8596): `setJobGroup(module, activity)`, so the module lands in
  `spark.jobGroup.id` and `spark.job.description` holds the bare activity, `Tagging: my_table`.

Matching runs against the raw description first, then against the description with a leading
`module:` stripped. Activity strings are matched as **case-insensitive prefixes**, longest first,
because most of them end with a separator where Hudi appends the table name. Two descriptions embed a
count mid-string and are matched by a narrow anchored regex instead.

A description that matches nothing yields phase `UNKNOWN`; the tool falls back to reporting the stage
name and **never guesses**.

### The stage name refines the sub-phase; the description always decides the phase

**The job description is authoritative for the phase.** The Spark stage name is used for two narrower
purposes, neither of which ever contradicts it.

**1. Refining the sub-phase.** One Hudi job description can cover several components.
`Building workload profile:` is the job under which **deduplication, index tagging and profiling all
run** — Hudi sets the description once and the whole lazy chain executes beneath it. The description
is correct, just coarser than the component boundary a reader cares about. Spark names each stage
after the class that created the RDD, which says which component actually ran:

| Stage name contains | subPhase |
|---|---|
| `SparkMetadataTableGlobalRecordLevelIndex` | `recordIndexLookup` |
| `SparkMetadataTableRecordLevelIndex` | `recordIndexLookup` |
| `SparkHoodieBloomIndexHelper`, `HoodieBloomIndexCheckFunction`, `HoodieGlobalBloomIndex`, `HoodieBloomIndex` | `bloomIndexLookup` |
| anything else | whatever the description implied (e.g. `workloadProfile`) |

This is scoped to `DEDUP_AND_INDEX_TAGGING`, the one bucket where a single description is known to
span several components. It adds detail rather than correcting anything, so **no caveat is raised**.

It matters most for **record-level index**: with `hoodie.index.type=RECORD_INDEX` the lookups run
inside the workload-profile job, and without naming them the cost of moving from SIMPLE to RLI cannot
be read off the report.

**2. Falling back when there is no description at all.** Some paths are genuinely uninstrumented:

| Stage name contains | Phase | subPhase |
|---|---|---|
| `SparkStreamingMetadataWriteHandler` | **`UNKNOWN`** (deliberately) | `streamingMetadataWrite` |

Consulted only after the description has failed, so it can rescue an `UNKNOWN` but never override a
resolved phase.

### Parallel listing

`FSUtils` tags its parallel listing with the paths being listed, and the paths are the only thing
saying what the listing is for. Paths under `/metadata/` resolve to `METADATA_TABLE_WRITE` with
`subPhase: "metadataListing"` and operation `INDEXING`. A listing of the **data** table is left
`UNKNOWN` rather than guessed at.

## The attribution caveat — read this before trusting a share

**A bucket names where work was *forced*, not where it logically belongs.** Hudi sets the job
description before the Spark action that triggers the job, and Spark evaluates lazily, so a bucket
routinely carries the cost of an untagged step that ran before it. A downstream optimizer must not
treat a single share as ground truth. The JSON surfaces this as a `caveats[]` array naming the
buckets in *that* log that are prone to it.

Three cases are known and verified in the source:

**Source read, transformation and record creation are inseparable.** `StreamSync` tags only
`Fetching next batch:` and `Checking if input is empty:`. Applying the user's transformer is
untagged, and `HoodieStreamerUtils` — which turns the batch into `HoodieRecord`s — contains no
`setJobStatus` at all. All three are forced inside whichever stage materialises the records. Rather
than footnote this, **the bucket is named for everything it contains**. When such a stage takes **15%
or more** of total stage wall clock, it additionally carries a `bundledSteps` list and the TABLE
output spells it out in words, because at that size a reader needs to know what the number covers.

**Deduplication and index tagging are inseparable.** `BaseWriteHelper` has exactly one
`setJobStatus` — `Tagging:` — and `deduplicateRecords` runs untagged immediately before it. Same
treatment: the bucket is named for both, with a `bundledSteps` note above the threshold.

**The same trap exists at the other end.** For datasource ingestion the source-read cost lands in the
first stage that forces it, typically deduplication. And a large `DATA_TABLE_WRITE` share may include
write work that was not forced until the commit action ran — on a real log, `Committing stats:`
showed 72% of wall clock while almost certainly doing the write.

## Known limitation: streaming writes to the metadata table

**With `hoodie.metadata.streaming.write.enabled=true` — Hudi's default on Spark — a large share of
the DAG is unattributable.** On a measured run it was **61% of stage wall clock**.

These stages are not merely fused into others; they are completely unlabelled. Hudi never calls
`setJobStatus` on that path, so stages appear as `mapToPair at
SparkStreamingMetadataWriteHandler.java:61`, `mapToPair at HoodieJavaRDD.java:177` and `collect at
SparkRDDWriteClient.java:441`, with no description to interpret.

The tool **does not invent an attribution** for them. Instead:

- The phase stays `UNKNOWN`, but stages on the handler carry `subPhase: "streamingMetadataWrite"`, so
  a reader can see *what* the unattributed work is without the tool claiming to know where it belongs.
- When `UNKNOWN` exceeds 25% of stage wall clock and those stages are present, a caveat leads the
  `caveats[]` list, naming `hoodie.metadata.streaming.write.enabled` so a user can confirm it and
  quantifying the unattributed share.

**So: this tool is accurate for non-streaming writes and 0.x-style disjoint DAGs, and degrades on the
1.x streaming default.** Setting that config to `false` gives a fully attributable breakdown. This is
an accepted v1 limitation.

Measured on three runs of the same workload differing only in metadata-table config:

| Phase | files only | files + RLI, streaming **on** | files + RLI, streaming **off** |
|---|---|---|---|
| `DATA_TABLE_WRITE` | 58.5% | 29.0% | 82.9% |
| `UNKNOWN` | 15.7% | **60.5%** | 3.9% |
| `METADATA_TABLE_WRITE` | 7.7% | 8.0% | 9.6% |

Note the `UNKNOWN` share in the streaming column: that is the limitation above, not a gap in the
vocabulary. The other two columns are close to fully attributed.

## `outputBytes` and `outputRecords` are structurally zero

Spark's `Output Metrics` (`Bytes Written`, `Records Written`) read **zero on every stage of a Hudi
write**. This is not a parse bug: **Hudi writes Parquet through its own file handles, bypassing
Spark's instrumented writer**, so Spark never observes the bytes.

**Never infer "no data was written" from these fields.** Use the Hudi commit metadata for written
bytes and record counts. Input metrics are unaffected and do report real numbers.

## Phases that are expected to be absent

`ARCHIVAL` and savepoint work are mostly driver-side. `TimelineArchiverV2` has exactly one
`setJobStatus` (`Delete archived instants`), and `SavepointActionExecutor` tags only
`Collecting latest files for savepoint` — the savepoint write itself is a metadata file. **Their
presence with a non-trivial duration is itself the signal worth reporting**, not their absence.

## JSON schema

`schemaVersion` follows semver and is bumped when the envelope changes shape. Consumers should check
the major version. Ordering is deterministic: stages by `(stageId, attemptId)`, summaries by
descending wall clock then name, maps by key.

Durations are milliseconds, sizes are bytes, timestamps are epoch milliseconds. **A value of `-1`
means "not derivable from this log"**, never zero — a stage whose completion event is missing has
`durationMillis: -1`, which is common in a log from a killed application. `skewRatio` and
`gcFractionOfRunTime` are `-1` when undefined rather than infinity.

```jsonc
{
  "schemaVersion": "1.1.0",
  "eventLogPath": "/path/to/eventlog",

  "application": {
    "name": "streamer-my_table",
    "id": "application_1700000000000_0001",
    "startTimeEpochMillis": 1700000000000,
    "endTimeEpochMillis":   1700000141000,
    "sparkVersion": "3.5.1",
    "observed": {
      "durationMillis": 141000,
      "criticalPathMillis": 112000,   // floor on wall clock: longest stage per job, summed
      "jobCount": 20, "failedJobCount": 0,
      "stageCount": 29, "failedStageCount": 0,
      "taskCount": 2717, "failedTaskCount": 0,
      "executorRunTimeMillis": 402000,
      "shuffleReadBytes": 240000000, "shuffleWriteBytes": 188000000,
      "memoryBytesSpilled": 0, "diskBytesSpilled": 0,
      "peakConcurrentExecutors": 8,
      "eventsRead": 14233
    },
    "executorRemovalReasons": { "7": "Container killed by YARN for exceeding memory limits" }
  },

  // Descending by stageWallClockMillis. Same shape as phaseSummary, keyed by "operation".
  "operationSummary": [
    { "operation": "WRITE_OPERATION", "stageCount": 24, "taskCount": 2600,
      "stageWallClockMillis": 118000, "executorRunTimeMillis": 380000,
      "shuffleBytes": 420000000, "spilledBytes": 0, "shareOfStageWallClock": 0.84 }
  ],

  // Descending by stageWallClockMillis. The headline an optimizer reads.
  "phaseSummary": [
    { "phase": "DEDUP_AND_INDEX_TAGGING", "stageCount": 4, "taskCount": 1051,
      "stageWallClockMillis": 23900, "executorRunTimeMillis": 180400,
      "shuffleBytes": 103700000, "spilledBytes": 0, "shareOfStageWallClock": 0.194 }
  ],

  // Ascending by (stageId, attemptId). Always every stage, regardless of --top-n.
  "stages": [
    {
      "stageId": 15, "attemptId": 0,
      "name": "mapPartitions at HoodieWriteHelper.java:73",
      "jobId": 7,
      "jobDescription": "HoodieWriteHelper:Tagging: my_table",  // verbatim, to check the attribution
      "module": "HoodieWriteHelper",                            // null if neither encoding gave one
      "operation": "WRITE_OPERATION",
      "operationSource": "module",                              // "module" | "sequence" | "none"
      "phase": "DEDUP_AND_INDEX_TAGGING",
      "subPhase": "tagging",                                    // the finer label; null if none
      "bundledSteps": ["deduplication", "index tagging"],       // only when the stage is large
      "submissionTimeEpochMillis": 1700000012000,
      "completionTimeEpochMillis": 1700000025800,
      "failureReason": null,                                    // first line only, when it failed
      "observed": {
        "numTasksPlanned": 35, "numTasksObserved": 35,
        "durationMillis": 13800,
        "executorRunTimeMillis": 42000,
        "executorDeserializeTimeMillis": 300,
        "jvmGcTimeMillis": 1200, "gcFractionOfRunTime": 0.029,
        "resultSizeBytes": 120000,
        "inputBytes": 0, "inputRecords": 0,
        "outputBytes": 0, "outputRecords": 0,   // structurally zero for Hudi writes; see above
        "shuffleReadBytes": 51800000, "shuffleReadRecords": 1200000,
        "shuffleFetchWaitTimeMillis": 400,
        "shuffleWriteBytes": 0, "shuffleWriteRecords": 0, "shuffleWriteTimeNanos": 0,
        "memoryBytesSpilled": 0, "diskBytesSpilled": 0,
        "tasksSucceeded": 35, "tasksFailed": 0, "tasksKilled": 0, "tasksSpeculative": 0,
        "skewRatio": 3.8,                     // max task duration over p50; -1 when undefined
        "taskDurationMillis": {
          "p50": 1200, "p95": 4100, "max": 4400
          // with --include-tasks, also: "p25", "p75", "p99"
        }
      },
      // Distinct task failure reasons with counts. ExceptionFailure is qualified by the exception
      // class, so an OOM and a timeout do not collapse into one bucket.
      "taskFailureReasons": { "ExceptionFailure: java.lang.OutOfMemoryError": 3 }
    }
  ],

  // How far to trust the attribution, for the buckets present in THIS log. See the caveat section.
  "caveats": [
    "A bucket names where work was forced, not where it logically belongs. ..."
  ],

  "warnings": [
    "1 line(s) in eventlog could not be parsed as JSON and were skipped ..."
  ]
}
```

### Notes on particular fields

- **`criticalPathMillis`** — for each job, the longest single stage in it, summed over jobs. Jobs in a
  Hudi write run essentially back to back and the stages that matter within a job run in a dependency
  chain, so no amount of extra parallelism brings the run below this. Comparing it to
  `durationMillis` says what scheduling and job overhead cost.
- **`shareOfStageWallClock`** — the denominator is the sum of stage wall clock, not application
  duration, so the shares sum to 1. Stages overlap, so this is an attribution, not a timeline.
- **`subPhase`** — the finer label, kept so the coarse bucket does not lose information a reader may
  need: `workloadProfile`, `smallFileProbe`, `bloomComparisonFanout`, `partitionReplace`,
  `rollbackInstantListing`, `compactionPlan` and so on. Identifying which instants to roll back is
  often the expensive part of a rollback; it is folded into `PLANNING` with
  `subPhase: "rollbackInstantListing"` rather than given a bucket of its own.
- **`numTasksPlanned` vs `numTasksObserved`** — they differ when tasks were retried, or when the log
  is truncated. `numTasksObserved` counts `TaskEnd` events actually seen.
- **`peakConcurrentExecutors`** — replayed from `ExecutorAdded`/`ExecutorRemoved` in timestamp order,
  removals before additions on a tie. The driver is not counted; Spark emits no `ExecutorAdded` for it.

## Extending it

The activity-string to bucket mapping lives in `eventlog/HudiPhaseResolver.java`, grouped by bucket
with one rule per line, each carrying its optional sub-phase. The module-name to operation mapping
lives in `eventlog/HudiOperationResolver.java`. When a new `setJobStatus` call site is added to
`hudi-client`, add a rule to the first; when a new action executor appears, add one to the second.
