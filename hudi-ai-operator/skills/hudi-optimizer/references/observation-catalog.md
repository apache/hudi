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

# Observation catalog

Thirty rows describing a Hudi deployment that **works, but could work better**. Nothing here is a
failure. A row fires on a job that completes, a table that is consistent, and a service that is
running — and says what it is costing.

The sibling `hudi-rca` failure catalog answers "why did this break". This one answers "why is this
slower, larger or more expensive than it needs to be". Sixteen rows are the healthy-side reading of
an RCA pattern already tagged `alsoAppliesWhenHealthy`; the rest have no failure analogue at all.

## How to read a row

| Field | Meaning |
|---|---|
| **Costs** | What the user pays today, in terms they care about — wall clock, storage, request count, query latency |
| **Signals** | The exact field in one of the three JSON outputs. If a signal does not name a field, it is not a signal |
| **Corroboration** | Which of the three inputs must agree. **Never act on one** |
| **Change** | The `hoodie.*` or `spark.*` key, and the **direction** to move it |
| **Risk** | `safe` · `operational` · `durable` |
| **Payoff** | What to expect, and when to expect nothing |
| **Verify** | The measurement that proves it worked |

### The three inputs

1. **`props`** — the writer properties (`--props` / `--hoodie-conf`). 237 of 939 configs are
   performance levers and 93 of those are conditionally applied, so a recommendation made without the
   config may be silently inert.
2. **`eventlog`** — `HoodieSparkEventLogAnalyzer` JSON. Fields below are JSON paths into it.
3. **`table`** — the table itself, read through `HoodieTableHealthChecker` and
   `HoodieTableLayoutAnalyzer` JSON, plus the timeline.

**A row with one input agreeing is a hypothesis, not an observation.** Say which inputs you had.

### Risk tiers

| Tier | Meaning |
|---|---|
| `safe` | A reversible config change with no effect on what is on storage. Set it, run, set it back |
| `operational` | Changes what a table service does. Reversible, but you need a service cycle to see the result, and the intermediate state is on storage |
| `durable` | **Changes what is on storage in a way that is hard or impossible to undo.** Table type, partitioning, key generator, index type, record-key encoding |

**No row in this catalog is `durable`.** That is deliberate: a durable change is a design decision,
not an optimization, and belongs to `hudi-architect` with an ADR behind it. Durable candidates live
in `capability-catalog.md`, clearly marked, so the refusal has somewhere to point.

### Derived-expectation checks are the backbone

Most rows are not absolute thresholds. They compare **what the configuration implies** against **what
was measured**, which needs no history and works on a single run. When a row reads "config says X,
measurement says Y", that is the whole mechanism — and it is why `props` is mandatory rather than
nice to have.

### Carry the analyzer's caveats

Two caveats from `HoodieSparkEventLogAnalyzer` govern every row that cites `phaseSummary`:

- **A phase share names where work was *forced*, not where it belongs.** Hudi sets the job
  description before the Spark action and Spark is lazy, so a bucket routinely carries the cost of an
  untagged step. Read `caveats[]` in the JSON and repeat what it says.
- **With `hoodie.metadata.streaming.write.enabled=true` — the Spark default — a large share of the
  DAG is unattributable.** On a measured run, `UNKNOWN` was 60.5% of stage wall clock. When `UNKNOWN`
  is large, you are reasoning over a partial picture; say so instead of dividing the remainder.

---

## Index A — by theme

| Theme | Rows |
|---|---|
| File sizing and layout | OBS-001, OBS-002, OBS-003, OBS-004, OBS-005 |
| Parallelism and skew | OBS-006, OBS-007, OBS-008, OBS-009, OBS-010 |
| Index cost | OBS-011, OBS-012, OBS-013, OBS-014 |
| Table service cadence | OBS-015, OBS-016, OBS-017, OBS-018, OBS-019 |
| Timeline and metadata table | OBS-020, OBS-021, OBS-022, OBS-023 |
| Query-side | OBS-024, OBS-025, OBS-026 |
| Write path and driver | OBS-027, OBS-028, OBS-029, OBS-030 |

## Index B — rows seeded from an RCA pattern

| Observation | RCA pattern | What changes on the healthy side |
|---|---|---|
| OBS-006, OBS-011 | HUDI-RCA-014 | The same parallelism and skew analysis, run before the OOM |
| OBS-007, OBS-027 | HUDI-RCA-015 | Spill and parallelism on a slow but succeeding upsert |
| OBS-012 | HUDI-RCA-016 | File-group consolidation is an optimization long before it is a fix |
| OBS-004 | HUDI-RCA-017 | Pinning the record-size estimate stabilizes file sizing on a wide schema |
| OBS-015, OBS-016, OBS-017 | HUDI-RCA-018 | Service cadence tuning, the archetypal healthy-table optimization |
| OBS-029 | HUDI-RCA-019 | Poll pacing against a shard quota on a healthy stream |
| OBS-020 | HUDI-RCA-020 | A forgotten savepoint, found long before it becomes a 413 |
| OBS-021, OBS-028 | HUDI-RCA-021 | File-system-view and compaction sizing, before the wedge |
| OBS-018 | HUDI-RCA-022 | Clustering cadence against partition creation rate |
| OBS-022 | HUDI-RCA-024 | Listing reduction on any healthy job at scale |
| OBS-030 | HUDI-RCA-031 | `DIRECT` markers as a standing choice on a busy driver |
| OBS-019 | HUDI-RCA-037 | The cleaner that is correctly doing nothing — and what to stop tuning |
| OBS-023 | HUDI-RCA-006 | Lock wait tuning on a merely-slow multi-writer setup |
| OBS-026 | HUDI-RCA-038 | A pipeline whose cost is dominated by polling an empty source |
| OBS-005 | HUDI-RCA-039 | Benign spurious-file guard firings as a sign of wasted task retries |

The fourteen remaining rows (OBS-001, 002, 003, 008, 009, 010, 013, 014, 024, 025) have no failure
analogue — nothing breaks, ever, which is exactly why nobody finds them.

---

# File sizing and layout

## OBS-001 — Base files are well under the configured target size

- **Costs:** More files per query means more footer reads, more object-store requests, more Spark
  task overhead, and a larger metadata table. Read latency grows roughly with file count, and the
  effect compounds on every downstream consumer of the table.
- **Signals:**
  - `layout.tableSizeStats.p50` and `.mean` well below the configured `hoodie.parquet.max.file.size`
    (default 120 MB). A p50 under a third of target is the threshold worth acting on.
  - `layout.tableCharacteristics.detectors[name=small-files].effectiveConfigs["observed.flagged.pct"]`
    — the share of qualifying partitions averaging under the detector threshold.
  - `layout.partitions[].sizeStats.p50` per partition, so you can tell a table-wide problem from one
    bad partition.
- **Corroboration:** `table` + `props`. The layout numbers alone cannot say whether small files are a
  misconfiguration or a deliberate choice; the configured target is what makes it an observation.
- **The derived-expectation check:** `hoodie.parquet.max.file.size` says what a file should grow to.
  `hoodie.parquet.small.file.limit` says what counts as small enough to append to. **If
  `small.file.limit` is below the average size of files your batches produce, small-file handling
  never engages** — every batch writes new files and none of them ever grow.
- **Change:** Raise `hoodie.parquet.small.file.limit` toward (but below)
  `hoodie.parquet.max.file.size`; the default is 100 MB against a 120 MB target. Where the writer
  cannot bin-pack enough, schedule clustering (OBS-003).
- **Risk:** `safe` for the two sizing configs.
- **Payoff:** Fewer, larger files on subsequent writes. **Existing small files are not touched** —
  these configs govern new writes only, which is why clustering is the companion move.
- **Verify:** Re-run the layout analyzer after several commits and compare `tableSizeStats.p50`. The
  number should climb; if it does not, the limit is still below your per-batch-per-partition volume.

## OBS-002 — Small-file handling is configured but inert for this batch size

- **Costs:** The same as OBS-001, with the added cost of a user who believes the problem is handled.
- **Signals:**
  - `props` has a non-default `hoodie.parquet.small.file.limit`, **and**
  - `layout.tableSizeStats.p50` is still far below `hoodie.parquet.max.file.size`, **and**
  - `eventlog.phaseSummary[phase=DATA_TABLE_WRITE]` contains a stage with
    `subPhase: "smallFileProbe"` that runs — so the probe is happening and finding nothing.
- **Corroboration:** `props` + `table`, with `eventlog` confirming the probe ran at all.
- **The derived-expectation check:** small-file handling appends to an existing file only when that
  file is under `small.file.limit` **and** the incoming records for that partition fit. A workload
  writing a few MB per partition per commit into a partition whose files are already at 110 MB will
  correctly decline to append, forever. The config is live; the condition never holds.
- **Change:** This is the row where the answer may be "the config is right and the batch is the
  problem". Either increase bytes per commit per partition (fewer, larger commits), or accept that
  clustering is the consolidation mechanism for this shape and schedule it (OBS-003).
- **Risk:** `safe` to re-check; `operational` if the answer is clustering.
- **Payoff:** Mostly diagnostic. Its value is stopping a user from tuning a lever that is already
  doing what it is told.
- **Verify:** Count `smallFileProbe` stages before and after. If the probe still runs and file sizes
  still do not move, the batch shape is the binding constraint, not the config.

## OBS-003 — Clustering has never been scheduled on a table that would benefit

- **Costs:** Small files accumulate with no mechanism to consolidate them, and data with no locality
  means no useful column-stats pruning. Both get worse monotonically.
- **Signals:**
  - No `.replacecommit` from clustering anywhere on the timeline (`table`), **and**
  - `layout.tableCharacteristics.detectors[name=small-files].status` is `MODERATE` or `SEVERE`,
    **and**
  - `props` has `hoodie.clustering.inline=false` and `hoodie.clustering.async.enabled=false` (both
    default `false`), so nothing is going to schedule it.
- **Corroboration:** `table` + `props`. All three when the event log also shows no `CLUSTERING`
  operation in `operationSummary[]`.
- **Change:** Enable one scheduling path, not both:
  - `hoodie.clustering.inline=true` with `hoodie.clustering.inline.max.commits` for small tables
    where a periodic pause is acceptable;
  - `hoodie.clustering.async.enabled=true` with `hoodie.clustering.async.max.commits` where
    ingestion latency matters.
  - Bound the first plan: `hoodie.clustering.plan.strategy.max.num.groups` **down** from its default
    and `hoodie.clustering.plan.strategy.max.bytes.per.group` sized to what one round can execute.
    Starting unbounded on a table with a long backlog is how HUDI-RCA-022 happens.
- **Risk:** `operational`. Clustering writes replacecommits and rewrites file groups; reversible in
  the sense that you can stop scheduling it, but the rewritten files stay.
- **Payoff:** File count falls and `tableSizeStats.p50` rises over several rounds. With
  `hoodie.clustering.plan.strategy.sort.columns` set to a frequently-filtered column, column stats
  become selective and query pruning follows (see OBS-024).
- **Verify:** `layout.totalFiles` falls and `tableSizeStats.p50` rises round over round. In the event
  log, `operationSummary[operation=CLUSTERING]` appears with `PLANNING` and `EXECUTION` phases, and
  `EXECUTION` should dominate — planning dominating means the plan is too large for the round.

## OBS-004 — Record-size estimation is unstable on a wide schema

- **Costs:** File sizes swing commit to commit because the estimate driving row-group sizing is
  wrong. You get a mix of undersized and oversized files from an unchanged config.
- **Signals:**
  - `layout.tableSizeStats` shows a wide `min`-to-`max` spread with no matching spread in records
    written per commit (`table`, commit metadata).
  - Schema contains large string/JSON columns (`table`).
  - `props` leaves `hoodie.copyonwrite.record.size.estimate` at its 1024 default.
- **Corroboration:** `table` + `props`.
- **Change:** Pin the estimate rather than computing it: `hoodie.copyonwrite.record.size.estimate`
  **down**, well below the default, for a wide-payload table. **Counter-intuitive** — the configured
  value is deliberately lower than the real record size, and that is the point: it makes Hudi's
  sizing arithmetic conservative instead of letting a bad estimate drive it.
- **Risk:** `safe`.
- **Payoff:** File sizes stop swinging. This is the healthy-side reading of HUDI-RCA-017, where the
  same instability eventually throws a Parquet encoder assertion.
- **Verify:** The `max`/`min` ratio in `layout.tableSizeStats` narrows over subsequent commits.

## OBS-005 — Task retries are producing spurious files the commit guard then catches

- **Costs:** Every retried task writes a file that is later discarded. You pay the write, the storage
  until cleaning, and occasionally a failed commit that the next run fixes.
- **Signals:**
  - `eventlog.stages[].observed.tasksFailed` or `.tasksSpeculative` greater than zero on
    `DATA_TABLE_WRITE` stages.
  - `eventlog.stages[].taskFailureReasons` showing retries rather than a single terminal failure.
  - `props` has `spark.speculation=true`.
- **Corroboration:** `eventlog` + `props`. `table` corroborates if commits occasionally abort with
  the spurious-data-files guard.
- **Change:** Turn `spark.speculation` **off** for Hudi write jobs. Speculative execution against a
  writer that creates files outside Spark's output committer produces duplicate output that Hudi's
  marker reconciliation must then discard. If retries are not speculative, chase the task failures
  instead — they are the real signal.
- **Risk:** `safe`.
- **Payoff:** Wasted write work disappears. On a job with heavy speculation this can be a visible
  share of `DATA_TABLE_WRITE`.
- **Verify:** `tasksSpeculative` returns to zero; the guard stops firing.
- **Do not over-read this one.** An occasional spurious-file abort that the next run clears is benign
  (HUDI-RCA-039). It becomes an observation only when retries are routine.

---

# Parallelism and skew

## OBS-006 — A stage is under-parallelised against the shuffle config

- **Costs:** Wall clock. A stage with twelve tasks and forty minutes of work is forty minutes you
  cannot shorten by adding executors.
- **Signals:**
  - `eventlog.stages[].observed.numTasksPlanned` low relative to
    `eventlog.application.observed.peakConcurrentExecutors` — tasks fewer than available slots means
    idle capacity.
  - `eventlog.stages[].observed.taskDurationMillis.p50` large and `skewRatio` near 1.0 — uniformly
    long tasks, which is the signature of too few of them rather than skew.
  - `props`: `hoodie.upsert.shuffle.parallelism` / `hoodie.insert.shuffle.parallelism` /
    `hoodie.bulkinsert.shuffle.parallelism` / `hoodie.delete.shuffle.parallelism`.
- **Corroboration:** `eventlog` + `props`. The event log says how many tasks ran; only the config
  says whether that number was chosen or inherited.
- **The derived-expectation check, and the thing most advice gets wrong:** these configs **default to
  `0`, not 200**. `0` means "inherit the input RDD's partition count". So a stage with twelve tasks
  on a default config is reporting the source's partitioning, not a Hudi setting — and raising the
  Hudi config is the second move, not the first. **Fix the input partitioning first**, because the
  tagging and write stages inherit it.
- **Change:** Raise the source's partition count first (for Kafka, `hoodie.streamer.source.kafka.minPartitions`).
  Then set the relevant `hoodie.*.shuffle.parallelism` explicitly, **up**, toward roughly two to
  three times the executor-core count.
- **Risk:** `safe`.
- **Payoff:** Proportional to the gap between task count and available slots. Nothing, if the stage
  was already spreading across every slot.
- **Verify:** `numTasksPlanned` rises, stage `durationMillis` falls, and `skewRatio` does not rise. If
  duration does not fall, the stage was not parallelism-bound.
- **Note the exception:** the data-table write stage's task count is **bucket-shaped** — it follows
  the file groups being written, not the shuffle config. Tuning
  `hoodie.upsert.shuffle.parallelism` does not move it. Check `subPhase` before concluding.

## OBS-007 — Stages spill to disk under a healthy-looking job

- **Costs:** Spill is work done twice. The job completes, so nothing alerts, but the stage pays
  serialization, disk write and disk read for data that should have stayed in memory.
- **Signals:**
  - `eventlog.stages[].observed.memoryBytesSpilled` and `.diskBytesSpilled` greater than zero.
  - `eventlog.application.observed.memoryBytesSpilled` as the application-level total.
  - Cross-reference `eventlog.stages[].phase` — spill in `DEDUP_AND_INDEX_TAGGING` and spill in
    `DATA_TABLE_WRITE` have different remedies.
- **Corroboration:** `eventlog` + `props`. Which config to move depends entirely on the phase, and
  the phase is only trustworthy read against the config.
- **Change:**
  - Spill in `DEDUP_AND_INDEX_TAGGING`: raise the relevant shuffle parallelism (OBS-006) so each task
    handles less.
  - Spill in `DATA_TABLE_WRITE` on a MOR table: raise `hoodie.memory.merge.max.size`, which governs
    the in-memory map used for log merge, and `spark.executor.memoryOverhead`.
  - Spill under GC pressure rather than hard memory limits: **lower** `spark.memory.fraction` and
    `spark.memory.storageFraction`. **Counter-intuitive** — giving Spark's managed regions less
    leaves more heap for the user objects Hudi's merge path actually allocates.
- **Risk:** `safe`.
- **Payoff:** Removing spill from a stage that was spilling heavily is often the single largest
  cheap win available on a healthy job.
- **Verify:** `memoryBytesSpilled` and `diskBytesSpilled` fall to zero on the target stage while
  `durationMillis` falls. If spill falls but duration does not, the stage was not spill-bound.

## OBS-008 — A stage is skewed and the job absorbs it

- **Costs:** The stage runs as long as its slowest task. Every other executor idles. Measured on a
  real log: a small-file-probe stage at 56.7% of wall clock with `p50=17ms` and `max=3m00s` — one
  pathological task out of a thousand.
- **Signals:**
  - `eventlog.stages[].observed.skewRatio` — max task duration over p50. Above ~10x is worth a look;
    above 100x is pathological. **`-1` means undefined, not zero.**
  - `eventlog.stages[].observed.taskDurationMillis.p95` against `.max` — a `max` far above `p95` is
    one straggler; `p95` near `max` is a broad tail.
  - `eventlog.stages[].observed.shuffleReadBytes` with the spread across tasks.
  - `eventlog.stages[].subPhase` to know which component to attribute it to.
- **Corroboration:** `eventlog` + `table`. Skew in the write path is usually partition skew, and
  `layout.skew.cv` / `.gini` / `.largestPartitionShare` is where you confirm it.
- **Change:** Depends on where it is, and the diagnosis must come first:
  - Partition skew (`layout.skew.largestPartitionShare` high): this is a layout property. Clustering
    helps within partitions; across them it is a partitioning question, which is `durable` and goes to
    `hudi-architect`.
  - Bloom-index skew with random keys: `hoodie.bloom.index.fileid.key.sorting.enable=true` **and**
    `hoodie.bloom.index.bucketized.checking=false`. **Both are required** — bucketized checking stays
    on unless explicitly disabled, so setting only the first changes nothing.
  - A single straggler on a small-file probe is usually one partition with a very large number of
    base files; fix that partition's file count (OBS-003).
- **Risk:** `safe` for the index configs; `operational` for clustering.
- **Payoff:** Bounded by the gap between the stage's duration and its p50 times task count over slot
  count. Compute that before promising anything.
- **Verify:** `skewRatio` falls on the target stage. Watch that it does not simply move to the next
  stage downstream.

## OBS-009 — Many stages are over-parallelised for the work they do

- **Costs:** Scheduling overhead, one output file per task on shuffle write, and a map-status
  structure that grows with partition count. The pathological end of this is HUDI-RCA-016, where the
  broadcast becomes unfetchable — but long before that it is simply waste.
- **Signals:**
  - `eventlog.stages[].observed.numTasksPlanned` high while `taskDurationMillis.p50` is tiny (tens of
    milliseconds) — a thousand tasks each doing nothing.
  - `eventlog.application.observed.criticalPathMillis` far below `application.observed.durationMillis`
    — the gap is scheduling and job overhead, not work.
  - `props` shows an explicitly-set high `hoodie.simple.index.parallelism` or a shuffle parallelism
    set well above the executor-core count.
- **Corroboration:** `eventlog` + `props`.
- **Change:** **Reduce** the offending parallelism config toward two to three times the executor-core
  count. For `hoodie.simple.index.parallelism` specifically, reducing it is also the remedy for the
  broadcast failure — **raising it makes that failure worse**, which is exactly where an operator
  reaching for the obvious lever ends up.
- **Risk:** `safe`.
- **Payoff:** Recovers the overhead gap. On a job whose critical path is a small fraction of its
  duration, this can be a large share.
- **Verify:** The gap between `criticalPathMillis` and `durationMillis` narrows; stage count and task
  count fall without stage durations rising.

## OBS-010 — The job's wall clock is dominated by scheduling, not work

- **Costs:** Executors sit idle between stages. You are paying for a cluster that is coordinating
  rather than computing.
- **Signals:**
  - `eventlog.application.observed.criticalPathMillis` divided by `.durationMillis` well below 1.0.
    The critical path is a **floor** on wall clock — the gap is everything else.
  - `eventlog.application.observed.executorRunTimeMillis` low relative to
    `durationMillis × peakConcurrentExecutors` — low cluster utilisation.
  - `eventlog.application.observed.jobCount` and `.stageCount` high for the volume of data moved.
- **Corroboration:** `eventlog` + `props`. Many small jobs on a Hudi write usually means many small
  commits, which is a writer-cadence question the config answers.
- **Change:** Fewer, larger batches at the source. On the streamer path, raise the per-sync batch
  size so each sync does more work. There is no single Hudi config for this — it is the source's
  pacing, and the right lever is source-specific.
- **Risk:** `safe`, but it trades latency for throughput, so it is a product decision rather than a
  pure win. Say that.
- **Payoff:** Fewer commits also means a shorter timeline, a smaller metadata table, and less
  archival work — the benefit compounds beyond the job.
- **Verify:** `criticalPathMillis / durationMillis` rises toward 1.0 and `jobCount` falls for the same
  volume.
- **Caveat:** `criticalPathMillis` is the longest stage per job, summed, which assumes jobs do not
  overlap. Async table services do overlap, so on a table running async compaction or clustering this
  ratio reads lower than reality. Check `operationSummary[]` for concurrent operations first.

---

# Index cost

## OBS-011 — Index tagging is a large share of wall clock

- **Costs:** On a SIMPLE index, tagging cost scales with **table size, not batch size** — so this
  share grows as the table grows even when the workload does not. It is the clearest case of a cost
  that is invisible until you measure it.
- **Signals:**
  - `eventlog.phaseSummary[phase=DEDUP_AND_INDEX_TAGGING].shareOfStageWallClock` — above ~0.3 is
    worth investigating.
  - `eventlog.stages[].subPhase` within that bucket: `bloomIndexLookup`, `recordIndexLookup` or
    `workloadProfile`. **This distinction is load-bearing** — on a measured log, isolating
    `recordIndexLookup` separated 3.6s of real index work from the profile stages it was pooled with.
  - `eventlog.stages[].observed.numTasksPlanned` on those stages against
    `layout.totalFiles` — a task count tracking the **base-file count** rather than the batch size is
    the smoking gun for a SIMPLE index.
  - `props`: `hoodie.index.type`.
- **Corroboration:** All three. The phase share needs the config to say which index produced it and
  the table to say what the cost scales with.
- **Change:** Within the current index type: raise `hoodie.simple.index.parallelism` if tasks are few
  and long (OBS-006), or `hoodie.bloom.index.parallelism` and lower
  `hoodie.bloom.index.keys.per.bucket` on a bloom index. **Lowering keys per bucket is
  counter-intuitive** — you shrink the per-task batch to produce more, smaller units of work. Raising
  it makes memory pressure worse.
- **Risk:** `safe`.
- **Payoff:** Within an index type, modest. **If tagging dominates and the task count tracks
  file count rather than batch size, the real answer is a different index** — see
  `capability-catalog.md`, CAP-001. Changing `hoodie.index.type` is `durable` and is not this
  catalog's to recommend.
- **Verify:** The `DEDUP_AND_INDEX_TAGGING` share falls. Re-measure after the table has grown, not
  just after the config change — that is the test of whether the cost still scales with size.
- **Caveat:** This bucket contains deduplication too. `BaseWriteHelper` tags only `Tagging:` and
  leaves `deduplicateRecords` untagged, so both land together. A large share may be dedup, not the
  index. `bundledSteps` on the stage says when the tool thinks this is material.

## OBS-012 — File-group count is growing without bound

- **Costs:** Simple-index tagging and write profiling fan out with file-group count, so both get
  slower as file groups accumulate. The shuffle map-status broadcast grows with them. Nothing fails
  until it does.
- **Signals:**
  - `layout.totalFiles` rising across runs with `layout.tableSizeStats.mean` flat or falling — new
    file groups rather than growing ones.
  - `layout.numPartitions` against `layout.fileCountPerPartition.mean`.
  - `layout.tableCharacteristics.detectors[name=micro-partition].status` is `FLAGGED`, with
    `effectiveConfigs["observed.size.rule.triggered"]` true.
  - `eventlog.stages[].observed.numTasksPlanned` on `workloadProfile` sub-phase stages tracking that
    count.
- **Corroboration:** `table` + `eventlog`. `props` third, to check whether anything is scheduled to
  consolidate them.
- **Change:** Consolidation, not parallelism. Schedule clustering (OBS-003) with
  `hoodie.clustering.plan.strategy.small.file.limit` set so small groups are picked up and
  `hoodie.clustering.plan.strategy.target.file.max.bytes` set to the size you want out. On MOR,
  confirm compaction is keeping up (OBS-015) — uncompacted file slices count too.
- **Risk:** `operational`.
- **Payoff:** Tagging and profiling cost stop growing with the table. The benefit is mostly in
  avoided future cost, which makes it easy to defer and expensive to have deferred.
- **Verify:** `layout.totalFiles` flattens or falls while total bytes keep growing. That divergence is
  the signal.

## OBS-013 — Column stats are maintained but never used

- **Costs:** You pay to compute and store column statistics on every write and get no pruning back.
  On Spark the column-stats index is **on by default**, so this is the common case, not an exotic
  one.
- **Signals:**
  - `props`: `hoodie.metadata.index.column.stats.enable` is on — and note the **engine-effective**
    default is `true` on Spark even though the declared default is `false`. Reading the declared
    default here gives the wrong answer about every default Spark table.
  - `props`: `hoodie.enable.data.skipping` on the **read** side. If readers do not set it, nothing
    consumes the stats.
  - `table`: a `column_stats` partition exists under the metadata table.
  - `eventlog.phaseSummary[phase=METADATA_TABLE_WRITE].shareOfStageWallClock` — the write-side cost
    you are paying for them.
- **Corroboration:** `props` + `table`. The read-side config is the decisive half and is frequently
  not in the writer properties at all — **say so rather than assuming**.
- **Change:** Two directions, and the measurement decides:
  - If queries filter on columns the stats cover: ensure readers set `hoodie.enable.data.skipping=true`,
    and give the data locality to prune on with
    `hoodie.clustering.plan.strategy.sort.columns` set to those columns. Unsorted data makes every
    file's min/max span the whole range and pruning finds nothing.
  - If no query filters on anything the stats cover: narrow them with
    `hoodie.metadata.index.column.stats.column.list` rather than disabling the index, so you keep
    pruning where it pays.
- **Risk:** `safe` for the read config; `operational` for clustering and for narrowing the column
  list (the index is rebuilt).
- **Payoff:** Either queries get faster or metadata-table write cost falls. Both are real; which one
  depends on the query evidence, which you usually do not have — **ask for it rather than guessing**.
- **Verify:** With skipping on and data sorted, a filtered query reads fewer files. Without a query
  plan you cannot verify this, so say that it is unverified.

## OBS-014 — Partition stats are enabled by inheritance rather than intent

- **Costs:** Partition-level statistics are derived from column stats and written on every commit.
  On a table with few partitions they prune almost nothing.
- **Signals:**
  - `props`: `hoodie.metadata.index.partition.stats.enable`. **It has no independent effect** —
    partition stats require `hoodie.metadata.index.column.stats.enable` and are inert without it.
  - `layout.numPartitions` small (single digits, or unpartitioned) — partition-level pruning cannot
    help when there is nothing to prune between.
  - `eventlog.phaseSummary[phase=METADATA_TABLE_WRITE]` for the cost.
- **Corroboration:** `props` + `table`.
- **Change:** On a table with few partitions where queries do not filter on the partition key, turn
  `hoodie.metadata.index.partition.stats.enable` **off**. Leave it on where partition count is high
  and queries filter on partition columns.
- **Risk:** `operational` — the index partition is dropped and rebuilt if you turn it back on.
- **Payoff:** Small but free. The reason to include it is that it is a lever users do not know they
  are pulling, since it rides on the column-stats default.
- **Verify:** `METADATA_TABLE_WRITE` share falls slightly; the `partition_stats` partition disappears
  from the metadata table.

---

# Table service cadence

## OBS-015 — Compaction trails the configured trigger without having stopped

- **Costs:** Read-side merge cost on every query of an uncompacted file slice, growing log files, and
  a rollback surface that widens with each uncompacted slice. The table works; it just works harder
  every day.
- **Signals:**
  - `health.checks[name=compaction].effectiveConfigs["observed.delta.commits.since.last.compaction"]`
    against `["effective.unhealthy.threshold.delta.commits"]`. **Read the numbers, not the verdict** —
    the health checker applies a 2.0x slack factor specifically so it only flags a service that has
    *stopped*. An observation fires in the band between the configured trigger and that threshold,
    where the checker correctly reports `HEALTHY`.
  - `props`: `hoodie.compact.inline.max.delta.commits` (or `.max.delta.seconds` under the TIME
    strategy), and `hoodie.compact.inline.trigger.strategy`.
  - `eventlog.operationSummary[operation=COMPACTION]` — whether compaction ran in this window and how
    `PLANNING` compared with `EXECUTION`.
- **Corroboration:** `table` + `props`. The accumulated count means nothing without the trigger.
- **The derived-expectation check:** the trigger says compaction should happen every N delta commits.
  If 2N have accumulated, compaction is running at half the configured rate. That is an observation
  long before the health checker's threshold.
- **Change:** Either compact more often (**lower** `hoodie.compact.inline.max.delta.commits`) or give
  each round more capacity (**raise** `hoodie.compaction.target.io`). Which one depends on
  `EXECUTION` duration: short executions that are too rare want a lower trigger; long executions that
  do not finish the backlog want more target IO. **Raise target IO substantially rather than nudging
  it** — if only half the candidate file groups fit in a round, a tail accumulates forever.
- **Risk:** `operational`.
- **Payoff:** Query-side merge cost falls and stops growing. On a MOR table under steady updates this
  is usually the highest-value service tuning available.
- **Verify:** `observed.delta.commits.since.last.compaction` stabilises near the configured trigger
  rather than drifting up. In the event log, `EXECUTION` should dominate `PLANNING` within the
  `COMPACTION` operation.

## OBS-016 — The cleaner trails its trigger while still running

- **Costs:** Storage for file versions past the retention you asked for, plus a timeline that cannot
  archive past the cleaner's `earliestCommitToRetain`.
- **Signals:**
  - `health.checks[name=cleaner]` — again the numbers, not the verdict. The checker multiplies the
    trigger by 2.0 **and floors at 5 commits**, so a cleaner several commits behind reports healthy.
  - `props`: `hoodie.clean.trigger.max.commits`, `hoodie.cleaner.policy`,
    `hoodie.cleaner.commits.retained`.
  - `health.checks[name=cleaner].effectiveConfigs["observed.last.clean.earliest.commit.retained"]` —
    the retention watermark the last clean reached — and the active instant count.
- **Corroboration:** `table` + `props`.
- **Change:** `hoodie.clean.automatic` on (its default) with
  `hoodie.clean.trigger.max.commits` tuned to your commit rate; or `hoodie.clean.async=true` where a
  synchronous clean is costing ingestion latency. Raise `hoodie.cleaner.parallelism` if the clean
  itself is slow rather than infrequent.
- **Risk:** `operational`.
- **Payoff:** Storage reclaimed on the retention you configured, and archival unblocked behind it.
- **Verify:** `observed.last.clean.earliest.commit.retained` advances; active instant count stops
  growing.
- **Read OBS-019 first.** A cleaner that appears behind is very often a cleaner correctly finding
  nothing to do, and changing retention in response is how a non-incident becomes an incident.

## OBS-017 — Archival trails the configured retention

- **Costs:** Every timeline load parses more instants. The cost is paid by every write, every table
  service and every reader — it is the most broadly-felt slow degradation in Hudi.
- **Signals:**
  - `health.checks[name=archival].effectiveConfigs` — the observed active instant count against
    `hoodie.keep.max.commits`. The checker allows 1.1x slack and an absolute ceiling of 5000; an
    observation fires between the configured retention and that slack.
  - `props`: `hoodie.keep.min.commits`, `hoodie.keep.max.commits`,
    `hoodie.commits.archival.batch`, `hoodie.archive.automatic`.
  - `eventlog.phaseSummary[phase=ARCHIVAL]` — **archival is driver-dominated, so its presence with
    non-trivial duration is itself the signal.** Absence tells you nothing.
- **Corroboration:** `table` + `props`.
- **Change:** Check the chain before touching archival config: **archival silently overrides
  `hoodie.keep.min.commits`** when the cleaner's `earliestCommitToRetain` demands more retention.
  "I set min commits to 20 and have 400 instants" is almost always the cleaner holding the line —
  **fix the cleaning config, not the archival one** (OBS-016). Once the cleaner is right, lower
  `hoodie.commits.archival.batch` if archival itself is heavy.
- **Risk:** `operational`.
- **Payoff:** Timeline load time is a fixed tax on everything. Removing it helps uniformly.
- **Verify:** Active instant count falls toward `hoodie.keep.max.commits` and stays there.
- **Check OBS-020 first.** A savepoint pins archival regardless of configuration, and tuning archival
  with a savepoint in place prunes nothing while costing object-store requests.

## OBS-018 — Clustering cadence is behind the partition-creation rate

- **Costs:** Each round has more to do than the last. The plan grows, and a plan large enough becomes
  its own problem — it is re-read and fully deserialised on every timeline refresh.
- **Signals:**
  - `table`: a pending `.replacecommit.requested` whose **plan file size** is more than a few MB.
    This is the giveaway, and it is cheap to check.
  - `layout.numPartitions` growing faster than clustering replacecommits complete (`table`).
  - `props`: `hoodie.clustering.plan.strategy.max.num.groups`,
    `hoodie.clustering.plan.strategy.max.bytes.per.group`,
    `hoodie.clustering.inline.max.commits` or `hoodie.clustering.async.max.commits`.
  - `eventlog.operationSummary[operation=CLUSTERING]` with `PLANNING` share exceeding `EXECUTION` —
    the planner is working harder than the executor, which is backwards.
- **Corroboration:** `table` + `props`.
- **Change:** Increase clustering frequency (**lower** the max-commits trigger) and **cap each plan**
  via `hoodie.clustering.plan.strategy.max.num.groups` and
  `hoodie.clustering.plan.strategy.max.bytes.per.group`. On a date-partitioned table,
  `hoodie.clustering.plan.strategy.daybased.lookback.partitions` bounds the candidate set directly.
- **Risk:** `operational`.
- **Payoff:** Rounds stay bounded and predictable. Mostly avoided future cost — the failure mode at
  the end of this road is a multi-day outage (HUDI-RCA-022), which is a reason to act early rather
  than a reason to panic now.
- **Verify:** Pending plan file size stays small, and `EXECUTION` dominates `PLANNING` in
  `operationSummary[operation=CLUSTERING]`.

## OBS-019 — The cleaner is correctly doing nothing, and the config is being tuned anyway

- **Costs:** None directly. The cost is a user about to make a real change in response to a
  non-problem — **which is the most expensive thing in this catalog.**
- **Signals:**
  - `table`: few or no recent `.clean` instants, possibly with a growing active timeline.
  - **The decisive, cheap check:** in recent commit metadata compare `numWrites` and `numInserts` per
    file. **Equal means the write created a new file group** — there is no older version to clean and
    the cleaner is correct. Hudi also retains one prior version as a buffer, so a file group with two
    base files still has nothing to clean.
  - `health.checks[name=cleaner]` reporting `HEALTHY`, with its findings distinguishing "behind" from
    "nothing to do".
- **Corroboration:** `table` + `props`. All three if the event log shows no `CLEANING` operation —
  which, on its own, means nothing.
- **Change:** **None.** Confirm it is benign and say so. The legitimate causes are: append-only or
  immutable data; a recent retention change producing a quiet window equal to the difference;
  clustering having just consolidated many file groups into few; or a MOR table whose file groups are
  only appended to — the cleaner cannot clean a group compaction has not yet given a new version.
- **Risk:** n/a.
- **Payoff:** A change not made.
- **Verify:** Nothing to verify. **One thing worth noting**: incremental clean can permanently miss
  partitions that received no new ingests, leaving old file versions indefinitely. A periodic full
  clean is the answer there, and that is a real recommendation rather than a non-action.
- **If clean genuinely is not running and a compaction or rollback is also stuck**, that is the real
  problem and it belongs to `hudi-rca`, not here.

---

# Timeline and metadata table

## OBS-020 — A savepoint is quietly pinning the timeline

- **Costs:** Archival stops at the **earliest** savepoint, so one forgotten savepoint pins everything
  after it. The timeline grows without bound, every timeline load gets slower, and data files the
  cleaner would release stay alive. Nothing errors while this happens.
- **Signals:**
  - `health.checks[name=savepoint]` — two signals: a savepoint further back than the configured
    archival retention (blocking now), and a savepoint older than 7 days (likely forgotten).
  - `props`: `hoodie.keep.max.commits`, and `hoodie.archive.beyond.savepoint`.
  - `table`: the savepoint instant time against the active timeline's span.
- **Corroboration:** `table` + `props`.
- **Change:** Release the savepoint once whatever it was taken for is done (`savepoint delete` in
  `hudi-cli`). Archival resumes and the timeline shrinks on its own.
- **Risk:** `operational` — **and genuinely consequential.** A savepoint protects files from cleaning;
  releasing one makes those files eligible for deletion. **Confirm with a human that the savepoint is
  no longer needed. Never recommend releasing one as a routine optimization.**
- **Payoff:** Archival resumes and the timeline shrinks. On a long-pinned table this can be thousands
  of instants.
- **Verify:** Active instant count falls after the next archival round.
- **Do not reach for aggressive archival scheduling first.** With the savepoint in place a more
  frequent archival job prunes nothing and costs object-store requests. It is pointless until the
  savepoint is gone.

## OBS-021 — The file-system view is on heap and the table has outgrown it

- **Costs:** Driver heap pressure, GC time on the driver, and timeline-server responses that get
  slower as the view grows. On a large enough view the serialisation path hits a structural ceiling
  that more heap cannot move.
- **Signals:**
  - `eventlog.application.executorRemovalReasons` and driver-side GC — but note the driver is **not**
    in `peakConcurrentExecutors`, so the event log is weak evidence here.
  - `eventlog.stages[].observed.jvmGcTimeMillis` and `.gcFractionOfRunTime` above ~0.1 across many
    stages. **`-1` means undefined.**
  - `layout.totalFiles` and `layout.numPartitions` — view size tracks file-group count.
  - `props`: `hoodie.filesystem.view.type` (default `MEMORY`),
    `hoodie.filesystem.view.spillable.mem`, `hoodie.common.spillable.diskmap.type`.
- **Corroboration:** `table` + `props`. The event log is corroborating, not deciding.
- **Change:** For a table with a very large view, `hoodie.filesystem.view.type=SPILLABLE_DISK` with
  `hoodie.filesystem.view.spillable.mem` sized around 1 GB and
  `hoodie.common.spillable.diskmap.type=ROCKS_DB`.
- **Risk:** `safe` — a writer-side config with no storage effect.
- **Payoff:** Driver heap pressure drops on a large table. **Nothing, on a small one.**
- **Verify:** Driver GC fraction falls; timeline-server timeouts (OBS-030) stop.
- **Apply this per table, not globally, and note the direct tension:** on a **long-lived driver
  hosting many small tables**, `SPILLABLE_DISK` is the wrong answer — each spillable map registers a
  JVM shutdown hook and those accumulate across table-service cycles (HUDI-RCA-023). A table with a
  very large view needs spillable; a driver with many small tables does not.

## OBS-022 — File listing is going to storage rather than the metadata table

- **Costs:** Object-store list requests scale with partition count on every operation. On a large
  table this is both latency and a line on the storage bill.
- **Signals:**
  - `props`: `hoodie.metadata.enable`. Note the **engine-effective** default is `true` on Spark and
    Flink but `false` on the Java engine — reading the declared default gives the wrong answer for a
    Java-engine writer.
  - `layout.mdtEnabled` — the layout analyzer reports whether it read through the metadata table.
  - `eventlog.stages[].jobDescription` containing `Parallel listing paths`, and
    `eventlog.stages[].subPhase == "metadataListing"` for the metadata-table side.
  - `eventlog.stages[].observed.durationMillis` on those listing stages as the direct cost.
  - `props`: `hoodie.file.listing.parallelism`.
- **Corroboration:** `props` + `eventlog`, with `table` confirming partition count makes it matter.
- **Change:** `hoodie.metadata.enable=true` where it is off and partition count is non-trivial.
- **Risk:** `operational` — the metadata table is bootstrapped on first enable, which costs a cycle,
  and it then has its own write cost on every commit (OBS-023).
- **Payoff:** Listing cost becomes a metadata-table read instead of a storage listing. Grows with
  partition count; **negligible on a small or unpartitioned table, where the metadata table's write
  overhead may exceed what it saves.**
- **Verify:** `Parallel listing paths` stages against the data table disappear from the event log;
  `layout.mdtEnabled` reads true.

## OBS-023 — Metadata-table write cost is a large share of the commit

- **Costs:** Every enabled index partition is written on every commit. Enabling indexes you do not
  query is a per-commit tax with no return.
- **Signals:**
  - `eventlog.phaseSummary[phase=METADATA_TABLE_WRITE].shareOfStageWallClock`. On measured runs this
    sat around 0.06-0.10; materially above that is worth a look.
  - `props`, the full set of index switches: `hoodie.metadata.index.column.stats.enable`,
    `hoodie.metadata.index.partition.stats.enable`, `hoodie.metadata.index.bloom.filter.enable`,
    `hoodie.metadata.index.secondary.enable`, `hoodie.metadata.global.record.level.index.enable`.
    **Three of these default differently per engine** — on Spark, column stats and secondary index
    are effectively `true` regardless of the declared default.
  - `table`: which partitions actually exist under the metadata table.
  - `props`: `hoodie.metadata.compact.max.delta.commits` — how often the metadata table compacts
    itself.
- **Corroboration:** `props` + `eventlog`. `table` confirms which indexes were actually built.
- **Change:** Turn off index partitions nothing queries. The common one is
  `hoodie.metadata.index.bloom.filter.enable`, which is off by default but gets switched on and
  forgotten when the write index is not bloom. Conversely, if metadata-table **compaction** is behind,
  the cost is log-block accumulation rather than too many indexes — lower
  `hoodie.metadata.compact.max.delta.commits`.
- **Risk:** `operational` — disabling an index partition deletes it; re-enabling rebuilds it.
- **Payoff:** Proportional to the share. Modest in absolute terms, paid on every single commit.
- **Verify:** `METADATA_TABLE_WRITE` share falls and the corresponding partition disappears.
- **Caveat that applies to this row more than any other:** with
  `hoodie.metadata.streaming.write.enabled=true` (the **Spark default**), a large share of the
  metadata write path is unlabelled and lands in `UNKNOWN`, not in `METADATA_TABLE_WRITE`. On a
  measured run, toggling that one config moved 56 points of wall clock between the two buckets. **If
  `UNKNOWN` is large, the `METADATA_TABLE_WRITE` share understates the real cost and you must say so.**

---

# Query-side

## OBS-024 — Column stats exist but the data has no locality to prune on

- **Costs:** Full scans on queries that look prunable. The index is built, maintained and ignored,
  because min/max per file spans the whole range when data arrives unsorted.
- **Signals:**
  - `props`: `hoodie.metadata.index.column.stats.enable` effectively on (the Spark default).
  - `props`: `hoodie.clustering.plan.strategy.sort.columns` **unset** — nothing is ordering the data.
  - `table`: no clustering `.replacecommit` on the timeline.
  - `layout.partitions[].sizeStats` showing many files per partition, each of which will carry the
    full range.
- **Corroboration:** `props` + `table`. **Query evidence would be the decisive third input and you
  almost certainly do not have it — ask rather than assume.**
- **Change:** Set `hoodie.clustering.plan.strategy.sort.columns` to the columns queries filter on and
  schedule clustering (OBS-003). Sorting is what turns a maintained index into a selective one.
- **Risk:** `operational`.
- **Payoff:** Potentially large on a selective filter, and **exactly zero** if queries do not filter
  on those columns. This row depends entirely on query evidence; without it, present it as a
  hypothesis and name the evidence that would settle it.
- **Verify:** The same filtered query reads fewer files after clustering. Without a before-and-after
  query plan this is unverified, and you should say so rather than claiming the change worked.

## OBS-025 — A Merge-on-Read table carries an append-only workload

- **Costs:** Every read merges log files against base files. On a workload with no updates there is
  nothing to merge and the merge machinery is pure overhead — plus compaction running to compact logs
  that only ever contained inserts.
- **Signals:**
  - `table`, commit metadata across recent commits: `numUpdateWrites` near zero against `numInserts`.
    **This is the decisive signal** and it needs several commits, not one.
  - `props`: `hoodie.datasource.write.operation` is `insert` or `bulk_insert` rather than `upsert`.
  - `table`: `hoodie.properties` says `MERGE_ON_READ`.
  - `eventlog.operationSummary[operation=COMPACTION]` — compaction cost being paid on a workload that
    produces nothing to compact.
- **Corroboration:** `table` + `props`.
- **Change:** **None from this skill.** Table type is `durable` — it is on storage and cannot be
  switched in place. This row exists to be *reported*, with the evidence, and **handed to
  `hudi-architect`**. Record what you measured so the design conversation starts from numbers.
- **Risk:** `durable`. **The agent must refuse to recommend this as an optimization.**
- **Payoff:** Not this skill's to promise. Say what you measured and hand it over.
- **Verify:** n/a.
- **What you *can* do safely meanwhile:** if updates genuinely never happen, confirm the write
  operation is `insert`/`bulk_insert` rather than `upsert` — that is a `safe` config change which
  skips index tagging entirely, and it is often the real win hiding behind this observation.

## OBS-026 — The pipeline spends most of its time reading an empty source

- **Costs:** Cluster time, and a commit per sync whether or not there was data — so timeline growth,
  metadata-table writes and archival work for nothing.
- **Signals:**
  - `eventlog.phaseSummary[phase=SOURCE_READ_AND_TRANSFORM].shareOfStageWallClock` large while
    `table` commit metadata shows near-zero records written in the same window.
  - `eventlog.application.observed.jobCount` high against records written.
  - `table`: many commits with tiny `numWrites`.
- **Corroboration:** `eventlog` + `table`.
- **Change:** Pace the syncs to the source's actual arrival rate rather than polling continuously.
  This is source-specific pacing, not a Hudi config, so name the mechanism rather than inventing a
  key.
- **Risk:** `safe`, but it trades latency for efficiency — a product decision.
- **Payoff:** Fewer commits means a shorter timeline and less service work downstream. The compounding
  benefit is larger than the direct one.
- **Verify:** Commits per hour falls while records per commit rises; `jobCount` falls.
- **Caveat:** `SOURCE_READ_AND_TRANSFORM` **bundles source read, user transformation and record
  creation** — `StreamSync` tags only the fetch and the emptiness check, and `HoodieStreamerUtils`
  carries no job status at all. A large share here may be an expensive transformer rather than an
  empty source. Check `bundledSteps` and the commit record counts before concluding.

---

# Write path and driver

## OBS-027 — Write parallelism is inherited rather than chosen

- **Costs:** The write stage's shape is whatever the source happened to produce. Sometimes that is
  fine; when it is not, nothing says so.
- **Signals:**
  - `props`: `hoodie.upsert.shuffle.parallelism`, `hoodie.insert.shuffle.parallelism`,
    `hoodie.bulkinsert.shuffle.parallelism`, `hoodie.delete.shuffle.parallelism` all at their `0`
    default.
  - `eventlog.stages[].observed.numTasksPlanned` on `DEDUP_AND_INDEX_TAGGING` stages varying run to
    run for similar volumes — the tell that it is inherited.
- **Corroboration:** `props` + `eventlog`.
- **Change:** Set the operation-relevant config explicitly so the shape is stable and intentional.
  **Only the one for the operation you run** — setting `bulkinsert` parallelism on an upsert workload
  does nothing, and that inertness is exactly the class of advice this catalog exists to avoid.
- **Risk:** `safe`.
- **Payoff:** Predictability more than speed. Variance between runs collapses, which makes every other
  measurement in this catalog more trustworthy.
- **Verify:** `numTasksPlanned` becomes stable across runs.
- **Again, the exception:** this does not move the data-table write stage's task count, which is
  bucket-shaped and follows the file groups being written.

## OBS-028 — Compaction target IO is at its default on a table it does not fit

- **Costs:** Each compaction round sweeps only part of the backlog, so a tail accumulates
  permanently. The table looks like compaction is running — because it is — while never catching up.
- **Signals:**
  - `props`: `hoodie.compaction.target.io` left at its default.
  - `eventlog.operationSummary[operation=COMPACTION]` present with
    `phaseSummary[phase=EXECUTION].stageWallClockMillis` large.
  - `health.checks[name=compaction].effectiveConfigs["observed.delta.commits.since.last.compaction"]`
    drifting up across runs despite compaction completing each time. **That drift, with compaction
    succeeding, is the whole signal.**
  - `layout.totalFiles` and `fileCountPerPartition` growing.
- **Corroboration:** `table` + `props` + `eventlog` — this is a three-input row, and it reads wrong
  with any two.
- **Change:** Raise `hoodie.compaction.target.io` **substantially** so one round sweeps most
  candidate file groups. Nudging it is the common mistake: if only half the file groups fit, half of
  the remainder still does not fit next round.
- **Risk:** `operational`. A larger round needs more executor capacity for the duration.
- **Payoff:** The backlog converges instead of drifting. Until it converges, every other MOR
  observation in this catalog is reading a moving target.
- **Verify:** Delta commits since last compaction stops drifting up and stabilises.

## OBS-029 — Source polling is pacing against a quota rather than the workload

- **Costs:** Throughput sits below what the cluster could do, and the ceiling is the source's
  per-shard request quota rather than anything about Hudi.
- **Signals:**
  - `eventlog.phaseSummary[phase=SOURCE_READ_AND_TRANSFORM].shareOfStageWallClock` large with low
    `inputBytes` on those stages.
  - `eventlog.stages[].observed.inputBytes` — the input side **does** report real numbers.
  - `props`: the source-specific throughput keys under `hoodie.streamer.source.*` for your connector.
- **Corroboration:** `eventlog` + `props`.
- **Change:** Counter-intuitively, **slow the poll** and **reduce** records per request step-wise
  until stable. Derive the target from the per-shard byte limit divided by average record size
  divided by polls per second. **Do not copy a number from another deployment** — the right value is
  payload-specific.
- **Risk:** `safe`.
- **Payoff:** Steadier throughput. Nothing, if the source is not the constraint.
- **Verify:** `inputBytes` per unit time rises; source-side throttling stops.
- **Verify the key actually took effect.** A misspelled Hudi config key produces **no error at all** —
  the default silently applies. Resolve the exact key for your source and release rather than
  assuming, and confirm it in the writer's resolved configuration.

## OBS-030 — Timeline-server markers are costing more than direct markers on a busy driver

- **Costs:** Every marker creation is an HTTP round trip to an endpoint on the Spark driver. On a
  saturated driver those calls queue, and the write stage waits on them.
- **Signals:**
  - `eventlog.phaseSummary[phase=MARKER_RECONCILIATION].shareOfStageWallClock` — on measured runs this
    sat at 0.012-0.036; materially above that is the signal.
  - `eventlog.stages[].observed.taskDurationMillis.max` on those stages, with a high `skewRatio` —
    tasks waiting on a queued endpoint rather than doing work.
  - `props`: `hoodie.write.markers.type`. **The engine-effective default is `TIMELINE_SERVER_BASED` on
    Spark** and `DIRECT` on Flink and Java — so a Spark user is on timeline-server markers by
    default, not by choice.
  - `props`: `hoodie.markers.timeline_server_based.batch.num_threads`,
    `hoodie.embed.timeline.server`.
- **Corroboration:** `eventlog` + `props`.
- **Change:** Either raise `hoodie.markers.timeline_server_based.batch.num_threads` to keep up on a
  busy table, or set `hoodie.write.markers.type=DIRECT` to remove the round trip entirely at the cost
  of more object-store writes. `DIRECT` is a reasonable standing choice on a busy driver, and on
  object storage with cheap writes it is often simply better.
- **Risk:** `safe`.
- **Payoff:** Removes marker latency from the write path. Small share, but it is latency on the
  critical path rather than throughput.
- **Verify:** `MARKER_RECONCILIATION` share falls and its stages stop showing high skew.
- **Check the ordering.** If the same table also has a large instant count (OBS-017) or a pinned
  timeline (OBS-020), marker pressure is a **symptom** of driver saturation and will clear when those
  are fixed. Treat it as primary only on an otherwise-healthy table.

---

## Adding a row

Give it the next free ID; never reuse or renumber. **Every signal must name a field that exists in
one of the three JSON outputs** — if you cannot write the path, you do not have a signal, you have an
intuition. State the corroborating inputs honestly; a one-input row does not belong here. Cite the
exact `hoodie.*` key and verify it against the source tree rather than from memory. If the only
remedy is `durable`, the row's "Change" is a handoff to `hudi-architect`, written as OBS-025 is.
