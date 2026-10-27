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

# Capability catalog

Seven rows that say **adopt this feature** — or, just as often, **do not**.

A different kind of claim from the observation catalog. A config observation says a lever is set
wrongly. A capability row says a mechanism the table is not using would suit this workload, and it
needs the workload's *shape* rather than one run's measurements. Payoff is sometimes 10x rather than
10-30%, and the risk is adoption cost rather than a slower job.

**This is the part documentation cannot do.** Any Hudi page can say "the record-level index gives
faster lookups". None of them can say *for your table, with your update pattern, at your size* —
that needs the three inputs, which is why they are mandatory here too.

## The discipline, restated for this catalog

**Every row must be able to say "don't".** A capability row with no counter-signals is marketing. The
counter-signals below are as measurable as the signals for, and when they fire the right answer is to
say so plainly and stop. A user who adopts the record-level index on a time-ordered append-only
table has paid metadata-table overhead for nothing, and will trust you less afterwards.

| Field | Meaning |
|---|---|
| **Preconditions** | Hard gates. If one fails, the row does not apply and no amount of signal changes that |
| **Signals FOR** | Measured fields in the three JSON outputs that argue for adoption |
| **Signals AGAINST** | Measured fields that argue against. Weighted equally |
| **Payoff** | What to expect, with the mechanism that produces it |
| **Adoption cost** | Bootstrap time, ongoing metadata-table overhead, operational burden |
| **Risk** | `safe` · `operational` · `durable` |
| **Verify** | What proves it worked |

### Risk tiers, and the refusal

Same three tiers as the observation catalog. The difference is that **this catalog contains `durable`
rows**, because the honest answer to some measurements is a change to what is on storage.

> **The agent must refuse to recommend a `durable` change as an optimization.** For CAP-001 and
> CAP-006 the output is a *report* — the measurements, what they imply, and a handoff to
> `hudi-architect` for an ADR. Not a config block, not a migration script, not "you could just set".
> This is structural: the rows are written as handoffs and say so in their Change field.

The reason is specific rather than squeamish: an index type or table type change is written into
`hoodie.properties` and reflected in what is on storage. Reverting means rewriting the table. An
optimization pass run against a live table is the wrong place for a decision with that blast radius,
however good the evidence.

## Index

| ID | Capability | Risk | One line |
|---|---|---|---|
| CAP-001 | SIMPLE/BLOOM → **record-level index** | `durable` | The flagship. Tagging cost stops scaling with table size |
| CAP-002 | → **bucket index** | `durable` | Predictable cardinality, no index lookup at all |
| CAP-003 | → **clustering** | `operational` | Consolidate small files, and give column stats something to prune on |
| CAP-004 | → **column and partition stats** | `operational` | Query pruning — but only with locality |
| CAP-005 | → **secondary index** | `operational` | Lookups on a non-key column |
| CAP-006 | CoW → **Merge-on-Read** | `durable` | Write amplification, traded for read-side merge |
| CAP-007 | → **metadata table** | `operational` | Stop listing storage. The prerequisite for most of the above |

---

## CAP-001 — SIMPLE or BLOOM index → record-level index

**The flagship row.** Read it carefully, including the counter-signals.

### Preconditions

- The metadata table is enabled and healthy. **The record-level index is a metadata-table
  partition** — without `hoodie.metadata.enable=true` it cannot exist. See CAP-007.
- The table has a meaningful rate of **updates**. An append-only table does not need an index at all;
  see "Signals AGAINST".
- The writer is on a release that has the index (0.14.0 for the global form).

### Start from the default, not from a choice

**`hoodie.index.type` defaults to `SIMPLE` on Spark** — the declared default is overridden per engine
in the index-config builder, so a Spark user who never set it is on `SIMPLE`. Word every
recommendation accordingly: *"you are on the default index; here is why the record-level index may
suit your workload"*. Never imply the user chose SIMPLE and chose wrong. They did not choose.

(The same per-engine override makes Flink default to `INMEMORY` and Java to `SIMPLE`. If you are
reading a declared default from documentation, you are reading the wrong number.)

### The mechanism, verified in source

`HoodieSimpleIndex.fetchRecordLocations` computes its parallelism as
`getParallelism(config.getSimpleIndexParallelism(), baseFiles.size())` and reads key columns from
each base file. **Tagging cost therefore scales with the number of base files in the table, not with
the size of the batch being written.** A 500-record update against a 200,000-file table does the same
index work as a 5-million-record update.

The record-level index is a key-to-location map in the metadata table. A lookup is a point read
against that map, so its cost scales with the **batch**. That difference — table-size-scaling versus
batch-scaling — is the whole argument, and it is why the benefit grows as the table does.

### Signals FOR

| Signal | Field | What it tells you |
|---|---|---|
| The premise | `props`: `hoodie.index.type` is `SIMPLE`, `GLOBAL_SIMPLE`, `BLOOM` or `GLOBAL_BLOOM` | Nothing to adopt otherwise |
| **The smoking gun** | `eventlog.stages[].observed.numTasksPlanned` on `DEDUP_AND_INDEX_TAGGING` stages ≈ `layout.totalFiles`, **not** the batch's record count | Cost is scaling with the table |
| Payoff size | `eventlog.phaseSummary[phase=DEDUP_AND_INDEX_TAGGING].shareOfStageWallClock` | How much of the run is in play |
| Which component | `eventlog.stages[].subPhase` is `bloomIndexLookup` (or the bucket has no RLI sub-phase) | Confirms an index lookup, not just profiling |
| Updates are real | `table` commit metadata: `numUpdateWrites` a meaningful share of `numInserts`, over several commits | An index earns its cost only against updates |
| Updates are scattered | Updated file groups spread across partitions and ages, over N commits | Scattered updates defeat key-range pruning |
| The table is large and growing | `layout.totalFiles`, `layout.totalBytes` rising | SIMPLE degrades with size; RLI does not |
| Prerequisite met | `layout.mdtEnabled` true, `health.checks[name=compaction]` on the metadata table healthy | The index lives there |

### Signals AGAINST — weighted equally

| Signal | Field | Why it argues against |
|---|---|---|
| **Append-only** | `numUpdateWrites` near zero across many commits | No lookups to accelerate. The index is pure write-side cost |
| **Time-ordered keys with recent-partition updates** | Updated file groups concentrated in the newest partitions; keys monotonic | BLOOM's key-range pruning is already near-optimal here. RLI adds metadata-table overhead for nothing — **this is the single most important counter-signal** |
| Small table | `layout.totalFiles` small; tagging share already low | The cost RLI removes is not being paid |
| Tagging share already small | `phaseSummary[DEDUP_AND_INDEX_TAGGING].shareOfStageWallClock` low | There is no headroom to recover |
| Metadata table unhealthy | `health.checks` on the metadata table showing a compaction backlog | Adding a partition to a struggling metadata table makes things worse |
| The bucket share is dedup, not index | `eventlog.stages[].bundledSteps` names deduplication | `BaseWriteHelper` leaves `deduplicateRecords` untagged, so dedup lands in this bucket. A large share may be dedup |
| Cardinality is predictable and fixed | — | CAP-002 may be the better answer: no index lookup at all |

### How to size it before recommending it

**Do not recommend the index without estimating what it will cost.** Hudi's own sizing logic
(`HoodieTableMetadataUtil.estimateFileGroupCount`) takes the record count, multiplies by a growth
factor, divides by the records that fit in one file group, and clamps to a configured range:

- `hoodie.metadata.record.index.growth.factor` — default `2.0`, the headroom multiplier
- `hoodie.metadata.record.index.max.filegroup.size` — default 1 GiB per file group
- `hoodie.metadata.global.record.level.index.min.filegroup.count` — default `10`
- `hoodie.metadata.global.record.level.index.max.filegroup.count` — default `10000`

Take the table's record count from `layout` with `--include-row-counts`, or from commit metadata, and
work it through. Report the result: *"your record count implies roughly N file groups; bootstrap
writes that index once over the whole table"*. A recommendation that carries its own cost estimate is
the difference between advice and a sales pitch.

If the estimate lands at the `max.filegroup.count` ceiling, say so — the index will be clamped and
each file group will exceed the target size, which makes metadata-table compaction heavier.

### Payoff

Index tagging stops scaling with table size. On the measured e2e runs, `recordIndexLookup` isolated
to **3.6 seconds** for the batch — and crucially, that number is a function of the batch, so it does
not grow as the table does. The gain is whatever `DEDUP_AND_INDEX_TAGGING` currently costs minus a
batch-scaled lookup, and it **compounds**: the comparison gets better every month the table grows.

The measurement that makes this row possible is recent. The record-level index lookup runs inside the
job Hudi describes as `Building workload profile:`, so its cost used to be pooled with profiling and
was not separable. `HoodieSparkEventLogAnalyzer` resolves it to `subPhase: "recordIndexLookup"` from
the stage name. **Without that sub-phase, this row would have no measurable before-and-after.**

### Adoption cost

- **Bootstrap.** The index is built over the whole table on first enable. On a large table this is a
  significant one-time job — size it with the file-group estimate above.
- **Ongoing metadata-table overhead.** Every commit writes record-index entries. This shows up in
  `phaseSummary[phase=METADATA_TABLE_WRITE]`, and partly in `UNKNOWN` under the streaming-write
  default (OBS-023).
- **Operational.** One more metadata-table partition that must stay compacted. The metadata table's
  own compaction cadence (`hoodie.metadata.compact.max.delta.commits`) now matters more.
- **Irreversible in practice.** The index type is table configuration and the index is on storage.

### Use the global key — and never recommend partitioned RLI for pruning

Two keys exist and they are **not** interchangeable:

- `hoodie.metadata.global.record.level.index.enable` (alternative:
  `hoodie.metadata.record.index.enable`) — **the global form. This is the one to use.**
- `hoodie.metadata.record.level.index.enable` — the **partitioned** form, where a partition-path plus
  record-key pair is unique across the table.

**File pruning with the partitioned form is not implemented.** The code path throws
`IllegalArgumentException: File pruning with partitioned rli has not yet been implemented`
(`HoodieBackedTableMetadata`). **Never recommend the partitioned form expecting pruning benefits.**
This was found the direct way during the e2e work: a run configured with the partitioned key died on
that exception.

### Risk

**`durable`.** `hoodie.index.type` changes what is on storage and how records map to file groups.

> **The agent does not recommend this change.** It reports: the measurements, the file-group
> estimate, the counter-signals that did and did not fire, and a handoff to `hudi-architect` for an
> ADR. The user makes the decision with the design skill, not with the optimizer.

The report should be good enough that the design conversation starts from numbers rather than from
first principles. That is this row's actual job.

### Verify (once adopted, by whoever adopted it)

- `eventlog.stages[].subPhase == "recordIndexLookup"` appears, and its total duration tracks **batch
  size** rather than `layout.totalFiles`. **That relationship is the proof**, not the absolute number.
- `phaseSummary[phase=DEDUP_AND_INDEX_TAGGING].shareOfStageWallClock` falls.
- `phaseSummary[phase=METADATA_TABLE_WRITE]` rises — this is the cost side, and it should be visible.
  If it is not, check whether `UNKNOWN` grew instead (the streaming-write caveat).
- Re-measure after the table has grown materially. The point of the change is that the cost stops
  growing; a single post-change measurement cannot show that.

---

## CAP-002 — → bucket index

### Preconditions

- Record-key cardinality is **predictable and reasonably stable**. This is a hard gate: bucket count
  is fixed at table creation for the simple engine and resizing is not free.
- The workload is upsert-heavy against a known key space.

### Signals FOR

| Signal | Field |
|---|---|
| Index tagging is a large share | `eventlog.phaseSummary[phase=DEDUP_AND_INDEX_TAGGING].shareOfStageWallClock` |
| Key space is stable | `table` commit metadata: records per commit steady across many commits |
| Updates dominate | `numUpdateWrites` high against `numInserts` |
| Multi-writer lock contention is the bottleneck | `props`: `hoodie.write.concurrency.mode` set, with long lock waits observed |

Bucket index computes the target file group from a hash of the key. **There is no index lookup at
all** — not a base-file scan, not a metadata-table read. That is why it can beat the record-level
index on write latency, and why it pairs well with non-blocking concurrency control on MOR.

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| **Cardinality is growing or unknown** | Records per commit trending up; table young | A fixed bucket count that was right at 1K file groups is wrong at 500K. The failure is gradual and hard to reverse |
| Partition sizes are very uneven | `layout.skew.cv`, `.gini`, `.largestPartitionShare` high | Buckets are per partition, so a skewed table gets skewed buckets |
| Updates are scattered across partitions and the key is not the partition key | — | The global form is more constrained; check it supports your access pattern first |
| Queries need point lookups on the key | — | Bucket index helps the **write** path. The record-level index also serves reads |

### Payoff

Index tagging cost goes to approximately zero on the write path. Largest where tagging currently
dominates and the key space is genuinely stable.

### Adoption cost

- Bucket count must be chosen up front: `hoodie.bucket.index.num.buckets`. Getting it wrong is
  expensive to correct.
- `hoodie.index.bucket.engine` selects `SIMPLE` (fixed) or `CONSISTENT_HASHING` (resizable, with its
  own operational surface — resizing is a table service).
- `hoodie.bucket.index.hash.field` if hashing on something other than the record key.
- Consistent hashing also changes which clustering strategy class is selected by default, so the
  clustering configuration interacts with this choice.

### Risk

**`durable`.** Index type and bucket count are on storage.

> **Handoff to `hudi-architect`.** Report the measurements and the cardinality evidence. Bucket count
> is exactly the kind of sizing decision that wants an ADR with a revisit condition attached.

### Verify

`DEDUP_AND_INDEX_TAGGING` share falls sharply. Watch `layout.skew` — bucket index can concentrate
writes if the hash field is skewed, and that shows up as partition-level skew.

---

## CAP-003 — → clustering

**The highest-value `operational` row in this catalog**, because it is the only one here that is both
reversible and frequently transformative.

### Preconditions

- The table has small files to consolidate, or queries that would prune with locality.
- There is capacity to run it — inline clustering pauses ingestion; async needs its own resources.

### Signals FOR

| Signal | Field |
|---|---|
| Small files | `layout.tableCharacteristics.detectors[name=small-files].status` is `MODERATE` or `SEVERE` — **read `observed.flagged.pct`, not the status alone** |
| File sizes under target | `layout.tableSizeStats.p50` well below `props`: `hoodie.parquet.max.file.size` |
| Micro-partitions | `detectors[name=micro-partition].effectiveConfigs["observed.size.rule.triggered"]` true |
| Nothing scheduled | `props`: `hoodie.clustering.inline` and `hoodie.clustering.async.enabled` both `false`; no clustering `.replacecommit` on the timeline |
| Column stats maintained without locality | `hoodie.metadata.index.column.stats.enable` effectively on (the Spark default) with `hoodie.clustering.plan.strategy.sort.columns` unset |
| File-group count growing | `layout.totalFiles` rising while `tableSizeStats.mean` flat |

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| Files already at target | `layout.tableSizeStats.p50` near `hoodie.parquet.max.file.size` | Nothing to consolidate |
| **Heavy update traffic on the same file groups** | `numUpdateWrites` high and concentrated | Clustering rewrites file groups that updates will immediately rewrite again. Wasted IO, and conflict risk with concurrent writers |
| A large backlog with no plan cap | Table old, `layout.totalFiles` very large, no clustering history | **Enabling clustering unbounded on a big backlog is how HUDI-RCA-022 happens.** Cap the first plan or do not start |
| Compaction already behind on MOR | `health.checks[name=compaction]` numbers drifting | Fix compaction first; two services competing for the same file groups helps neither |

### Payoff

File count falls, file sizes rise toward target, and — with `sort.columns` set — column stats become
selective so query pruning starts working. The second effect is often larger than the first and is
the one users do not anticipate.

### Adoption cost

- A rewrite of the file groups in each plan: real IO, real cluster time.
- Clustering produces `.replacecommit` instants; the timeline and the cleaner both see more work.
- With concurrent writers, clustering participates in conflict resolution. On a multi-writer table
  this needs thinking about rather than switching on.

### Configuration, with directions

| Key | Direction |
|---|---|
| `hoodie.clustering.inline` **or** `hoodie.clustering.async.enabled` | one of them on, not both |
| `hoodie.clustering.inline.max.commits` / `hoodie.clustering.async.max.commits` | set to your commit cadence |
| `hoodie.clustering.plan.strategy.max.num.groups` | **down** from default for the first rounds |
| `hoodie.clustering.plan.strategy.max.bytes.per.group` | sized to what one round can execute |
| `hoodie.clustering.plan.strategy.small.file.limit` | the size below which a file is a candidate |
| `hoodie.clustering.plan.strategy.target.file.max.bytes` | the size you want out |
| `hoodie.clustering.plan.strategy.sort.columns` | the columns queries filter on — this is what makes stats selective |
| `hoodie.clustering.plan.strategy.daybased.lookback.partitions` | on a date-partitioned table, bounds the candidate set |

### Risk

**`operational`.** Reversible by ceasing to schedule it, but the rewritten files stay. Start bounded.

### Verify

`layout.totalFiles` falls and `tableSizeStats.p50` rises round over round. In the event log,
`operationSummary[operation=CLUSTERING]` appears with `EXECUTION` dominating `PLANNING` — planning
dominating means the plan is too big for the round, and that is the early warning for OBS-018.

---

## CAP-004 — → column and partition statistics

### Preconditions

- The metadata table is enabled (CAP-007).
- **Queries filter on specific columns.** Without this, the row does not apply and no measurement
  changes that.

### Check what you already have before recommending anything

**On Spark, `hoodie.metadata.index.column.stats.enable` is effectively `true`** — the declared
default is `false` and the Spark builder overrides it. So the common case is not "adopt column
stats"; it is "you already have them and they are not pruning anything". That is OBS-013 and OBS-024,
not this row.

This row applies to a **Flink or Java-engine writer**, where the effective default is `false`, or to
a table where they were explicitly disabled.

### Signals FOR

| Signal | Field |
|---|---|
| Stats genuinely off | `props`: `hoodie.metadata.index.column.stats.enable` false, and no `column_stats` partition under the metadata table |
| Queries would prune | Query evidence — **you probably do not have it; ask** |
| Data has locality, or clustering can give it | `hoodie.clustering.plan.strategy.sort.columns` set, or clustering adoptable (CAP-003) |
| Many files per partition | `layout.fileCountPerPartition.mean` high — more files to prune from means more to gain |

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| **No locality and no plan to create it** | No clustering, no sort columns, random arrival order | Every file's min/max spans the full range. The index is maintained and prunes nothing — the most common way this disappoints |
| Queries do not filter | — | Pure write-side cost |
| Very wide schema with stats on every column | Schema column count high | Stats are written per column per file. Narrow it with `hoodie.metadata.index.column.stats.column.list` |
| Metadata-table write cost already high | `phaseSummary[phase=METADATA_TABLE_WRITE].shareOfStageWallClock` | You are adding to a cost that is already visible |

### Payoff

Files skipped at query planning time. Potentially large on a selective filter over sorted data;
**exactly zero** without locality. Be explicit about that conditional — it is the difference between
a useful recommendation and one that quietly fails.

### Adoption cost

- Stats computed and written on every commit, per column, per file.
- Readers must set `hoodie.enable.data.skipping=true` to consume them. **This is a read-side config
  and is frequently absent from the writer properties entirely** — say so rather than assuming it.
- `hoodie.metadata.index.partition.stats.enable` rides on column stats and has **no independent
  effect**; there is no standalone partition-stats switch.

### Risk

**`operational`.** The index partition is built on enable and deleted on disable.

### Verify

A filtered query reads fewer files. **Without a before-and-after query plan this is unverified** —
say that rather than claiming success from the write side.

---

## CAP-005 — → secondary index

### Preconditions

- The metadata table is enabled, **and the record-level index is enabled** — a secondary index maps a
  non-key column value to record keys, so it is built on top of the primary key index. Without it,
  there is nothing to map to.
- A specific non-key column is being looked up repeatedly.
- Hudi 1.0.0 or later.

### Signals FOR

| Signal | Field |
|---|---|
| Record-level index present | `props`: `hoodie.metadata.global.record.level.index.enable` true; the index partition exists |
| A known non-key lookup column | Query evidence — **ask for it; this row cannot be derived from write-side telemetry** |
| Queries on that column scan broadly | Query evidence |

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| No record-level index | `props` | Precondition fails. CAP-001 comes first, and it is `durable` |
| Low-cardinality column | — | A near-constant column maps to almost every record; the index prunes nothing |
| Column stats already prune it well | — | Cheaper mechanism, already in place |
| Metadata-table write cost already high | `phaseSummary[phase=METADATA_TABLE_WRITE]` | Another partition on every commit |

### Payoff

Point lookups on the indexed column stop scanning. Depends entirely on cardinality and query
frequency — both of which are query-side facts you must **ask for** rather than infer.

### Adoption cost

- Another metadata-table partition, written on every commit.
- The index is created through `CREATE INDEX` in Spark SQL per column, not by a single config.
  `hoodie.metadata.index.secondary.enable` is the master switch that keeps them consistent; on Spark
  its effective default is already `true`, so the switch is usually not the missing piece — the
  `CREATE INDEX` statement is.
- Dropping it is `DROP INDEX`.

### Risk

**`operational`.**

### Verify

Query-side. `phaseSummary[phase=METADATA_TABLE_WRITE]` rising is the cost side, visible from the
write path; the benefit is not, so do not claim it from write-side evidence.

---

## CAP-006 — Copy-on-Write → Merge-on-Read

### Preconditions

- Write amplification on CoW is measurably costing something. This row is **not** a default upgrade
  path; most tables are correctly CoW.

### Signals FOR

| Signal | Field |
|---|---|
| Write phase dominates | `eventlog.phaseSummary[phase=DATA_TABLE_WRITE].shareOfStageWallClock` high |
| Small updates rewriting large files | `table`: `numUpdateWrites` small per commit while bytes written is large — **the write-amplification ratio, and the core signal** |
| Ingestion latency is the binding constraint | — a product fact, so **ask** |
| Updates scattered across many file groups | Updated file groups spread wide per commit |

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| **Append-only** | `numUpdateWrites` near zero | MOR solves write amplification from updates. With no updates there is none, and you have added read-side merge for nothing — **this is OBS-025 read in reverse** |
| Read latency matters more than write latency | — | MOR trades write cost for read-side merge |
| No capacity to run compaction | `health.checks[name=compaction]` would be a new obligation | **MOR without working compaction degrades without bound.** This is the most common MOR regret |
| Updates concentrated in few file groups | | CoW rewrite cost is already small |
| Non-partitioned table | `layout.numPartitions` is 1 or absent | A non-partitioned MOR table is pathological for the listing-based rollback path and the whole-table file-system view (HUDI-RCA-021) |

### Payoff

Write amplification falls — updates append to log files instead of rewriting base files. Paid back on
the read side, and in compaction cost.

### Adoption cost

- **Compaction becomes mandatory.** Cadence, target IO, and monitoring are all new obligations.
- Readers see merge cost on uncompacted slices; snapshot queries get slower.
- Log files are not counted by the layout analyzer's size statistics, so your layout visibility gets
  slightly worse — a small but real operational cost.
- More table-service surface overall.

### Risk

**`durable`.** Table type is written into `hoodie.properties` and determines what is on storage.

> **The agent does not recommend this change.** Report the write-amplification measurement, the
> counter-signals, and the compaction obligation, and hand off to `hudi-architect` for an ADR. A
> table-type change on a live table is a design decision with a migration, not an optimization.

### Verify

Not applicable here. If a user has already made the change, `DATA_TABLE_WRITE` share should fall and
a `COMPACTION` operation should appear in `operationSummary[]` — and OBS-015 and OBS-028 become the
rows that matter.

---

## CAP-007 — → metadata table

Listed last because it is a **prerequisite** for CAP-001, CAP-004 and CAP-005 rather than a
destination of its own.

### Preconditions

None beyond a writable table.

### Check the engine default first

`hoodie.metadata.enable` is effectively **`true` on Spark and Flink** and **`false` on the Java
engine**. On Spark this row almost never applies — the metadata table is already there. Confirm from
`layout.mdtEnabled` rather than from the declared default.

### Signals FOR

| Signal | Field |
|---|---|
| Not enabled | `props`: `hoodie.metadata.enable` false; `layout.mdtEnabled` false |
| Listing cost visible | `eventlog.stages[].jobDescription` containing `Parallel listing paths`, with real `observed.durationMillis` |
| Many partitions | `layout.numPartitions` high — listing cost scales with it |
| A capability above is wanted | CAP-001, CAP-004 or CAP-005 is the actual goal |

### Signals AGAINST

| Signal | Field | Why |
|---|---|---|
| Small or unpartitioned table | `layout.numPartitions` small, `layout.totalFiles` small | **Metadata-table write overhead can exceed the listing it saves.** The clearest "don't" in this catalog |
| Very high commit rate with tiny commits | `table`: many commits, few records each | Metadata-table write cost is per commit |
| No capacity for another compacting service | — | The metadata table compacts itself and that cadence must be managed |

### Payoff

File listing becomes a metadata-table read instead of a storage listing. Grows with partition count
and with object-store listing latency; negligible on a small local table.

### Adoption cost

- One-time bootstrap over the existing table.
- Per-commit write cost — and on Spark, much of it is **unattributable** in the event log because
  `hoodie.metadata.streaming.write.enabled` defaults to `true`. Expect `UNKNOWN` to grow, and say
  that is expected rather than treating it as a measurement failure.
- The metadata table has its own timeline, its own compaction, and its own health
  (`hoodie.metadata.compact.max.delta.commits`).

### Risk

**`operational`.** Bootstrapped on enable; the partitions are removed on disable.

### Verify

`layout.mdtEnabled` reads true. `Parallel listing paths` stages against the data table disappear.
`phaseSummary[phase=METADATA_TABLE_WRITE]` appears as the cost — and if it does not appear while
`UNKNOWN` grows, that is the streaming-write caveat, not an error.

---

## Adding a row

Give it the next free ID. **A row without measurable counter-signals does not belong here** — if you
cannot state what would make the answer "don't", you have not finished the row. Name the exact JSON
field for every signal. Verify every `hoodie.*` key against the source tree, and check whether its
effective default is overridden per engine before citing a default at all. If the change is
`durable`, write the Change field as a handoff and say the agent refuses to recommend it.
