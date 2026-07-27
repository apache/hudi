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
# Decision tables

Reference for each decision domain. Consult when deriving a design choice from workload answers.

## Engine

Ask, don't default. If user picks Spark or Flink, proceed. If undecided:

**Flink candidate:** append-only workloads with sub-5-minute visibility target AND continuous streaming source.

**Spark default:** everything else.

For mutable workloads at 5-minute visibility, Spark handles cleanly — Flink's advantage doesn't apply.

## Writer

Derived from source + pipeline_shape (see question-flow.md Q2.9).

### Kafka source (special case)

**Default: HoodieStreamer.** Rationale (surface these in dialogue if user asks why):
- Schema registry integration (Confluent + custom).
- Format support built-in (AvroKafkaSource, JsonKafkaSource, ProtoKafkaSource).
- Exactly-once from Kafka (checkpoint stored in Hudi commits).
- Kafka meta fields propagation.
- Error table for dead-letter routing.
- Continuous mode: ingestion + async compaction + async clustering in one Spark job.
- Transformer chain (SQL-based, custom-class, chained) handles most enrichment and CDC-mapping.

### Kafka source class + schema provider

Derived from the record format answered at Q1.2b. Required — without it the writer decision is incomplete and the bundle cannot name a source class.

| Record format | Source class | Schema provider |
|---|---|---|
| Avro + schema registry | `AvroKafkaSource` | `SchemaRegistryProvider` |
| Avro + schema file | `AvroKafkaSource` | `FilebasedSchemaProvider` |
| JSON | `JsonKafkaSource` | Optional — file-based or registry if schema is managed |
| Protobuf | `ProtoKafkaSource` | Proto class on classpath |

Avro on Kafka effectively requires a schema provider. For JSON it is optional but recommended in production — inferred schemas drift silently.

**Reach for Spark DataSource for Kafka only when:**
- Multi-source complexity (multiple Kafka topics + JDBC lookup + multi-table writes).
- ML DataFrame-native library work.
- One-off backfills where a HoodieStreamer job feels heavier than needed.

### In-job DataFrame (derived / silver / gold tables)

When the data is already a DataFrame inside the user's own Spark job — the output of an ETL query, join, or aggregation — the write is `.write.format("hudi")` at the end of that job.

**Writer: Spark DataSource. Not a choice.** HoodieStreamer exists to poll an external source; here there isn't one. Don't present the HoodieStreamer-vs-DataSource tradeoff, and don't ask Q1.2b (no source class, no schema provider).

Consequences to surface:

- **Async compaction isn't free.** HoodieStreamer continuous mode runs compaction in-process; DataSource can't. If the design lands on MOR, the options are inline compaction (blocks commits periodically) or a standalone `HoodieCompactor` job.
- **The standalone compactor makes this a concurrent-writer deployment** — two processes writing one table. Requires OCC and an identically-configured lock provider in both jobs. See warnings.md → COMPACTOR_CONCURRENCY_REQUIRED.
- **No submit-command template applies.** The write lives in the user's application; emit the properties as `.option(...)` calls plus the required session config instead.
- **Q2.9 (pipeline shape) is largely answered already** — it's custom application code. Still worth confirming whether the write sits inside a `forEachBatch` callback (continuous) or a plain batch job, because that changes the integration snippet, not the writer.

### Non-Kafka external sources (DFS, JDBC, another Hudi table, S3/GCS events, Kinesis, Pulsar)

HoodieStreamer and DataSource are co-equal defaults. Choose based on pipeline_shape.

**Schema provider is optional and orthogonal to source type.** Any provider can pair with any source. Many sources infer their schema, and simple pipelines with no expected evolution commonly set none — that's a valid configuration, not a gap. Ask Q1.2c and map the answer: schema files → `FilebasedSchemaProvider`, Hive metastore → `HiveSchemaProvider`, upstream DB → `JdbcbasedSchemaProvider`, registry → `SchemaRegistryProvider`. See config-templates.md for keys.

**Kafka differs in prior, not in mechanism.** Producers and consumers on a topic already need schema coordination, so a registry is usually in place before Hudi arrives. For Kafka, assume a provider exists and ask which one (Q1.2b already does this). For other sources, whether to have one at all is a real question.

```
if pipeline_shape == "config-driven":
  → HoodieStreamer
  mode = "continuous" if continuous ingest declared else "run-once"

elif pipeline_shape == "custom code":
  → Spark DataSource

elif pipeline_shape == "SQL-centric":
  → Spark SQL

elif pipeline_shape == "streaming with primitives":
  Ask: writeStream sink vs forEachBatch
  - forEachBatch → Spark DataSource
  - writeStream sink → Ask: stateful primitives needed?
    - Yes → Spark Structured Streaming
    - No → nudge toward HoodieStreamer
```

### Popularity as battle-tested signal

HoodieStreamer + Spark DataSource are the two most-deployed Hudi writer paths — battle-tested, no tuning knobs required, just works out of the box.

Spark SQL: niche, only when user has SQL-only requirement.
Spark Structured Streaming (writeStream sink): rare, only when company-wide streaming framework OR genuine stateful primitives.

For first-time users on Kafka: HoodieStreamer is the safest first Hudi table. For non-Kafka sources: either HoodieStreamer or DataSource is fine.

## Table type

Derived from mutability + experience + update distribution (for mutable).

| Signals | Derived table type + compaction |
|---|---|
| Immutable | COW — silent. Don't show the COW/MOR tradeoff table, and don't ask Q1.6 (experience): with no updates there are no log files to merge, so MOR offers nothing and there's no compaction posture to derive. |
| Mutable + first-time / fire-and-forget | COW |
| Mutable + some experience | MOR + inline compaction |
| Mutable + experienced + writer is HoodieStreamer continuous | MOR + async (free) |
| Mutable + experienced + writer is DataSource/SQL/Structured Streaming | MOR + async via advanced deployment (standalone compactor) |
| Mutable + some experience + writer is HoodieStreamer continuous | MOR + async (upgrade for free — bonus from writer choice) |

**Apply the free-async upgrade as soon as the writer is known — do not defer it to the §8.3 checkpoint.**

For Kafka sources the writer is HoodieStreamer by derivation, so this resolves during Round 1 and the upgraded table type should be stated there. Deferring it to §8.3 would hide the upgrade from PROTOTYPING and PRODUCTIONIZING_INITIAL users entirely, since that checkpoint only runs between Rounds 2 and 3 at PRODUCTION_AT_SCALE.

Table type stays genuinely provisional only when the writer is still unknown — non-Kafka sources whose writer is decided by pipeline shape at Q2.9. In that case, revisit table type once Q2.9 lands and surface the upgrade then.

Tradeoff table (in ADR):

|                    | Copy-on-Write (CoW)                     | Merge-on-Read (MoR)                                                              |
| ------------------ | --------------------------------------- | -------------------------------------------------------------------------------- |
| **Write cost**     | High — rewrites whole base files        | Low — appends log blocks                                                         |
| **Read latency**   | Low — reads are plain parquet           | Snapshot reads merge base + logs; read-optimized reads skip logs; compaction periodically brings MoR in line with CoW |
| **Ops surface**    | Minimal — no compaction to run          | Compaction runs as an ongoing service                                            |
| **Typical fit**    | Batch BI, reference tables, first-time users | Streaming upserts, CDC ingestion, experienced operators                     |

### Handling workload-vs-experience tension

If mutability + update distribution point toward MOR but user picked fire-and-forget:

> "Your workload signals point toward MOR (mutable + uniform updates at scale = high write amp for COW), but you picked fire-and-forget which typically means COW. Three reconciliations:
> - (a) Accept COW; ADR flags concrete revisit conditions if write amp materializes.
> - (b) Step up to MOR with inline compaction. Slightly more per-batch latency, no separate service to deploy.
> - (c) Keep the workload smaller and rely on the Operations Agent to flag if COW hits a wall.
>
> Which matches your priorities?"

## Index

Derived from six signals: engine, mutability, partitioning, partition-column stability, projected table size, key characteristics.

### Decision table

| Index | Write cost | Storage | Scope | Best when | Engine (1.2.0) |
|---|---|---|---|---|---|
| **SIMPLE / Global SIMPLE** | O(files listed) per commit | Minimal | Partition or Global | Small tables (<~100M rows), random updates | Spark |
| **BLOOM / Global BLOOM** | Range prune + bloom check | Bloom filters in MDT | Partition or Global | Sub-1-2TB + monotonic keys | Spark |
| **Partitioned RLI** | O(1) via MDT hash-shard | ~few % of record count in MDT | Partition (uniqueness within partition) | Any real scale, partition-stable | Spark + Flink |
| **Global RLI** | O(1) via MDT hash-shard | ~few % of record count in MDT | Global (table-wide uniqueness) | Any scale, unpartitioned or partition-unstable | Spark + Flink |
| **BUCKET** | O(1) via bucket hash | No MDT partition | Partition-scoped only | Bounded key cardinality + balanced partition sizes | Spark + Flink (Flink dominant) |

### Decision pseudocode

```
if immutable:
  → SIMPLE (index cost irrelevant; no tagging happens)

elif engine == "flink":
  if unpartitioned or partition-unstable:
    → GLOBAL_RLI (added in 1.2.0)
  elif partition-stable + key_cardinality_bounded + partition_sizes_balanced:
    → BUCKET
  else:
    → PARTITIONED_RLI

elif engine == "spark":
  if unpartitioned:
    if key_cardinality_bounded_and_stable:
      → BUCKET
    else:
      → GLOBAL_RLI
  elif partitioned + partition-unstable:
    → GLOBAL_RLI (or GLOBAL_BLOOM if sub-1-2TB + monotonic + cost-sensitive)
  elif partitioned + partition-stable:
    if projected_table_size < ~1-2TB:
      if monotonic_keys: → BLOOM
      elif key_cardinality_bounded_and_stable + partition_sizes_balanced: → BUCKET
      else: → SIMPLE (or PARTITIONED_RLI)
    else (projected >= ~1-2TB):
      if key_cardinality_bounded_and_stable + partition_sizes_balanced: → BUCKET
      else: → PARTITIONED_RLI
```

### BUCKET when to prefer over RLI

- Bounded and predictable key cardinality.
- Partition sizes roughly balanced.
- Writer latency tight, MDT record_index sync cost matters.
- Smaller MDT footprint desired.

### BUCKET fails when

- Key cardinality unbounded or growing (any new-record-generating workload — trips, events, orders, logs).
- Skewed partition sizes → recommend RLI/Partitioned RLI (do NOT recommend CONSISTENT_HASHING at design time; niche escape hatch).

### BLOOM caveats

- Effective sub-1-2TB only.
- `hoodie.bloom.index.use.metadata=true` is experimental at 1.2.0 — do NOT recommend at design time.

### Async-buildable framing

Most index decisions are no longer durable at table creation. RECORD_INDEX (both variants), BLOOM (experimental), col stats, secondary index, expression index — all buildable async on live tables using HoodieIndexer, no rewrite needed.

**Two durability exceptions:**

1. **BUCKET** — bucket count fixed at creation.
2. **RLI file-group count** — fixed when the record index is *initialized*. Adding an RLI later is free; **resizing an initialized one is not possible without a table rewrite.** See "RLI file-group sizing" below.

The distinction matters: "index is async-buildable" is true about *adding* an index and false about *resizing* one. Do not tell users the RLI is a fully reversible choice.

Design implication: for smaller mutable tables, recommend lighter index (SIMPLE) with ADR note that RLI can be added later without rewrite. Avoid over-engineering.

### RLI file-group sizing (PRODUCTION_AT_SCALE — durable decision)

Hudi derives the RLI file-group count when the index is initialized, from the records present at that moment multiplied by a growth factor (`hoodie.metadata.record.index.growth.factor`, default **2.0**). That assumes the table is already fully loaded. A table that bootstraps small and then grows repeatedly gets an index sized for its infancy, permanently.

**The count is pinned only when `min == max` (both non-zero).** Otherwise Hudi estimates from record count × growth factor and clamps into the min/max window — i.e. a range hands the decision back to Hudi.

**Config keys — four of them, two per variant. Use the modern keys:**

| Index | Key | Default | Scope |
|---|---|---|---|
| Global RLI | `hoodie.metadata.global.record.level.index.{min,max}.filegroup.count` | 10 / 10000 | Table-wide |
| Partitioned RLI | `hoodie.metadata.record.level.index.{min,max}.filegroup.count` | 1 / 10 | **Per partition** |

`hoodie.metadata.record.index.{min,max}.filegroup.count` are deprecated aliases for the **global** properties. Do not use them for partitioned RLI.

**Sizing formula:**

```
bytes_per_rli_record = 50      # safe starting point; assumes UUID-shaped record keys
shard_size_mb        = 500
file_group_count     = (projected_record_count * bytes_per_rli_record) / 1024 / 1024 / shard_size_mb
```

- **Global RLI** — apply to the projected table-wide record count.
- **Partitioned RLI** — apply to the projected *per-partition* record count (projected total / projected active partition count), because the config is per partition.
- Size for the 3-4 year projection, not today. Under-sizing is unfixable; over-sizing costs little.
- 50 bytes assumes UUID-shaped keys. Longer record keys mean a larger per-record RLI footprint — scale the constant and say so in the ADR.

Worked example: 40B projected records → `(40_000_000_000 × 50) / 1024 / 1024 / 500` = **3815 file groups**; round up to **3900** for headroom (over-sizing costs little). Emit `min = max = 3900`.

**Why the defaults are a trap.** Partitioned RLI defaults to `min=1, max=10`. With a 1GB max file-group size and 50-byte records, one file group holds ~21.5M records, so a partition needs ~215M projected records before the estimate even reaches the ceiling of 10. Below that it silently under-sizes.

**When the user cannot project record count:**

Recommend they land the **first commit / bulk load with RLI disabled**, then enable it (async build via `HoodieIndexer`, no rewrite). The estimator then sees a truthful record count instead of a near-empty first commit.

This fixes the *bootstrap* problem, not the *growth* problem — the estimator still applies growth factor 2.0 to whatever exists at initialization, so it sizes for today × 2. For a fast-growing table that headroom is consumed quickly and the count is already frozen. Consider raising `hoodie.metadata.record.index.growth.factor` above 2.0, and record a measurable revisit condition: if record count approaches `initial_count × growth_factor`, the RLI is undersized and only a rewrite fixes it.

## Partitioning

Query-alignment-first, not size-first.

### Rule engine flow

1. If consumer reads filter on natural low-cardinality dimension → partition by that dimension.
2. If consumer reads filter on time (recent-N-day scans, incremental) → partition by date.
3. If consumer reads are scan-heavy or point-lookup (no partition-aligned filter) → consider unpartitioned (subject to size threshold).

### Projected partition count guardrails

Formula: `projected_partition_count = cardinality(business_dimension) × time_buckets_over_table_lifetime`

Time buckets accumulate over the table's **lifetime**, projected to the 2-3 year horizon — not over the retention window. Retention governs timeline lookback (see → Retention), not how many date partitions exist on disk; date partitions are only ever added, never expired by the cleaner.

For date-only: `cardinality = 1`, `time_buckets = days (or months) from first commit to the projection horizon` (daily × 3 years ≈ 1095 — matches the ADR example).
For composite `<business_dim>/<date>`: multiply.

- **Green: < 10K partitions** — proceed.
- **Yellow: 10K – 50K** — warn (see warnings.md → PROJECTED_PARTITION_COUNT_YELLOW).
- **Red: > 50K** — reject (see warnings.md → PROJECTED_PARTITION_COUNT_RED).

### Time granularity default

- **Daily** — default. Recent-N-day read patterns align.
- **Monthly** — when daily pushes into yellow/red.
- **Hourly** — rarely recommended. Only when volume >~10GB/hour and consumers explicitly need hourly pruning.

### Immutable raw layer

Default to **ingestion-time partitioning**, not event-time. Raw layer consumers ask "give me new data in the last N hours" — ingestion-time question. Raw doesn't apply business logic.

Override to event time if:
- User explicitly names event-time-filtered downstream reads as dominant.
- Raw layer is unusual with strong event-time semantic upstream.

### Unpartitioned viability

Viable when both hold:
- Total table stays under ~500GB at 2-3 year horizon.
- Consumer read pattern is point-lookup / join / full-scan (not filtered on natural partition dimension).

For point-lookup-dominated workloads with growing key set (like unpartitioned DIM tables), unpartitioned + Global RLI works up to larger sizes (~2TB+) because RLI keeps lookup cost bounded.

Above threshold → partition, even if no natural business filter. Fallback: partition by date-derived column with daily granularity.

## Small-files posture (immutable only)

Three postures — user picks (see question-flow.md Q2.8).

### Recommendation prose adapts to two axes

**Partition cardinality:**
- Low-card (date-only) → any posture viable.
- High-card (composite business dim) → posture (c) recommended.

**Future-consumers axis:**
- Closed universe (all silver consumers exist today) → any posture viable.
- Open universe (new silver pipelines may spin up 6+ months later) + terabytes → (b) or (c) required; (a) becomes warning.

### Matrix

| Scenario | Recommended posture |
|---|---|
| Low-cardinality partition + closed-universe + <500GB | (a) or (b) viable |
| Low-cardinality partition + open-universe or terabytes | (b) — clustering handles async |
| High-cardinality partition | (c) — every batch fans across many partitions; inline small-file handling per file group pays off |

## Retention

Time-travel + incremental lookback window. NOT record lifetime.

### Cleaner policy selection

- Continuous ingest → `KEEP_LATEST_BY_HOURS`.
- Scheduled batch → `KEEP_LATEST_COMMITS`.
- **NEVER `KEEP_LATEST_FILE_VERSIONS`** — operates at file-group level, savepoint interaction awkward, archival can't make progress cleanly.

### Commit-cadence-aware retention default

Timeline latency degrades past ~5K entries; practical target ~1000.

Formula (COW baseline):
```
base_entries_per_commit = 6  # 3 ingestion + 3 cleaner
if MOR + async compaction: adjust += 3 / compaction_cadence_commits  # typically +0.6
if async clustering: adjust += 3 / clustering_cadence_commits  # typically +0.6
entries_per_commit = base_entries_per_commit + adjust

commits_per_day = 1440 / commit_cadence_minutes
timeline_entries_per_day = commits_per_day * entries_per_commit
```

### Safe defaults by commit cadence (COW baseline, cleaner retained = 500)

| Commit cadence | Safe max retention | Wall-clock lookback |
|---|---|---|
| 5 min | ~500 commits | ~1.7 days |
| 10 min | ~500 commits | ~3.5 days |
| 15 min | ~500 commits | ~5 days |
| 30 min | ~500 commits | ~10 days |
| 60 min | ~500 commits | ~20 days |

### Sub-5-minute cadence

If computed safe retention < 1 day (e.g., 1-min cadence):

> "At 1-minute cadence, safe retention drops below 1 day. As a best practice, stabilize a 5-minute cadence pipeline first before attempting sub-5-min ingest."

Not a hard block — user can proceed.

## Cleaner + archival config (inline autopilot)

Emit silently. No user question about cadence.

```
# Automatic inline cleaning and archival are Hudi defaults — emit no on/off switches.
# (hoodie.clean.automatic, hoodie.clean.async.enabled, hoodie.archive.automatic,
#  hoodie.archive.async, hoodie.commits.archival.batch all already default to the desired values.)
hoodie.clean.policy=<KEEP_LATEST_BY_HOURS or KEEP_LATEST_COMMITS>
hoodie.clean.hours.retained OR hoodie.clean.commits.retained=<derived>

hoodie.keep.min.commits=<derived — see below>
hoodie.keep.max.commits=<derived — see below>
```

**Archival window derivation — from commit cadence, never a constant.**

Archival must outlast the cleaner. If instants are archived while the cleaner still treats those file versions as live, incremental and time-travel readers lose the timeline entries they depend on. So the window is always the cleaner window plus a margin.

```
commits_per_day  = 1440 / commit_cadence_minutes
cleaner_commits  = commits_per_day × cleaner_retention_days
                   (or cleaner.commits.retained directly, when the policy is KEEP_LATEST_COMMITS)

keep.min.commits = max(100, ceil(cleaner_commits × 1.1))    # cleaner window + ~10% margin
keep.max.commits = ceil(keep.min.commits × 1.2)
```

**At daily cadence or slower, emit nothing.** Don't set cleaner or archival config at all — leave Hudi's out-of-the-box defaults in place. A table committing once a day accumulates timeline entries so slowly that the active timeline is nowhere near its limits, and there is nothing for us to protect it from. Overriding here adds config surface for no benefit.

The floor of 100 covers the middle ground, where derivation produces a number small enough to make look-back impractical but the cadence is still fast enough that the defaults aren't a good fit.

Worked values at a 48h cleaner window:

| Cadence | commits/day | Cleaner commits | +10% | `keep.min.commits` | `keep.max.commits` |
|---|---|---|---|---|---|
| 5 min | 288 | 576 | 634 | **634** | 761 |
| 15 min | 96 | 192 | 211 | **211** | 253 |
| 1 hour | 24 | 48 | 53 | **100** (floor) | 120 |
| Daily or slower | ≤1 | — | — | **emit nothing** | **emit nothing** |

This replaces the older `2 × cleaner.commits.retained` rule. The +10% relationship applies uniformly, whichever cleaner policy is in force.

Same principle as not emitting `hoodie.metadata.index.column.stats.enable=false`: config that restates a default is noise. Emit what changes behavior.

**Never emit a fixed 1000 / 1200.** The active-timeline target is ~1000 entries; an archival floor at 1000 leaves no headroom above the number it exists to protect.

**Bucketization note:** archival bucketizes by instant type — ingestion commits and table-service commits (clean, compaction, clustering, rollback) are tracked separately with their own thresholds. `keep.min.commits` governs the ingestion bucket. The ~6-entries-per-commit figure behind the active-timeline math is the combined count across buckets, which is why the window can be sized off ingestion commits without the timeline overrunning.

**Archival bucketization:** archival bucketizes by instant type. Two buckets: ingestion commits and table-service commits. Each has its own min/max threshold. 2x ratio holds because per-bucket accounting keeps combined active timeline bounded.

## Compaction (MOR only)

Derived from writer + experience.

| Writer | Compaction mode |
|---|---|
| HoodieStreamer continuous | Async in-process, automatic. **No config emitted.** |
| Spark Structured Streaming (writeStream sink) | Inline default; async via `hoodie.datasource.compaction.async.enable=true` if experienced. |
| Spark DataSource | Inline default. Async requires standalone `HoodieCompactor` (advanced deployment). |
| Spark SQL | Same as DataSource. |

For inline:
```
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5
hoodie.compact.inline.trigger.strategy=NUM_COMMITS
```

### Compaction target IO trap

`hoodie.compaction.target.io` defaults to **500GB per round**. At TB-scale MOR, file groups accumulate uncompacted → log files grow forever → read latency degrades.

If projected size ≥ 1TB with MOR → surface ADR flag: "Bump `hoodie.compaction.target.io` to 2-5TB."

## Clustering

Off by default. Fires only when user asks or when workload signals strongly suggest benefit.

**When Architect surfaces clustering:**
- Immutable + small-files posture (b) — clustering is on the path by choice.
- MOR + async services + workload signals suggest fragmentation over time.

**When enabled:**
```
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=5
hoodie.clustering.plan.strategy.small.file.limit=300MB
hoodie.clustering.plan.strategy.target.file.max.bytes=1GB
hoodie.table.services.incremental.enabled=true  # 1.2.0 win
```

## Meta-fields

For mutable: silent default — keep all meta fields. No user question.

For immutable + record size ≤1KB: prompt (see question-flow.md Q2.7).

Rule engine mapping:

**At 1.2.0 the rule is simple: any incremental or CDC consumer → keep all meta fields.** Record size doesn't change this. The config is all-or-nothing and incremental queries require `_hoodie_commit_time`, so there is no middle option to trade storage against.

| Record size | Incremental / CDC needed? | Recommendation |
|---|---|---|
| any | **Yes** | **Keep all meta fields — no alternative at 1.2.0.** Note the storage cost in the ADR. |
| >1KB or unknown | No | Keep all — overhead is rounding error |
| 200B–1KB | No | Keep all — saving is marginal at this size |
| <200B | No | Offer disable-entirely |

Only the last row is a real decision. Everything else is determined.

### Selective mode is not available at 1.2.0

At **1.2.0**, `hoodie.populate.meta.fields` is a boolean — all meta fields or none. Do not offer a selective option; there is no config to emit for it.

**Coming after 1.2.0** (apache/hudi#19205, targeting 1.3.0, not yet merged): `hoodie.meta.fields.mode`, a comma-separated list of meta columns to populate when `hoodie.populate.meta.fields=false`. Allowed tokens are `_hoodie_commit_time` and `_hoodie_file_name`.

| `populate.meta.fields` | `meta.fields.mode` | Effective mode |
|---|---|---|
| `true` (default) | ignored | ALL |
| `false` | empty | NONE |
| `false` | `_hoodie_commit_time` | COMMIT_TIME_ONLY |
| `false` | `_hoodie_file_name` | FILE_NAME_ONLY |
| `false` | both | COMMIT_TIME_AND_FILE_NAME |
| `true` | non-empty | rejected at writer init |

When that lands, COMMIT_TIME_ONLY becomes the balanced middle for small-record append-only tables — it preserves incremental queries while dropping the other four fields. Constraints to respect when adding it: **CoW only** (MoR rejected at writer init pending a follow-up), **Spark only** (Flink RowData and Java client rejected), and **immutable at table creation** (settable only at init, via hudi-cli, or during upgrade). Pre-1.3.0 readers see such a table as NONE.

Until it ships in a release the Skill targets, the choice remains binary.

### What disabling actually costs

Hudi's config documentation: *"When disabled, no meta fields are populated and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing."*

Treat incremental queries as **unavailable**, not degraded. An earlier version of this file claimed they fall back to a slower snapshot-read + filter path and remain "still functional" — that is wrong, and it is the kind of error that produces a table needing a rewrite to fix.

**Hard gate:** if any consumer is incremental or streaming, don't offer the disable option. Keep meta fields and record the storage cost in the ADR.

**Framing:** state the cost plainly. Disabling is a legitimate choice for append-only batch data with a closed, non-incremental consumer set — and a trap for anything else.

### Mutual exclusion with auto-gen

Auto-gen keys require `_hoodie_record_key` materialized. Two coherent immutable presets:
- User-provided natural key + disable meta fields entirely → max storage saving.
- Auto-gen key + keep meta fields → efficient ingest, no stable identity.

## Read behavior

Rule engine mapping from consumer-read-pattern answer to Hudi query type:

| Consumer behavior | Hudi query type |
|---|---|
| Bulk analytical | Snapshot |
| Targeted lookups on record key | Snapshot with RLI-driven file skipping |
| Targeted lookups on non-key column | Snapshot with secondary index (surfaces as ADR flag) |
| Streaming / incremental | Incremental query |
| Read-optimized-tolerant + latency-sensitive on MOR | Read-optimized query |
| Change data capture | CDC query |

Query type is derived, not asked.

## Key generator

| Answer shape | Key generator |
|---|---|
| Single field | `SimpleKeyGenerator` |
| Multi-field (composite) | `ComplexKeyGenerator` |
| Auto-gen (immutable only) | No key generator, no `recordkey.field` |
| Timestamp-derived partition | `TimestampBasedKeyGenerator` |
| Mixed (business + timestamp) | `CustomKeyGenerator` |
| Unpartitioned | `NonpartitionedKeyGenerator` |

Auto-detection: date-string partition columns + natural business record key → `SimpleKeyGenerator`; no explicit `TimestampBasedKeyGenerator` question needed.
