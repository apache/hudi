# RLI Lookup Observability — Design

**Ticket:** ENG-44764 / ENG-44400 · **Target:** Hudi 1.2 · **Lands:** apache/hudi (OSS) first
**Status:** Design, approved in outline. Not yet planned or implemented.
**Scope:** Spark write path at P0. Flink as a follow-up the design must not preclude.

---

## 1. Problem

`HoodieMetadataMetrics` has declared RLI lookup metric names since 0.7.0 —
`lookup_record_index_time`, `lookup_record_index_key_count`,
`lookup_record_index_key_hit_count` (`HoodieMetadataMetrics.java:60-64`) — and nothing
references them. `HoodieBackedTableMetadata#readRecordIndexLocationsWithKeys` carries the
standing explanation:

```
// TODO [HUDI-9544]: Metric does not work for rdd based API due to lazy evaluation.
```

The counts exist as local variables on the executor inside the lookup function, but the
function returns only hits — a miss produces no output row — so the driver cannot recover
the denominator from the resulting RDD.

There are two further reasons the obvious fix does not work:

1. **The metadata reader cannot report its own metrics.** `BaseTableMetadata` constructs
   `HoodieMetadataMetrics` in its constructor (`BaseTableMetadata.java:95`), and
   `HoodieTable.context` is `transient` (`HoodieTable.java:159`), so on an executor
   `getContext()` falls back to `HoodieLocalEngineContext` (`:1062`) and `getTableMetadata()`
   builds a **fresh metadata instance per Spark task** (`:1235-1247`). Enabling
   `hoodie.metadata.metrics.enable` today therefore instantiates `Metrics` — and starts
   reporters — inside every executor JVM, writing counters the driver never sees.
2. **The existing distributed transport cannot express what we need.** `DistributedRegistry`
   is a `String -> long` accumulator whose only safe operation is `add`; `set()` is
   last-writer-wins, which is neither commutative nor associative. It cannot express a
   keyed idempotent merge, which is what retry correctness requires (§8).

## 2. Requirements

### P0 — Spark

| # | Requirement | Source |
|---|---|---|
| 1 | `shards_read` (shard = an RLI file group: base file + log files) | Brief |
| 2 | `log_files_read` | Brief |
| 3 | Data scanned, as a **footprint** measure | Brief, scoped by decision §A |
| 4 | Hit / miss counts (hit = key present in index = update) | Brief |
| 5 | Counts, **not** histograms | Brief |
| 6 | DeltaStreamer and Spark DataSource, upsert / tag-location path | Brief |
| 7 | Per-commit gauges through the existing `MetricsReporter` | Decision |
| 8 | Per-commit totals persisted to commit metadata | Decision |
| 9 | Dedicated config key, default OFF | Decision |
| 10 | Multiple tables in one JVM must not interfere | Decision |
| 11 | Counts correct under task retry, speculation, and RDD recomputation | Decision |
| 12 | Collection point in `hudi-common`, shared with Flink's future adapter | Decision |

### P1 — after the demo

True bytes read via reader instrumentation · skew summary · Flink adapter ·
`max_shard_lookup_millis` · silent-fallback marker · `base_files_read` ·
`hottest_shard_file_group_id` in commit metadata · deduplicated `keys_submitted` ·
partitioned-RLI coverage.

### Non-goals

Read-path RLI lookups (Spark queries, CLI, Flink source) · secondary-index and
column-stats instrumentation · lookup latency as a primary metric · histograms anywhere ·
any dependency on the in-flight `DistributedRegistry` hardening.

## 3. Approach

### The seam

Both engines already converge on a single `hudi-common` interface method:

| Engine | Call site |
|---|---|
| Spark | `SparkMetadataTableGlobalRecordLevelIndex.java:196` |
| Spark, partitioned RLI | `SparkMetadataTableRecordLevelIndex.java:134` |
| Flink | `GlobalRecordLevelIndexBackend.java:136` |

All three call `readRecordIndexLocationsWithKeys(...)`. Collection therefore belongs at
that method and below it — **not** in the Spark index class. Instrumenting the Spark class
would require Flink to reimplement it, and would produce two definitions of "hit" that
drift.

Transport, by contrast, is irreducibly engine-specific: Spark aggregates through an
`AccumulatorV2` drained on the driver; Flink reports per-subtask through `MetricGroup`
(`FlinkPartitionedIndexBackendMetrics`) with no driver to drain to, and on a per-checkpoint
rather than per-commit cadence. **Shared collection, engine-specific transport** is the
organizing principle of this design.

### Rejected alternatives

**Instrument the Spark index class only; derive file facts on the driver.** Attractive
because it needs no `hudi-common` change: `TaskContext.partitionId()` *is* the shard index
by construction of `PartitionIdPassthrough`
(`SparkMetadataTableGlobalRecordLevelIndex.java:215-231`), so the driver can join touched
shards against the RLI file-slice list and compute file counts and sizes itself. Rejected
once Flink became a requirement — the instrumentation lives in a Spark-only class. It also
had a latent defect worth recording: `partitionBy` creates exactly `numFileGroups`
partitions and Spark runs a task for every one of them, including empty ones, so
"increment once per task" would have reported `shards_read == total file group count` on
every commit.

**Reuse `DistributedRegistry`.** Rejected on three counts: its `String -> long` contract
cannot express the keyed idempotent merge that requirement 11 needs; it resolves through
the process-wide static `Registry.REGISTRY_MAP`, which is the direct cause of the
multi-table interference requirement 10 forbids; and it would make this work depend on the
in-flight hardening of that class landing first.

**Derive everything from the returned RDD.** Rejected: recovering misses means changing the
lookup's return contract or adding a second Spark action, and file-level facts (log files
touched, bytes) are internal to the reader and not present in the result at all.

## 4. Components

### `RecordIndexShardLookupStats` — `hudi-common`, `org.apache.hudi.metadata`

Immutable value type describing one shard's lookup. Serializable.

| Field | Type | Filled by |
|---|---|---|
| `shardIndex` | `int` | `hudi-common`, at read |
| `fileGroupId` | `String` | `hudi-common`, from `FileSlice.getFileId()` |
| `keysSubmitted` | `long` | `hudi-common`, at read |
| `keysHit` | `long` | `hudi-common`, counted on the result iterator |
| `logFilesRead` | `long` | `hudi-common`, from `FileSlice.getLogFiles()` |
| `bytesInShard` | `long` | `hudi-common`, from `FileSlice.getTotalFileSize()` |
| `lookupMillis` | `long` | `hudi-common`, at read (collected at P0, emitted at P1) |

Every field is populated at the point of read, where the `FileSlice` is already in scope.
`FileSlice.getTotalFileSize()` (`FileSlice.java:175`) is exactly the footprint measure —
base file size plus the sum of log file sizes — and needs no new code.

An earlier revision derived `logFilesRead` and `bytesInShard` on the driver by matching
touched shards against the record index file slices. That was a holdover from the rejected
Spark-only approach, where the executor had no `FileSlice` to consult. Once collection moved
to the `hudi-common` seam it became strictly worse: it required resolving a file-system view
on the commit path, introduced a "file group no longer present" failure mode, and reported
what the *current* slice looks like rather than what was read. Collecting at the read site
removes all three.

P1 reader instrumentation adds a *separate* `bytesRead` field for true I/O rather than
redefining `bytesInShard`. Adding a field to an immutable value type used only inside this
feature is not a breaking change, so it is not carried speculatively at P0.

### `RecordIndexLookupStatsCollector` — `hudi-common`, `org.apache.hudi.metadata`

```java
@FunctionalInterface
public interface RecordIndexLookupStatsCollector extends Serializable {
  RecordIndexLookupStatsCollector NOOP = stats -> { };

  void collect(RecordIndexShardLookupStats stats);
}
```

Deliberately one method. A small surface has few opinions to have about it in review, and
it is the entire contract Flink's adapter will implement.

### Interface change — `HoodieTableMetadata`

Add one overload:

```java
HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
    HoodieData<String> recordKeys,
    Option<String> dataTablePartition,
    RecordIndexLookupStatsCollector collector);
```

The existing 1-arg and 2-arg signatures (`HoodieTableMetadata.java:247`, `:259`) remain and
delegate with `NOOP`. No source or binary breakage for the three implementations
(`HoodieBackedTableMetadata`, `FileSystemBackedTableMetadata`, `NoOpTableMetadata`) or for
any existing caller. This matters for review velocity as much as for compatibility.

Inside `HoodieBackedTableMetadata`, the collector threads down to `lookupIndexRecords`
(`:271`), where the file slice being read is in scope. The result iterator is wrapped in a
counting decorator that emits one `RecordIndexShardLookupStats` when the iterator is
exhausted or closed.

> **Precondition:** the caller fully consumes the returned data. Both current callers do —
> Spark via `HoodieDataUtils.dedupeAndCollectAsList`, Flink via `forEach`. A caller that
> abandons the iterator early under-counts hits; it never over-counts, and it never fails.
>
> **De-risking fallback.** If the counting decorator proves awkward inside the one-week
> window, hits can instead be supplied by each caller, which already materializes the
> result. Less pure, identical interface, and upgradeable later without an interface change.

### `RecordIndexLookupStatsAccumulator` — `hudi-spark-client`, `org.apache.hudi.metrics`

```java
AccumulatorV2<RecordIndexShardLookupStats, Map<Integer, RecordIndexShardLookupStats>>
```

Implements `RecordIndexLookupStatsCollector`, so it is passed straight into the lookup call.
Created per write client, registered once against the live `SparkContext`, `reset()` after
each drain. Merge semantics in §8.

## 5. Data flow

```
tagLocation
  └─ partitionBy(PartitionIdPassthrough)             partition id == shard index
       └─ one task per shard:
            readRecordIndexLocationsWithKeys(keys, partition, accumulator)
              └─ hudi-common reads the file slice, wraps the result iterator
                   └─ on exhaustion: collector.collect(shard stats)
  (lazy — nothing has executed yet)

write action materializes the RDD                    ← accumulator populates here

commit path, before the commit file is written:
  drain accumulator  →  Map<shardIndex, ShardStats>
  fold to totals (no I/O, no file-system view, no table rebuild)
  → HoodieCommitMetadata.addMetadata("hoodie.rli.lookup.stats", compact JSON)
  → Metrics gauges → existing MetricsReporter
  → accumulator.reset()
```

**Ordering constraint.** The drain hooks into `preCommit(HoodieCommitMetadata)`
(`BaseHoodieWriteClient.java:430`), which runs at `commitStats:273` — after write statuses
have been collected, so the accumulator is populated, and before `commit(...)` writes the
file at `:277`, so `HoodieCommitMetadata.addMetadata` (`:98`) still lands in the timeline.

Two nearby hooks are *not* usable and the reason is worth recording. `updateExtraMetadata`
(`BaseHoodieClient.java:462`) runs at `commitStats:258`, before the table exists.
`emitCommitMetrics` (`:414`) runs after the commit file is written. `preCommit` is the only
point that satisfies both constraints.

**Why the accumulator is owned by the write client.** `HoodieTable.index` is `transient` and
lazily rebuilt per table instance (`HoodieTable.java:144,439-443`), and `createTable(config)`
constructs a fresh table. An accumulator held on the index would therefore be a *different
object* by the time `preCommit` runs. The write client is the only component whose lifetime
spans `tagLocation` and commit, so it owns the accumulator and attaches it to each table it
creates.

## 6. Metric catalog

All values are `Long`. Totals are computed by folding the drained per-shard map.

| Metric | Derivation | Name |
|---|---|---|
| Shards read | `map.size()` | `lookup_record_index_shards_read` |
| Keys submitted | Σ `keysSubmitted` | `lookup_record_index_key_count` *(reuses the dead constant)* |
| Keys hit | Σ `keysHit` | `lookup_record_index_key_hit_count` *(reuses the dead constant)* |
| Log files read | Σ `logFilesRead` | `lookup_record_index_log_files_read` |
| Bytes in shards read | Σ `bytesInShard` | `lookup_record_index_bytes_in_shards_read` |

Misses (inserts) are `keysSubmitted − keysHit`, derived by the consumer. **No ratios are
emitted** — the brief asked for counts.

### Semantics that must be documented, not assumed

- **`bytes_in_shards_read` is a footprint, not I/O.** It is bytes resident in the shards
  touched. RLI lookups push the key set into the HFile reader as a `Predicates.In`
  (`HoodieBackedTableMetadata.java:686`), so the reader reads a fraction of that. The metric
  is an upper bound and its name says so. True bytes is P1 (§2).
- **There is no "scan."** Because of the same pushdown, records materialized ≈ keys hit. The
  brief's "number of records scanned" has no distinct meaning on this path, and a separate
  records metric would restate the hit count. This should be relayed to the requester.
- **`keys_submitted` is raw; `keys_hit` is distinct.** With duplicate record keys in a batch
  the ratio understates the true hit rate. Deduplicating the denominator costs a `HashSet`
  over every key in each partition — real memory for metric precision. P1.
- **These are index-efficiency metrics, not the batch's update/insert split.** Commit
  metadata already carries per-record `numInserts` / `numUpdateWrites` on `HoodieWriteStat`.
  Duplicating that would produce two numbers that disagree under duplicate keys.
- **Name collision with Flink.** `GlobalRecordLevelIndexBackend` already emits
  `updateLookupCacheHitRatio` and `updateRemoteLookupKeysCount`, where "hit" means *local
  cache hit* — a different layer from ours, where hit means *key present in the index*. Our
  names are qualified (`key_hit_count`, not `hit_count`) so the two cannot be confused.

## 7. Sinks

### Metrics reporters

One numeric gauge per metric, per commit, through `Metrics.registerGauge` →
`MetricsReporter`. Per-commit gauge semantics match every existing Hudi metric
(`HoodieMetadataMetrics.updateMetrics`), so existing dashboard assumptions carry over.

Two hard constraints, both derived from reporter source rather than convention:

- **Every value must be a `Long`.** `PushGatewayReporter.java:160` does
  `.set((Long) gaugeEntry.getValue().getValue())` and `DatadogReporter.java:96-97` does
  `(long) metric.getValue()`. A `String`-valued gauge works on Console, Slf4j and JMX and
  throws `ClassCastException` inside the reporting path on Datadog and Prometheus — passing
  a console demo and breaking production.
- **No shard-valued metric labels.** `MetricUtils` supports `name;key:value` labels
  (`MetricUtils.java:35-39`), which become Datadog tags and Prometheus labels. A shard-id
  label mints a new time series per shard, unbounded as shards split. This is also why per-
  shard rows are excluded from every sink except, at P1, Console/JMX.

Known limitation, inherent to Hudi's existing model rather than to this feature:
`Metrics.flush()` reports, removes every gauge name, then re-registers (`:144-153`), so a
pull-based `PrometheusReporter` scraping on its own clock can miss a commit's values.

### Commit metadata

One `extraMetadata` key, `hoodie.rli.lookup.stats`, holding compact JSON. A single
versioned key rather than ten flat keys keeps the timeline tidy and lets the payload gain
fields (skew, true bytes) without new keys. Payload is fixed-size — per-commit totals only,
never per-shard rows, so a table committing every five minutes accrues no meaningful
timeline bloat.

## 8. Correctness

### Multi-table isolation (requirement 10)

**Invariant: no RLI lookup stats state is reachable through a static field.**

The collector is an instance created per write client and passed *explicitly* as a method
argument. Two write clients for two tables produce two collectors that never meet. An
explicit parameter is chosen over a setter on `HoodieBackedTableMetadata` precisely because
a mutable field invites the cross-wiring and ordering bugs that a shared static slot
already causes elsewhere. The sinks are per-table independently:
`Metrics.METRICS_INSTANCE_PER_BASEPATH` is keyed by base path, and commit metadata is
per-table by definition.

### Retry and recomputation correctness (requirement 11)

Sum-merged counters are irreducibly at-least-once: a retried or recomputed task adds a
second time, and no amount of documentation fixes it. Instead the accumulator carries a map
keyed by shard, merged **per key by field-wise `max`**:

```
merge(a, b):  for each shard key present in either map,
              field-wise max of the two RecordIndexShardLookupStats
```

| Property | Why it holds |
|---|---|
| Retry-idempotent | A retried task re-reports the *same shard*. Max-merge overwrites rather than accumulates; running it ten times yields the same total. |
| Recomputation-idempotent | Same mechanism. Covers the uncached tagged-RDD case, which is a real exposure: `getRecordIndexUseCaching` persists the *input* records, not the tagged output. |
| A legal accumulator merge | Per-key `max` is commutative and associative, unlike last-writer-wins. Spark merges executor-local copies in an unspecified order, so this is a correctness requirement, not a nicety. |
| Correct under partial retry | A task that failed mid-read then succeeded resolves to the complete read, not merely to a deterministic one. |
| Empty shards excluded | Only an actual read inserts a key, so `shards_read` counts shards genuinely touched — not the `numFileGroups` tasks Spark launches. |

**Cost.** Driver-side accumulator memory is O(shards touched) rather than O(1). Each
executor ships only its own entry; the driver holds one small struct per shard — a few
hundred KB at 5,000 shards. It never reaches the timeline. At extreme file-group counts
this warrants a documented bound.

**Bonus.** The skew summary (P1) is a scan of this map. It requires no additional
collection, no additional merge fields, and no interface change — the data is already
present because retry-exactness demands it.

## 9. Configuration

`hoodie.metadata.record.index.lookup.stats.enable`, default `false`, `markAdvanced()`,
`sinceVersion("1.2.0")`. Gates collection, commit-metadata persistence and reporting
together.

Default OFF is consistent with every neighbouring knob — `hoodie.metrics.on`,
`hoodie.metadata.metrics.enable`, `hoodie.metadata.enable.detailed.metrics`,
`hoodie.metrics.executor.enable` are all `false` — and follows the precedent set by
`ENABLE_DETAILED_METRICS` (`HoodieMetadataConfig.java:686`) for second-tier metrics that
cost something.

When disabled, the collector reference is `RecordIndexLookupStatsCollector.NOOP`: the
accumulator is never created, no per-shard object is allocated, and the executor path is
byte-for-byte the behaviour that ships today. This provable no-op is also the argument that
should carry the `hudi-common` diff through review quickly.

## 10. Flink forward-compatibility

Flink is P1, but P0 makes it an adapter rather than a rewrite:

- Collection already happens at the shared `hudi-common` seam Flink calls
  (`GlobalRecordLevelIndexBackend.java:136`), so no instrumentation is written twice and
  there is one definition of "hit".
- Flink's adapter implements the one-method collector, backed by `MetricGroup` in the style
  of `FlinkPartitionedIndexBackendMetrics`, and reports per subtask.
- The keyed max-merge is idempotent under restart-from-checkpoint replay, so exactness is
  not a Spark-only property.

Two differences are acknowledged and left to the Flink work, not designed for now:
Flink's cadence is per-checkpoint rather than per-commit, and its house style for these
metrics is histograms (`DropwizardHistogramWrapper`) rather than the gauges the brief
mandates for Spark. Designing an engine-neutral lifecycle contract before that consumer
exists would be speculative.

## 11. Error handling

Instrumentation must never fail a write. The drain, the derivation and the reporting are
each wrapped so that any exception logs at WARN and drops that commit's metrics — matching
`Metrics.registerGauge`, which already swallows exceptions specifically so the upsert
pipeline is unaffected. A file slice that cannot be resolved during derivation yields a
partial count, never a failure.

## 12. Testing

- **Merge algebra (unit).** Commutativity and associativity over shuffled merge orders —
  the property Spark's accumulator contract actually requires. Idempotence under repeated
  merge of the same shard. Partial-then-complete retry resolving to complete. Empty/zero
  identity.
- **Driver-side derivation (unit).** From a synthetic `List<FileSlice>`, asserting full
  expected objects with `assertEquals`, not counts.
- **Multi-table isolation (integration).** Two write clients, two tables, one JVM, writing
  concurrently; assert each commit's metadata carries only its own table's counts.
- **End-to-end (Spark, local).** Upsert against a table with a known insert/update mix
  across multiple shards; assert exact `keys_hit`, `keys_submitted`, `shards_read`,
  `log_files_read`, and the presence and shape of the `extraMetadata` JSON.
- **Feature off (negative).** No gauges, no `extraMetadata` key, and
  `verify(collector, never())` on the collection path.
- **Interface compatibility.** Existing 1-arg and 2-arg callers compile and behave
  unchanged; `NoOpTableMetadata` and `FileSystemBackedTableMetadata` still satisfy the
  interface.

## 13. Risks

| Risk | Mitigation |
|---|---|
| **Review latency on a `hudi-common` public-interface change** — the schedule risk to "Spark in a week", and larger than the code risk | Additive overload only; existing signatures delegate with `NOOP`; provable zero behaviour change when disabled; collector interface kept to one method |
| Counting decorator on a lazy iterator proves fiddly mid-week | Documented fallback to caller-supplied hit counts — same interface, upgradeable later without an interface change |
| Accumulator memory at extreme file-group counts | Per-shard struct is small and driver-only; document a bound |
| Two metric transports coexist (this accumulator, plus `DistributedRegistry` for FS counters) | Justified in §3 — a `String -> long` map cannot express keyed idempotent merge. Whether this pattern should eventually supersede the registry is an open product question, recorded not decided |

## 14. Open items

1. Whether "how much data scanned" means capacity planning to the requester. If so,
   footprint does not answer it and true bytes moves from P1 to P0. **Worth one question to
   Surya.**
2. Whether this typed-accumulator pattern is intended to supersede `DistributedRegistry`
   long term, or the two coexist permanently.
3. Exact JSON schema and version field for the `hoodie.rli.lookup.stats` payload —
   deferred to the implementation plan.
