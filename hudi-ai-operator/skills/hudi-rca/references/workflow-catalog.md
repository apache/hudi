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
# Hudi write-path workflow catalog

A map from **what a user sees in the Spark UI** to **what Hudi was actually doing**. Use it when
a stage failed and the root cause is not obvious: identify the phase, then read its governing
configs and its known failure modes.

Every non-obvious claim cites `file:line` against this checkout. Where a statement is an
inference rather than something read from source — most notably the exact text Spark renders in
its UI — it is marked **[inferred]**.

---

## How to read this

### 1. The job description is the strongest signal

Hudi sets a Spark *job description* before most phases and clears it afterwards. On Spark the
call is:

```java
javaSparkContext.setJobDescription(String.format("%s:%s", activeModule, activityDescription));
```

`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/common/HoodieSparkEngineContext.java:232-234`

So the description is always `<SimpleClassName>:<free text>`, and the free text usually ends with
the table name. The Spark UI shows the job description in the Jobs tab, and the stages of that
job inherit it in the "Description" column **[inferred — this is standard Spark UI behaviour,
not something Hudi controls]**.

Descriptions Hudi emits that are worth memorising:

| Description (prefix:text) | Phase | Source |
|---|---|---|
| `…:Tagging: <table>` | index lookup | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/BaseWriteHelper.java:73` |
| `…:Building workload profile:<table>` | `buildProfile` / `countByKey` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/BaseSparkCommitActionExecutor.java:263` |
| `…:Doing partition and writing data: <table>` | shuffle + write handles | `.../BaseSparkCommitActionExecutor.java:246` |
| `…:Commit write status collect: <table>` | `collectAsList` of `WriteStatus` | `.../BaseSparkCommitActionExecutor.java:390` |
| `…:Handling updates which are under clustering: <table>` | clustering update strategy | `.../BaseSparkCommitActionExecutor.java:119` |
| `…:Getting small files from partitions: <table>` | small-file probe in `UpsertPartitioner` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:287` |
| `…:Obtain key ranges for file slices (range pruning=on): <table>` | bloom-index range load | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bloom/HoodieBloomIndex.java:173` |
| `…:Compacting file slices: <table>` | compaction execution | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java:129` |
| `…:Clustering records for <table>` | clustering execution | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/BaseCommitActionExecutor.java:287` |
| `…:Creating Listing Rollback Plan: <table>` | listing-based rollback plan | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/rollback/ListingBasedRollbackStrategy.java:105` |
| `…:Delete all partially written files: <table>` | marker reconciliation cleanup | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/HoodieTable.java:847` |
| `…:Fetching next batch: <table>` | HoodieStreamer source read | `hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/StreamSync.java:658` |

`clearJobStatus()` sets the description back to `null`
(`HoodieSparkEngineContext.java:237-239`), so a stage with **no** description is usually either
a lazily-evaluated RDD whose job was launched outside a `setJobStatus` block, or a phase Hudi
does not instrument. Treat "no description" as "look at the task count and the shuffle shape
instead".

### 2. Task count tells you which partitioner ran

Hudi produces a small number of distinctive parallelism shapes. Matching the task count against
these is usually decisive:

| Task count equals | Phase | Why |
|---|---|---|
| Number of input Spark partitions, unchanged | dedupe skipped, or a narrow `mapPartitions` tagging (bucket index) | `hoodie.*.shuffle.parallelism` defaults to `0` = "inherit input parallelism", see below |
| `hoodie.upsert.shuffle.parallelism` (if set) | dedupe `reduceByKey`, or `countByKey` for the workload profile | `HoodieWriteHelper.java:78-80` |
| A number with no relation to input or any config | the write stage — task count = **total buckets** from `UpsertPartitioner` | `UpsertPartitioner.java:336-338` (`numPartitions()` returns `totalBuckets`) |
| Number of file groups in the compaction plan | compaction execution | `HoodieCompactor.java:154` + `HoodieEngineContext.java:75-80` |
| Number of output file groups in one clustering group | one clustering sub-job | `SparkSortAndSizeExecutionStrategy.java:65-68` |
| Number of table partitions (capped) | clean plan, listing rollback, small-file probe | see each section |
| 200 (and nothing else explains it) | `hoodie.finalize.write.parallelism`, default 200 | `HoodieWriteConfig.java:561-563` |
| 100 (and nothing else explains it) | `hoodie.rollback.parallelism`, default 100 | `HoodieWriteConfig.java:481-483` |

### 3. The parallelism defaults are not what most people think

`hoodie.upsert.shuffle.parallelism`, `hoodie.insert.shuffle.parallelism`,
`hoodie.bulkinsert.shuffle.parallelism` and `hoodie.delete.shuffle.parallelism` **all default to
`0`**, not 200:

```java
public static final ConfigProperty<String> UPSERT_PARALLELISM_VALUE = ConfigProperty
    .key("hoodie.upsert.shuffle.parallelism")
    .defaultValue("0")
```

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java:454-456`
(the same shape at `:399-401`, `:413-415`, `:469-471`).

`0` means "deduce from the input":

```java
protected int deduceShuffleParallelism(I input, int configuredParallelism) {
  if (configuredParallelism > 0) {
    return configuredParallelism;
  }
  return partitionNumberExtractor.apply(input);
}
```

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/ParallelismHelper.java:31-40`

The config's own documentation records the history: before 0.13.0 the default was a literal 200;
from 0.13.0 the default is Spark-deduced parallelism
(`HoodieWriteConfig.java:458-467`). **Consequence for RCA:** if a user says "I didn't set
parallelism so it must be 200", that is wrong on any recent version. A 16 000-task shuffle with
no parallelism config set means the *input* RDD had 16 000 partitions.

### 4. The write stage is bucket-shaped, not partition-shaped

The single most misread stage in a Hudi upsert is the write stage. Its task count is
`UpsertPartitioner.totalBuckets`, which is `(#file groups receiving updates) + (#insert buckets)`
— derived from the workload profile, not from any shuffle-parallelism config
(`UpsertPartitioner.java:130-137`, `:242-256`, `:336-338`). Tuning
`hoodie.upsert.shuffle.parallelism` will **not** change this stage's task count. Tuning
`hoodie.parquet.max.file.size` / `hoodie.copyonwrite.insert.split.size` will.

### 5. RDD names

Hudi does not call `RDD.setName()` on its main write-path RDDs anywhere in this checkout, so the
Spark UI DAG shows the generic operator names (`MapPartitionsRDD`, `ShuffledRDD`,
`ParallelCollectionRDD`) **[inferred from the absence of `setName` calls; verified by grep over
`hudi-client`]**. The DAG shape is still informative:

- `ParallelCollectionRDD` at the root of a stage ⇒ the stage was driven from a driver-side list
  (`context.parallelize(...)`): compaction operations, clean partitions, rollback requests.
- A `ShuffledRDD` whose parent is a `MapPartitionsRDD` producing `(HoodieFileGroupId, String)`
  pairs ⇒ bloom-index file-comparison shuffle.
- `UnionRDD` near the end of a replacecommit ⇒ clustering group results being unioned
  (`MultipleSparkJobExecutionStrategy.java:134`).

### 6. Walk the phases in order

A Spark job that fails at a given point tells you every phase that already *succeeded*. Use the
ASCII diagrams below: if "write and handle" is running, tagging and profiling already completed,
so the problem is in file writing, not index lookup.

---

## 1. UPSERT — Copy-on-Write

### Phase diagram

```
  driver                                                        executors
  ------                                                        ---------
  [1] startCommit / requested instant
        |
        v
  [2] dedupe (optional) ------------------------------------>  reduceByKey
        |                                                      (shuffle #1)
        v
  [3] index tagging --------------------------------------->   varies by index
        |                                                      (shuffle #2, bloom/simple/RLI)
        v
  [4] buildProfile  <--------- countByKey ------------------   (shuffle #3)
        |
  [5] UpsertPartitioner construction
        |   (small-file probe: Spark job over partition paths)
        v
  [6] save workload profile -> inflight instant on timeline
        |
        v
  [7] partitionBy(UpsertPartitioner) ---------------------->   shuffle #4
        |                                                      then mapPartitionsWithIndex:
        |                                                      merge handles / create handles
        v
  [8] WriteStatus collect  <-------------------------------    collectAsList
        |
  [9] finalizeWrite: marker reconciliation
        |
  [10] metadata table update (deltacommit on MDT)
        |
  [11] saveAsComplete -> .commit on timeline  (inside txn lock)
        |
  [12] post-commit inline table services (clean / archive / compact / cluster)
```

### Phase table

| # | Phase | Driver / executor | Spark op | Governing configs | Common failures |
|---|---|---|---|---|---|
| 2 | **Dedupe** `HoodieWriteHelper.deduplicateRecords` | executor | `mapToPair` → `reduceByKey(parallelism)` → `map` (`HoodieWriteHelper.java:70-80`) | `hoodie.combine.before.upsert`; parallelism from `hoodie.upsert.shuffle.parallelism` via `combineOnCondition` (`BaseWriteHelper.java:90-94`) | Skew on a hot record key (one reducer task runs forever). Payload/merger `IOException` surfaces as `HoodieException("Error to merge two records…")` (`BaseWriteHelper.java:163-165`). Note the record is **copied** on the map side (`HoodieWriteHelper.java:74-77`) — this roughly doubles peak map-side memory and is a common OOM source on wide rows. |
| 3 | **Index tagging** `HoodieIndex.tagLocation` | mixed | see per-index table below | `hoodie.index.type` and the per-index configs | see per-index table below |
| 4 | **buildProfile** | driver collects, executors count | `mapToPair(...).countByKey()` (`BaseSparkCommitActionExecutor.java:284-288`) | parallelism inherited from the tagged RDD | `countByKey` returns a `Map` **to the driver**, one entry per `(partitionPath, location)` pair. On a table with very many file groups receiving updates this is a driver-side OOM. This is the classic "driver OOM at the end of tagging" signature. |
| 5 | **UpsertPartitioner** | driver (plus one small Spark job) | `context.mapToPair(partitionPaths, …, partitionPaths.size())` (`UpsertPartitioner.java:289`) | `hoodie.parquet.small.file.limit` (`HoodieCompactionConfig.java:111`), `hoodie.parquet.max.file.size` (`HoodieStorageConfig.java:40`), `hoodie.copyonwrite.insert.split.size` (`:177`), `hoodie.copyonwrite.insert.auto.split` (`:186`) | Task count here is `partitionPaths.size()` — on a table with tens of thousands of partitions this stage alone can take minutes. Short-circuits entirely when `hoodie.parquet.small.file.limit <= 0` or there are no completed commits (`UpsertPartitioner.java:279-285`). |
| 6 | **Workload profile → inflight** | driver | none (timeline write) | `hoodie.write.concurrency.*` | `transitionRequestedToInflight` (`BaseCommitActionExecutor.java:164`); failure raises `HoodieCommitException("…unable to save inflight metadata")` (`:166`). |
| 7 | **Partition + write** `mapPartitionsAsRDD` | executor | `mapToPair` → `partitionBy(partitioner)` **or** `repartitionAndSortWithinPartitions` when `table.requireSortedRecords()` → `mapPartitionsWithIndex` → `flatMap` (`BaseSparkCommitActionExecutor.java:323-353`) | task count = `UpsertPartitioner.numPartitions()` = total buckets. File sizing via `hoodie.parquet.max.file.size`. | **Executor OOM is most often here, not in tagging** — each `UPDATE` bucket opens a `HoodieMergeHandle` that holds the incoming records for that file group. Every failure in this stage is wrapped as `HoodieUpsertException("Error upserting bucketType … for partition :N")` (`:413-417`), so the exception message gives you the bucket index directly. Note the HFile/LSM path sorts with a UTF-8 byte comparator, not Java string order (`:333-337`). |
| 8 | **WriteStatus collect** | driver | `persist` then `map(WriteStatus::getStat).collectAsList()` (`:359`, `:381`) | `hoodie.write.status.storage.level` (`WRITE_STATUS_STORAGE_LEVEL_VALUE`, `:91`) | One `HoodieWriteStat` per written file arrives on the driver. With `hoodie.write.statuses.track.success.records` on (not shown here), `WriteStatus` carries per-record detail and this collect becomes a driver OOM. |
| 9 | **finalizeWrite / markers** | both | `getInvalidDataPaths` then `deleteInvalidFilesByPartitions` at `config.getFinalizeWriteParallelism()` (`HoodieTable.java:781-782`, `:775`) | `hoodie.finalize.write.parallelism` (default 200, `HoodieWriteConfig.java:561-563`); `hoodie.markers.type`; `hoodie.consistency.check.enabled`; `hoodie.fail.on.duplicate.data.file.detection` | Reconciliation compares marker-derived paths against `WriteStat` paths and deletes the difference — files written by speculative/retried tasks (`HoodieTable.java:813-825`). If `shouldFailOnDuplicateDataFileDetection` is set, the commit **fails** rather than cleaning up (`:828-830`). Early return if the marker dir does not exist (`:806-809`), which is why an empty write never reconciles. |
| 10 | **Metadata table update** | both | a full MOR deltacommit on the MDT — see §9 | `hoodie.metadata.enable` and the per-partition index configs | A failure here fails the data commit: `writeTableMetadata` is called *before* `saveAsComplete` (`BaseCommitActionExecutor.java:230`, `:233`). |
| 11 | **Commit** | driver | none | `hoodie.write.concurrency.mode`, lock provider configs | Runs inside `txnManager.beginStateChange(...)` / `endStateChange(...)` (`BaseCommitActionExecutor.java:199-209`) with `TransactionUtils.resolveWriteConflictIfAny` in between (`:204-205`). A `HoodieWriteConflictException` here is a concurrent-writer conflict, not a bug. |
| 12 | **Post-commit services** | driver schedules | varies | `hoodie.clean.automatic`, `hoodie.archive.automatic`, `hoodie.compact.inline`, `hoodie.clustering.inline` | `runTableServicesInline` then `autoCleanOnCommit` / `autoArchiveOnCommit` (`BaseHoodieWriteClient.java:711-712`, `:719-723`). Inline services run **after** the commit is durable, so a failure here leaves a valid commit with stale services. |

### Index tagging, per index type

This is where the "stage 14 OOM" questions usually land. The shapes differ a lot.

| Index | Spark shape | Parallelism | Characteristic failure |
|---|---|---|---|
| **BLOOM** (`HoodieBloomIndex`) | `mapToPair` to `(partitionPath, recordKey)` (`HoodieBloomIndex.java:82-83`) → driver `countByKey` for per-partition counts (`:117`) → driver-side load of key ranges → **`explodeRecordsWithFileComparisons`**: one output row per `(record, candidate file group)` (`:300-310`) → sort/repartition → `mapPartitions` probing files | `hoodie.bloom.index.parallelism`; if `0`, `max(inputParallelism, 10)` (`SparkHoodieBloomIndexHelper.java:97-98`) | **The fan-out is the problem, not the memory setting.** `explodeRecordsWithFileComparisons` multiplies the record count by the number of candidate file groups per key. Random/UUID keys defeat range pruning, so every record is compared against every file group in its partition — a 1M-record batch against 5 000 file groups becomes 5 billion comparison rows. Symptoms: a stage whose input record count is orders of magnitude larger than the batch, shuffle spill, executor OOM. Mitigations are `hoodie.bloom.index.prune.by.ranges` (`HoodieIndexConfig.java:103`), `hoodie.bloom.index.use.metadata` (`:121`), `hoodie.bloom.index.bucketized.checking` (`:139`) and `hoodie.bloom.index.keys.per.bucket` (`:202`). |
| **BLOOM over metadata bloom_filters** | `repartitionAndSortWithinPartitions(AffineBloomIndexFileGroupPartitioner)` → `mapPartitionsToPair(HoodieMetadataBloomFilterProbingFunction)` → `mapPartitions(HoodieFileProbingFunction)` (`SparkHoodieBloomIndexHelper.java:156-160`) | parallelism rounded **up to a multiple of** `hoodie.metadata.index.bloom.filter.file.group.count` so each Spark partition maps to exactly one MDT file group (`:138-148`) | Task count that is not a round number and not any config value — it is the rounded-up multiple. Failures here are MDT read failures, not index logic. |
| **BLOOM bucketized** | `mapToPair` → `repartitionAndSortWithinPartitions(BucketizedBloomCheckPartitioner)` → `mapPartitions(HoodieSparkBloomIndexCheckFunction)` (`SparkHoodieBloomIndexHelper.java:170-173`) | `hoodie.bloom.index.keys.per.bucket`, optionally dynamic (`:165-168`) | Needs an extra `countByKey` over the comparison RDD to size buckets (`:226`) — an extra full shuffle before the real one. |
| **GLOBAL_BLOOM** (`HoodieGlobalBloomIndex`) | same as bloom, but candidate files come from **all** partitions | same | Fan-out is across the whole table, not one partition. On a wide table this is the single most expensive tagging mode. |
| **SIMPLE** (`HoodieSimpleIndex`) | `mapToPair` → driver collects affected partition list (`distinct().collectAsList()`, `HoodieSimpleIndex.java:135`) → `parallelize(baseFiles)` + `flatMap` reading **every key** of every affected base file (`:146-149`) → `leftOuterJoin` (`:112`) | `hoodie.simple.index.parallelism`, default `0`; when `0` or ≥ file count, parallelism = **number of base files** (`:144`, `:152-155`) | Reads the full key column of every base file in every touched partition on every write. Cost is proportional to *table* size in those partitions, not batch size. A batch of 100 records touching 2 000 file groups still reads 2 000 files. |
| **GLOBAL_SIMPLE** | as SIMPLE but across all partitions | same | Scans the whole table's keys per write. Only viable on small tables. |
| **RECORD_INDEX / RLI** (`SparkMetadataTableRecordLevelIndex`, global variant `SparkMetadataTableGlobalRecordLevelIndex`) | `map` to keys → `keyBy(hash → fileGroupIndex)` → `partitionBy(PartitionIdPassthrough(numFileGroups))` → `mapPartitionsToPair(lookup)` (`SparkMetadataTableGlobalRecordLevelIndex.java:127-139`; partitioned variant `SparkMetadataTableRecordLevelIndex.java:75-86`) | **task count = number of RLI file groups in the MDT**, asserted at `:134` / `:82`. Not a shuffle-parallelism config. | A suspiciously round, config-free task count (e.g. exactly 64) means RLI. Under-sized RLI file groups ⇒ too few tasks ⇒ each task looking up millions of keys. Note the silent fallback: if `record_index` is not initialised, Hudi logs a warning and falls back to `SIMPLE` (non-global) or `GLOBAL_SIMPLE` (global) (`:81-94`) — so "my RLI table suddenly started doing full base-file scans" is this branch firing. |
| **BUCKET** (`HoodieBucketIndex`) | `records.mapPartitions(...)` only — **no shuffle** (`HoodieBucketIndex.java:81-91`) | none; location is computed from the key hash | Tagging is nearly free; a bucket-index table that is slow is slow somewhere else. Failure mode is wrong bucket count, not tagging cost. Note `BaseSparkCommitActionExecutor.handleUpdate` falls back to insert when the base file is missing under `BUCKET` (`:437-440`). |

### Merge handles

`handleUpdate` builds a handle via `HoodieMergeHandleFactory.create(...)` and runs it through
`MergeUtils.runMerge` (`BaseSparkCommitActionExecutor.java:443-444`, `:447-465`). For a
bootstrapped table it additionally resolves partition values from the bootstrap base path
(`:453-460`). `handleInsert` instead returns a `SparkLazyInsertIterable` with a
`CreateHandleFactory` (`:468-476`) — new file groups, no read of an old file.

---

## 2. UPSERT — Merge-on-Read

Everything in §1 applies up to and including the workload profile. The divergence is in the
partitioner and in what `handleUpdate` does.

```
   ... identical through buildProfile ...
        |
        v
  [5'] SparkUpsertDeltaCommitPartitioner      (instead of UpsertPartitioner)
        |
        v
  [7'] per bucket:
        canIndexLogFiles() ?
           yes -> HoodieAppendHandle  (inserts AND updates go to log blocks)
           no  -> small file id ?
                    yes -> HoodieMergeHandle  (rewrite base file, "small file correction")
                    no  -> HoodieAppendHandle (append log block)
        |
        v
   ... identical: WriteStatus, markers, MDT, commit ...
   action type is deltacommit, not commit
```

| Divergence | Detail | Source |
|---|---|---|
| Partitioner | `SparkUpsertDeltaCommitPartitioner` extends `UpsertPartitioner`, overriding only `getSmallFiles` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/deltacommit/SparkUpsertDeltaCommitPartitioner.java:47-55` |
| Small-file candidates | If the index can index log files, **any** file slice under `hoodie.parquet.max.file.size` is a candidate (`isSmallFile` uses `getTotalFileSizeAsParquetFormat`). Otherwise only slices with **zero log files** qualify, sorted ascending by base-file size, limited by `hoodie.small.file.group.candidates.limit` | `SparkUpsertDeltaCommitPartitioner.java:90-117`, `:124-127` |
| Updates | Normally `HoodieAppendHandle.doAppend()` — appends a log block, **no base-file rewrite**. Falls back to the CoW merge path only when the index cannot index log files **and** the fileId is in the small-file set | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/deltacommit/BaseSparkDeltaCommitActionExecutor.java:72-86` |
| Inserts | If `canIndexLogFiles()`, inserts also go to log files via `AppendHandleFactory`; otherwise they create base files as in CoW | `BaseSparkDeltaCommitActionExecutor.java:89-97` |
| Markers | Reconciliation deliberately ignores log files appended for update (they are idempotent-safe), but *newly created* log files are included | `HoodieTable.java:811-813` |
| Workload profile semantics | Only updates are recorded in the inflight workload metadata; log-block updates are unknown across batches, so inserts are rolled back by commit time instead | `BaseCommitActionExecutor.java:122-127` |

**What commonly fails here:** the write stage produces far less I/O than CoW (no base rewrite),
so an MOR upsert that is slow in the write stage is usually hitting the small-file-correction
branch — i.e. it *is* rewriting base files. Check whether `getSmallFileIds()` is large. Separately,
a MOR table whose log files grow without bound means compaction is not keeping up (§5), which
shows up at read time, not write time.

---

## 3. INSERT and BULK_INSERT

### INSERT

`INSERT` runs the same `BaseSparkCommitActionExecutor` path as `UPSERT`, with two differences:

- `getPartitioner` dispatches on `WriteOperationType.isChangingRecords(operationType)`; for
  non-changing operations it calls `getInsertPartitioner`, which in the base class simply returns
  `getUpsertPartitioner` (`BaseSparkCommitActionExecutor.java:312-321`, `:485-487`). So INSERT
  still gets small-file packing.
- Index tagging is skipped when `table.getIndex().requiresTagging(operationType)` is false
  (`BaseWriteHelper.java:71-75`). Dedupe is still controlled by `hoodie.combine.before.insert`.

Parallelism comes from `hoodie.insert.shuffle.parallelism` (default `0`,
`HoodieWriteConfig.java:399-401`).

### BULK_INSERT

```
  [1] transitionRequestedToInflight                              (driver)
        |
  [2] optional dedupe (hoodie.combine.before.insert)             reduceByKey
        |
  [3] partitioner.repartitionRecords(records, targetParallelism) sort / repartition / nothing
        |
  [4] mapPartitionsWithIndex(BulkInsertMapFunction) -> flatMap   write handles only
        |
  [5] updateIndexAndMaybeRunPreCommitValidations                 (no tagging)
        |
  [6] commit
```

`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/SparkBulkInsertHelper.java:78-93`,
`:121-140`.

**Why bulk_insert skips index tagging:** there is simply no `tagLocation` call on this path.
`SparkBulkInsertHelper.bulkInsert` goes dedupe → repartition → write, and then calls
`updateIndexAndMaybeRunPreCommitValidations` (`:92`), which only calls `index.updateLocation` —
and for bloom and RLI that is a no-op returning the input unchanged
(`HoodieBloomIndex.java:329-333`, `SparkMetadataTableGlobalRecordLevelIndex.java:155-159`).
Bulk_insert therefore **cannot detect that an incoming key already exists**; it writes new file
groups unconditionally. That is the operation's defining trade-off, not a bug.

### Sort modes

| Mode | Partitioner | Spark op | Notes |
|---|---|---|---|
| `NONE` | `NonSortPartitioner` | `coalesce(n)` if `enforceNumOutputPartitions`, else the RDD untouched (`NonSortPartitioner.java:60-66`) | Fastest; file sizing is whatever the input partitioning gives you. |
| `GLOBAL_SORT` | `GlobalSortPartitioner` | `records.sortBy(partitionPath + "+" + recordKey, true, outputSparkPartitions)` (`GlobalSortPartitioner.java:54-59`) | Full global shuffle+sort — the expensive one. **Throws** if meta fields are disabled (`:49-51`). Best file sizes. |
| `PARTITION_SORT` | `RDDPartitionSortPartitioner` | `mapToPair` then sort within Spark partitions (`RDDPartitionSortPartitioner.java:60`) | Sorts only within each Spark partition; lower memory than global sort, worse sizing. |
| `PARTITION_PATH_REPARTITION` | `PartitionPathRepartitionPartitioner` | repartition by partition path | One executor per table partition. |
| `PARTITION_PATH_REPARTITION_AND_SORT` | `PartitionPathRepartitionAndSortPartitioner` | `mapToPair(partitionPath, record)` → `repartitionAndSortWithinPartitions(partitioner)` (`PartitionPathRepartitionAndSortPartitioner.java:70-71`) | As above plus an intra-partition sort by partition path. |

Mode descriptions, including the skew warning on the two `PARTITION_PATH_REPARTITION*` modes, are
in the enum itself: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/execution/bulkinsert/BulkInsertSortMode.java:28-51`.

Selection happens in
`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/execution/bulkinsert/BulkInsertInternalPartitionerFactory.java:66-85`,
with two earlier overrides: a `BUCKET` index routes to `RDDConsistentBucketBulkInsertPartitioner`
or `RDDSimpleBucketBulkInsertPartitioner` regardless of sort mode (`:41-47`), and an LSM-layout
table *rejects* `NONE` and the `PARTITION_PATH_REPARTITION` mode outright because they do not
guarantee record ordering (`:48-62`).

**Common failures:** `GLOBAL_SORT` on a large import is a single full shuffle and is the usual
cause of "bulk_insert spilled to disk / lost executor". Using `NONE` with few input partitions
produces few, enormous files. The two `PARTITION_PATH_REPARTITION*` modes on skewed partition
paths put almost all data on one executor — the enum documentation says so explicitly
(`BulkInsertSortMode.java:40-51`).

### Row-writer path vs record path

`hoodie.datasource.write.row.writer.enable` defaults to `true`
(`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DataSourceOptions.scala:546-548`).
When on, bulk_insert operates on `Dataset[Row]` / `InternalRow` and never materialises
`HoodieRecord` objects:

- `prepareForBulkInsert` prepends the five meta fields as a schema change and fills record key /
  partition path with a `mapPartitions` over `queryExecution.toRdd`
  (`hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/HoodieDatasetBulkInsertHelper.scala:81-126`).
- When meta fields are *not* populated, it avoids dereferencing the DataFrame into an RDD at all
  and just projects null stubs (`:135-149`) — so the DAG looks completely different.
- Dedupe, if enabled, is `dedupeRows` on the `InternalRow` RDD (`:128-132`).
- Then `partitioner.repartitionRecords(updatedDF, targetParallelism)` (`:151`).

The record path (`SparkBulkInsertHelper`) is what runs when the row writer is off. Practically:
if the Spark UI shows `WholeStageCodegen` and `Exchange` nodes with a SQL query plan, you are on
the row-writer path; if you see plain RDD stages with `MapPartitionsRDD`, you are on the record
path **[inferred from the two code paths' use of Dataset vs JavaRDD — the UI rendering itself is
standard Spark behaviour]**.

---

## 4. DELETE and DELETE_PARTITION

### DELETE

```
  dedupe/repartition keys -> create delete records -> tagLocation
      -> filter(isCurrentLocationKnown) -> normal commit executor
```

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/HoodieDeleteHelper.java:86-107`.

- Parallelism: `hoodie.delete.shuffle.parallelism`, default `0` (`HoodieWriteConfig.java:469-471`),
  deduced as usual (`HoodieDeleteHelper.java:86`).
- Dedupe is `distinctWithKey(HoodieKey::getRecordKey, parallelism)` for a global index, plain
  `distinct(parallelism)` otherwise (`:67-73`). If dedupe is off, the keys are still
  `repartition(targetParallelism)`-ed (`:92`) — so there is always a shuffle.
- **Tagging always runs** for DELETE (`:99`), unconditionally, unlike the
  `requiresTagging(operationType)` gate on the upsert path.
- Keys that tag as new are dropped: `filter(HoodieRecord::isCurrentLocationKnown)` (`:103`).

**Common failure:** deleting keys that do not exist costs a full index lookup and then writes
nothing. A DELETE whose Spark job looks identical in cost to an UPSERT but produces zero write
stats is this.

### DELETE_PARTITION

Not a data-writing operation at all. `SparkDeletePartitionCommitActionExecutor.execute()`:

- `context.parallelize(partitions).distinct().mapToPair(p -> (p, getAllExistingFileIds(p))).collectAsMap()`
  — one task per distinct partition, result collected to the driver
  (`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/SparkDeletePartitionCommitActionExecutor.java:63-67`).
- Write statuses are **empty** (`:71`); the whole effect is `partitionToReplaceFileIds` on a
  `replacecommit`.
- Pre-flight check: `DeletePartitionUtils.checkForPendingTableServiceActions` (`:58`) — this is
  what raises when a pending compaction or clustering touches a partition you are dropping.
- If the requested instant file is missing it throws
  `HoodieDeletePartitionException("Commit instant was not created for instant …")` (`:76-80`).

---

## 5. COMPACTION (MOR)

```
  SCHEDULING (driver)
    needCompact(trigger strategy) ?
        |  yes
        v
    plan generator -> strategy.orderAndFilter -> HoodieCompactionPlan
        |
    write .compaction.requested to timeline
        |
  EXECUTION
        v
    context.parallelize(operations)  <- ONE TASK PER FILE GROUP
        .map(compact) .flatMap
        |
    commit as a `commit` action on the MOR timeline
```

### Scheduling

`ScheduleCompactionActionExecutor.needCompact` (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java:197-250`)
dispatches on `hoodie.compact.inline.trigger.strategy` (`HoodieCompactionConfig.java:105`):

| Strategy | Meaning | Source |
|---|---|---|
| `NUM_COMMITS` | ≥ N delta commits since last **completed** compaction | `CompactionTriggerStrategy.java:28-30` |
| `NUM_COMMITS_AFTER_LAST_REQUEST` | ≥ N delta commits since last completed **or requested** compaction | `:32-35` |
| `TIME_ELAPSED` | ≥ N seconds since last compaction | `:37-39` |
| `NUM_AND_TIME` | both conditions | `:41-44` |
| `NUM_OR_TIME` | either condition | `:46-49` |

N and the seconds come from `hoodie.compact.inline.max.delta.commits`
(`HoodieCompactionConfig.java:90`) and the delta-seconds config, read at
`ScheduleCompactionActionExecutor.java:208-209`.

### Plan strategies

`hoodie.compaction.strategy` (`HoodieCompactionConfig.java:152`) selects a `CompactionStrategy`,
whose `orderAndFilter` picks which file groups make the plan
(`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/strategy/CompactionStrategy.java:101`).

- **`BoundedIOCompactionStrategy`** walks operations in order and accepts them while a running
  budget lasts: `long targetIORemaining = writeConfig.getTargetIOPerCompactionInMB();` then
  `targetIORemaining -= opIo` per operation (`BoundedIOCompactionStrategy.java:47-52`). When the
  budget is exhausted it stops (`:55-57`).
- **`LogFileSizeBasedCompactionStrategy`** (the default family) first *filters out* file groups
  whose total log size is below `hoodie.compaction.logfile.size.threshold`
  (`HoodieCompactionConfig.java:137`), sorts the rest by descending log size, then delegates to
  the bounded-IO filter (`LogFileSizeBasedCompactionStrategy.java:47-66`).

**`hoodie.compaction.target.io` is the lever.** Default `500 * 1024` MB = 512 GB
(`HoodieCompactionConfig.java:130-134`) — large enough that most users never hit it, which is
exactly why it is the right knob when compaction runs are too long. Lowering it caps the per-run
I/O (and therefore the runtime), at the cost of more runs. Note the budget check is `> 0` *before*
subtracting, so one oversized operation can still exceed the budget by its own size
(`BoundedIOCompactionStrategy.java:50-52`).

### Execution

```java
return context.parallelize(operations).map(
        operation -> compact(config, operation, compactionInstantTime, ...))
    .flatMap(List::iterator);
```

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java:154-156`

and `parallelize(List)` with no explicit parallelism uses `data.size()`
(`hudi-common/src/main/java/org/apache/hudi/common/engine/HoodieEngineContext.java:75-80`).

**So compaction task count = number of file groups in the plan. There is no compaction
parallelism config.** This is the single most useful fact in this section: a compaction stage
with 37 tasks has 37 file groups in its plan, and if 36 finish in a minute and one runs for an
hour, that one file group has a pathological log-to-base ratio.

Other execution facts:
- `maxInstantTime` bounds which log blocks are merged (`HoodieCompactor.java:127`, `:189-194`).
- `LOG_COMPACT` is a separate branch using a different reader context (`:134-145`).
- On the metadata table, the reader context is forced to Avro with HFile caching props
  (`:148-152`).

**Common failures:** executor OOM on a single task (one file group with an enormous log),
`HoodieCompactionException` wrapping a log-block read failure, and compaction scheduled but never
executed (async compaction configured but no async service running — the plan accumulates as
`.compaction.requested` instants and blocks archival, see §8).

---

## 6. CLUSTERING

```
  PLAN (driver)
    commits since last clustering >= hoodie.clustering.inline.max.commits ?
        |
    plan strategy -> clustering groups (each group = N input slices -> M output file groups)
        |
    write requested clustering / replacecommit instant
        |
  EXECUTION
    thread pool of min(#groups, hoodie.clustering.max.parallelism) threads
        each thread launches an INDEPENDENT Spark job:
            read group -> bulk-insert-style repartition -> write
        |
    union all groups' WriteStatus
        |
  COMMIT as replacecommit: new file groups REPLACE the input file groups
```

### Plan

`ClusteringPlanActionExecutor.createClusteringPlan` gates on
`config.getInlineClusterMaxCommits() > commitsSinceLastClustering`
(`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/ClusteringPlanActionExecutor.java:57-69`),
i.e. `hoodie.clustering.inline.max.commits` (`HoodieClusteringConfig.java:157`) or
`hoodie.clustering.async.max.commits` (`:164`).

The instant action is `CLUSTERING_ACTION` on timeline layout v2 and `REPLACE_COMMIT_ACTION`
otherwise (`ClusteringPlanActionExecutor.java:93-94`) — a version-dependent detail worth knowing
when reading a timeline by hand.

`PartitionAwareClusteringPlanStrategy.buildClusteringGroupsForPartition` fills groups up to
`hoodie.clustering.plan.strategy.max.bytes.per.group` and computes the number of output file
groups from `hoodie.clustering.plan.strategy.target.file.max.bytes`
(`PartitionAwareClusteringPlanStrategy.java:83-86`, `:106-108`), stopping at
`hoodie.clustering.plan.strategy.max.num.groups` (`:92-93`). Config keys are built from the prefix
`hoodie.clustering.plan.strategy.` (`HoodieClusteringConfig.java:51`) plus `small.file.limit`
(`:103-104`), `max.bytes.per.group` (`:201-202`), `max.num.groups` (`:210-211`),
`target.file.max.bytes` (`:217-218`), `sort.columns` (`:241-242`).

### Execution

`MultipleSparkJobExecutionStrategy.performClustering` creates a fixed thread pool of
`min(#inputGroups, hoodie.clustering.max.parallelism)` (default 15,
`HoodieClusteringConfig.java:171-172`) and submits one async job per group
(`MultipleSparkJobExecutionStrategy.java:108-132`), then unions the results
(`:134`).

**This is why clustering looks like many small Spark jobs rather than one big one.** Each group's
job is effectively a bulk_insert: `SparkSortAndSizeExecutionStrategy` builds a *new* write config
with `withBulkInsertParallelism(numOutputGroups)` and
`PARQUET_MAX_FILE_SIZE = hoodie.clustering.plan.strategy.target.file.max.bytes`, then calls the
bulk-insert helper (`SparkSortAndSizeExecutionStrategy.java:66-76` for the row path, `:90-97` for
the RDD path). So each group's write stage has exactly `numOutputGroups` tasks.

Row writer vs RDD is chosen by `hoodie.datasource.write.row.writer.enable`, read with a
string literal rather than the config constant (`MultipleSparkJobExecutionStrategy.java:112`).

Group reads use `jsc.parallelize(clusteringOps, readParallelism)` where the parallelism comes from
`hoodie.clustering.group.read.parallelism` (default 20, `HoodieClusteringConfig.java:181-182`;
used at `MultipleSparkJobExecutionStrategy.java:293`).

### Replacecommit semantics

The commit is a `replacecommit` carrying `partitionToReplaceFileIds`. The old file groups remain
on storage until cleaning removes them (§7) — so a clustering commit does **not** free space
immediately.

**Concurrency hazard worth calling out:** updates landing on a file group that has pending
clustering are handled by `hoodie.clustering.updates.strategy`
(`BaseSparkCommitActionExecutor.clusteringHandleUpdate`, `:118-169`). With
`SparkAllowUpdateStrategy` and `hoodie.clustering.rollback.pending.replacecommit.on.conflict`
false, Hudi returns early and defers to conflict resolution (`:135-137`). Otherwise it *rolls back
the pending clustering instant* mid-write (`:148-167`) — and the code itself flags the race:
"there could be race condition, for example, if the clustering completes after instants are
fetched but before rollback completed" (`:147`). A write that mysteriously rolled back someone
else's clustering is this path.

---

## 7. CLEANING

```
  PLAN
    getPartitionPathsToClean(earliestRetainedInstant)   <- incremental or full, per policy
        |
    per partition (parallelism = min(#partitions, hoodie.clean.parallelism)):
        getFilesToCleanKeepingLatest{Versions,Commits,ByHours}
        |
    write .clean.requested with the file list
        |
  EXECUTE
    mapPartitionsToPairAndReduceByKey(deleteFilesFunc, PartitionCleanStat::merge, parallelism)
        |
    write .clean completed
```

### Policies

`HoodieCleaningPolicy` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieCleaningPolicy.java:38-46`),
selected by `hoodie.clean.policy` (`HoodieCleanConfig.java:78`):

| Policy | Keeps | Config | Deletes |
|---|---|---|---|
| `KEEP_LATEST_COMMITS` (default) | file slices needed to serve the last N commits | `hoodie.clean.commits.retained`, default 10, alt key `hoodie.cleaner.commits.retained` (`HoodieCleanConfig.java:52-53`, `:105-108`) | older file slices, and replaced file groups eligible to clean |
| `KEEP_LATEST_FILE_VERSIONS` | last N versions of each file group | `hoodie.clean.fileversions.retained`, default 3 (`:56-57`, `:122-124`) | all older versions |
| `KEEP_LATEST_BY_HOURS` | file slices within the last N hours | `hoodie.clean.hours.retained`, default 24 (`:54-55`, `:113-115`) | older slices |

`getPartitionPathsToClean` treats `KEEP_LATEST_COMMITS` and `KEEP_LATEST_BY_HOURS` identically
(both can clean incrementally from the previous clean's `earliestCommitToRetain`) and
`KEEP_LATEST_FILE_VERSIONS` separately (`CleanPlanner.java:156-162`). Incremental planning is
gated by `hoodie.clean.incremental.enabled` (`HoodieCleanConfig.java:144`).

`getEarliestCommitToRetain` seeds from the previous clean's metadata and is capped by
`hoodie.clean.max.commits.to.clean` (`CleanPlanner.java:621-650`, config at
`HoodieCleanConfig.java:248`) — so a table that has not been cleaned in a long time does **not**
try to clean everything in one run.

### Savepoint interaction

Savepointed data files are never deleted:

- `KEEP_LATEST_FILE_VERSIONS` collects every savepoint's data files and skips any slice present in
  that set — the comment is explicit: "do not clean up a savepoint data file"
  (`CleanPlanner.java:364-371`, `:394-395`).
- `isFileSliceExistInSavepointedFiles` checks both the base file **and** every log file of the
  slice (`:338-348`) — an MOR slice is protected if *any* of its files is savepointed.
- Removing a savepoint invalidates incremental cleaning: "Since savepoints have been removed
  compared to previous clean, triggering clean planning for all partitions"
  (`:232`, logic at `:250-258`). So the first clean after deleting a savepoint is a **full**
  planning pass over every partition — a sudden slow clean right after a savepoint was dropped is
  this.

### Parallelism and failures

Plan: `int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());`
(`CleanPlanActionExecutor.java:145`). Execute:
`context.mapPartitionsToPairAndReduceByKey(..., deleteFilesFunc, PartitionCleanStat::merge, cleanerParallelism)`
(`CleanActionExecutor.java:160-161`), with the same `min` applied at `:148-149`. Config is
`hoodie.clean.parallelism` (`HoodieCleanConfig.java:168`).

**Common failures:** object-store delete throttling (S3 503s) in the execute stage; a very long
plan stage on a table with huge partition counts when incremental cleaning is off or was
invalidated; and the quiet failure mode where cleaning never runs at all
(`hoodie.clean.automatic` false, no async cleaner) so storage grows without bound.

---

## 8. ARCHIVAL

Archival moves completed instants out of the active timeline (`.hoodie/`) into the archived /
LSM timeline, then deletes the active-timeline files.

```
  getCommitInstantsToArchive()
      |- bail out if completedCommits <= maxInstantsToKeep
      |- compute earliestInstantToRetain = MIN of several candidates
      |- filter out everything at/after the first savepoint
      |- limit to (completedCommits - minInstantsToKeep)
      |
  getCleanAndRollbackInstantsToArchive(latestCommitInstantToArchive)
      |
  timelineWriter.write(...)  -> archived timeline
  deleteArchivedActions(...) -> remove from active timeline
  timelineWriter.compactAndClean(...)
```

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/TimelineArchiverV2.java:96-146`,
`:180-312`, `:314-328`.

### Min / max commits

`hoodie.keep.min.commits` (`HoodieArchivalConfig.java:79`) and `hoodie.keep.max.commits` (`:59`).
Nothing is archived until the completed-commits count exceeds `maxInstantsToKeep`
(`TimelineArchiverV2.java:183-185`); when it does, archival trims down to `minInstantsToKeep`
(`:310`).

**These get silently overridden.** `ArchivalUtils.getMinAndMaxInstantsToKeep` compares the
configured minimum against what cleaning needs. If the cleaner's `earliestCommitToRetain` implies
more commits must be kept than `hoodie.keep.min.commits` allows, Hudi raises both values and logs
a warning (`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/ArchivalUtils.java:72-105`):

```java
int minInstantsToKeepBasedOnCleaning =
    completedCommitsTimeline.findInstantsAfter(earliestCommitToRetain.get().requestedTime())
        .countInstants() + 2;
```
(`:73-75`)

So "I set `hoodie.keep.min.commits=20` but my timeline has 400 instants" is usually this
adjustment, and the fix is the *cleaning* config, not the archival config. The warning text names
the relevant cleaner key per policy (`:86-101`).

### What blocks archival

`earliestInstantToRetain` is the minimum over several candidates (`TimelineArchiverV2.java:188-290`):

1. the greatest completed commit before the earliest **pending** instant (`:190-213`) — so a single
   stuck inflight instant pins the whole timeline;
2. the earliest instant needed for pending **compaction** (`:219-226`);
3. the earliest instant needed for pending **clustering** (`:232-235`);
4. when the metadata table is enabled, the first commit modified after the MDT's latest
   compaction — and if the MDT has never compacted, **nothing is archived at all**: "Not archiving
   as there is no compaction yet on the metadata table" (`:240-251`);
5. for the MDT's own timeline, the earliest instant still live in the data table (`:257-281`).

### Savepoints stop archival

```java
Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
...
return !firstSavepoint.isPresent()
    || compareTimestamps(s.requestedTime(), LESSER_THAN, firstSavepoint.get().requestedTime());
```
`TimelineArchiverV2.java:294-306`

Archival stops at the **earliest** savepoint — not the latest. An old forgotten savepoint pins the
entire timeline after it. `hoodie.archive.beyond.savepoint` (`HoodieArchivalConfig.java:107`)
changes this to merely skipping the savepointed commits themselves (`TimelineArchiverV2.java:299-301`),
but that is a deliberate trade: the savepoint is no longer restorable once the surrounding
timeline is archived.

Clean and rollback instants are archived separately, bounded by the latest commit instant being
archived (`:159-178`) — the ASCII diagram in that comment (`:167-173`) is the clearest statement
of the rule.

Archival takes the state-change lock for its whole run (`:98-101`, `:142-144`) and increments
dedicated OOM / failure metrics (`:134-140`).

---

## 9. METADATA TABLE writes

### MDT is a MOR table

```java
HoodieTableMetaClient.newTableBuilder()
    .setTableType(HoodieTableType.MERGE_ON_READ)
    ...
    .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
    .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
```
`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadataWriter.java:559-572`

HFile base files, Avro-payload log blocks, keyed by a single record-key field.

### One deltacommit per data-table commit

Every MDT write — bulk insert during initialisation, upsert during steady state, and partition
deletes — commits with `DELTA_COMMIT_ACTION`:

`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metadata/SparkHoodieBackedTableMetadataWriter.java:169-170`
(bulk insert), `:181-182` and `:190-191` (upsert), `:206-210` (delete partitions, which commits as
`REPLACE_COMMIT_ACTION`).

The MDT deltacommit is written from `writeTableMetadata` inside the data table's commit, before
`saveAsComplete` (`BaseCommitActionExecutor.java:230`, `:233`) — so the MDT is updated first and
the data commit becomes visible second.

### MDT compaction

`compactIfNecessary` picks a compaction instant *below* the earliest instant that is pending on
the data table but completed on the MDT, minus 1 millisecond, so that instant can still be rolled
back (`HoodieBackedTableMetadataWriter.java:1643-1654`). It skips if a completed instant already
exists at that time (`:1660`), then schedules and runs synchronously (`:1664-1670`), optionally
followed by log compaction (`:1681-1691`). Both paths bump failure metrics and rethrow
(`:1672-1676`, `:1693-1697`).

### Partitions

`hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java`:

| Partition | Enum line | Written when |
|---|---|---|
| `files` | `:93` | always, when MDT is enabled — the file listing index |
| `column_stats` | `:109` | `hoodie.metadata.index.column.stats.enable`; per column, per file |
| `bloom_filters` | `:135` | `hoodie.metadata.index.bloom.filter.enable`; consumed by bloom tagging (§1) |
| `record_index` | `:166` | `hoodie.metadata.record.index.enable`; consumed by RLI tagging (§1) |
| `expression_index` | `:191` | per explicitly created expression index |
| `secondary_index` | `:213` | per explicitly created secondary index |
| `partition_stats` | `:247` | `hoodie.metadata.index.partition.stats.enable`; partition-level column ranges |

`ALL_PARTITIONS` (`:274`) shares the `files` partition path and is not a separate physical
partition.

**Common failures:** the MDT is the single most common cause of "my commit failed for no visible
reason" — a failure in `writeTableMetadata` aborts the data commit. RLI file-group count
under-provisioning shows up as slow tagging (§1). An MDT that has never compacted blocks the data
table's archival entirely (§8, candidate 4). MDT compaction failures are logged and rethrown, so
they surface as the data write's exception.

---

## 10. ROLLBACK

### Which strategy runs

```java
this.shouldRollbackUsingMarkers = shouldRollbackUsingMarkers && !instantToRollback.isCompleted();
```
`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/rollback/BaseRollbackPlanActionExecutor.java:65`

then

```java
if (shouldRollbackUsingMarkers) {
  return new MarkerBasedRollbackStrategy(table, context, config, instantTime);
} else {
  return new ListingBasedRollbackStrategy(table, context, config, instantTime, isRestore);
}
```
`:87-93`

**This is the fact most people get wrong.** `hoodie.rollback.using.markers`
(`ROLLBACK_USING_MARKERS_ENABLE`, `HoodieWriteConfig.java:320`) is necessary but not sufficient:
rolling back a **completed** instant — which is what a restore or a savepoint rollback does —
always falls through to the listing-based strategy regardless of the config, because the markers
for a completed commit have already been cleaned up.

### Marker-based

`MarkerBasedRollbackStrategy.getRollbackRequests` reads the marker paths and maps over them with
`parallelism = max(min(markerPaths.size(), hoodie.rollback.parallelism), 1)`
(`MarkerBasedRollbackStrategy.java:76-79`). Cost is proportional to the number of files the failed
write created — which is the whole point.

### Listing-based

```java
List<String> partitionPaths = FSUtils.getAllPartitionPaths(context, table.getMetaClient(), false);
int numPartitions = Math.max(Math.min(partitionPaths.size(), config.getRollbackParallelism()), 1);
...
return context.flatMap(partitionPaths, partitionPath -> { ... });
```
`ListingBasedRollbackStrategy.java:101-120`

**Why it is expensive on wide tables:** it lists **every partition of the table**, not just the
ones the failed write touched, and for each one lists files and matches them against the instant
being rolled back (`fetchFilesFromInstant`, `:123-130`). On a table with 100 000 partitions this
is 100 000 storage listings capped at `hoodie.rollback.parallelism` (default 100,
`HoodieWriteConfig.java:481-483`) — ~1 000 sequential listing batches. On an object store this is
minutes to hours, and it is a listing-bound workload, so adding executors past the parallelism cap
does nothing.

The MOR branch additionally reloads the active timeline per partition
(`ListingBasedRollbackStrategy.java:135`) and rewrites the action type when the instant was a
compaction or log-compaction (`:137-141`) — compaction instants carry the `commit` action, so this
override is needed to find the right files.

**Symptom to recognise:** a stage with exactly 100 tasks (or exactly
`hoodie.rollback.parallelism`) whose description is `…:Creating Listing Rollback Plan: <table>`
(`:105`), running far longer than the write it is rolling back.

---

## 11. The streaming ingestion loop (HoodieStreamer)

```
  syncOnce()                                    StreamSync.java:529
    |
  [1] initializeMetaClientAndRefreshTimeline    :535
    |
  [2] readFromSource                            :630
        resolveCheckpointToResumeFrom           :632
        fetchNextBatchFromSource (retry loop)   :638-653
        |  -> source read -> transformer -> schema reconciliation
        |  -> if checkpoint unchanged and !allowCommitOnNoCheckpointChange: RETURN NULL (no commit)
        |                                       :673-679
    |
  [3] initializeWriteClientAndRetryTableServices :556
        new schema seen -> reInitWriteClient     :564-579
    |
  [4] writeToSinkAndDoMetaSync                   :875
        startCommit                              :1065
        writeToSink                              :1090
        pre-commit validators -> rollback on failure  :955-962
        count records                            :966-971
        write-error gate (commitOnErrors)        :984-997
        writeClient.commit(...)                  :1000-1001
    |
  [5] source.onCommit(checkpointKey)             :1010-1011
    |
  [6] scheduleCompaction if async                :1013-1015
      runMetaSync if records > 0                 :1017-1021
    |
  [7] schemaProvider.refresh()                   :546-548
    |
  loop
```

| Stage | Where it stalls | Signature |
|---|---|---|
| Source read | `fetchFromSourceAndPrepareRecords` sets job description `…:Fetching next batch: <table>` (`:658`). Source timeouts are retried `cfg.maxRetryCount` times with `cfg.retryIntervalSecs` sleeps (`:638-652`) | repeated `HoodieSourceTimeoutException` in the log, each followed by a sleep; the job appears hung |
| No-op cycle | If the checkpoint did not move and `allowCommitOnNoCheckpointChange` is false, the whole cycle returns `null` and nothing commits (`:672-679`) | "No new data, source checkpoint has not changed" in the log; empty-data metrics bumped (`:677`); a streamer that looks alive but never commits |
| Schema change | Any new source or target schema triggers `reInitWriteClient` (`:564-579`) | a pause between batches with "Seeing new schema" logged (`:570`); repeated re-init every batch means the schema is oscillating |
| Row writer eligibility | Only `BULK_INSERT` + a `ROW`-typed source + `hoodie.streamer.write.row.writer.enable` (default **false**) (`:694-697`) | users expecting the row-writer fast path on upsert never get it |
| Spark record type guard | Spark record type + MOR + non-bulk-insert + non-parquet log block ⇒ `UnsupportedOperationException("Spark record only support parquet log.")` (`:660-665`) | immediate hard failure at batch start |
| Write | the full §1/§2 pipeline | see those sections |
| Validators | failure rolls back the instant and throws `HoodieStreamerWriteException` (`:956-962`) | write succeeded, then a rollback immediately after |
| Write errors | `commitOnErrors=false` (default) ⇒ log top N errors, **roll back**, throw (`:988-996`); `true` ⇒ warn and commit (`:985-987`) | repeated write-then-rollback cycles with no forward progress |
| Checkpoint commit | `source.onCommit(checkpointKey)` runs **after** the Hudi commit (`:1010-1011`) | a crash between the two re-reads the batch; Hudi's own checkpoint in commit metadata is the source of truth |
| Table services | async compaction scheduled only after a successful commit (`:1013-1015`) | compaction plans accumulate if the async executor is not running |
| Meta sync | skipped entirely when `totalSuccessfulRecords == 0` unless `forceEmptyMetaSync` (`:1017-1021`) | catalog drifts behind on low-volume tables |

---

## Read paths (lower detail)

Enough to orient a failed *query*. This section is deliberately shallower than the write sections.

`hoodie.datasource.query.type` (`DataSourceOptions.scala:59-67`) takes `snapshot` (default),
`read_optimized`, or `incremental` (`:56-58`).

| Query type | Reads | Typical failure |
|---|---|---|
| **snapshot** | CoW: latest base files. MOR: latest base file **merged with** its log files at read time | On MOR this is where un-compacted logs hurt: read latency grows with log size per file slice. Executor OOM in a scan stage on an MOR table usually means a file slice with a very large log set — fix compaction (§5), not the reader. |
| **read_optimized** | base files only; log files ignored | Cheap and stable, but returns stale data on MOR between compactions. A user reporting "my upsert didn't show up" on an MOR table is very often on this query type. |
| **incremental** | records between a begin and end instant | Fails when the begin instant has been **archived** (§8) — the timeline no longer has the commit metadata needed to resolve the range. The fix is archival/cleaning retention, not the reader. |
| **time travel** | `as.of.instant` (`TIME_TRAVEL_AS_OF_INSTANT`, `DataSourceOptions.scala:184`, aliasing `HoodieCommonConfig.TIMESTAMP_AS_OF`) | Fails when the requested instant's file slices have been **cleaned** (§7). Savepoints are the mechanism for guaranteeing a time-travel point survives. |

Data skipping (`hoodie.enable.data.skipping`, `DataSourceOptions.scala:187`) prunes files using
the metadata table's `column_stats` / `partition_stats` partitions (§9). If it is on and those
partitions do not exist, pruning silently does nothing — a query that is slower than expected with
data skipping "enabled" may simply have no index to skip with.

---

## Cross-cutting lookup: Spark stage symptom → likely Hudi phase

| Spark symptom | Likely phase | Confirm by | Typical cause |
|---|---|---|---|
| Stage input record count is 10–1000× the batch size | **Bloom index fan-out** (`explodeRecordsWithFileComparisons`) | `HoodieBloomIndex.java:300-310`; job description contains `Tagging:` | Random/UUID keys defeat range pruning; every record compared against every file group |
| Executor OOM during tagging, many small tasks | **Bloom index probe** (`HoodieSparkBloomIndexCheckFunction`) | `SparkHoodieBloomIndexHelper.java:173` | Too many keys per bucket; raise `hoodie.bloom.index.parallelism` or lower `hoodie.bloom.index.keys.per.bucket` |
| **Driver** OOM right after tagging finishes | **`buildProfile` `countByKey`** | `BaseSparkCommitActionExecutor.java:284-288`; description `Building workload profile:` | One map entry per `(partitionPath, fileId)` touched; very many file groups receiving updates |
| Task count unrelated to any config, write stage, long tail | **`UpsertPartitioner` buckets** | `UpsertPartitioner.java:336-338`; description `Doing partition and writing data:` | Skewed bucket; one file group receiving most updates |
| Executor OOM in the write stage, `HoodieUpsertException: Error upserting bucketType UPDATE for partition :N` | **Merge handle** | `BaseSparkCommitActionExecutor.java:413-417` | Too many records into one file group; reduce `hoodie.parquet.max.file.size` or fix key skew |
| Stage reads thousands of base files for a tiny batch | **SIMPLE / GLOBAL_SIMPLE index** | `HoodieSimpleIndex.java:146-149`; task count = number of base files | Wrong index for the workload; move to bloom, bucket, or RLI |
| Task count is an exact round number with no matching config (e.g. 64, 128) | **RLI lookup** — one task per MDT record-index file group | `SparkMetadataTableGlobalRecordLevelIndex.java:127-139` | RLI under-provisioned; each task looks up too many keys |
| RLI table suddenly scanning base files | **RLI fallback to SIMPLE / GLOBAL_SIMPLE** | `SparkMetadataTableGlobalRecordLevelIndex.java:81-94` ("Record index not initialized") | `record_index` partition missing or not yet built |
| Tagging stage has no shuffle at all | **Bucket index** | `HoodieBucketIndex.java:81-91` | Expected — look elsewhere for the slowdown |
| Stage with exactly 200 tasks deleting files | **`finalizeWrite` marker reconciliation** | `HoodieWriteConfig.java:561-563`; description `Delete all partially written files:` | Speculative execution / task retries produced duplicate files |
| `HoodieDuplicateDataFileDetectedException` | **marker reconciliation with fail-on-duplicate** | `HoodieTable.java:828-830` | `hoodie.fail.on.duplicate.data.file.detection` is on and retries produced extras |
| Stage with exactly 100 tasks, description `Creating Listing Rollback Plan` | **Listing-based rollback** | `ListingBasedRollbackStrategy.java:101-105`; `HoodieWriteConfig.java:481-483` | Rolling back a **completed** instant, or markers disabled; cost ∝ total partitions |
| One compaction task runs for hours while the rest finish fast | **Compaction, one pathological file group** | `HoodieCompactor.java:154-156` — one task per file group | A file group with a huge log-to-base ratio; cap with `hoodie.compaction.target.io` |
| Many small concurrent Spark jobs during a replacecommit | **Clustering groups** | `MultipleSparkJobExecutionStrategy.java:108-134` | Expected; concurrency is `hoodie.clustering.max.parallelism` (default 15) |
| Single very long shuffle during an initial load | **`GLOBAL_SORT` bulk insert** | `GlobalSortPartitioner.java:54-59` | Switch to `PARTITION_SORT` if global file sizing is not required |
| Bulk insert produces huge or tiny files | **`NONE` sort mode** — input partitioning passed through | `NonSortPartitioner.java:60-66` | Set `hoodie.bulkinsert.shuffle.parallelism` or change sort mode |
| Most data on one executor during bulk insert | **`PARTITION_PATH_REPARTITION[_AND_SORT]` with skewed partitions** | `BulkInsertSortMode.java:40-51` | Documented limitation of those modes |
| Task count = number of table partitions, before the write stage | **`UpsertPartitioner` small-file probe** | `UpsertPartitioner.java:289`; description `Getting small files from partitions:` | High partition count; disable with `hoodie.parquet.small.file.limit <= 0` if acceptable |
| Clean stage slow and listing every partition right after a savepoint was deleted | **Incremental cleaning invalidated** | `CleanPlanner.java:232`, `:250-258` | Expected one-time full pass |
| Timeline grows without bound despite `hoodie.keep.min.commits` | **Archival blocked** | `TimelineArchiverV2.java:188-290`; `ArchivalUtils.java:72-105` | A stuck inflight instant, a pending compaction/clustering, an old savepoint, an MDT that never compacted, or cleaning retention overriding the archival config |
| Nothing archives, log says "no compaction yet on the metadata table" | **MDT never compacted** | `TimelineArchiverV2.java:244-246` | MDT compaction not running |
| Commit fails with no visible data-write error | **Metadata table write** | `BaseCommitActionExecutor.java:230` runs before `:233` | MDT deltacommit or MDT compaction failed |
| `HoodieWriteConflictException` at commit time | **Concurrency conflict resolution** | `BaseCommitActionExecutor.java:204-205` | Overlapping concurrent writers; expected under OCC |
| A write mysteriously rolled back someone else's clustering | **`clusteringHandleUpdate` rollback branch** | `BaseSparkCommitActionExecutor.java:148-167` (race documented at `:147`) | `hoodie.clustering.rollback.pending.replacecommit.on.conflict` is on |
| Streamer alive but never commits | **Checkpoint unchanged** | `StreamSync.java:672-679` | No new source data, or `allowCommitOnNoCheckpointChange` off |
| Streamer commits then immediately rolls back, repeatedly | **Write-error gate or pre-commit validator** | `StreamSync.java:956-962`, `:988-996` | Records failing to write; `commitOnErrors=false` |
| MOR query slow / OOM in a scan stage | **Snapshot read merging large logs** | §5, §"Read paths" | Compaction not keeping up |
| Incremental query fails to resolve its begin instant | **Begin instant archived** | §8 | Retention too aggressive for the consumer's lag |
| Time-travel query fails on a valid-looking instant | **File slices cleaned** | §7 | Use a savepoint to pin the point in time |
| Stage has no job description at all | Not instrumented, or lazily evaluated outside a `setJobStatus` block | `HoodieSparkEngineContext.java:237-239` | Fall back to task count and shuffle shape |

---

## Coverage notes

Written against this checkout. Operations 1, 2, 3, 5, 7, 8, 10 and 11 are covered at full depth
with per-phase source citations. Operations 4 (DELETE / DELETE_PARTITION), 6 (CLUSTERING) and 9
(METADATA TABLE) are covered at phase-and-config depth — their plan-strategy internals and, for
the MDT, the per-partition record-generation code, go deeper than this catalog does. The read-path
section is deliberately orientation-only.

Claims marked **[inferred]** are: the exact rendering of job descriptions in the Spark UI Stages
tab; the absence of `RDD.setName` meaning generic RDD names in the DAG; and the visual distinction
between the row-writer and record bulk-insert paths in the UI. Everything else is read from the
files cited.
