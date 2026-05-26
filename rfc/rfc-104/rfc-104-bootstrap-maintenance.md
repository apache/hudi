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

# RFC-104 Supporting Document: Bootstrap and Maintenance

## Scope

This document covers vector-index lifecycle work that is outside the steady-state read
path and write path:

- index scheduling
- bootstrap
- rebuild
- compaction/rebalancing
- update/delete maintenance
- clustering interaction
- cleaner and generation lifecycle

The design is MDT-only. The vector index does not create hidden columns in base-table
Parquet files and does not use separate `assignments/` or `rabitq_codes/` metadata
sub-partitions. Each index owns one dynamic MDT partition named `vector_index_<name>`.

## Index Scheduling

Vector index creation reuses Hudi's metadata index scheduling flow.

Current POC implementation:

- `HoodieSparkIndexClient#createVectorIndex`
- `ScheduleIndexActionExecutor#resolvePartitionName`
- `HoodieBackedTableMetadataWriter` vector index initialization path

The scheduler registers the dynamic metadata partition (`vector_index_<name>`) and
schedules an index action against a completed data timeline instant.

Each vector index is independent. A table with `embedding` and `image_embedding` indexes
gets two MDT partitions, for example:

```text
.hoodie/metadata/vector_index_embedding_idx/
.hoodie/metadata/vector_index_image_idx/
```

## Bootstrap Flow

Bootstrap creates the first complete vector index generation:

1. Read latest base files from the data table.
2. Extract record key, partition path, file name, and source vector column.
3. Convert `VECTOR` values into Spark ML vectors.
4. Train IVF centroids with KMeans.
5. Assign each record to a centroid.
6. Compute file-group mappings and cluster populations.
7. Derive shard counts per cluster.
8. Encode each vector with `RaBitQEncoder`.
9. Write MDT records:
   - `__centroids__`
   - `__quantizer__`
   - `__manifest__`
   - `M|<generation>`
   - `C|<generation>|<cluster>`
   - `A|<record_key>`
   - `P|<generation>|<cluster>|<shard>|<record_key>`
   - `__fg__/<cluster>/<partition_path>`

Example generation layout:

```text
vector_index_embedding_idx/
├── __manifest__                         # active generation pointer
├── __centroids__                        # latest centroid blob for compatibility
├── __quantizer__                        # RaBitQ type, code width, seed
├── M|0000007B                           # immutable generation manifest
├── C|0000007B|00000000                  # cluster 0 metadata
├── C|0000007B|00000001                  # cluster 1 metadata
├── A|p001                               # record p001 assignment and file-group pointer
├── P|0000007B|00000000|0000|p001        # record p001 RaBitQ posting row
└── __fg__/00000000/amer/us              # cluster-to-file-group pruning row
```

Current POC implementation:

- `SparkHoodieBackedTableMetadataWriter#getVectorIndexRecords`
- `buildQuantizerRecord`
- `buildManifestRecord`
- `buildGenerationManifestRecord`
- `buildClusterManifestRecords`
- `buildPostingRecords`
- `buildFgMappingRecords`

## Generation Model

RaBitQ metadata is part of the generation contract. Each generation records the
quantizer type, packed code width, random seed, vector dimension, and `assume_normalized`
flag so readers can score postings without consulting base-table schema changes.

Posting rows are generation-scoped. A generation can be rebuilt independently and activated
by updating `__manifest__`.

```text
__manifest__         -> active generation id
M|0000007B           -> immutable generation metadata
C|0000007B|0000000A  -> cluster metadata
P|0000007B|...       -> posting rows for that generation
```

This keeps readers from mixing posting rows across rebuilds.

Generation activation is atomic from the reader's perspective:

```text
1. Build all M|, C|, A|, P|, and __fg__ rows for generation 0000007C.
2. Commit those rows to the MDT partition.
3. Update __manifest__ from 0000007B to 0000007C.
4. Readers that see 0000007C only scan P|0000007C|... rows.
```

Old generations remain available until the metadata cleaner removes them.

## Rebuild

`REFRESH INDEX` or a future rebuild operation creates a new generation from the latest table
snapshot, then atomically publishes that generation through `__manifest__`.

Old generations remain readable until cleaner policy permits removal.

Rebuild is preferred when the index needs a global change:

| Reason | Rebuild behavior |
|---|---|
| Change `numLists` / cluster count | Re-run KMeans and publish a new generation |
| Change RaBitQ seed or code width | Re-encode all vectors and publish a new generation |
| Repair suspected metadata drift | Recreate `A|`, `P|`, `C|`, and `__fg__` rows from the table snapshot |
| Major data distribution shift | Train fresh centroids and retire the old generation after retention |

## Compaction and Rebalancing

LIRE-style rebalancing is treated as metadata lifecycle work, not as part of the steady
read or write path.

### Problem

As new vectors arrive, they are assigned to existing centroids. Over time this can cause:

1. **Cluster imbalance**: some clusters contain many more vectors than others.
2. **Centroid drift**: a centroid no longer represents the current mean of its cluster.
3. **NPA violations**: a vector may be closer to a neighboring centroid than to its
   assigned centroid.

A full rebuild fixes all three but requires scanning all vectors. LIRE-style maintenance
is the incremental alternative: inspect affected clusters, update MDT rows, and publish
the refreshed metadata as a new generation or as a generation-local delta.

### MDT-Only Maintenance Model

The maintenance job may update:

| Row family | Maintenance action |
|---|---|
| `M|<generation>` | record the new generation's metadata and build instant |
| `C|<generation>|<cluster>` | update centroid bytes, vector counts, shard counts, and timestamps |
| `A|<record_key>` | update the current cluster and base-table file-group pointer for a record |
| `P|<generation>|<cluster>|<shard>|<record_key>` | update RaBitQ posting placement and payload |
| `__fg__/<cluster>/<partition_path>` | update cluster-to-file-group pruning summaries |
| `__manifest__` | publish the active generation |

The base table remains untouched. Exact vector values stay in the base table; only compact
index metadata changes in MDT.

```text
LIRE maintenance pass:
  read active generation manifest
  inspect affected C|, A|, P|, and __fg__ rows
  recompute centroid/assignment/posting changes
  write a replacement generation or generation-local update
  atomically publish the visible generation through __manifest__
```

### Triggers

| Trigger | Condition | Action |
|---|---|---|
| Size imbalance | `max_cluster / avg_cluster > threshold` | split or merge affected clusters |
| Centroid drift | `avg(dist(centroid, mean(vectors))) > threshold` | recompute affected centroids |
| NPA violations | `violations / vector_count > threshold` | reassign boundary vectors |
| Periodic fallback | every N maintenance windows | validate cluster health and repair drift |

### Cadence

There are two separate maintenance loops:

| Loop | Scope | Vector-index impact |
|---|---|---|
| Hudi MOR compaction | base-table log files to base files | unchanged by vector index |
| Vector-index maintenance | `vector_index_<name>` MDT rows | updates generations, assignments, postings, and pruning rows |

Hudi MOR compaction should remain tuned for normal table read/write performance. Vector
maintenance should be trigger-gated so it does not run after every table compaction.

Recommended default:

```text
Streaming / near-real-time:
  MOR compaction             : standard table policy, usually async
  vector-index maintenance   : trigger-gated on imbalance, drift, or NPA violations

Batch ingestion:
  MOR compaction             : after batch or table policy
  vector-index maintenance   : after large batches, plus periodic validation
```

### Concurrency

If a write commits while maintenance is building a new generation, the write can still use
the currently active generation. The next maintenance pass will fold those records into the
newer centroid layout if needed. This gives bounded pruning staleness but preserves query
correctness because final results are always re-ranked from base-table vectors.

## Update and Delete Maintenance

Updates and deletes change the set of vectors represented by the active index generation.
The MDT-only design records those changes as metadata rows.

### Inserts and Vector Updates

For an inserted record, or an update where the indexed vector changed, the writer computes:

```text
nearest cluster
RaBitQ binaryCode
optional scalar
fileGroupId
partitionPath
posting shard
```

It then writes or updates:

```text
A|<record_key>
P|<active_generation>|<cluster>|<shard>|<record_key>
__fg__/<cluster>/<partition_path>
```

The `A|` row is the latest assignment for the record. The `P|` row is the compact posting
used by read-time approximate scoring. The `__fg__` row keeps file-group pruning current.

If a vector update moves a record between clusters, the old posting must be invalidated or
excluded by generation visibility so readers do not score stale candidates. The exact
mechanism can be either a tombstone for the old `P|` key or a replacement generation; the
POC currently focuses on bootstrap-time posting generation.

### Non-Vector Updates

If only non-vector columns change, the vector index does not need a new assignment or
posting. The base-table file group pointer may still need refresh if the write moves the
record to a different file group.

### Deletes

When a record is deleted, the vector index must stop returning it as a candidate. The
delete path marks or removes the corresponding `A|` row and invalidates the active
generation's `P|...|<record_key>` row. File-group mapping rows are refreshed during MDT
compaction or the next maintenance pass.

## Clustering and Replace Commits

Hudi clustering can replace file groups. Because vector posting payloads and `A|` rows
carry `fileGroupId`, a replace commit must keep vector metadata aligned with the new file
groups before old file groups are cleaned.

The MDT-only design has two valid strategies:

| Strategy | Behavior |
|---|---|
| Affected-file-group refresh | Re-read vectors from replaced file groups, rewrite affected `A|`, `P|`, and `__fg__` rows |
| Generation rebuild | Build and publish a new generation after large clustering operations |

Small replace commits should use affected-file-group refresh. Large reclustering jobs can
prefer a generation rebuild because they already rewrite a broad portion of the table.

The important invariant is that committed vector metadata points to live base-table file
groups for every snapshot the metadata cleaner still serves.

## Cleaner Coordination

The metadata cleaner must retain vector generations long enough for valid table snapshots
to find matching metadata. This is the same broad invariant Hudi already needs for other
metadata indexes: metadata retention cannot be shorter than the table snapshots it serves.

Cleaner rules:

| Cleaner scenario | Vector-index requirement |
|---|---|
| Old base-table file slice removed | No action if the file group is still live |
| File group replaced by clustering | Refresh affected `A|`, `P|`, and `__fg__` rows before old file groups disappear |
| Old vector generation removed | Safe only after no retained table snapshot needs it |
| Time travel past metadata retention | Fails consistently with other MDT indexes |

The `__manifest__` row controls the active generation, but retained old generations allow
snapshot readers to find matching postings while the main table snapshot is still retained.

## Open Work

- finalize the exact tombstone/update mechanism for stale `P|...|<record_key>` rows during
  incremental vector updates
- decide whether LIRE publishes generation-local deltas or always publishes a full
  replacement generation
- add validation tooling to compare MDT postings against base table vectors
- implement affected-file-group refresh for clustering and replace commits
