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

# RFC-104 Proposal A: Vector Index Write Path (MDT-Only)

## Scope

This proposal defines how Hudi writers produce vector-index metadata in the metadata table. Bootstrap, rebuild, compaction, and LIRE-style maintenance are covered separately in [`rfc-104-bootstrap-maintenance.md`](./rfc-104-bootstrap-maintenance.md).

Out of scope for this proposal:

- hidden columns in base table parquet files
- dual storage modes (`hidden_columns`, `both`)
- query execution and ANN scoring

## Goals

1. Keep vector index writes transactionally aligned with Hudi commits.
2. Store vector assignments and RaBitQ payloads in the MDT vector index partition.
3. Avoid base-table schema augmentation.
4. Keep write-side records compatible with the read-path POC already implemented in this branch.

## Metadata Partition

Each vector index uses a metadata partition named:

```text
vector_index_<index_name>
```

The write path emits these record families:

| Key family | Purpose |
|---|---|
| `A|<record_key>` | Latest assignment for a record. |
| `P|<generation>|<cluster>|<shard>|<record_key>` | RaBitQ posting payload for approximate scoring. |
| `C|<generation>|<cluster>` | Cluster manifest with shard counts, file groups, and vector counts. |
| `__fg__/<cluster>/<partition_path>` | Legacy-compatible cluster-to-file-group mapping used by the current Spark pruning POC. |

Index-level metadata rows (`__centroids__`, `__quantizer__`, `__manifest__`, `M|<generation>`) are produced by bootstrap/rebuild and described in the lifecycle document.

## Write-Time Record Shape

For inserts and vector-changing updates, the writer records:

```text
record_key
generation_id
cluster_id
shard_id
file_group_id
partition_path
binary_code
scalar
last_updated_ts
```

`binary_code` is the packed RaBitQ code (`ceil(dimension / 8)` bytes). `scalar` is omitted when the index is configured with `vector.rabitq.assume_normalized=true`.

## Write Flow

![Write Commit Flow](diagrams/06-write-commit-flow.png)

For each incoming vector:

1. Resolve the active vector index definition from table index metadata.
2. Load or reuse active centroids and quantizer metadata.
3. Assign the vector to the nearest centroid.
4. Compute `shard_id` from the record key and cluster shard count.
5. Encode the vector with `RaBitQEncoder`.
6. Write/update assignment, posting, and affected cluster/file-group mapping rows as needed.

These writes happen through the metadata table commit path so readers see a consistent Hudi timeline state.

## RaBitQ Encoding

RaBitQ is the default quantizer for the MDT posting payload. It is training-free after index creation because the random rotation matrix is derived deterministically from `(dimension, seed)` rather than stored as a learned codebook.

For each vector `v`, the write path computes:

1. `norm = ||v||` unless vectors are configured as already normalized.
2. `v_normalized = v / norm`.
3. `v_rotated = R @ v_normalized`, where `R` is regenerated from the stored seed.
4. `binary_code = sign(v_rotated)` packed into `ceil(dimension / 8)` bytes.
5. `scalar = norm`, omitted when `assume_normalized=true`.

The MDT posting row stores `binary_code` and optional `scalar`; the base table schema is not changed. For a 768-dimensional vector, the packed code is 96 bytes per posting.

## Current POC Implementation to Carry Forward

The branch already contains the core MDT-native write pieces:

- `HoodieMetadataPayload`
  - vector entry types: `ASSIGNMENT`, `POSTING`, `CLUSTER`, `FG_MAPPING`, `MANIFEST`, `CENTROIDS`, `QUANTIZER`
  - factory methods for vector assignment, delete, posting, cluster manifest, fg mapping, and quantizer records
- `HoodieTableMetadataUtil`
  - key builders for `A|`, `P|`, `C|`, `M|`, and legacy `__fg__/` keys
- raw key wrappers
  - `VectorAssignmentRawKey`
  - `VectorPostingPrefixRawKey`
  - `VectorClusterRawKey`
  - `VectorGenerationManifestRawKey`
- `RaBitQEncoder`
  - deterministic random rotation from seed
  - packed binary code generation
  - scalar handling for unnormalized vectors

`SparkHoodieBackedTableMetadataWriter#getVectorIndexRecords` currently produces these records during bootstrap. The same record shapes should be reused by incremental writes.

## Delete Semantics

Deletes write tombstone metadata for the affected assignment and posting rows. The MDT compaction path is responsible for collapsing older versions and removing obsolete entries.

## Consistency

The write path does not store `cluster_id`, `binary_code`, or `scalar` as base-table hidden columns. The metadata table is the sole storage layer for vector index payloads, and Hudi's timeline determines visibility.
