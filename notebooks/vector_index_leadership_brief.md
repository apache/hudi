# Hudi Vector Search: Leadership Brief

This note explains, in short form, how Hudi vector search works, what the Metadata Table (MDT) stores, how IVF and RaBitQ help, and what happens during insert, update, delete, and compaction.

## Diagram Files

Generated PNG diagrams for presentation use:

- `/Users/r0c0ezv/.cursor/projects/Users-r0c0ezv-hudi/assets/hudi-vector-ivf-rabitq-architecture.png`
- `/Users/r0c0ezv/.cursor/projects/Users-r0c0ezv-hudi/assets/hudi-vector-lire-mdt-maintenance.png`
- `/Users/r0c0ezv/.cursor/projects/Users-r0c0ezv-hudi/assets/hudi-vector-storyboard-overview.png`

## Executive Summary

- The main Hudi table stores the real business rows and the real vector column.
- The Metadata Table (MDT) stores a lightweight search directory on the side.
- K-Means groups similar vectors into clusters and produces cluster centers called centroids.
- IVF uses those centroids to narrow the search to only likely file groups instead of scanning the whole table.
- RaBitQ adds a very compact approximation step so candidate rows can be ranked faster before an exact final check.
- LIRE keeps the index fresh incrementally, so Hudi updates only touched mappings instead of rebuilding the full index every time.

## Plain-English Flow

1. Create a Hudi table with a vector column.
2. Run `CREATE INDEX ... USING VECTOR`.
3. Hudi runs K-Means to group similar vectors.
4. Hudi stores the resulting centroids and mappings in MDT.
5. At search time, Hudi compares the query to the centroids first.
6. Hudi reads only the file groups that belong to the nearest clusters.
7. Hudi optionally uses RaBitQ to score those candidates quickly.
8. Hudi does an exact rerank on the final small set and returns the top results.

## What MDT Stores

MDT stores the shared vector-search directory:

- `__centroids__`: the cluster centers produced by K-Means.
- `assignments`: row key to cluster id.
- `fg_mapping`: cluster id to file group ids.
- quantizer metadata: whether the index is using flat scan or RaBitQ-style refinement metadata.

This lets Hudi answer two important questions quickly:

- Which cluster is this query closest to?
- Which file groups should be read for that cluster?

## Example: Create a Small Vector Table

Assume the table has four rows:


| Row  | Vector              | Product Type |
| ---- | ------------------- | ------------ |
| `r1` | `[0.1, 0.2, 0.1]`   | shoe         |
| `r2` | `[0.2, 0.1, 0.2]`   | shoe         |
| `r3` | `[9.8, 10.1, 9.9]`  | laptop       |
| `r4` | `[10.2, 9.9, 10.0]` | laptop       |


K-Means with `K = 2` may produce:

- Cluster 0 centroid: `[0.15, 0.15, 0.15]`
- Cluster 1 centroid: `[10.0, 10.0, 9.95]`

Assignments then become:


| Row  | Cluster |
| ---- | ------- |
| `r1` | `0`     |
| `r2` | `0`     |
| `r3` | `1`     |
| `r4` | `1`     |


If rows `r1` and `r2` live in file group `FG1`, and rows `r3` and `r4` live in `FG2`, then MDT stores:

- `cluster 0 -> FG1`
- `cluster 1 -> FG2`

This is the key reason IVF helps. The query does not need to read every file group.

## Example: Search

Suppose the query vector is:

`[0.12, 0.18, 0.11]`

Hudi compares it with the centroids:

- close to cluster 0
- far from cluster 1

So Hudi:

1. picks cluster 0,
2. looks up MDT,
3. finds `cluster 0 -> FG1`,
4. reads `FG1` only,
5. compares candidates in `FG1`,
6. returns the nearest rows.

Without MDT-backed IVF, the table scan would be much wider.

## How Clusters Are Read and Broadcast

At query time or maintenance time:

1. the Spark driver reads centroids and mappings from MDT,
2. the driver broadcasts those centroids to executors,
3. executors process rows in parallel using the same cluster centers,
4. each executor can decide which cluster a row belongs to without asking the driver again.

Leadership takeaway:

- MDT is the shared directory,
- broadcast makes the cluster centers available everywhere,
- workers can process data in parallel at scale.

## How RaBitQ Helps

IVF narrows the search to likely file groups. RaBitQ helps inside those candidates.

In simple terms:

- the original vector is the precise data,
- RaBitQ builds a compact sketch of each vector,
- Hudi can score those sketches much faster than full vectors,
- then Hudi does an exact check only on the top few.

So the combined strategy is:

- IVF: avoid scanning the whole table,
- RaBitQ: avoid doing full exact math on every candidate,
- exact rerank: preserve final answer quality.

## Example: Insert

A new row arrives:


| Row  | Vector               |
| ---- | -------------------- |
| `r5` | `[0.16, 0.14, 0.15]` |


Because the new vector is near cluster 0:

- it is assigned to cluster 0,
- MDT `assignments` adds `r5 -> 0`,
- MDT `fg_mapping` is refreshed if the new row lands in a new file group,
- if RaBitQ hidden columns are enabled, the compact code is also materialized for that row.

Business meaning:

- new data becomes searchable without rebuilding the whole index.

## Example: Update

Assume row `r2` changes from:

- old vector: `[0.2, 0.1, 0.2]`

to:

- new vector: `[10.1, 10.0, 9.8]`

This row may move from cluster 0 to cluster 1.

What Hudi updates:

- old assignment `r2 -> 0` is replaced with `r2 -> 1`,
- old cluster-to-file-group references are refreshed if the row lands in a different file group,
- the fast-scoring representation is refreshed if RaBitQ is enabled,
- only the touched mappings need to change.

Leadership meaning:

- an updated row can move to a different similarity neighborhood,
- Hudi repairs the affected index entries instead of rebuilding the entire index.

## Example: Delete

Assume row `r3` is deleted.

What Hudi does:

- the assignment for `r3` is removed or tombstoned,
- the row stops participating in future search results,
- if a file group no longer has rows for that cluster, MDT `fg_mapping` is refreshed,
- any optional fast-scoring data for the row is no longer considered valid.

Leadership meaning:

- deleted rows stop being returned by the search index,
- Hudi keeps the search directory aligned with table reality.

## Example: Compaction or Clustering

Compaction or clustering may rewrite file groups even when business rows do not change.

That means:

- the row may stay in the same cluster,
- but the physical file group can change,
- MDT `fg_mapping` must be refreshed to point to the new file groups.

Example:

- before compaction: `cluster 1 -> FG2`
- after compaction: data is rewritten into `FG5`
- MDT is refreshed to: `cluster 1 -> FG5`

Leadership meaning:

- the data layout can change for storage optimization,
- MDT keeps the vector index aligned with the new storage layout.

## How LIRE Helps

LIRE means lightweight incremental repair and rebalancing.

The practical value is:

- small changes stay small,
- only impacted clusters and file groups are updated,
- Hudi avoids expensive full index rebuilds for routine table activity.

This is especially important for:

- large tables,
- frequent upserts,
- ongoing deletes,
- periodic compaction and clustering.

## Short Takeaway for Leadership

- The main table stores the real vectors.
- MDT stores the search directory.
- IVF reduces how much data must be read.
- RaBitQ reduces how much exact scoring must be done.
- LIRE keeps the index fresh incrementally as the table changes.
- The result is faster search with a maintenance model that fits Hudi's normal write lifecycle.
