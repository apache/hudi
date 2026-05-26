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

# RFC-104: Native Vector Search Support in Apache Hudi

## Proposers

- @Revanth Chandupatla
- @Rahil
- @Surya

## Approvers

- @<approver-github-username>

## Status

Issue: <Link to GH feature issue>

> Please keep the status updated in `rfc/README.md`.

---

## Table of Contents

- [Abstract](#abstract)
- [Implementation](#implementation)
  - [Write Path](#write-path)
  - [Read Path](#read-path)
- [Supporting Documents](#supporting-documents)
- [Rollout/Adoption Plan](#rolloutadoption-plan)
- [Test Plan](#test-plan)
- [Alternatives](#alternatives)
- [References](#references)

---

## Abstract

This RFC proposes native vector similarity search support in Apache Hudi, enabling approximate nearest-neighbor (ANN) queries on `VECTOR` columns stored in Hudi tables.

The design is **metadata-table native**:

- base table files continue to store the authoritative `VECTOR` column
- the Hudi metadata table stores routing metadata, vector assignments, RaBitQ postings, quantizer metadata, and active-generation manifests
- query engines use MDT to prune and rank candidate records before exact re-ranking

Hidden-column storage is not part of this proposal. Earlier hidden-column experiments are treated as transitional implementation detail and are not the accepted RFC direction.

## Architecture Overview

![Architecture Overview](diagrams/01-architecture-overview.png)

At the highest level, Hudi keeps the full vector value in the base table and stores the ANN routing/index payload in MDT. Reads use MDT first to narrow the search space, then consult the base table only for exact vector re-ranking.

---

## Implementation

RFC-104 is split into two implementation proposals: write path and read path. Bootstrap, rebuild, compaction, and lifecycle maintenance are separated into a supporting document so the core RFC stays focused on the two data paths reviewers need to evaluate.

### Write Path

The write path is responsible for producing and maintaining MDT-native vector index records. The accepted write-path shape is documented in [`rfc-104-write-path.md`](./rfc-104-write-path.md).

At a high level, writers produce records in the `vector_index_<name>` metadata partition:

- `__centroids__` and `__quantizer__` for index-level metadata
- `__manifest__` and `M|<generation>` for active generation management
- `C|<generation>|<cluster>` for cluster manifest rows
- `P|<generation>|<cluster>|<shard>|<record_key>` for RaBitQ posting rows
- `A|<record_key>` for record-to-cluster assignments
- `__fg__/<cluster>/<partition_path>` while the current POC still carries legacy-compatible file-group mapping rows

Current POC implementation included in the proposal:

- `SparkHoodieBackedTableMetadataWriter#getVectorIndexRecords`
- `HoodieMetadataPayload` vector entry types
- `HoodieTableMetadataUtil` vector key builders
- `VectorGenerationManifestRawKey`, `VectorClusterRawKey`, `VectorPostingPrefixRawKey`, `VectorAssignmentRawKey`
- `RaBitQEncoder`

### Read Path

The read path is responsible for query-time use of vector index metadata. Query-related details are documented in [`rfc-104-read-path.md`](./rfc-104-read-path.md).

At a high level, the read path:

1. parses query vector options or future SQL vector predicates
2. loads centroids and active generation metadata from MDT
3. probes the nearest IVF clusters
4. prunes candidate file groups through MDT cluster metadata
5. prefix-scans MDT posting rows for the selected cluster shards
6. computes RaBitQ approximate scores
7. keeps a bounded candidate shortlist
8. performs exact re-ranking from base table vectors

Current POC implementation included in the proposal:

- `VectorIndexSupport` for Spark datasource file pruning
- `VectorIndexPruner` for engine-agnostic centroid probing and file-group pruning
- `VectorIndexMdtSearchUtils` for MDT posting lookup, RaBitQ scoring, record-location attachment, and top-k reduction
- read options in `DataSourceReadOptions`:
  - `hoodie.datasource.read.vector.index.name`
  - `hoodie.datasource.read.vector.query.vector`
  - `hoodie.datasource.read.vector.query.nprobes`

---

## Supporting Documents

- [`rfc-104-write-path.md`](./rfc-104-write-path.md): MDT-native vector index write path.
- [`rfc-104-read-path.md`](./rfc-104-read-path.md): MDT-native vector query/read path.
- [`rfc-104-bootstrap-maintenance.md`](./rfc-104-bootstrap-maintenance.md): bootstrap, rebuild, compaction, LIRE-style maintenance, cleaner, and generation lifecycle.
- [`vector-mdt-rabitq-poc.md`](./vector-mdt-rabitq-poc.md): brief inventory of the current MDT RaBitQ POC.

---

## Tasks and Plan

1. Land MDT payload/key families and bootstrap generation.
2. Enable Spark file pruning with vector read options.
3. Wire MDT posting shortlist generation into Spark vector query execution.
4. Add exact re-ranking integration and SQL function support.
5. Add lifecycle jobs for rebuild, compaction/rebalancing, and validation.

---

## Test Plan

- Unit tests for `RaBitQEncoder`, distance metrics, key encoding/decoding, and vector payload construction.
- Metadata bootstrap tests validating centroids, assignments, manifests, cluster rows, and posting rows.
- Spark datasource tests for vector read options and file pruning.
- MDT posting lookup tests for prefix scans, top-k reduction, and record-index location attachment.
- Integration tests comparing ANN results against brute-force exact search for recall and correctness.

---

## Alternatives

### Hidden Columns in Base Table Files

Rejected for this RFC. Hidden columns couple vector index payloads to base table file schema and complicate lifecycle operations. The accepted direction keeps RaBitQ payloads inside MDT posting rows.

### Full Vector Duplication in MDT

Rejected for the primary design. MDT stores compact routing and quantized posting payloads, not a full duplicate of every vector.

### HNSW-First Index

Deferred. IVF + RaBitQ maps cleanly to Hudi metadata and file-group pruning. HNSW over centroids can be considered later for very large cluster counts.

---

## References

1. SPFresh: Enabling Efficient and Near-Real Vector Search in SPANN-based Systems, SOSP 2023.
2. LanceDB cloud-native vector search discussions for coarse-to-fine ANN search over object storage.
3. Apache Hudi metadata table documentation.
4. Apache Hudi RFC-103: Secondary Index for Vector Data.
