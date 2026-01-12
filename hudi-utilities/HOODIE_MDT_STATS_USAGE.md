
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

# MetadataBenchmarkingTool Configuration Guide

## Overview

`MetadataBenchmarkingTool` is a utility for testing Hudi Metadata Table column statistics functionality. It creates a Hudi table, generates column statistics, and demonstrates data skipping capabilities.

## Configuration Parameters

### Required Parameters

| Parameter | Short Flag | Description | Example Value |
|-----------|------------|-------------|---------------|
| `--table-base-path` | `-tbp` | Base path where the Hudi table will be created | `/tmp/hudi_table` |
| `--num-cols-to-index` | `-num-cols` | Number of columns to index (1 for tenantID, 2 for tenantID & age) | `2` |
| `--col-stats-file-group-count` | `-col-fg-count` | Number of file groups for the column stats partition in metadata table | `10` |
| `--num-files-to-bootstrap` | `-nfb` | Number of files to create during bootstrap phase | `1000` |
| `--num-partitions` | `-np` | Number of date partitions to generate (starting from 2025-01-01) | `5` |

### Optional Parameters

| Parameter | Short Flag | Description | Usage | Default Value |
|-----------|------------|-------------|-------|---------------|
| `--mode` | `-m` | Benchmark mode: `BOOTSTRAP` (write only), `QUERY` (read only), `BOOTSTRAP_AND_QUERY` (both) | `--mode BOOTSTRAP` | `BOOTSTRAP_AND_QUERY` |
| `--num-files-for-incremental` | `-nfi` | Total number of files to create across incremental commits | `--num-files-for-incremental 200` | `0` |
| `--num-commits-for-incremental` | `-nci` | Number of incremental commits to distribute files across | `--num-commits-for-incremental 2` | `0` |
| `--partition-filter` | `-pf` | Partition filter predicate for querying (e.g., `"dt > '2025-01-01'"`) | `--partition-filter "dt > '2025-01-01'"` | `"dt = '2025-01-01'"` |
| `--data-filter` | `-df` | Data filter predicate for querying (e.g., `"age > 70"`). Can specify multiple filters separated by commas | `--data-filter "tenantID > 50000"` | `""` (uses default filters based on indexed columns) |
| `--props` | | Path to properties file containing Hudi configurations | `--props /path/to/config.properties` | None |
| `--hoodie-conf` | | Individual Hudi configuration (can be specified multiple times) | `--hoodie-conf hoodie.metadata.enable=true` | None |
| `--help` | `-h` | Display help message | `--help` | - |

## Configuration Details

### `--table-base-path`
The location where the test Hudi table will be created. Can be a local filesystem path or HDFS path.

### `--mode`
Benchmark execution mode:
- `BOOTSTRAP`: Only creates the table and bootstraps the metadata table (write-only)
- `QUERY`: Only runs data skipping benchmark queries (read-only, requires existing table)
- `BOOTSTRAP_AND_QUERY`: Runs both bootstrap and query phases (default)

### `--num-cols-to-index`
Number of columns to index for column statistics. Accepts values 1 or 2.

**Supported columns:**
- `tenantID` - Long type, values range 30000-60000 (30k-60k), always indexed
- `age` - Integer type, values range 20-100, indexed only when `--num-cols-to-index 2`

**Examples:**
- Single column (tenantID only): `--num-cols-to-index 1`
- Two columns (tenantID & age): `--num-cols-to-index 2`

### `--col-stats-file-group-count`
Determines the parallelism and file distribution in the metadata table's column stats partition. Higher values mean:
- More parallel write/read operations
- More files in the column stats partition
- Better distribution of column statistics records

### `--num-files-to-bootstrap`
Number of data files to create during the bootstrap phase. Files are evenly distributed across partitions.
- Example: 1000 files with 5 partitions = 200 files per partition

### `--num-partitions`
Number of date-based partitions to create. Partitions are generated sequentially starting from `2025-01-01`:
- `1` → `["2025-01-01"]`
- `3` → `["2025-01-01", "2025-01-02", "2025-01-03"]`
- `10` → `["2025-01-01" through "2025-01-10"]`

### `--num-files-for-incremental` and `--num-commits-for-incremental`
These parameters enable incremental ingestion after bootstrap:
- `--num-files-for-incremental`: Total number of files to create across all incremental commits
- `--num-commits-for-incremental`: Number of incremental commits to distribute files across

Files are evenly distributed across commits. Both parameters must be set to a value > 0 to enable incremental ingestion.

**Example:** `--num-files-for-incremental 200 --num-commits-for-incremental 2` creates 100 files per commit across 2 commits.

### `--partition-filter`
Partition filter predicate used during query benchmarking. Must match the partition format (`dt = 'yyyy-MM-dd'` or `dt > 'yyyy-MM-dd'`).

**Default:** `"dt = '2025-01-01'"` (matches first partition)

**Examples:**
- `--partition-filter "dt = '2025-01-01'"` - Query only first partition
- `--partition-filter "dt > '2025-01-03'"` - Query partitions after the third one

### `--data-filter`
Data filter predicate(s) used during query benchmarking. Multiple filters can be specified separated by commas.

**Default behavior:** If not specified, uses default filters based on indexed columns:
- 1 column indexed: `age > 70` (note: age is not indexed, so this won't skip files)
- 2 columns indexed: `age > 70` and `tenantID > 50000`

**Examples:**
- `--data-filter "tenantID > 50000"` - Filter on tenantID
- `--data-filter "age > 70, tenantID > 50000"` - Multiple filters


## Example Usage

### Example 1: Index tenantID only (Bootstrap and Query)
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 1 \
  --col-stats-file-group-count 10 \
  --num-files-to-bootstrap 1000 \
  --num-partitions 5
```

### Example 2: Index tenantID & age
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 2 \
  --col-stats-file-group-count 10 \
  --num-files-to-bootstrap 1000 \
  --num-partitions 5
```

### Example 3: Bootstrap only (no query)
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --mode BOOTSTRAP \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 2 \
  --col-stats-file-group-count 10 \
  --num-files-to-bootstrap 1000 \
  --num-partitions 5
```

### Example 4: Query only (table must already exist)
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --mode QUERY \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 2 \
  --col-stats-file-group-count 10 \
  --num-files-to-bootstrap 1000 \
  --num-partitions 5 \
  --partition-filter "dt > '2025-01-02'" \
  --data-filter "tenantID > 50000"
```

### Example 5: Bootstrap with incremental commits
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 2 \
  --col-stats-file-group-count 10 \
  --num-files-to-bootstrap 100 \
  --num-partitions 3 \
  --num-files-for-incremental 200 \
  --num-commits-for-incremental 2
```

This example:
- Bootstraps with 100 files across 3 partitions
- Then performs 2 incremental commits with 100 files each (200 total)
- Runs query benchmarking at the end