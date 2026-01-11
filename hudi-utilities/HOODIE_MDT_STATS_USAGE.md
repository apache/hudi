
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
| `--num-files` | `-nf` | Total number of data files to create across all partitions | `1000` |
| `--num-partitions` | `-np` | Number of date partitions to generate (starting from 2020-01-01) | `5` |

### Optional Parameters

| Parameter | Short Flag | Description | Usage |
|-----------|------------|-------------|-------|
| `--props` | | Path to properties file containing Hudi configurations | `--props /path/to/config.properties` |
| `--hoodie-conf` | | Individual Hudi configuration (can be specified multiple times) | `--hoodie-conf hoodie.metadata.enable=true` |
| `--help` | `-h` | Display help message | `--help` |

## Configuration Details

### `--table-base-path`
The location where the test Hudi table will be created. Can be a local filesystem path or HDFS path.

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

### `--num-files`
Total number of data files that will be created. Files are evenly distributed across partitions.
- Example: 1000 files with 5 partitions = 200 files per partition

### `--num-partitions`
Number of date-based partitions to create. Partitions are generated sequentially starting from `2020-01-01`:
- `1` → `["2020-01-01"]`
- `3` → `["2020-01-01", "2020-01-02", "2020-01-03"]`
- `10` → `["2020-01-01" through "2020-01-10"]`


## Example Usage

### Basic Example
### Example 1: Index tenantID only
```bash
spark-submit \
  --class org.apache.hudi.utilities.benchmarking.MetadataBenchmarkingTool \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --table-base-path /tmp/hudi_test \
  --num-cols-to-index 1 \
  --col-stats-file-group-count 10 \
  --num-files 1000 \
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
  --num-files 1000 \
  --num-partitions 5
```