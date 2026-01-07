
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

# HoodieMDTStats Configuration Guide

## Overview

`HoodieMDTStats` is a utility for testing Hudi Metadata Table column statistics functionality. It creates a Hudi table, generates column statistics, and demonstrates data skipping capabilities.

## Configuration Parameters

### Required Parameters

| Parameter | Short Flag | Description | Example Value |
|-----------|------------|-------------|---------------|
| `--table-base-path` | `-tbp` | Base path where the Hudi table will be created | `/tmp/hudi_table` |
| `--cols-to-index` | `-cols` | Comma-separated list of columns to index | `age,salary` |
| `--col-stats-file-group-count` | `-col-fg-count` | Number of file groups for the column stats partition in metadata table | `10` |
| `--num-files` | `-nf` | Total number of data files to create across all partitions | `1000` |
| `--num-partitions` | `-np` | Number of date partitions to generate (starting from 2020-01-01) | `5` |

### Optional Parameters

| Parameter | Short Flag | Description | Usage |
|-----------|------------|-------------|-------|
| `--files-per-commit` | `-fpc` | Number of files to create per commit. If not specified or >= num-files, all files will be in one commit | `--files-per-commit 1000` |
| `--props` | | Path to properties file containing Hudi configurations | `--props /path/to/config.properties` |
| `--hoodie-conf` | | Individual Hudi configuration (can be specified multiple times) | `--hoodie-conf hoodie.metadata.enable=true` |
| `--help` | `-h` | Display help message | `--help` |

## Configuration Details

### `--table-base-path`
The location where the test Hudi table will be created. Can be a local filesystem path or HDFS path.

### `--cols-to-index`
Comma-separated list of columns for which statistics will be generated. Specify column names as a string.

**Supported columns:**
- `age` - Integer type, values range 20-100
- `salary` - Long type, values range 50000-250000

**Examples:**
- Single column: `--cols-to-index age`
- Multiple columns: `--cols-to-index age,salary`
- All columns: `--cols-to-index age,salary` (current maximum supported)

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

### `--files-per-commit`
**Optional parameter** that controls how many files are created in each commit. This allows testing scenarios with multiple commits instead of a single commit. Default value is 1000

**Examples:**
- `--num-files 1000 --files-per-commit 1000` → 1 commit with 1000 files
- `--num-files 5000 --files-per-commit 1000` → 5 commits, each with 1000 files
- `--num-files 2500 --files-per-commit 1000` → 3 commits (1000, 1000, 500 files)

## Example Usage

### Basic Example
```bash
spark-submit \
  --class org.apache.hudi.utilities.HoodieMDTStats \
  --master "local[*]" \
  packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.2.0-SNAPSHOT.jar \
  --table-base-path /tmp/hudi_test \
  --cols-to-index age,salary \
  --col-stats-file-group-count 10 \
  --num-files 1000 \
  --num-partitions 5 \
  --files-per-commit 1000
```
Omit `--files-per-commit` to create all files in a single commit, or set it to a value less than `--num-files` to create multiple commits.