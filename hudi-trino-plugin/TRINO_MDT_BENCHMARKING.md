
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

# Trino MDT Column Stats Benchmarking Guide

## Overview

This module provides a two-stage benchmarking pipeline for measuring the query-time speedup delivered
by Hudi's Metadata Table (MDT) column statistics index in the Trino query path.

**Stage 1 — Bootstrap** (`MetadataBenchmarkingTool`): Creates a synthetic Hudi table with stub Parquet
files and a fully populated column stats MDT using Spark. No real data is written — only file footers
and metadata table records, so bootstrap is fast even at millions of files.

**Stage 2 — Trino benchmark** (`TrinoBenchmarkToolV2`): Reads the bootstrapped table through an
in-process Trino cluster, runs a configurable SQL query with col-stats ON and col-stats OFF, and
reports split counts and wall-clock time for each scenario.

The end-to-end pipeline is driven by `run_benchmarks.sh`.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Java 23 | Used by Trino. Detected automatically via `/usr/libexec/java_home -v 23`. |
| Spark 3.5.x | Used by MetadataBenchmarkingTool. Set `SPARK_HOME` in the script. |

---

## Quick Start

```bash
# 1. Edit the CONFIG section in run_benchmarks.sh (see below)
# 2. Run the pipeline
cd hudi-trino-plugin
./run_benchmarks.sh
```

The script will:
1. Auto-detect the Hudi version and Scala version from the root `pom.xml`.
2. Build `hudi-utilities-bundle` if the jar is missing.
3. Bootstrap the table (unless `SKIP_BOOTSTRAP=true`).
4. Compile the Trino plugin test classes.
5. Run the Trino benchmark and print a summary.

---

## Configuration Reference (`run_benchmarks.sh`)

### Table

| Variable | Default | Description |
|----------|---------|-------------|
| `TABLE_BASE_PATH` | `/tmp/hudi_bench_table_500K` | Local path where the synthetic Hudi table is written (bootstrap) or read from (skip bootstrap). |
| `SKIP_BOOTSTRAP` | `false` | Set to `true` to skip Stage 1 and reuse an existing table at `TABLE_BASE_PATH`. |

### Bootstrap (MetadataBenchmarkingTool)

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_FILES` | `500000` | Total stub Parquet files created across all partitions. This is the file count seen by Trino with col-stats OFF — higher values amplify the pruning benefit. Files are distributed evenly across partitions. |
| `NUM_PARTITIONS` | `365` | Number of date-based partitions (one per day starting `2025-01-01`). More partitions give finer partition pruning granularity but increase the MDT FILES partition size. |
| `NUM_COLS_TO_INDEX` | `1` | Columns to include in the column stats index: `1` = `tenantID` only, `2` = `tenantID` + `age`. More columns increase MDT size and index build time. |
| `COL_STATS_FG_COUNT` | `1` | Number of file groups for the MDT column stats partition. More file groups allow more parallelism when reading the index but add metadata overhead. |
| `TENANT_ID_RANGE` | `3000000` | Spread of `tenantID` values distributed across files (`tenantID` min = 30000, max = 30000 + range). A wider range means more files survive the filter (lower pruning efficiency); a narrower range means the filter is more selective. |
| `HOODIE_CONF` | `hoodie.metadata.file.cache.max.size.mb=200` | Extra Hoodie configuration passed to MetadataBenchmarkingTool via `--hoodie-conf key=value`. |
| `SPARK_HOME` | `~/Applications/spark-3.5.3-bin-hadoop3/` | Path to a local Spark 3.5.x installation. |
| `SPARK_MASTER` | `local[*]` | Spark master URL. Use `local[*]` for local mode or a YARN/cluster URL for distributed runs. |

### Query Filters (TrinoBenchmarkToolV2)

Filters are applied to the SQL query run by TrinoBenchmarkToolV2. They exercise both column-stats
pruning (data filter) and partition pruning (partition filter).

> **Note:** Filters are passed as structured arguments rather than raw SQL to avoid single-quote
> stripping by Maven's exec plugin argument parser.

#### Data filter

Filters on a non-partition column to measure column-stats index pruning. All three variables must
be set together; leave `DATA_FILTER_COL` empty to run without a data filter.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_FILTER_COL` | `tenantID` | Column to filter on. Must be indexed (controlled by `NUM_COLS_TO_INDEX`). |
| `DATA_FILTER_OP` | `EQ` | Comparison operator: `EQ`, `GT`, `GTE`, `LT`, `LTE`, or `RANGE`. |
| `DATA_FILTER_VAL` | `35000` | Filter value. For `RANGE`, supply `lo,hi` (e.g. `40000,50000`). |

Examples:

```bash
# Equality: tenantID = 35000
DATA_FILTER_COL="tenantID"  DATA_FILTER_OP="EQ"    DATA_FILTER_VAL="35000"

# Range: tenantID BETWEEN 40000 AND 50000
DATA_FILTER_COL="tenantID"  DATA_FILTER_OP="RANGE"  DATA_FILTER_VAL="40000,50000"

# Greater-than: age > 70  (requires NUM_COLS_TO_INDEX=2)
DATA_FILTER_COL="age"       DATA_FILTER_OP="GT"     DATA_FILTER_VAL="70"
```

#### Partition filter

Restricts the query to a date range within the `dt` partition column. Leave `PARTITION_START`
empty to run without a partition filter. When start equals end, an equality predicate is used
(`dt = 'YYYY-MM-DD'`); otherwise a range predicate is used (`dt >= '...' AND dt <= '...'`).

| Variable | Default | Description |
|----------|---------|-------------|
| `PARTITION_START` | `2025-01-01` | Inclusive start date (`YYYY-MM-DD`). |
| `PARTITION_END` | `2025-01-31` | Inclusive end date (`YYYY-MM-DD`). |

### Trino Benchmark (TrinoBenchmarkToolV2)

| Variable | Default | Description |
|----------|---------|-------------|
| `FILE_SLICE_PROCESSING_MS` | `10` | Simulated processing time per split in milliseconds. Models the real cost of opening a Parquet file footer. Higher values amplify the wall-clock difference between col-stats ON and OFF. |
| `COL_STATS_TIMEOUT` | `2s` | Trino session property `hudi.column_stats_wait_timeout`. If the MDT lookup takes longer than this, Trino falls back to scanning all splits. Increase for very large tables. |
| `WARMUP_RUNS` | `1` | Query executions before measurement (discarded). Lets the JIT compiler and file metadata caches warm up. |
| `MEASUREMENT_RUNS` | `5` | Timed query executions per scenario (col-stats ON and col-stats OFF). Results are averaged and a speedup ratio is reported. |

---

## Example Runs

### Local small-scale run (quick validation)

```bash
TABLE_BASE_PATH="/tmp/hudi_bench_10K"
NUM_FILES=10000
NUM_PARTITIONS=10
COL_STATS_FG_COUNT=1
TENANT_ID_RANGE=30000
DATA_FILTER_COL="tenantID"  DATA_FILTER_OP="EQ"  DATA_FILTER_VAL="35000"
PARTITION_START="2025-01-01"  PARTITION_END="2025-01-01"
WARMUP_RUNS=1  MEASUREMENT_RUNS=3
```

### Large-scale run (matches production benchmark)

```bash
TABLE_BASE_PATH="/tmp/hudi_bench_1M"
NUM_FILES=1000000
NUM_PARTITIONS=365
COL_STATS_FG_COUNT=1
TENANT_ID_RANGE=3000000
DATA_FILTER_COL="tenantID"  DATA_FILTER_OP="EQ"  DATA_FILTER_VAL="35000"
PARTITION_START="2025-01-01"  PARTITION_END="2025-01-31"
WARMUP_RUNS=2  MEASUREMENT_RUNS=5
HOODIE_CONF="hoodie.metadata.file.cache.max.size.mb=200"
```

---

## Understanding the Output

After all measurement runs, `TrinoBenchmarkToolV2` prints a summary:

```
======================================================================
  TRINO QUERY BENCHMARK SUMMARY
  SQL: SELECT COUNT(*) FROM hudi.tests.test_mdt_stats_tbl WHERE ...
======================================================================
  With col stats ON:   avg=120ms  avg-splits=42   [min=115ms max=130ms p95=128ms]
  With col stats OFF:  avg=9800ms avg-splits=500000 [min=9600ms max=10100ms p95=10050ms]
  Speedup: 81.67x  (col stats is faster)
======================================================================
```

| Field | Description |
|-------|-------------|
| `avg-splits` | Average number of file-slice splits processed per query. With col-stats ON this reflects how many files survived the index filter; with col-stats OFF all files are scanned. |
| `Speedup` | Ratio of avg time OFF to avg time ON. Values below 1.0 indicate the index is slower than a full scan — check MDT health or increase `FILE_SLICE_PROCESSING_MS`. |

---

## Tuning Tips

- **Speedup looks too low?** Increase `FILE_SLICE_PROCESSING_MS` to make each split more expensive, or narrow the filter to prune more files.
- **col-stats ON is timing out?** Increase `COL_STATS_TIMEOUT` (`5s`, `10s`). This typically happens when `COL_STATS_FG_COUNT` is too low for the file count.
- **Bootstrap is slow?** The bottleneck is MDT writes. Increase `COL_STATS_FG_COUNT` for more write parallelism, or reduce `NUM_FILES`.
- **Rerunning after changing filters only?** Set `SKIP_BOOTSTRAP=true` — the table does not need to be rebuilt.
