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

# Hudi Benchmark — DSv1 vs DSv2 Read Comparison (COW)

## Overview

Read-focused benchmark comparing Hudi **DSv1 vs DSv2** read paths on **COW** tables.
Measures full scan, column projection, filter pushdown, limit pushdown, and aggregate pushdown.
Targets real-world Parquet data on HDFS.
Currently, the benchmark expects run on local machine.

## Test Data

| Property      | Value               |
|---------------|---------------------|
| Source format | Parquet             |
| Files         | ~800                |
| Columns       | 300 (all primitive) |
| Total size    | ~100GB              |
| Storage       | HDFS                |

The source path supports globs for quick smoke tests:
- `part-0000*.parquet` — ~10 files, ~1.25 GB (small)
- `part-000*.parquet` — ~100 files, ~12.5 GB (medium)
- `part-00*.parquet` — ~800 files, ~100 GB (full)

## Benchmark Scenarios

| Scenario             | Modes            | Description                                               |
|----------------------|------------------|-----------------------------------------------------------|
| Full scan            | DSv1, DSv2       | `SELECT * FROM table` via noop sink                       |
| Projected read       | DSv1, DSv2       | `SELECT col_subset FROM table` via noop sink              |
| Filter pushdown      | DSv1, DSv2       | `SELECT cols FROM table WHERE expr` via noop sink         |
| Limit pushdown       | DSv1, DSv2       | `SELECT * FROM table LIMIT N` via noop sink               |
| Aggregate pushdown   | DSv1, DSv2       | `COUNT(*)` and `MIN/MAX` on COW with column stats         |

## Environment Variables

| Variable                         | Required | Default | Description                                                                                      |
|----------------------------------|----------|---------|--------------------------------------------------------------------------------------------------|
| `HUDI_BENCHMARK_DATA_PATH`       | Yes      | —       | HDFS path to source Parquet files (supports globs)                                               |
| `HUDI_BENCHMARK_RECORD_KEY`      | Yes      | —       | Primary key column name                                                                          |
| `HUDI_BENCHMARK_PRECOMBINE_FIELD`| Yes      | —       | Ordering/precombine field name                                                                   |
| `HUDI_BENCHMARK_PARTITION_FIELD` | No       | (empty) | Partition field; empty = non-partitioned                                                         |
| `HUDI_BENCHMARK_ITERATIONS`      | No       | `1`     | Timed iterations per benchmark case                                                              |
| `HUDI_BENCHMARK_PROJECTED_COLS`  | No       | (auto)  | Comma-separated column subset for projected reads; auto-detects first 6 non-key columns if unset |
| `HUDI_BENCHMARK_LIMIT_VALUE`     | No       | `1000`  | LIMIT value for limit pushdown benchmark                                                         |
| `HUDI_BENCHMARK_FILTER_COL`      | No       | (auto)  | Column for filter benchmark; auto-detects first projected column if unset                        |

## Prerequisites

### Build the Hudi Spark bundle

```bash
mvn clean package -DskipTests -Dspark3.5 -pl packaging/hudi-spark-bundle -am
```

The resulting JAR is at `packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-*.jar`.

## Running the Benchmark

### Environment setup

Create a `.env` file or export variables:

```bash
export HUDI_BENCHMARK_DATA_PATH="hdfs:///data/benchmark/part-000*.parquet"
export HUDI_BENCHMARK_RECORD_KEY="uuid_key"
export HUDI_BENCHMARK_PRECOMBINE_FIELD="primary_key"
# Optional:
export HUDI_BENCHMARK_PARTITION_FIELD=""
export HUDI_BENCHMARK_ITERATIONS=1
export HUDI_BENCHMARK_PROJECTED_COLS=""
export HUDI_BENCHMARK_LIMIT_VALUE=1000
export HUDI_BENCHMARK_FILTER_COL=""
```

### Via spark-shell

```bash
source ./benchmarks/.env && \
$SPARK_HOME/bin/spark-shell \
    --master $SPARK_CLUSTER \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 6g \
    --total-executor-cores 9 \
    --jars $HUDI_JAR,$HIVE_JARS \
    --conf spark.local.dir=$SPARK_LOCAL_DIR \
    --conf spark.sql.catalog.spark_catalog.warehouse=$SPARK_WAREHOUSE \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalogImplementation=in-memory \
    -i ./benchmarks/hudi_benchmark.scala
```

Where `$HUDI_JAR` points to the Hudi Spark bundle JAR.

## Scenarios Configuration

### Setup

Before running read benchmarks, the script creates a single COW table via `bulk_insert`.
This is untimed (logged for informational purposes only) and provides the table used by
all subsequent read benchmarks.

```
hoodie.datasource.write.operation = bulk_insert
hoodie.bulkinsert.shuffle.parallelism = 200
hoodie.compact.inline = false
hoodie.clustering.inline = false
```

### Read (DSv1)
Standard Hudi read path:
```
hoodie.datasource.read.use.v2 = false
```

### Read (DSv2)
V2 read path with pushdown support:
```
hoodie.datasource.read.use.v2 = true
```

### Read (filter pushdown)
Filter expression is auto-detected from the first projected column. For numeric columns, uses
`col >= median_value`. For non-numeric columns, falls back to `col IS NOT NULL`. Override with
`HUDI_BENCHMARK_FILTER_COL`.

### Read (limit pushdown)
Uses `SELECT * FROM table LIMIT N` where N defaults to 1000. DSv2 stops reading after collecting
enough rows, while DSv1 reads all data first. This is the primary scenario where DSv2 should be
significantly faster.

### Aggregate pushdown
Creates a dedicated COW table with `hoodie.metadata.index.column.stats.enable = true`.
Benchmarks `COUNT(*)` and `MIN/MAX` (if numeric column detected). DSv2 resolves aggregates from
column stats metadata (near-instant), while DSv1 scans all files.

## Log Output

The benchmark script duplicates all stdout to a timestamped log file
(`hudi_benchmark_YYYYMMDD_HHmmss.log`) in the current working directory.
This preserves the full output for later analysis even if the terminal buffer is lost.

## Error Handling

The benchmark wraps all timed iterations in a `try`/`finally` block.
On completion (or failure), the `finally` block drops all benchmark tables created during the run,
drops the benchmark database, unpersists cached source data, and closes the log file.

## Expected Output

```
============================================================
BENCHMARK COMPLETE
============================================================

Read full (COW, DSv1)        : 277.7s, 279.4s, 275.8s  (min: 275.8s, max: 279.4s, avg: 277.7s)
Read full (COW, DSv2)        : 279.0s, 279.6s, 278.9s  (min: 278.9s, max: 279.6s, avg: 279.2s)
Read projected (COW, DSv1)   : 7.8s, 7.2s, 7.3s  (min: 7.2s, max: 7.8s, avg: 7.4s)
Read projected (COW, DSv2)   : 6.0s, 5.9s, 6.0s  (min: 5.9s, max: 6.0s, avg: 6.0s)
Read filter (COW, DSv1)      : 7.3s, 7.2s, 7.1s  (min: 7.1s, max: 7.3s, avg: 7.2s)
Read filter (COW, DSv2)      : 6.0s, 6.1s, 6.1s  (min: 6.0s, max: 6.1s, avg: 6.1s)
Read limit (COW, DSv1)       : 54.5s, 54.6s, 57.7s  (min: 54.5s, max: 57.7s, avg: 55.6s)
Read limit (COW, DSv2)       : 59.3s, 59.1s, 58.9s  (min: 58.9s, max: 59.3s, avg: 59.1s)
Aggregate COUNT(*) (DSv1)    : 3.5s, 3.6s, 3.5s  (min: 3.5s, max: 3.6s, avg: 3.5s)
Aggregate COUNT(*) (DSv2)    : 0.2s, 0.2s, 0.3s  (min: 0.2s, max: 0.3s, avg: 0.2s)
Aggregate MIN/MAX (DSv1)     : 3.9s, 3.7s, 3.7s  (min: 3.7s, max: 3.9s, avg: 3.8s)
Aggregate MIN/MAX (DSv2)     : 0.2s, 0.2s, 0.2s  (min: 0.2s, max: 0.2s, avg: 0.2s)

============================================================
DSv2 vs DSv1 PERFORMANCE COMPARISON
============================================================

Full scan (COW)                    : DSv1 avg 277.7s, DSv2 avg 279.2s, speedup 0.99x (DSv1 FASTER)
Projected (COW)                    : DSv1 avg 7.4s, DSv2 avg 6.0s, speedup 1.25x (DSv2 FASTER)
Filter (COW)                       : DSv1 avg 7.2s, DSv2 avg 6.1s, speedup 1.19x (DSv2 FASTER)
Limit (COW)                        : DSv1 avg 55.6s, DSv2 avg 59.1s, speedup 0.94x (DSv1 FASTER)
Aggregate COUNT(*)                 : DSv1 avg 3.5s, DSv2 avg 0.2s, speedup 14.98x (DSv2 FASTER)
Aggregate MIN/MAX                  : DSv1 avg 3.8s, DSv2 avg 0.2s, speedup 19.55x (DSv2 FASTER)

PASS: DSv2 is faster than DSv1 in 4 of 6 scenarios
```

(Numbers are illustrative — actual results depend on hardware and cluster config.)
