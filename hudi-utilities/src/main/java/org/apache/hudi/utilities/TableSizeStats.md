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

# TableSizeStats — Runbook

`org.apache.hudi.utilities.TableSizeStats` is a Spark-based tool that produces
per-table and per-partition size statistics for a Hudi table. It reads only
the active timeline + file system (or Metadata Table when available); no
ingest/compaction is performed.

Source: `hudi-utilities/src/main/java/org/apache/hudi/utilities/TableSizeStats.java`.

## What it produces

The default run emits a **table-level** distribution: total bytes, file
count, min / max / mean / median / p50 / p90 / p95 / p99 of base file size,
and a per-partition file-count distribution (numFiles per partition's
percentiles).

With `--enable-partition-stats` it adds a sorted-by-size-desc partition
table (file count, total bytes, size percentiles per partition).

With `--include-row-counts` it adds `numRecords` + `avgRowSize` columns,
preferring the MDT `column_stats` partition when available, else falling
back to Parquet footer reads per file (slow).

With `--analyze-table-characteristics` it adds three detectors:

- **Micro-partitioned (table-level)** — flagged when `numPartitions > N` OR
  when there exist partitions with `fileCount >= X` AND `avgFileSize < Y MB`
  (size rule only applies on tables older than `--micro-partition-min-age-days`).
- **Small-file pile-up (table-level)** — verdict `CLEAN` / `MODERATE` / `SEVERE`
  based on the *prevalence* of underfilled partitions. A partition is
  "qualifying" if `fileCount >= 5` and "flagged" if `avgFileSize < 50 MB`.
  Verdict thresholds: `MODERATE` at 10% flagged, `SEVERE` at 30% flagged
  (defaults). Skipped on tables with fewer than 10 ingest commits
  (active + archived).
- **Hot partitions (per-partition)** — by recent write volume, scanning
  the last N completed ingest commits (`commit` + `deltacommit`,
  **excluding** `COMPACT` and `CLUSTER`). Flagged when a partition appears
  in `>= 0.5 × N` of those commits.

## Classpath

Same model as TimelineInspector — bundled jars mark Hadoop & Jackson as
`provided`. Use `spark-submit` (recommended) or `hadoop jar`.

### Preferred — `spark-submit`

```bash
spark-submit \
  --class org.apache.hudi.utilities.TableSizeStats \
  --master "local[2]" \
  --driver-memory 4g \
  --conf spark.log.level=WARN \
  /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  --base-path /tmp/your_table
```

For S3/GCS base paths, add `--packages org.apache.hadoop:hadoop-aws:3.3.4`
(or matching `gcs-connector` / `hadoop-azure`).

### Fallback — `hadoop jar`

```bash
hadoop jar /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  org.apache.hudi.utilities.TableSizeStats \
  --base-path /tmp/your_table
```

## Flag reference

### Required (one of)
| Flag | Meaning |
|---|---|
| `--base-path <path>` | Table base path (local fs / `s3://…` / `gs://…` / `abfs://…`). |
| `--props-path <path>` | File listing one base path per line — runs against each. |

### Output
| Flag | Default | Meaning |
|---|---|---|
| `--output TABLE\|JSON` | `TABLE` | Output format. JSON output is a single object on stdout. |
| `--enable-table-stats` / `-fs` | off | Force the table-level distribution + skew section even when partition stats are requested. (Table-level is always emitted when partition stats are off.) |
| `--enable-partition-stats` / `-ps` | off | Add the per-partition table. |
| `--top-n N` / `-tn N` | 0 (all) | When partition stats are on, cap the partition rows to the top N by total bytes. |

### Date filtering (only works on date-partitioned tables)
| Flag | Meaning |
|---|---|
| `--num-days N` | Include partitions whose date is within the last N days. |
| `--start-date YYYY/M/D` | Include partitions on or after this date. |
| `--end-date YYYY/M/D` | Include partitions strictly before this date. |

Partition names must conform to `yyyy/M/d`, `yyyy-M-d`, or `column=<date>`.
If they don't, the tool throws — date filtering is silently incompatible
with hash / customer-id / UUID partitioning schemes.

### Row counts
| Flag | Default | Meaning |
|---|---|---|
| `--include-row-counts` / `-irc` | off | Add `numRecords` + `avgRowSize`. Fast path uses MDT `column_stats` partition when present, else falls back to Parquet footer reads (slow on large tables). |

### Table characteristics (detectors)
| Flag | Default | Meaning |
|---|---|---|
| `--analyze-table-characteristics` / `-atc` | off | Master switch: runs micro-partition, small-file, and hot-partition detectors. |
| `--print-top-k` | 0 | When >0 (and `--analyze-table-characteristics` is set), also print the top-K partitions with the smallest avg file size under the micro and small-file detectors — an evidence-oriented complement to the verdicts. |
| `--micro-partition-count-threshold` | 10000 | Trigger micro verdict when `numPartitions` exceeds this. |
| `--micro-partition-min-files` | 25 | Per-partition file-count gate for the size-based micro rule. |
| `--micro-partition-max-avg-bytes` | 52428800 (50 MB) | Per-partition avg-file-size threshold for the size-based rule. |
| `--micro-partition-min-age-days` | 30 | Skip the size-based rule on tables newer than this. |
| `--small-files-min-files-per-partition` | 5 | Per-partition file-count gate. Partitions with fewer files are excluded from the prevalence ratio. |
| `--small-files-threshold-bytes` | 52428800 (50 MB) | Avg-file-size threshold for flagging a qualifying partition. |
| `--small-files-moderate-pct` | 0.10 | Fraction of qualifying partitions flagged to trigger `MODERATE` verdict. |
| `--small-files-severe-pct` | 0.30 | Fraction of qualifying partitions flagged to trigger `SEVERE` verdict. |
| `--small-files-min-table-commits` | 10 | Minimum total ingest commits (active+archived) before emitting the small-file verdict. |
| `--hot-window-commits` | 50 | Number of recent ingest commits to scan. |
| `--hot-partition-commit-share` | 0.5 | Minimum share of hot-window commits a partition must appear in. |

### Spark / misc
| Flag | Default | Meaning |
|---|---|---|
| `--spark-master` / `-ms` | (from env) | Spark master URL. |
| `--spark-memory` / `-sm` | `1g` | Executor memory. |
| `--parallelism` / `-pl` | 200 | Spark parallelism. |
| `--hoodie-conf k=v` | — | Repeatable Hudi config override. |

## Examples

### 1. Default table-level distribution
```bash
spark-submit --class org.apache.hudi.utilities.TableSizeStats --master "local[2]" \
  /path/to/hudi-utilities-bundle.jar \
  --base-path /tmp/orders
```

### 2. Per-partition view, top 20 largest, table + skew section
```bash
spark-submit --class org.apache.hudi.utilities.TableSizeStats --master "local[2]" \
  /path/to/hudi-utilities-bundle.jar \
  --base-path /tmp/orders \
  --enable-partition-stats --enable-table-stats --top-n 20
```

### 3. JSON output for piping
```bash
spark-submit ... TableSizeStats \
  --base-path s3://bucket/orders \
  --enable-partition-stats --output JSON \
  | jq '.tableSizeStats, .skew'
```

### 4. Row counts via MDT col-stats (fast)
```bash
spark-submit ... TableSizeStats \
  --base-path /tmp/orders \
  --enable-partition-stats --include-row-counts
```

### 5. Full detector pass
```bash
spark-submit ... TableSizeStats \
  --base-path /tmp/orders \
  --enable-partition-stats --analyze-table-characteristics
```

### 6. Last 7 days, date-partitioned
```bash
spark-submit ... TableSizeStats \
  --base-path /tmp/orders \
  --enable-partition-stats --num-days 7
```

## Output schema (JSON)

```json
{
  "basePath": "/tmp/orders",
  "mdtEnabled": true,
  "numPartitions": 412,
  "totalBytes": 1234567890,
  "totalFiles": 5678,
  "totalRecords": 90123456,
  "tableSizeStats": {
    "count": 5678, "min": 1024, "max": 134217728,
    "mean": 17234567, "median": 16000000,
    "p50": 16000000, "p90": 40000000, "p95": 60000000, "p99": 120000000
  },
  "fileCountPerPartition": {
    "count": 412, "min": 1, "max": 25, "mean": 14,
    "median": 14, "p50": 14, "p90": 22, "p95": 23, "p99": 24
  },
  "skew": {
    "cv": 0.4231,
    "gini": 0.2104,
    "largestPartitionShare": 0.0214,
    "top10Share": 0.1532,
    "outliers": [
      {"partition": "country=US/date=2026-06-09", "totalBytes": 35000000, "files": 47}
    ]
  },
  "partitions": [
    {"partition": "...", "files": 47, "totalBytes": 35000000,
     "sizeStats": {"count": 47, "min": 100000, "max": 1500000, "mean": 744680, ...}}
  ],
  "tableCharacteristics": {
    "tableAgeDays": 92,
    "numPartitions": 412,
    "microPartitioned": {
      "verdict": false,
      "countRuleTriggered": false,
      "sizeRuleTriggered": false,
      "sizeMatchCount": 0,
      "countThreshold": 10000,
      "sizeRuleEligible": true
    },
    "smallFiles": {
      "verdict": "MODERATE",
      "thresholdBytes": 52428800,
      "minFilesPerPartition": 5,
      "ingestCommitCount": 412,
      "qualifyingPartitions": 280,
      "flaggedPartitions": 47,
      "flaggedPct": 0.1679,
      "moderatePct": 0.10,
      "severePct": 0.30
    },
    "hotPartitions": {
      "windowCommits": 50,
      "shareThreshold": 0.5,
      "partitions": [
        {"partition": "country=US/date=2026-06-09", "commitCount": 50, "bytesWritten": 12400000000, "recordsWritten": 489000}
      ]
    }
  }
}
```

## Caveats

- **Base files only.** Log files (MOR `.log.*`) are not counted. On
  MOR tables this **undercounts** real on-disk size; the detector emits
  a one-line note when small-file pile-up is flagged.
- **Reservoir sampling.** Percentiles use Codahale `UniformReservoir(1M)`
  per histogram. On tables with >1M files the percentile values are
  approximations.
- **`--num-days` requires date-partition naming.** Tables with
  hash / id / UUID partitioning will throw on date filtering — omit the
  date flags in that case.
- **MDT col-stats fast path** requires the table to have written its
  `column_stats` partition. On tables without it, `--include-row-counts`
  falls back to Parquet footer reads (~1 round-trip per file on object
  storage — slow).
- **Hot-partition detection excludes COMPACT and CLUSTER ops.** They
  legitimately touch many partitions per commit and would otherwise
  show every partition as "hot."
- **Skew metrics need ≥ 2 partitions.** Single-partition / unpartitioned
  tables skip the section.

## Common failure modes

| Symptom | Fix |
|---|---|
| `NoClassDefFoundError: org/apache/hadoop/fs/FileSystem` | Bare `java -cp <bundle>` — switch to `spark-submit` or `hadoop jar`. |
| `HoodieException: Cannot apply --start-date when partition does not contain date` | Drop `--num-days` / `--start-date` / `--end-date` for non-date-partitioned tables. |
| `--output must be TABLE or JSON (got …)` | Misspelled output format. |
| OOM on driver | Bump `--driver-memory` (the run holds per-partition histograms in memory; a 100K-partition table can need 8G+). |
| Slow `--include-row-counts` on object storage | Enable the MDT column-stats partition on the table, or accept the Parquet footer read cost. |

## Tests

Integration test class: `hudi-utilities/src/test/java/org/apache/hudi/utilities/TestTableSizeStats.java`.
30 tests, JUnit 5, builds a synthetic Hudi table in `@TempDir`. Run:

```bash
export SPARK_LOCAL_IP=127.0.0.1
mvn test -pl hudi-utilities -Dspark3.5 -Dtest=TestTableSizeStats
```
