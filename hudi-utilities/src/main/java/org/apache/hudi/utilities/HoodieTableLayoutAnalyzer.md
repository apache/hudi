<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# HoodieTableLayoutAnalyzer

Reports how a Hudi table's bytes and base files are spread across partitions, how skewed that
spread is, and whether the layout shows the symptoms that make queries and table services slow:
too many tiny partitions, partitions piling up small files, and a few partitions absorbing most
recent writes.

The tool is strictly read-only. It reads the timeline and the file listing (through the metadata
table when the table has one) and prints a report; it never writes to the table.

## Quick start

```bash
spark-submit \
  --master "local[2]" \
  --driver-memory 4g \
  --class org.apache.hudi.utilities.HoodieTableLayoutAnalyzer \
  $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
  --base-path s3://bucket/path/to/table \
  --enable-partition-stats --analyze-table-characteristics
```

This is a Spark application, not a plain Java CLI: partition listing goes through the metadata
table reader and the file system view, both of which need an engine context. `local[2]` is a fine
default for a laptop or bastion host; add `--packages org.apache.hadoop:hadoop-aws:3.3.4` (or the
matching GCS / Azure connector) for object-store base paths.

## What it reports

**Always:** the table-level base-file size distribution (count, total, min / max / mean / median,
p50 / p90 / p95 / p99) and the per-partition file-count distribution, followed by the partition-size
skew section (coefficient of variation, Gini coefficient, largest and top-10 partition share, and
partitions above mean + 2 sigma). When `--enable-partition-stats` is on, the table-level block is
only printed if `--enable-table-stats` is also set.

**With `--enable-partition-stats`:** one row per partition, sorted by total bytes descending, with
file count and size percentiles. `--top-n N` caps the rows shown.

**With `--include-row-counts`:** `numRecords` and `avgRowSize` per partition and for the table.
Row counts come from the metadata table's `column_stats` partition when the table has one (one
lookup per partition), and from Parquet footer reads otherwise (one read per file, which is slow
on object storage). If column stats cover only some files of a partition, only the missing files
fall back to footer reads.

**With `--analyze-table-characteristics`:** three detectors, each with a verdict, the thresholds
it used, and findings:

| Detector | Verdict | Fires when |
|---|---|---|
| `micro-partition` | `CLEAN` / `FLAGGED` | `numPartitions > --micro-partition-count-threshold`, **or** (once the table is at least `--micro-partition-min-age-days` old) any partition holds `>= --micro-partition-min-files` base files averaging under `--micro-partition-max-avg-bytes`. |
| `small-files` | `CLEAN` / `MODERATE` / `SEVERE` / `SKIPPED` | Among partitions with `>= --small-files-min-files-per-partition` base files ("qualifying"), the share whose average base file is under `--small-files-threshold-bytes` reaches `--small-files-moderate-pct` (MODERATE) or `--small-files-severe-pct` (SEVERE). `SKIPPED` when the table has fewer than `--small-files-min-table-commits` ingest commits. |
| `hot-partitions` | `CLEAN` / `FLAGGED` / `SKIPPED` | A partition was written by at least `--hot-partition-commit-share` of the last `--hot-window-commits` completed ingest commits (`commit`, `deltacommit`, and `replacecommit` from insert-overwrite; compaction and clustering are excluded). `SKIPPED` when no ingest commit is on the active timeline. |

`--print-top-k K` adds, under the micro-partition and small-files detectors, the K partitions
with the smallest average base file size among those passing the detector's file-count gate.

## Options

### Input (one required)

| Flag | Meaning |
|---|---|
| `--base-path`, `-bp` | Base path of the table (local fs, `s3://`, `gs://`, `abfs://`, ...) |
| `--props-path`, `-pp` | File with one base path per line; the analyzer runs against each in turn |

### Output

| Flag | Default | Meaning |
|---|---|---|
| `--output`, `-o` | `TABLE` | `TABLE` for humans, `JSON` for machines (one object per table on stdout) |
| `--enable-table-stats`, `-fs` | off | Print the table-level distribution and skew section even when partition stats are on |
| `--enable-partition-stats`, `-ps` | off | Print one row per partition |
| `--top-n`, `-tn` | `0` (all) | With partition stats, show only the N largest partitions |
| `--include-row-counts`, `-irc` | off | Add `numRecords` and `avgRowSize` |
| `--analyze-table-characteristics`, `-atc` | off | Run the three detectors |
| `--print-top-k` | `0` | With the detectors, list the K smallest-average-file partitions under each |

### Date filtering (date-partitioned tables only)

| Flag | Meaning |
|---|---|
| `--num-days`, `-nd` | Only partitions dated within the last N days |
| `--start-date`, `-sd` | Only partitions dated on or after this date (`yyyy-M-d` or `yyyy/M/d`) |
| `--end-date`, `-ed` | Only partitions dated strictly before this date |

Partition names must be `yyyy/M/d`, `yyyy-M-d`, or `column=<date>`. On any other scheme the run
fails fast with "Cannot apply --start-date, --end-date, or --num-days when partition does not
contain date"; drop the date flags. `--start-date` wins over `--num-days` when both are given.

### Detector thresholds

| Flag | Default | Meaning |
|---|---|---|
| `--micro-partition-count-threshold` | `10000` | Partition count above which the table is micro-partitioned |
| `--micro-partition-min-files` | `25` | File-count gate for the size-based micro-partition rule |
| `--micro-partition-max-avg-bytes` | `52428800` (50 MB) | Average base file size below which the size rule matches |
| `--micro-partition-min-age-days` | `30` | Table age before the size rule applies; younger tables legitimately have small partitions |
| `--small-files-min-files-per-partition` | `5` | File-count gate for a partition to count toward the small-files ratio |
| `--small-files-threshold-bytes` | `52428800` (50 MB) | Average base file size below which a qualifying partition is flagged |
| `--small-files-moderate-pct` | `0.10` | Flagged / qualifying ratio for `MODERATE` |
| `--small-files-severe-pct` | `0.30` | Flagged / qualifying ratio for `SEVERE` |
| `--small-files-min-table-commits` | `10` | Ingest commits (active + archived) before a verdict is given |
| `--hot-window-commits` | `50` | Recent ingest commits scanned for hot partitions |
| `--hot-partition-commit-share` | `0.5` | Share of the window a partition must appear in to be hot |

### Spark

| Flag | Default | Meaning |
|---|---|---|
| `--spark-master`, `-ms` | none | Spark master, when not supplied by `spark-submit` |
| `--spark-memory`, `-sm` | `1g` | Executor memory |
| `--parallelism`, `-pl` | `200` | Reserved |
| `--hoodie-conf` | none | `key=value` Hudi property; repeatable |
| `--help`, `-h` | | Usage |

## JSON output

```json
{
  "basePath": "/path/to/table",
  "mdtEnabled": false,
  "numPartitions": 2,
  "totalBytes": 26624,
  "totalFiles": 18,
  "tableSizeStats": {"count": 18, "min": 1024, "max": 2048, "mean": 1479, "median": 1024, "p50": 1024, "p90": 2048, "p95": 2048, "p99": 2048},
  "fileCountPerPartition": {"count": 2, "min": 8, "max": 10, "mean": 9, "median": 9, "p50": 9, "p90": 10, "p95": 10, "p99": 10},
  "skew": {"cv": 0.2308, "gini": 0.1154, "largestPartitionShare": 0.6154, "top2Share": 1.0000, "outliers": []},
  "partitions": [
    {"partition": "p1", "files": 8, "totalBytes": 16384, "sizeStats": {"count": 8, "min": 2048, "max": 2048, "mean": 2048, "median": 2048, "p50": 2048, "p90": 2048, "p95": 2048, "p99": 2048}},
    {"partition": "p0", "files": 10, "totalBytes": 10240, "sizeStats": {"count": 10, "min": 1024, "max": 1024, "mean": 1024, "median": 1024, "p50": 1024, "p90": 1024, "p95": 1024, "p99": 1024}}
  ],
  "tableCharacteristics": {
    "tableAgeDays": 271,
    "numPartitions": 2,
    "overallStatus": "FLAGGED",
    "detectors": [
      {"name": "micro-partition", "status": "FLAGGED",
       "summary": "2 partitions (count threshold 10000); 2 partitions hold >= 5 base files averaging under 50.00 MB.",
       "findings": ["2 partitions hold at least 5 base files averaging under 50.00 MB on a table 271 days old; run clustering (HoodieClusteringJob) to merge them, and check that the writer's small-file limit and max file size let ingestion bin-pack new records into existing files."],
       "effectiveConfigs": {"effective.micro.partition.count.threshold": 10000, "effective.micro.partition.min.files": 5, "effective.micro.partition.max.avg.bytes": 52428800, "effective.micro.partition.min.age.days": 0, "observed.num.partitions": 2, "observed.table.age.days": 271, "observed.count.rule.triggered": false, "observed.size.rule.eligible": true, "observed.size.rule.triggered": true, "observed.size.rule.matching.partitions": 2},
       "topKSmallestPartitions": []},
      {"name": "small-files", "status": "SEVERE",
       "summary": "2 of 2 partitions with >= 5 base files average under 50.00 MB (100.0%; MODERATE at >= 10%, SEVERE at >= 30%).",
       "findings": ["2 of 2 qualifying partitions (100.0%) average under 50.00 MB per base file; run clustering (HoodieClusteringJob) to merge small files, and review hoodie.parquet.small.file.limit and hoodie.parquet.max.file.size on the writer so new writes bin-pack into existing files."],
       "effectiveConfigs": {"effective.small.files.threshold.bytes": 52428800, "effective.small.files.min.files.per.partition": 5, "effective.small.files.moderate.pct": 0.1, "effective.small.files.severe.pct": 0.3, "effective.small.files.min.table.commits": 10, "observed.ingest.commit.count": 10, "observed.qualifying.partitions": 2, "observed.flagged.partitions": 2, "observed.flagged.pct": 1.0000},
       "topKSmallestPartitions": []},
      {"name": "hot-partitions", "status": "FLAGGED",
       "summary": "1 partition(s) were written by >= 50% of the last 10 ingest commits (compaction and clustering excluded).",
       "findings": ["Partition 'p0' was written by 10 of the last 10 ingest commits (100%), 100.00 KB and 900 records.", "If these are not the partitions expected to absorb current writes (for example the latest date partition of a time-partitioned table), revisit the partition key so writes spread across partitions; otherwise size the writer's small-file handling and clustering around these partitions."],
       "effectiveConfigs": {"effective.hot.window.commits": 50, "effective.hot.partition.commit.share": 0.5, "effective.hot.partition.min.commit.count": 5, "observed.hot.window.effective.commits": 10, "observed.hot.partition.count": 1},
       "partitions": [{"partition": "p0", "commitCount": 10, "bytesWritten": 102400, "recordsWritten": 900}]}
    ]
  }
}
```

`partitions` is present only with `--enable-partition-stats`; `totalRecords`, `numRecords` and
`avgRowSize` only with `--include-row-counts`; `tableCharacteristics` only with
`--analyze-table-characteristics`. Inside each detector, keys prefixed `observed.` are
measurements taken from the table and keys prefixed `effective.` are the thresholds the
detector actually used, after command-line overrides. `overallStatus` is `FLAGGED` when any
detector found something and `CLEAN` otherwise. Under `--props-path`, one JSON object is printed
per table, in file order.

## Examples

```bash
SS='spark-submit --class org.apache.hudi.utilities.HoodieTableLayoutAnalyzer --master local[2] --conf spark.log.level=WARN'

# Table-level distribution and skew only
$SS $BUNDLE --base-path /tmp/orders

# Twenty largest partitions, plus the table-level block
$SS $BUNDLE --base-path /tmp/orders --enable-partition-stats --enable-table-stats --top-n 20

# Row counts, using metadata column stats when the table has them
$SS $BUNDLE --base-path /tmp/orders --enable-partition-stats --include-row-counts

# Full detector pass as JSON, with the five smallest partitions as evidence
$SS $BUNDLE --base-path s3://bucket/orders --enable-partition-stats \
  --analyze-table-characteristics --print-top-k 5 --output JSON

# Last seven days of a date-partitioned table
$SS $BUNDLE --base-path /tmp/orders --enable-partition-stats --num-days 7

# Several tables, one base path per line
$SS $BUNDLE --props-path /tmp/tables.txt --analyze-table-characteristics --output JSON
```

## Caveats

- **Base files only.** Log files of a Merge-on-Read table are not counted, so on-disk size is
  under-reported there and a small-file pile-up can be worse than the verdict says. The
  small-files detector adds a finding to that effect on Merge-on-Read tables.
- **Percentiles are sampled.** Each histogram uses a 1M-slot uniform reservoir at table level
  and a 4096-slot reservoir per partition; above those counts the percentiles are estimates.
- **Table age is a lower bound.** It is the earlier of the `hoodie.properties` modification time
  and the first instant on the active timeline. Both can be later than table creation (the
  properties file is rewritten on upgrades; early commits get archived), so a mature table may
  read as younger than it is. This only delays the size-based micro-partition rule.
- **Ingest-commit count.** The archived timeline is consulted only when the active timeline is
  under `--small-files-min-table-commits`, and its replacecommits are not read, so the count
  from the archive is a lower bound.
- **Hot partitions on log-heavy Merge-on-Read tables** may under-report, because the write stats
  of log files are noisier than those of base files.
- **Skew needs at least two partitions.** Single-partition and unpartitioned tables skip it.

## Common failure modes

| Symptom | Fix |
|---|---|
| `NoClassDefFoundError: org/apache/hadoop/fs/FileSystem` | Bare `java -cp` cannot run this tool; use `spark-submit`. |
| `Cannot apply --start-date, --end-date, or --num-days when partition does not contain date` | The table is not date-partitioned; drop the date flags. |
| `--output must be TABLE or JSON (got ...)` | Only `TABLE` and `JSON` are accepted (case-insensitive). |
| Driver OOM | Raise `--driver-memory`; a table with 100K+ partitions can need 8g or more for per-partition histograms. |
| `--include-row-counts` is slow | The table has no `column_stats` metadata partition, so every file's footer is read. Enable column stats on the writer, or drop the flag. |
| `numFiles=0` on a table that is not empty | No completed `commit` / `deltacommit` / `replacecommit` on the active timeline; inspect `.hoodie/timeline`. |

## Tests

`hudi-utilities/src/test/java/org/apache/hudi/utilities/TestHoodieTableLayoutAnalyzer.java` builds
synthetic tables in a temp directory and drives the analyzer end to end.

```bash
export SPARK_LOCAL_IP=127.0.0.1
mvn test -pl hudi-utilities -Dtest=TestHoodieTableLayoutAnalyzer
```
