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

# Detectors, thresholds, and remedies

## JSON envelope

```json
{
  "basePath": "...",
  "mdtEnabled": true,
  "numPartitions": 412,
  "totalBytes": 1234567890,
  "totalFiles": 5678,
  "totalRecords": 90123456,
  "tableSizeStats": {"count": 5678, "min": 1024, "max": 134217728, "mean": 17234567, "median": 16000000, "p50": 16000000, "p90": 40000000, "p95": 60000000, "p99": 120000000},
  "fileCountPerPartition": {"count": 412, "min": 1, "max": 25, "mean": 14, "median": 14, "p50": 14, "p90": 22, "p95": 23, "p99": 24},
  "skew": {"cv": 0.4231, "gini": 0.2104, "largestPartitionShare": 0.0214, "top10Share": 0.1532, "outliers": [{"partition": "country=US/date=2026-06-09", "totalBytes": 35000000, "files": 47}]},
  "partitions": [
    {"partition": "...", "files": 47, "totalBytes": 35000000, "sizeStats": {"count": 47, "min": 100000, "max": 1500000, "mean": 744680, "median": 700000, "p50": 700000, "p90": 1200000, "p95": 1400000, "p99": 1500000}, "numRecords": 120000, "avgRowSize": 291}
  ],
  "tableCharacteristics": {
    "tableAgeDays": 92,
    "numPartitions": 412,
    "overallStatus": "FLAGGED",
    "detectors": [
      {
        "name": "small-files",
        "status": "MODERATE",
        "summary": "one line for the operator",
        "findings": ["actionable sentence", "..."],
        "effectiveConfigs": {
          "effective.small.files.threshold.bytes": 52428800,
          "effective.small.files.min.files.per.partition": 5,
          "effective.small.files.moderate.pct": 0.1,
          "effective.small.files.severe.pct": 0.3,
          "effective.small.files.min.table.commits": 10,
          "observed.ingest.commit.count": 412,
          "observed.qualifying.partitions": 280,
          "observed.flagged.partitions": 47,
          "observed.flagged.pct": 0.1679
        },
        "topKSmallestPartitions": []
      }
    ]
  }
}
```

`partitions` appears only with `--enable-partition-stats`; `totalRecords`, `numRecords` and
`avgRowSize` only with `--include-row-counts`; `tableCharacteristics` only with
`--analyze-table-characteristics`. All sizes are bytes; ratios are fractions (0.1679, not 16.79%).

`effectiveConfigs` key convention: `effective.*` are the thresholds the detector actually used,
after command-line overrides; `observed.*` are measurements taken from the table. `overallStatus`
is `FLAGGED` when any detector is `FLAGGED`, `MODERATE` or `SEVERE`, and `CLEAN` otherwise.
`micro-partition` and `small-files` carry `topKSmallestPartitions` (filled by `--print-top-k`);
`hot-partitions` carries `partitions`, the full hot list sorted by commit count then bytes.

## Skew section

| Metric | Reading |
|---|---|
| `cv` | Standard deviation over mean of per-partition bytes. Under 0.3 uniform; 0.3–0.6 moderate; above 0.6 significant skew. |
| `gini` | 0 = every partition the same size; approaches 1 as one partition takes everything. Under 0.2 balanced; above 0.5 highly concentrated. |
| `largestPartitionShare`, `top10Share` | Fraction of table bytes in the largest partition and in the ten largest. The key becomes `top<N>Share` when the table has fewer than ten partitions. |
| `outliers` | Partitions above mean + 2 standard deviations. |

Skew needs at least two partitions; on a single-partition or unpartitioned table the section is
`{"cv": 0, "gini": 0, "outliers": []}`. Skew is a description, not a verdict: time-partitioned
tables with a long tail of old, small partitions are legitimately skewed.

## Per detector

### micro-partition

| | |
|---|---|
| Measures | Whether the table has too many partitions, or partitions that hold many tiny base files. |
| Count rule | `observed.num.partitions` above `effective.micro.partition.count.threshold` (`--micro-partition-count-threshold`, default 10000). |
| Size rule | Any partition with at least `effective.micro.partition.min.files` base files (`--micro-partition-min-files`, default 25) averaging under `effective.micro.partition.max.avg.bytes` (`--micro-partition-max-avg-bytes`, default 50 MB). Only applied once the table is `effective.micro.partition.min.age.days` old (`--micro-partition-min-age-days`, default 30); `observed.size.rule.eligible` says whether it ran. |
| `FLAGGED` | Either rule fired; `observed.count.rule.triggered` and `observed.size.rule.triggered` say which. |
| `CLEAN` | Neither rule fired. When `observed.size.rule.eligible` is false the verdict rests on the count rule alone; say so. |
| Remedy, count rule | Revisit the partition key: coarser partition columns (month instead of day, region instead of city), or keep the column as a clustering / bucketing key instead of a partition. This means a new table; it cannot be changed in place. |
| Remedy, size rule | Schedule clustering (`HoodieClusteringJob`, or inline / async clustering on the writer) to merge files, and check that `hoodie.parquet.small.file.limit` and `hoodie.parquet.max.file.size` let ingestion bin-pack into existing files. Clustering rewrites data; it does not lose it. |

### small-files

| | |
|---|---|
| Measures | Prevalence of underfilled partitions. Partitions with at least `effective.small.files.min.files.per.partition` base files (`--small-files-min-files-per-partition`, default 5) are "qualifying"; a qualifying partition whose average base file is under `effective.small.files.threshold.bytes` (`--small-files-threshold-bytes`, default 50 MB) is "flagged". |
| `CLEAN` | Flagged / qualifying below `effective.small.files.moderate.pct` (`--small-files-moderate-pct`, default 0.10), or no qualifying partition. |
| `MODERATE` | Ratio at or above the moderate tier. |
| `SEVERE` | Ratio at or above `effective.small.files.severe.pct` (`--small-files-severe-pct`, default 0.30). |
| `SKIPPED` | `observed.ingest.commit.count` under `effective.small.files.min.table.commits` (`--small-files-min-table-commits`, default 10); a table this young has not had time to show the pattern. Do not read it as clean. |
| Why it matters | Every extra file is a file-open and a footer read per query, and a file the cleaner and metadata table must track. Pile-up compounds over time. |
| Remedy | Run clustering via `HoodieClusteringJob` (or enable inline / async clustering) to merge small files, and review `hoodie.parquet.small.file.limit` and `hoodie.parquet.max.file.size` on the writer so new writes bin-pack into existing files. On a Merge-on-Read table, also check that compaction is keeping up, since log files are not counted here. If the user's target file size is not 50 MB, re-run with `--small-files-threshold-bytes` before acting. |

### hot-partitions

| | |
|---|---|
| Measures | Which partitions were written by a large share of recent ingest commits. Scans the last `effective.hot.window.commits` completed `commit`, `deltacommit` and `replacecommit` instants (`--hot-window-commits`, default 50), skipping compaction and clustering; `observed.hot.window.effective.commits` is how many ingests were actually found. A partition is hot when it appears in at least `effective.hot.partition.commit.share` of them (`--hot-partition-commit-share`, default 0.5), which is `effective.hot.partition.min.commit.count` commits. |
| `FLAGGED` | At least one hot partition; `partitions[]` lists them with commit count, bytes and records written. |
| `CLEAN` | No partition reached the share. |
| `SKIPPED` | No completed ingest commit on the active timeline, or the timeline could not be read. |
| Expected hotness | On a time-partitioned table the latest date partition is supposed to be hot. Say so and do not recommend a change for it. |
| Unexpected hotness | On a value-partitioned table (customer, region, category), a few hot partitions mean the partition key concentrates writes: those partitions will accumulate small files and be the ones clustering has to chase. |
| Remedy | If the hotness is expected, size the writer's small-file handling and clustering around those partitions. If not, revisit the partition key (a higher-cardinality or composite key, or bucketing) so writes spread out; that means a new table. |

## Recommended reading order

1. `overallStatus`, then any `SKIPPED` detector so the user knows what was not judged.
2. `small-files` and `micro-partition` together: both point at clustering and writer file-size
   settings, so one recommendation usually covers both.
3. `hot-partitions`, interpreted against the partitioning scheme the user describes.
4. `skew` and `partitions` for the numbers behind the verdicts.
