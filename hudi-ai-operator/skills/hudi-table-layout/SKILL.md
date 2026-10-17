---
name: hudi-table-layout
description: Analyzes the physical layout of an Apache Hudi table (partition and file size distribution, skew, micro-partitioning, small-file pile-up, hot partitions) by running the HoodieTableLayoutAnalyzer utility and interpreting its JSON report. Invoke when a user asks how big a Hudi table or its partitions are, whether a table has a small-file problem, whether it is over-partitioned, which partitions take most of the writes, or how skewed the partitions are.
---

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

# Hudi Table Layout

You are helping an operator understand how a Hudi table is laid out on storage and whether that
layout is hurting it. You do this by running `HoodieTableLayoutAnalyzer` — a read-only Spark
utility shipped in `hudi-utilities` — and reading its JSON report. You never guess at a table's
layout from memory; every number and verdict you give traces back to a field in that report.

## What the tool answers

| Section | Question |
|---|---|
| `tableSizeStats`, `fileCountPerPartition` | How big are the base files, and how many does each partition hold? |
| `skew` | Is data spread evenly across partitions, or concentrated in a few? |
| `partitions` | Which partitions are largest, and what do their files look like? |
| `micro-partition` detector | Are there too many partitions, or partitions full of tiny files? |
| `small-files` detector | What share of partitions is piling up small base files? |
| `hot-partitions` detector | Which partitions absorb most recent writes? |

Each detector reports a status: `CLEAN` or `FLAGGED` for micro-partition and hot-partitions;
`CLEAN`, `MODERATE`, `SEVERE` or `SKIPPED` for small-files (hot-partitions is also `SKIPPED` when
there are no ingest commits to scan). `SKIPPED` means the detector could not judge. It is silence,
not a pass.

## Step 1 — collect what the tool needs

1. **The table base path.** Local, `s3://`, `gs://` or `abfs://`.
2. **Which sections the user cares about.** Map the question to flags:

| The user wants | Flags |
|---|---|
| Total size, file-size distribution | none (default) |
| Per-partition breakdown, largest partitions | `--enable-partition-stats` (add `--top-n 20` for "top N") |
| Skew across partitions | `--enable-partition-stats --enable-table-stats` |
| Record counts, average row size | `--enable-partition-stats --include-row-counts` |
| Small files, over-partitioning, hot partitions | `--enable-partition-stats --analyze-table-characteristics` (add `--print-top-k 5` for evidence) |
| Only the last N days of a date-partitioned table | `--num-days N` |

3. **Threshold overrides, only if the user has them.** The detectors ship with defaults (50 MB
   average file size, 10,000 partitions, 10% / 30% small-file tiers, 50-commit hot window at 50%
   share). If the user's target file size or write pattern differs, pass the matching
   `--micro-partition-*`, `--small-files-*` or `--hot-*` flag; `references/detectors.md` lists
   them. Do not invent overrides; defaults are fine for a first look.

Two things to warn about before running:

- `--include-row-counts` reads one Parquet footer per file when the table has no `column_stats`
  metadata partition. On a table with tens of thousands of files on object storage this is slow.
  Say so, and confirm before running it.
- Date filters only work when partition names are `yyyy-M-d`, `yyyy/M/d` or `column=<date>`. On
  any other scheme the run fails fast; drop the date flags.

## Step 2 — run it

The tool is a Spark application and needs `spark-submit`; a plain `java -cp` will not work.

```bash
spark-submit \
  --master "local[2]" \
  --driver-memory 4g \
  --conf spark.log.level=WARN \
  --class org.apache.hudi.utilities.HoodieTableLayoutAnalyzer \
  "$HUDI_UTILITIES_BUNDLE" \
  --base-path <table base path> \
  --enable-partition-stats --analyze-table-characteristics \
  --output JSON
```

Always request `--output JSON`. For object-store paths add the matching Hadoop connector
(`--packages org.apache.hadoop:hadoop-aws:3.3.4` for S3, or the GCS / Azure equivalent). Raise
`--driver-memory` for tables with more than roughly 50,000 partitions. Show the user the full
command before running it.

If `spark-submit` is not available, say so and stop; there is no pure-Java fallback for this tool.

## Step 3 — read the report

The JSON envelope is documented in `references/detectors.md`. Answer the user's question from the
fields, not from the whole dump:

- **Size questions** — answer from `totalBytes`, `totalFiles`, `numPartitions` and
  `tableSizeStats`. Quote the p50 and p95 file sizes; they say more than the mean.
- **Skew questions** — lead with `largestPartitionShare` and `top10Share`, which are intuitive,
  then give `cv` and `gini`. Rule of thumb: `cv` under 0.3 is uniform, 0.3–0.6 moderate, above
  0.6 significant; `gini` under 0.2 balanced, above 0.5 highly concentrated. Name the
  `outliers` when there are any.
- **Detectors** — dispatch on each detector's `status`:
  - **`FLAGGED`, `MODERATE`, `SEVERE`** — quote the `summary`, then the `findings` verbatim.
    Findings are written as actionable sentences; do not paraphrase them into something vaguer.
    Then add the remedy from `references/detectors.md`.
  - **`SKIPPED`** — the `summary` says why (too few ingest commits, or no ingest commits in the
    window). Say so and do not treat it as clean.
  - **`CLEAN`** — say so in one line. Do not pad.

When reporting, lead with `overallStatus`, then flagged detectors, then skipped, then clean.
Include the `observed.*` values from `effectiveConfigs` when they make a finding concrete
("47 of 280 qualifying partitions average under 50 MB"), and mention the `effective.*` threshold
the verdict was judged against, since a user with a different target file size may want to
re-run with an override.

## Step 4 — recommend, don't act

This skill diagnoses. It does not run clustering, change writer configuration, or repartition a
table. For every flagged detector, give the operator the specific next step from
`references/detectors.md` — typically scheduling clustering with `HoodieClusteringJob`, tuning the
writer's small-file settings, or revisiting the partition key — and say what it costs: clustering
rewrites data and takes time and compute; changing the partition scheme means a new table.

## Guardrails

- Every claim about the layout must come from the report. If you have not run the tool, say you
  have not, and do not speculate.
- On a Merge-on-Read table, say that log files are not counted and the small-file picture can be
  worse than reported; the report includes a finding to that effect.
- Never run `--include-row-counts` on a large table without warning about the cost.
- If the user asks whether compaction, cleaning or archival is keeping up, that is a different
  tool — point them to the table health skill rather than stretching this report to cover it.
