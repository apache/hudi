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

# TimelineInspector — Runbook

Pure-Java CLI for inspecting a Hudi table's active and archived timeline without
Spark. Lives at `org.apache.hudi.tools.TimelineInspector` in `hudi-common`.

## Modes at a glance

| Mode | What it does |
|---|---|
| `--show-instant <ts>` | Dump all states (REQUESTED / INFLIGHT / COMPLETED) of an instant with full metadata. |
| `--find-file-id <id>` | Sorted event table for every timeline action touching a given file id / filename / instant. |
| `--commit-stats` | Per-ingestion-commit records / files / bytes for a time range, with totals + averages. |
| `--phase-timings` | Per-ingestion-instant wall-clock phase split (source read + indexing + small-file handling, data write, MDT prep, MDT write, MDT tail) derived from `.hoodie/` state-marker file mtimes. Active timeline only. |
| `--parse-filename <name>` | Decode a Hudi parquet/log filename into its parts (no base path needed). |
| `--raw-archive <ts>` | Walk archive log files and dump the full `HoodieArchivedMetaEntry` for an instant. |

## Classpath

The tool only needs `hudi-common` + Hadoop + Jackson on the classpath. The
Hudi bundles mark Hadoop/Jackson as `provided`, so a bare
`java -cp hudi-utilities-bundle.jar ...` will fail with
`NoClassDefFoundError: org/apache/hadoop/fs/FileSystem` — those deps come
from the host (Spark, Flink, Hadoop). Pick the option that matches where
you're running.

### Option 1 — `spark-submit` (recommended)

Works inside any pod / container that already has Spark + Hudi, and works on a
laptop with a local Spark install.
`spark-submit` assembles `$SPARK_HOME/jars/*` + `$(hadoop classpath)` + the
application jar before launching the JVM, so every dep is resolved
automatically.

```bash
spark-submit \
  --class org.apache.hudi.tools.TimelineInspector \
  --master "local[1]" \
  --conf spark.log.level=WARN \
  /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  --base-path s3://my-bucket/lake/orders \
  --commit-stats --no-archived
```

Notes:

- `TimelineInspector.main` is a plain `main(String[])` and never touches
  `SparkSession` — the Spark machinery just sits unused. `spark-submit` is
  effectively being used as a classpath resolver.
- `--master "local[1]"` keeps it from contacting a cluster manager. Do **not**
  use `--master yarn` / `k8s://…` — you'd queue a real Spark app for no
  reason.
- `--deploy-mode client` (the default for `local[…]`) keeps stdout in the
  terminal. `--deploy-mode cluster` would route the table output to driver
  logs.
- Set `--driver-memory 4g` (or `8g`) when archived timeline is included with
  no time window — the tool calls `loadCompletedInstantDetailsInMemory()` in
  that case.
- `--conf spark.log.level=WARN` suppresses Spark's INFO banner so the table
  output is readable. Alternatively redirect `2>/dev/null`.

Equivalent bundles: `hudi-spark3.5-bundle_2.12-0.14.1-rc2.jar`,
`hudi-cli-bundle_2.12-0.14.1-rc2.jar`, `hudi-utilities-slim-bundle_2.12-0.14.1-rc2.jar`.

### Option 2 — Bundled jar with Hadoop on the classpath

For environments where `spark-submit` isn't available but Hadoop or a Spark
install is. Pick **one** of:

```bash
# 2a: hadoop jar prepends $(hadoop classpath) automatically
hadoop jar hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/tbl --commit-stats

# 2b: equivalent, explicit
java -cp "hudi-utilities-bundle_2.12-0.14.1-rc2.jar:$(hadoop classpath)" \
  org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/tbl --commit-stats

# 2c: use Spark's bundled Hadoop jars without invoking spark-submit
#     ($SPARK_HOME must point to a spark-X.Y.Z-bin-hadoopN build —
#      NOT the "without-hadoop" flavor)
java -cp "hudi-utilities-bundle_2.12-0.14.1-rc2.jar:$SPARK_HOME/jars/*" \
  org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/tbl --commit-stats
```

Notes:

- The `dir/*` form in `-cp` is a **JVM** glob, not a shell glob. Quote the
  entire `-cp` value so the shell doesn't expand it; the JVM expands
  `dir/*` to "every `.jar` in `dir`."
- On Java 11+, if you hit `IllegalAccessError` / `module … does not "opens" …`,
  add the same opens Spark uses:
  `--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`.
  Java 8 needs nothing extra.

### Option 3 — Build-tree classpath via Maven (laptop / dev only)

For running directly out of a build tree without any Hadoop/Spark install
(useful for local iteration / testing):

```bash
mvn -pl hudi-common dependency:build-classpath -DincludeScope=test \
  -Dmdep.outputFile=/tmp/ti_cp.txt

java -cp "hudi-common/target/hudi-common-0.14.1-rc2.jar:$(cat /tmp/ti_cp.txt)" \
  org.apache.hudi.tools.TimelineInspector --help
```

- `-DincludeScope=test` resolves every transitive dep of `hudi-common` —
  including the Hadoop and Jackson jars that are `provided`-scoped at
  runtime. The Maven step is one-shot; the `$CP` string is reusable until
  deps change.
- Use `hudi-common` as the `-pl` target. On the `0.14.1-rc2` branch there is
  no separate `hudi-hadoop-common` module — Hadoop FS classes come in
  transitively from `hudi-common`'s `hadoop-client` dependency.

### Option 4 — Cloud-backed base paths

If `--base-path` points to `s3://`, `gs://`, or `abfs://`, the host's
filesystem classpath needs the matching cloud connector. With
`spark-submit`, the easiest path is `--packages`:

```bash
spark-submit \
  --class org.apache.hudi.tools.TimelineInspector \
  --master "local[1]" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  --base-path s3://my-bucket/lake/orders \
  --commit-stats --no-archived
```

Equivalents: `hudi-gcp-bundle` for GCS, `hudi-azure-bundle` for Azure
Blob / ADLS (or `--packages` with the matching `hadoop-azure` /
`gcs-connector` artifact). Cloud credentials need to be wired the usual
way — `AWS_*` env vars or an instance profile for S3,
`GOOGLE_APPLICATION_CREDENTIALS` for GCS, etc.

### Quick sanity check

To confirm the classpath resolves without touching a real table:

```bash
spark-submit --class org.apache.hudi.tools.TimelineInspector --master "local[1]" \
  hudi-utilities-bundle_2.12-0.14.1-rc2.jar --help
```

If usage prints, the classpath is good — `--help` doesn't open any files.

## Common options

| Flag | Meaning |
|---|---|
| `--base-path <path>` | Table base path (required for every mode except `--parse-filename`). |
| `--start-instant <ts>` | Only consider instants `>= ts` (Hudi 17-digit `yyyyMMddHHmmssSSS`). |
| `--end-instant <ts>` | Only consider instants `<= ts`. |
| `--actions <csv>` | Restrict to actions; valid values: `commit,deltacommit,replacecommit,clean,rollback,restore,compaction,logcompaction,savepoint`. For `--commit-stats` and `--phase-timings` the set is restricted to `commit,deltacommit,replacecommit`. |
| `--no-archived` | Skip the archived timeline (default: include it; ignored for `--phase-timings` which is active-only). |
| `--include-archived <bool>` | Explicit form of the above. |
| `--limit <N>` | Max rows to print (default 100). |
| `--sort <asc\|desc>` | Instant sort order for `--commit-stats` and `--phase-timings` (default `desc` — newest first, so `--limit N` returns the latest N in range). |
| `--include-replacecommit` | In `--phase-timings`, also include `replacecommit` instants (clustering / insert-overwrite). Off by default. |
| `--output <table\|json>` | Output format (default `table`). |
| `--quiet` / `-q` | Suppress per-instant deserialization warnings on stderr. |

> When archived is included **and no time range is set**, the tool calls
> `loadCompletedInstantDetailsInMemory()` — fine on small tables, but heavy on
> long-lived ones. For large tables either set `--start-instant`/`--end-instant`
> or pass `--no-archived`.

## Mode: `--commit-stats`

Lists per-commit ingestion stats — records, files, bytes — for any range. Only
considers **completed** commit / deltacommit / replacecommit instants.

### Columns

| Column | Source / meaning |
|---|---|
| `instant` | 17-digit instant timestamp. |
| `timeline` | `ACTIVE` or `ARCHIVED`. |
| `action` | `commit` / `deltacommit` / `replacecommit`. |
| `operation` | `WriteOperationType` from the commit metadata (`INSERT`, `UPSERT`, `BULK_INSERT`, `DELETE`, `CLUSTER`, …). |
| `numInserts` | `HoodieCommitMetadata.fetchTotalInsertRecordsWritten()`. |
| `numUpdates` | `HoodieCommitMetadata.fetchTotalUpdateRecordsWritten()`. |
| `numDeletes` | `HoodieCommitMetadata.getTotalRecordsDeleted()`. |
| `filesInserted` | `fetchTotalFilesInsert()` — write stats whose `prevCommit == "null"`. |
| `filesUpdated` | `fetchTotalFilesUpdated()` — write stats whose `prevCommit` is set and not `"null"`. |
| `filesDeleted` | Replacecommit only: count of file ids in `partitionToReplaceFileIds` (logical replacement). Always `0` for plain commit/deltacommit. |
| `bytesWritten` | `fetchTotalBytesWritten()`. |
| `partitions` | Number of partitions touched (`partitionToWriteStats.size()`). |

### Footer

Prints totals and per-commit averages over **all** commits in the range
(not just the rows returned after `--limit`):

```
totals (over N commits): inserts=… updates=… deletes=…
avg per commit:           inserts=… updates=… deletes=…
```

### Examples

**1. Latest 50 ingestion commits, active timeline only:**

```bash
java -cp hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --commit-stats \
  --no-archived \
  --limit 50
```

**2. All commits in a calendar week (includes archived):**

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path s3://my-bucket/lake/orders \
  --commit-stats \
  --start-instant 20260601000000000 \
  --end-instant   20260608000000000
```

**3. Only deltacommits in a single day, JSON output:**

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --commit-stats \
  --actions deltacommit \
  --start-instant 20260601000000000 \
  --end-instant   20260601235959999 \
  --output json
```

**4. Oldest-first sort for chronological analysis:**

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --commit-stats \
  --start-instant 20260101000000000 \
  --sort asc \
  --limit 200 \
  --quiet
```

### Expected table output

```
instant            timeline  action         operation  numInserts  numUpdates  numDeletes  filesInserted  filesUpdated  filesDeleted  bytesWritten  partitions
-----------------  --------  -------------  ---------  ----------  ----------  ----------  -------------  ------------  ------------  ------------  ----------
20260604000000000  ACTIVE    replacecommit  CLUSTER    50          0           0           1              0             1             0             1
20260601000000000  ACTIVE    commit         INSERT     100         0           0           1              0             0             0             1

totals (over 2 commits): inserts=150 updates=0 deletes=0
avg per commit:           inserts=75.00 updates=0.00 deletes=0.00
```

## Mode: `--phase-timings`

Lists per-ingestion-instant wall-clock phase splits derived from the modification
times of the `.hoodie/` state-marker files on the **active timeline**. Useful for
checkpoint-SLA debugging and "where is the time going?" investigations.

### How the phases are derived

For each completed ingest instant, the tool stats six files:

```
T0 = mtime of  <base>/.hoodie/<ts>.<action>.requested
T1 = mtime of  <base>/.hoodie/<ts>.<action>.inflight
T2 = mtime of  <base>/.hoodie/metadata/.hoodie/<ts>.deltacommit.requested   (MDT)
T3 = mtime of  <base>/.hoodie/metadata/.hoodie/<ts>.deltacommit.inflight    (MDT)
T4 = mtime of  <base>/.hoodie/metadata/.hoodie/<ts>.deltacommit             (MDT, completed)
T5 = mtime of  <base>/.hoodie/<ts>.<action>                                  (data-table completed)
```

The MDT timeline always uses `deltacommit` regardless of whether the data table
is COW (`<action> = commit`) or MOR (`<action> = deltacommit`).

### Columns

| Column | Definition |
|---|---|
| `instant` | 17-digit instant timestamp. |
| `action` | `deltacommit`, `commit`, or `replacecommit` (with `--include-replacecommit`). |
| `sourceReadIndexSfh_ms` | `T1 − T0` — data table: source read + indexing + small-file handling. |
| `dataWrite_ms` | `T2 − T1` when MDT present; otherwise `T5 − T1` (data write extends through completion). |
| `mdtPrep_ms` | `T3 − T2` — metadata record preparation. Blank when MDT absent. |
| `mdtWrite_ms` | `T4 − T3` — metadata writes. Blank when MDT absent. |
| `mdtTail_ms` | `T5 − T4` — gap between MDT completion and data-table completion (often near-zero; non-trivial values indicate post-MDT bookkeeping). Blank when MDT absent. |
| `total_ms` | `T5 − T0`. |

### Behavior

- **MDT detection is per-instant.** If `<base>/.hoodie/metadata/.hoodie/` exists,
  the tool tries to stat all three MDT files for each instant; missing the MDT
  files for a particular instant (e.g. it predates MDT enablement) falls back to
  the 3-phase view for that row only. Other rows in the same output can still
  report the full 6 phases.
- **MDT absent entirely** → the report header says
  `(metadata table directory not found — MDT phases will be blank)` and every
  row uses the 3-phase view.
- **Missing data-table state file** (e.g. `.requested` deleted by a rollback,
  partial write) → the row is **excluded** from the body table and counted in
  the footer's `skipped` block, with a per-reason breakdown.
- **Archived instants are not supported.** Once an instant is archived, the
  `.requested` / `.inflight` files no longer exist on disk and mtimes are
  unrecoverable. `--phase-timings` always reads the active timeline only;
  `--include-archived` / `--no-archived` are ignored.
- **Default action set** is `deltacommit, commit` (the ingest actions).
  Pass `--include-replacecommit` to also report clustering / insert-overwrite
  replacecommits, which write to MDT the same way and fit the same 6-phase
  model. Explicit `--actions <csv>` overrides the default but is still
  restricted to commit / deltacommit / replacecommit.

### Footer

For each action present in the output:

```
aggregates (per action, ms):
  deltacommit (n=10):
    sourceReadIndexSfh:  mean=… p50=… p95=… max=…
    dataWrite:           mean=… p50=… p95=… max=…
    mdtPrep:             mean=… p50=… p95=… max=…
    mdtWrite:            mean=… p50=… p95=… max=…
    mdtTail:             mean=… p50=… p95=… max=…
    total:               mean=… p50=… p95=… max=…
    share of total (mean):  sourceReadIndexSfh=…%, dataWrite=…%, mdtPrep=…%, mdtWrite=…%, mdtTail=…%
```

Percentiles use nearest-rank (no interpolation). The `share of total` line uses
mean values, so percentages sum to ~100% modulo rounding.

If any instants were dropped, a trailing line reports them:

```
skipped 3 instant(s) with incomplete state-marker files: requested-missing=2, inflight-missing=1
```

### Caveats

- **Object-store mtimes** (S3 / GCS / ABFS) reflect server-side write-completion
  time, not the producer's wall clock. The relative phase durations are still
  accurate; clock-skew artifacts are bounded by the storage layer's commit
  granularity.
- **Local FS mtime resolution** can be second-level on macOS / older Linux
  setups; sub-second phase durations may round to 0 on those systems.
- The mtime of the completed marker is approximately the moment the writer
  finished — *not* when downstream readers observed the instant.

### Examples

**1. Latest 10 ingestion commits with phase splits:**

```bash
spark-submit --class org.apache.hudi.tools.TimelineInspector --master "local[1]" \
  /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  --base-path /tmp/conductor_soong/conductor_june1/tbl_path/tbl_path \
  --phase-timings --limit 10 --quiet
```

**2. Last hour of commits including clustering, JSON output:**

```bash
spark-submit --class org.apache.hudi.tools.TimelineInspector --master "local[1]" \
  /path/to/hudi-utilities-bundle_2.12-0.14.1-rc2.jar \
  --base-path s3://lake/orders \
  --phase-timings --include-replacecommit \
  --start-instant 20260601150000000 --end-instant 20260601160000000 \
  --output json --quiet
```

### Expected table output

```
instant            action       sourceReadIndexSfh_ms  dataWrite_ms  mdtPrep_ms  mdtWrite_ms  mdtTail_ms  total_ms
-----------------  -----------  ---------------------  ------------  ----------  -----------  ----------  --------
20260609044441149  deltacommit  18234                  91200         4810         63450        2710        180404
20260609042932195  deltacommit  17541                  84030         4632         60110        2105        168418

aggregates (per action, ms):
  deltacommit (n=2):
    sourceReadIndexSfh:  mean=17888 p50=17541 p95=18234 max=18234
    dataWrite:           mean=87615 p50=84030 p95=91200 max=91200
    mdtPrep:             mean=4721  p50=4632  p95=4810  max=4810
    mdtWrite:            mean=61780 p50=60110 p95=63450 max=63450
    mdtTail:             mean=2408  p50=2105  p95=2710  max=2710
    total:               mean=174411 p50=168418 p95=180404 max=180404
    share of total (mean):  sourceReadIndexSfh=10.3%, dataWrite=50.2%, mdtPrep=2.7%, mdtWrite=35.4%, mdtTail=1.4%
```

## Mode: `--show-instant`

Dumps every state of a single instant (REQUESTED, INFLIGHT, COMPLETED) with the
deserialized metadata body. Works against both active and archived timelines.

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --show-instant 20260530092852891
```

Filter to one state:

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --show-instant 20260530092852891 \
  --state COMPLETED \
  --output json
```

When `--show-instant` returns "(no body)" but you suspect the archive carries
more info, fall back to `--raw-archive` (below).

## Mode: `--find-file-id`

Prints every timeline event that mentions the given file id / filename / instant
time across both timelines. Useful for forensic "where did this file come from
and what happened to it" questions.

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --find-file-id 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0 \
  --actions commit,deltacommit,replacecommit,clean \
  --start-instant 20260101000000000 \
  --limit 500
```

Add `--lifecycle` to collapse the output to landmark rows (CREATED / REPLACED /
CLEANED / ROLLED_BACK) with a summary footer:

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --find-file-id 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0 \
  --lifecycle
```

Match types reported in the events table:

- `writeStat` — commit metadata write stat for the file.
- `replaceFileId` — replacecommit's `partitionToReplaceFileIds`.
- `clusteringInputSlice` — file is an input slice of a clustering plan.
- `cleanSuccessDelete` / `cleanFailedDelete` / `cleanDeletePattern` — clean metadata.
- `rollbackSuccessDelete` / `rollbackFailedDelete` / `rollbackPlanFile` / `rollbackPlanLogBlock` — rollback metadata / plan.
- `rollbackOfCommit` / `restoreOfCommit` / `restorePlanTargetCommit` / `restorePlanSavepoint` — when the needle matches a rolled-back / restored instant time.

## Mode: `--parse-filename`

Decode a parquet or log filename to its parts. No table required.

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --parse-filename 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0_0-1-1_20260530092852891.parquet
```

```
input                : 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0_0-1-1_20260530092852891.parquet
filename             : 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0_0-1-1_20260530092852891.parquet
kind                 : base
fileId               : 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0
commitTime           : 20260530092852891
fileExtension        : .parquet
writeToken           : 0-1-1
```

Log file:

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --parse-filename .5d85210a-fc17-411e-a01f-3bb44d9a12cc-0_20260530092852891.log.1_0-2-2
```

## Mode: `--raw-archive`

Walks `.hoodie/archived/commits.*` and dumps the **entire**
`HoodieArchivedMetaEntry` record for an instant — every sibling field, including
ones `--show-instant` doesn't render. Useful when `--show-instant` reports
"(no body)" against an archived instant: the actual metadata may be sitting on a
field name `show-instant` doesn't read.

```bash
java -cp ... org.apache.hudi.tools.TimelineInspector \
  --base-path /tmp/apna_table/tbl_path \
  --raw-archive 20260530092852891
```

Output is always JSON, grouped into `populatedFields` and `nullFields` per
archive entry.

## Troubleshooting

- **"WARN: failed to process … : …"** — a single instant's metadata couldn't be
  deserialized. The tool keeps going; pass `-q` / `--quiet` to suppress these.
- **"(no archive entries with commitTime=…)"** — the instant isn't in any
  archive log file under `.hoodie/archived/`. It might still be in active
  timeline — try `--show-instant` without `--no-archived`.
- **Heap pressure on huge tables** — narrow the time window
  (`--start-instant` + `--end-instant`) or pass `--no-archived`. Both
  `--commit-stats` and `--find-file-id` honor those filters.
- **`--state` only applies to `--show-instant`.** For `--find-file-id` and
  `--commit-stats` the tool restricts to completed instants by construction.
- **`--lifecycle` is `--find-file-id`-only**; it errors out otherwise.

## Tests

`TestTimelineInspector` in `hudi-common/src/test/java/org/apache/hudi/tools/`
builds a synthetic CoW table in `@TempDir`, populates ingest / clean / rollback /
replace instants, and exercises every mode end-to-end via `main(...)` with
stdout capture. Run from the repo root:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
mvn test -pl hudi-common -Dtest="org.apache.hudi.tools.TestTimelineInspector"
```
