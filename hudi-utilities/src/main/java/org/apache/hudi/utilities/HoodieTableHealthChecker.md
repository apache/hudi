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

# HoodieTableHealthChecker

Reports whether a Hudi table's table services are keeping up with ingestion. Meant to be run on a
schedule — daily is typical — against the tables you are responsible for, with the exit code wired
into whatever alerts you.

The tool is strictly read-only. It inspects timeline state and reports; it never schedules or runs
a table service, and never writes to the table.

## Quick start

```bash
spark-submit \
  --master local \
  --class org.apache.hudi.utilities.HoodieTableHealthChecker \
  $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
  --base-path s3://bucket/path/to/table \
  --props s3://bucket/path/to/writer.properties
```

The checks currently shipped are timeline-only, so no Spark session is actually required. Running
it as a plain Java process works too, and is cheaper:

```bash
java -cp $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
  org.apache.hudi.utilities.HoodieTableHealthChecker \
  --base-path /path/to/table \
  --props /path/to/writer.properties
```

## Why writer properties are required

Nearly every question this tool answers is relative to how the table is written. Whether fifty
delta commits since the last compaction is fine or alarming depends on the configured trigger.
Whether a thousand instants in the active timeline is fine depends on the configured retention.

Hudi supplies a default for each of these, so the tool *could* always print an answer. But when
your table is written with different settings, that answer is wrong — and wrong in the direction
that destroys trust, reporting a perfectly healthy table as unhealthy.

So by default, **a check whose governing property you did not supply reports `SKIPPED` and names
the property**, rather than guessing. Supply properties with `--props` or `--hoodie-conf`.

If you know the table runs on stock settings, or you just want a rough look, pass
`--apply-all-defaults` to evaluate against Hudi defaults explicitly.

Every check echoes the configuration it actually reasoned with (`--verbose`, or the
`effectiveConfigs` field in JSON), so a verdict can always be read alongside the values that
produced it.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `--base-path`, `-sp` | *(required)* | Base path of the table to check |
| `--checks` | `all` | Comma-separated check names, or `all` |
| `--apply-all-defaults` | `false` | Evaluate against Hudi defaults for any property not supplied |
| `--output` | `TABLE` | `TABLE` for humans, `JSON` for machines |
| `--verbose`, `-v` | `false` | Include the effective configuration each check used |
| `--props` | none | Properties file (local fs or dfs) with the table's writer configuration |
| `--hoodie-conf` | none | Single `key=value` writer property; repeatable |
| `--help`, `-h` | | Usage, plus the list of available checks |

## Checks

### `compaction`

Whether compaction is keeping pace with delta commits on a Merge-on-Read table. Skipped entirely on
Copy-on-Write.

Computes how many delta commits the configured trigger strategy allows before compaction should be
scheduled, then compares that against how many have accumulated since the last completed
compaction. A slack factor of 2.0 is applied before calling the table unhealthy: scheduling and
execution are separate concerns, the execution strategy may legitimately defer individual file
slices, and async compaction routinely trails ingestion by a cycle or two. The goal is catching
compaction that has *stopped*, not policing jitter.

When unhealthy, the report distinguishes two cases: nothing scheduled at all (the scheduler is not
running), versus plans scheduled but never completed (the compactor is running but failing).

Governing properties: `hoodie.compact.inline.trigger.strategy`,
`hoodie.compact.inline.max.delta.commits`, `hoodie.compact.inline.max.delta.seconds`.

### `cleaner`

Whether the cleaner is keeping pace with write commits.

Counts completed write commits since the last completed clean and compares against the configured
trigger, with two allowances: the trigger is multiplied by 2.0 for async cleaning running on its
own cadence, and the result floors at 5 commits. The floor matters because the trigger defaults to
one commit -- without it, a cleaner two commits behind (routine under async cleaning, or when a
clean briefly loses the table lock to a concurrent writer) would be an alert.

When unhealthy, distinguishes a clean that is scheduled but stuck from no clean being scheduled at
all. The last clean's retention watermark and files-deleted count are echoed for context.

Governing property: `hoodie.clean.trigger.max.commits`. `hoodie.clean.policy` and
`hoodie.clean.commits.retained` are echoed when supplied.

### `savepoint`

Whether a savepoint is holding back archival.

Archival stops at the earliest savepoint and will not advance past it. A savepoint taken for a
one-off restore and then forgotten therefore pins the active timeline indefinitely — the timeline
grows without bound, every timeline load gets slower, and data files that cleaning would otherwise
release stay alive. Nothing errors while this happens.

Two signals are reported. A savepoint sitting further back than the configured archival retention
is blocking archival right now. A savepoint older than 7 days is flagged separately as likely
forgotten, since savepoints are normally short-lived.

The remedy is always to release the savepoint once the operation it was taken for is complete
(`savepoint delete` in `hudi-cli`).

Governing property: `hoodie.keep.max.commits`.

### `archival`

Whether archival is keeping the active timeline bounded.

Two independent signals. The first compares the active timeline against the configured retention,
with 1.1x slack, since archival runs periodically rather than on every commit. The second is an
absolute ceiling of 5000 completed instants — past roughly that point, loading and parsing the
timeline is itself a per-operation performance problem regardless of what the configuration
permits.

This check does not attribute the cause. A stalled archival is most often a savepoint, which the
`savepoint` check diagnoses specifically; read the two together.

Governing property: `hoodie.keep.max.commits`.

### `mdt-sync`

Whether the metadata table is caught up with the data table, and whether the file listing it
serves agrees with storage. Skipped when the table has no metadata table, or when the writer runs
with `hoodie.metadata.enable=false`.

Two signals. **Sync lag**: every completed data-table write is mirrored by a delta commit on the
metadata table with the same instant time, so the latest completed metadata-table delta commit is
the instant the metadata table is synced to. Any completed data-table write newer than that is one
the metadata table does not know about; the report gives both instants and how many writes are
missing. **Listing consistency**: for a bounded sample of partitions, the latest base-file and
log-file names are listed once through the metadata table and once straight from the file
system, and compared. The sample is all partitions when there are 20 or fewer; otherwise the last
20 in sorted order — the most recently written on a date-partitioned table, which is where drift
from writes, cleans and rollbacks originates — plus the first 2, so drift confined to old data is
not invisible. The sample is bounded because a file-system listing is exactly the cost the
metadata table exists to avoid.

Any lag or any differing partition is unhealthy. Lag alone comes with the advice to rerun if a
writer is active and otherwise to look for incomplete metadata-table delta commits
(`metadata timeline show incomplete` in `hudi-cli`); it is never a reason to rebuild. A differing
partition names the first few files on each side and points at the complete comparison —
`HoodieMetadataTableValidator` with `--validate-latest-file-slices --validate-latest-base-files` —
before any rebuild, which is disruptive and should be scheduled deliberately.

This check lists files, so it costs more than the timeline-only checks, but it still needs no
Spark session.

Governing property: `hoodie.metadata.enable`.

### `mdt-compaction`

Whether compaction on the metadata table is keeping pace with the delta commits landing on it.
Skipped when the table has no metadata table, or when the writer runs with
`hoodie.metadata.enable=false`.

The metadata table is a Merge-on-Read table of its own under `.hoodie/metadata`, compacted inline
by the data-table writer after its commits, every `hoodie.metadata.compact.max.delta.commits`
delta commits. The check counts completed metadata-table delta commits since the last completed
metadata-table compaction and applies the same 2.0 slack factor as the `compaction` check. How
long the lag has lasted is reported too, as the span from the last compaction (or the first delta
commit, if there has never been one) to the latest delta commit.

The most common cause is not a failure. To keep a compacted base file consistent with the data
table, the writer pins the compaction instant just below the earliest data-table instant that is
still pending but already applied to the metadata table; once a compaction at that pinned instant
exists, every later attempt is skipped until the pending instant completes or is rolled back. So
when unhealthy, the report names the earliest pending data-table instant, if any, as the first
thing to check. Compaction lag recovers on its own once compaction runs; it is never a reason to
rebuild the metadata table.

Governing properties: `hoodie.metadata.enable`, `hoodie.metadata.compact.max.delta.commits`.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Every check that ran reported healthy (or was skipped) |
| `1` | At least one check reported unhealthy |
| `2` | Bad arguments, the table could not be read, or a check failed to run |

Note that a run where everything skipped exits `0`. A skipped check is *silent* about health, not
an assertion of it — if you want skips to be loud, check for `SKIPPED` in the output or use JSON.

## JSON output

```json
{
  "basePath" : "/path/to/table",
  "tableType" : "MERGE_ON_READ",
  "overallStatus" : "UNHEALTHY",
  "checks" : [ {
    "name" : "compaction",
    "status" : "UNHEALTHY",
    "summary" : "60 delta commit(s) have accumulated since compaction at 00001, past the threshold of 10",
    "findings" : [
      "Trigger strategy NUM_COMMITS expects compaction roughly every 5 delta commit(s); 60 have accumulated.",
      "No compaction is scheduled. Check that compaction is enabled for this table and that the scheduling job is running."
    ],
    "effectiveConfigs" : {
      "hoodie.compact.inline.trigger.strategy" : "NUM_COMMITS",
      "hoodie.compact.inline.max.delta.commits" : "5",
      "effective.unhealthy.threshold.delta.commits" : "10",
      "observed.delta.commits.since.last.compaction" : "60"
    }
  } ],
  "errors" : [ ]
}
```

Keys prefixed `observed.` are measurements taken from the table; keys prefixed `effective.` are
thresholds derived from the configuration. Everything else is a writer property as supplied.

## Scheduling it

```bash
#!/usr/bin/env bash
# Daily table health check.
set -uo pipefail

java -cp "$HUDI_BUNDLE" org.apache.hudi.utilities.HoodieTableHealthChecker \
  --base-path "$TABLE" \
  --props "$WRITER_PROPS" \
  --output JSON > /tmp/health.json
status=$?

if [ $status -eq 1 ]; then
  alert "Hudi table $TABLE is unhealthy" < /tmp/health.json
elif [ $status -eq 2 ]; then
  alert "Hudi health check could not run for $TABLE" < /tmp/health.json
fi
```

## Caveats

- Verdicts are only as good as the writer properties you supply. Point `--props` at the same
  properties your writer actually uses.
- Thresholds are deliberately generous. This tool is built to catch a service that has stopped or
  fallen badly behind, not to tune one that is running.
- The table-level verdict is the most severe individual verdict. One unhealthy check makes the
  table unhealthy, regardless of how many others passed.
