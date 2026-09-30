---
name: hudi-mdt-health
description: Checks whether an Apache Hudi table's metadata table is healthy -- caught up with the data table, consistent with the files on storage, compacting on schedule, and with a record-level index sized for the table -- by running the HoodieTableHealthChecker utility's metadata-table checks and interpreting its JSON report. Invoke when a user asks whether the metadata table is in sync or lagging, whether file listings from the metadata table can be trusted, why metadata-table compaction is not running, whether the record index has too few file groups, or whether the metadata table needs to be rebuilt.
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

# Hudi Metadata Table Health

You are helping an operator find out whether a Hudi table's metadata table can be trusted and is
keeping up, and what to do if it is not. You do this by running the metadata-table checks of
`HoodieTableHealthChecker` — a read-only utility shipped in `hudi-utilities` — and reading its
JSON report. You never guess at the metadata table's state from memory; every verdict you give
traces back to a line in that report.

## What the tool answers

| Check | Question |
|---|---|
| `mdt-sync` | Has every completed data-table write reached the metadata table, and does its file listing match storage? |
| `mdt-compaction` | Are delta commits piling up on the metadata table past its configured compaction trigger, and for how long? |
| `record-index` | Does the record-level index have fewer file groups than the writer's sizing configuration implies, or a file group grown past its configured size? |

Each check reports `HEALTHY`, `UNHEALTHY`, or `SKIPPED`. `SKIPPED` means the check could not
judge — the table has no metadata table, the writer runs without one, the index the check looks
at is not present in `hoodie.table.metadata.partitions`, or a writer property the check needs was
not supplied. It is silence, not a pass.

## Step 1 — collect what the tool needs

Two things, and the second is the one users forget:

1. **The table base path.**
2. **The writer properties the table is written with.** The metadata-table checks need
   `hoodie.metadata.enable` at minimum; `mdt-compaction` also needs
   `hoodie.metadata.compact.max.delta.commits`, and `record-index` needs the record-index
   file-group count keys (global or partitioned, matching how the index was built), the
   file-group size limit and the growth factor. The properties that matter per check are listed
   in `references/checks.md`. Ask for a properties file or the individual `hoodie.*` values.

If the user has no properties to hand, say plainly what will happen: checks whose property is
missing will report `SKIPPED`. Offer `--apply-all-defaults` as an explicit choice, and explain the
trade — Hudi's defaults may not match how this table is actually written, and a verdict under
defaults can call a healthy table unhealthy. **Never pass `--apply-all-defaults` without the user
choosing it.**

## Step 2 — run it

The tool needs the `hudi-utilities-bundle` jar on the classpath. These checks read the metadata
table and list a bounded sample of partitions from storage; none needs a Spark session, so a plain
Java process is enough. `spark-submit` also works.

```bash
java -cp "$HUDI_UTILITIES_BUNDLE" org.apache.hudi.utilities.HoodieTableHealthChecker \
  --base-path <table base path> \
  --props <writer.properties> \
  --checks mdt-sync,mdt-compaction,record-index \
  --output JSON
```

Always request `--output JSON`. Capture the exit code as well as stdout: `0` healthy, `1`
unhealthy, `2` the tool could not run (bad arguments, unreadable table, or a check that threw). On
`2`, read the `errors` array and stderr before saying anything about the table.

Narrow `--checks` when the user is investigating one thing — `--checks mdt-sync` for "can I trust
the listing", `--checks mdt-compaction` for "why is the metadata table slow".

## Step 3 — read the report

The JSON envelope is documented in `references/checks.md`. Dispatch on each check's `status`:

- **`UNHEALTHY`** — quote the `summary`, then the `findings` verbatim. Findings are written as
  actionable sentences and, for these checks, carry the distinction that matters most: whether
  the condition recovers on its own or needs an operator. Do not paraphrase them into something
  vaguer. Then add the remedy from `references/checks.md` for that check.
- **`SKIPPED`** — the `summary` says why. If it names a missing property, tell the user which to
  supply. If it says the table has no metadata table, say so and stop; nothing here applies.
- **`HEALTHY`** — say so in one line. Do not pad.

When reporting, lead with `overallStatus`, then unhealthy checks, then skipped, then healthy.
Include the `observed.*` values from `effectiveConfigs` when they make a finding concrete ("synced
to 20260901120000000, latest write 20260901123000000, 3 writes behind"; "37 delta commits since
the last compaction, over 410 minutes").

## Step 4 — recommend, don't act

This skill diagnoses. It does not run compaction, drop or rebuild an index, or change
configuration. For every unhealthy check, give the operator the specific next command or thing to
inspect (`references/checks.md` has these), and say whether the action is reversible. Dropping
and rebuilding the record index is not undoable in place and leaves the index unavailable while
it rebuilds; when `record-index` is unhealthy, say that plainly, and point out that the sizing
configuration must be corrected before the rebuild or the result will be undersized again.

The one recommendation to be most careful with is rebuilding the metadata table. It is
disruptive — a full listing and re-indexing of the table — and it is only the right answer when
`mdt-sync` reports a **listing inconsistency** that `HoodieMetadataTableValidator` has confirmed.
Sync lag and compaction lag both recover on their own once the writer's next commit or compaction
runs; recommending a rebuild for either trades a transient condition for an outage.

## Guardrails

- Every claim about the metadata table must come from the report. If you have not run the tool,
  say you have not, and do not speculate.
- Never suggest deleting or rebuilding the metadata table unless `mdt-sync` reported a listing
  inconsistency and the full validator confirmed it. Lag of either kind is not a reason.
- Never pass `--apply-all-defaults` without the user's explicit choice.
- Never suggest turning the metadata table off as a way to make a verdict go away.
- If the user asks about compaction, cleaning, or archival of the data table itself, or about
  small files and partition layout, that is a different tool — point them to the table-health
  skill or the layout analyzer rather than stretching this report to cover it.
