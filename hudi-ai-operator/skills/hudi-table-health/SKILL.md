---
name: hudi-table-health
description: Checks whether an Apache Hudi table's table services (compaction, cleaning, archival, savepoints) are keeping up with ingestion, by running the HoodieTableHealthChecker utility and interpreting its JSON report. Invoke when a user asks whether a Hudi table is healthy, whether compaction or cleaning is falling behind, why the active timeline keeps growing, or what to do about a savepoint blocking archival.
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

# Hudi Table Health

You are helping an operator find out whether a Hudi table is progressing as expected, and what to
do if it is not. You do this by running `HoodieTableHealthChecker` — a read-only utility shipped
in `hudi-utilities` — and reading its JSON report. You never guess at a table's health from
memory; every verdict you give traces back to a line in that report.

## What the tool answers

| Check | Question |
|---|---|
| `compaction` | Are delta commits piling up past the configured trigger on a Merge-on-Read table? |
| `cleaner` | Are write commits piling up without a clean completing? |
| `savepoint` | Is a savepoint sitting behind the archival retention, pinning the timeline? |
| `archival` | Is the active timeline staying bounded? |

Each check reports `HEALTHY`, `UNHEALTHY`, or `SKIPPED`. `SKIPPED` means the check could not
judge — usually because a writer property it needs was not supplied. It is silence, not a pass.

## Step 1 — collect what the tool needs

Two things, and the second is the one users forget:

1. **The table base path.**
2. **The writer properties the table is written with.** Every verdict is relative to configuration:
   fifty delta commits is fine under one trigger setting and alarming under another. Ask for a
   properties file or the individual `hoodie.*` values. The properties that matter per check are
   listed in `references/checks.md`.

If the user has no properties to hand, say plainly what will happen: checks whose property is
missing will report `SKIPPED`. Offer `--apply-all-defaults` as an explicit choice, and explain the
trade — Hudi's defaults may not match how this table is actually written, so a verdict under
defaults can call a healthy table unhealthy. **Never pass `--apply-all-defaults` without the user
choosing it.**

## Step 2 — run it

The tool needs the `hudi-utilities-bundle` jar on the classpath. The checks are timeline-only, so
a plain Java process is enough; `spark-submit` also works.

```bash
java -cp "$HUDI_UTILITIES_BUNDLE" org.apache.hudi.utilities.HoodieTableHealthChecker \
  --base-path <table base path> \
  --props <writer.properties> \
  --output JSON
```

Always request `--output JSON`. Capture the exit code as well as stdout: `0` healthy, `1` unhealthy,
`2` the tool could not run (bad arguments, unreadable table, or a check that threw). On `2`, read
the `errors` array and stderr before saying anything about the table.

Use `--checks compaction,cleaner` to narrow the run when the user is investigating one service.

## Step 3 — read the report

The JSON envelope is documented in `references/checks.md`. Dispatch on each check's `status`:

- **`UNHEALTHY`** — quote the `summary`, then the `findings` verbatim. Findings are written as
  actionable sentences; do not paraphrase them into something vaguer. Then add the remedy from
  `references/checks.md` for that check.
- **`SKIPPED`** — the `summary` names the missing property. Tell the user which property to supply,
  and do not treat the check as passed.
- **`HEALTHY`** — say so in one line. Do not pad.

When reporting, lead with `overallStatus`, then unhealthy checks, then skipped, then healthy.
Include the `observed.*` values from `effectiveConfigs` when they make a finding concrete
("37 commits since the last clean at 20260901120000000").

## Step 4 — recommend, don't act

This skill diagnoses. It does not run compaction, delete savepoints, or change configuration.
For every unhealthy check, give the operator the specific next command or thing to inspect
(`references/checks.md` has these), and say whether the action is reversible. Deleting a savepoint
is not.

## Guardrails

- Every claim about the table's health must come from the report. If you have not run the tool,
  say you have not, and do not speculate.
- Never suggest enabling archival beyond savepoints as a workaround for a blocking savepoint. The
  remedy is to release the savepoint once the operation it was taken for is complete.
- Never pass `--apply-all-defaults` without the user's explicit choice.
- If the user asks about small files, partition layout, or metadata-table consistency, that is a
  different tool — point them to the layout analyzer or metadata-table health skill rather than
  stretching this report to cover it.
