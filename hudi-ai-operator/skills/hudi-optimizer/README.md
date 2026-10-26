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
# hudi-optimizer

Finds what a working Apache Hudi deployment is leaving on the table. Bring your writer properties, a
Spark event log, and a table path; get back what the run cost, what it did not need to cost, and what
to change — with the risk of each change stated.

## What it does

1. **Collects three inputs** — writer properties, a Spark event log, and the table — and says which
   it has, because the answer's trustworthiness depends on it.
2. **Corroborates before it recommends.** An observation needs **at least two of the three inputs to
   agree**. One signal is a hypothesis and gets reported as one.
3. **Runs the read-only tools itself** rather than asking you to run three commands and paste three
   outputs.
4. **Matches against two catalogs** — thirty observations ("this works, but could be better") and
   seven capability rows ("adopt this feature", each able to say *don't*).
5. **Refuses durable changes.** Anything that alters what is on storage — table type, index type,
   partitioning — is reported with its measurements and handed to `hudi-architect` for an ADR, never
   recommended as a tuning step.

It recommends. It does not run table services, write to tables, or edit configuration files.

## What it is not

Not a monitor. You run it, you get a report. There is no daemon, no baseline store, no alerting.

Not a Spark profiler either. Generic stage analysis can tell you that stage 14 was slow with 16,000
tasks; it cannot tell you stage 14 was bloom-index probing inside an upsert, which is why generic
advice stays generic. The value here is the join from Spark stages onto **Hudi's own phases**, and
then onto a catalog that knows what each phase costs and what moves it.

Not a failure diagnoser. If the job is failing, that is `hudi-rca`. If you are designing a table, that
is `hudi-architect`.

## Install

Copy this directory into your Claude Code skills location:

```bash
# From the root of a Hudi checkout.
SKILL=hudi-ai-operator/skills/hudi-optimizer

# User-level (available in every project)
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/

# Or project-level, in the repo where your pipelines live
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then in Claude Code:

```
/hudi-optimizer
```

Point it at a table and hand it what you have.

## What evidence to bring, and how to capture it

| Input | How to get it | Why it matters |
|---|---|---|
| **Writer properties** | The `--props` file or `--hoodie-conf` values your writer actually runs with | **The one most often skipped and least skippable.** 237 of 939 Hudi configs are performance levers and 93 of those are conditionally applied — advice given without the config may be silently inert |
| **Spark event log** | Set `spark.eventLog.enabled=true` and `spark.eventLog.dir` on the job, then collect the file | Where the time actually went, resolved to Hudi phases |
| **Table path** | A path the skill can read: local, `s3://`, `gs://`, `abfs://` | File sizes, partition skew, service cadence, timeline state |

### Capturing the event log

```
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  ... your Hudi job ...
```

The file lands in that directory named after the application ID. It may be compressed (`.gz`, `.lz4`,
`.snappy`, `.zstd`) or a directory of rolling parts; all are handled.

**Ask for the event log, not a link to the Spark UI.** The UI is ephemeral and cannot be attached. The
event log is a file containing everything the UI shows, and it is what makes this skill possible.

### Two things worth doing before you capture

- **Run with `hoodie.metadata.streaming.write.enabled=false` for one diagnostic run** if you can.
  That config defaults to `true` on Spark, and on that default **a large share of the write DAG is
  unattributable** — on a measured run, 60.5% of stage wall clock landed in `UNKNOWN`. Turning it off
  for one run gives a fully attributed breakdown; turning it back on costs nothing. See the limits
  below.
- **Redact before pasting.** Properties files carry bucket names, hostnames and occasionally
  credentials.

### If you can only bring one thing

Bring the writer properties. With properties plus the table path the skill can run two of its three
tools and reach genuinely corroborated observations. With an event log alone it can describe where
time went and very little else.

## Scope and limits

**Covers:** file sizing and small-file handling, parallelism and skew, spill, index cost and index
economics, table-service cadence (compaction, cleaning, archival, clustering), timeline growth,
metadata-table cost and which index partitions earn it, marker overhead, driver-side file-system-view
sizing, and the capability question — whether the record-level index, bucket index, clustering,
column stats, secondary index, the metadata table or Merge-on-Read would suit *this* table.

**Does not cover:** live cluster access, continuous monitoring, applying anything, or query-plan
analysis. Several rows depend on query evidence the skill cannot obtain from write-side telemetry;
those rows say so and ask rather than guessing.

### The attribution limit, stated plainly

A phase share names **where work was forced, not where it logically belongs.** Hudi sets the Spark job
description before the action and Spark evaluates lazily, so a bucket routinely carries the cost of an
untagged step that ran before it. The analyzer reports this per run in a `caveats[]` array and the
skill repeats it. Three cases are known and verified in source: source read bundles transformation and
record creation; deduplication bundles with index tagging; and the write bucket can absorb work not
forced until the commit action ran.

**And the bigger one:** with `hoodie.metadata.streaming.write.enabled=true`, the Spark default, those
stages are not merely fused — they are **unlabelled**, because Hudi never sets a job description on
that path. The skill says so rather than dividing up the remainder as though it were the whole
picture. This is an accepted v1 limitation; the tooling is accurate for non-streaming writes and
0.x-style DAGs and degrades on the 1.x streaming default.

### Four rows whose answer may be "do nothing"

OBS-019 (the cleaner correctly finding nothing to clean), OBS-002 (small-file handling that is live
and correctly declining to act), OBS-005 (a benign commit guard firing), and every capability row
whose counter-signals fire. They are in the catalog because the natural reaction — change the config —
turns a non-problem into a problem.

### Durable changes are refused by construction

Three rows point at changes to what is on storage: index type (CAP-001), bucket index and its count
(CAP-002), and table type (CAP-006). The skill reports the measurements, the sizing estimate, and the
counter-signals, then **hands off to `hudi-architect`**. It does not emit a config block for them, and
it says why out loud rather than quietly skipping them. An index-type change on a live 10 TB table is
a design decision with a migration behind it, not an optimization.

## Engine coverage

The recipes here come from Spark deployments, because that is where the evidence came from — the
event-log analyzer reads Spark event logs, and the measurements behind these rows were taken on Spark
runs.

The **core patterns apply to Flink too**. File sizing, service cadence, timeline growth,
metadata-table cost, index economics and the capability tradeoffs are table-layer mechanisms rather
than engine ones. What does not carry is the event-log half: Flink produces no Spark event log, so a
Flink user arrives with at most two inputs and the phase breakdown is simply unavailable. Several
engine-effective defaults differ too — `hoodie.index.type` is `INMEMORY` on Flink against `SIMPLE` on
Spark, and column stats, secondary index and streaming metadata writes all default the other way.

Flink-specific coverage will deepen over time. If you are on Flink: the table-layer reasoning holds,
the stage-level reasoning does not, and the skill flags which is which rather than pretending the
distinction does not exist.

## Files

- `SKILL.md` — the skill definition: input collection, confidence scaling, the corroboration rule,
  the caveats to carry, the durable refusal, guardrails, output shape.
- `references/observation-catalog.md` — thirty rows, "this works, but could be better". Each with
  measurable signals as JSON field paths, corroborating inputs, the config key and direction, a risk
  tier, expected payoff, and how to verify. Sixteen are the healthy-side reading of a known failure
  pattern; fourteen have no failure analogue at all.
- `references/capability-catalog.md` — seven rows, "adopt this feature", each with signals **for and
  against**, adoption cost, risk tier and verification. The record-level index row is the one to read
  first.

## Companion tooling

All three ship in `hudi-utilities` and are strictly read-only. The skill invokes them rather than
asking you to.

- `HoodieSparkEventLogAnalyzer` — Spark event log summarised by **Hudi operation and phase**, with
  sub-phases, skew, spill, GC and a per-run caveats list. The join from Spark stages onto Hudi's own
  vocabulary is what makes the advice Hudi-specific.
- `HoodieTableHealthChecker` — compaction, cleaner, savepoint and archival cadence against the
  configured triggers, with JSON output.
- `HoodieTableLayoutAnalyzer` — base-file size distribution, per-partition statistics, partition skew,
  and the micro-partition / small-files / hot-partitions detectors.

The skill accepts `--health-json` and `--layout-json` to skip invocation when you already have the
output or cannot run `spark-submit`.

**It reads the measurements, not the rollup verdicts.** The layout analyzer's `overallStatus` fires on
essentially every actively-written streaming table, and the health checker's thresholds carry
deliberate slack because those tools exist to catch a service that has *stopped*. An optimization
observation lives in the band where both correctly report healthy, so the catalogs are written against
the underlying `observed.*` fields.

## Feedback

The catalogs are only as good as the deployments they have seen. If the skill recommends something
that did nothing, misses something that mattered, or names a config that turned out to be inert on
your setup, that is the useful report: what it said, what you changed, and what the measurements did
afterwards.
