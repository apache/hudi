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
# hudi-rca

Explains why an Apache Hudi job failed. Paste a stacktrace and a few artifacts; get back what Hudi was
doing, which phase died, what causes that, and an ordered list of things to try.

## What it does

1. **Walks the `Caused by` chain to the innermost cause.** Hudi failures reach users wrapped — a bare
   `HoodieException` or `HoodieUpsertException` that names nothing useful. The identifying exception
   is usually several levels down, and that is what gets classified.
2. **Matches against a catalog of 39 known failure patterns**, each with its signature, mechanism,
   governing configs, and an ordered mitigation ladder.
3. **States which evidence rung it is reasoning from**, so you know how much to trust the answer.
4. **Falls back to workflow narration.** When nothing matches, it explains what Hudi was doing and
   which phase failed, so you can investigate. This is a designed outcome, not a failure of the tool.

It diagnoses. It does not run table services, delete files, or mutate tables.

## What it is not

Not a root-cause oracle. A true RCA needs infrastructure access you are not going to grant a chat
session. What this does is **explain the failure** from pasted artifacts — which is both achievable
and more than Hudi users have today. Where it cannot reach a conclusion it says so and tells you what
would settle it.

## Install

Copy this directory into your Claude Code skills location:

```bash
# From the root of a Hudi checkout.
SKILL=hudi-ai-operator/skills/hudi-rca

# User-level (available in every project)
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/

# Or project-level, in the repo where your pipelines live
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then in Claude Code:

```
/hudi-rca
```

Paste your failure and answer the questions it asks.

## What evidence to bring

Confidence scales with the rung. Bring what you have; rung 0 still gets you an explanation.

| Rung | Bring | Why |
|---|---|---|
| 0 | The **full** stacktrace, including every `Caused by` | The truncated version almost always omits the identifying exception |
| 1 | `.hoodie/hoodie.properties` **and** your writer config | **The highest-value artifact.** Several patterns are only resolvable by diffing the two |
| 1 | Your **Hudi version** | Resolves or reclassifies a meaningful share of patterns on its own |
| 2 | `HoodieTableHealthChecker --output JSON` output | Table-state causes: savepoints, archival, compaction, cleaner, metadata table, record index |
| 3 | The driver log around the failure | The real exception often sits below the one that was thrown |
| 4 | The **Spark event log** (`spark.eventLog.enabled`) | A file you can attach, containing everything the Spark UI shows. The UI itself is ephemeral and unattachable |
| 5 | Executor logs, heap dump if you have one | OOM site, container kill reason, GC behaviour |

Two things worth knowing before you ask for help anywhere:

- **Capture evidence before restarting.** A restart is the most common and most expensive mistake in
  this problem space. For a rollback or clustering wedge it actively harms you — it abandons
  in-progress work and re-requests a larger unit. Take the heap dump first.
- **Redact before pasting.** Logs carry bucket names, hostnames, and occasionally credentials.

## Scope and limits

**Covers:** schema evolution and compatibility, record keys and key generators, multi-writer and
locking, table services (compaction, cleaning, archival, clustering), the metadata table and record
index, timeline and marker failures, HoodieStreamer source and schema-provider configuration, sizing
and OOM classification, classpath and bundle mismatches, and the silent patterns — duplicates, config
drift, query wiring — that produce no exception at all.

**Does not cover:** live cluster access, infrastructure-specific failures with no Hudi analogue, or
anything requiring forensic access to a running system. Deep managed-platform plumbing is out of scope
by construction.

**Three rows are explicitly "do nothing."** The cleaner that appears idle, the source with no new data,
and the spurious-data-files commit abort are usually correct behaviour. They are in the catalog
because the natural reaction — change a config, restart, reset a checkpoint — makes things worse, and
because nobody files a public issue about something that turned out to be fine.

**One row is explain-only.** HUDI-RCA-025 (dangling files from an incomplete rollback) is identified
and explained, but its repair is destructive and the skill will not script it. You get the
safe-deletion protocol and the instruction that a human drives it.

## Engine coverage

The initial root-cause recipes stem from users running Spark, because that is where the evidence came
from. The core and table-layer patterns — schema, keys, timeline, table services, the metadata table —
apply to **Flink deployments too**, though the surrounding frames and the engine-side remedies differ.
Flink-specific coverage (checkpointing, bucket fileID collisions on parallelism change, autoscaling
interactions) will be improved over time.

If you are on Flink: the table-layer reasoning holds, the engine-side specifics may not, and the skill
flags which is which rather than pretending the distinction does not exist.

## Files

- `SKILL.md` — the skill definition: procedure, guardrails, output shape.
- `references/failure-catalog.md` — 39 patterns with signatures, disambiguation rules, and ladders.
- `references/mitigation-ladders.md` — thirteen operational procedures (L1–L13), including Spark OOM
  classification, the safe-deletion protocol, and the stuck-table-service chain.
- `references/workflow-catalog.md` — Hudi write-operation phases and the Spark-stage-to-Hudi-phase
  lookup table.
- `references/test-corpus.md` — regression cases from public issues, each with its expected pattern
  and category.

## Companion tooling

- `HoodieTableHealthChecker` (`hudi-utilities`) — table-service, savepoint, metadata-table and
  record-index checks, with JSON output. The skill delegates table-state questions to it rather than
  inferring them from a stacktrace.
- `HoodieTableLayoutAnalyzer` (`hudi-utilities`) — file-group counts, small-file and partition-skew
  detectors.
- `hudi-config-consultant` — resolves exact `hoodie.*` keys, defaults, read sites, and gating
  conditions.

## Feedback

The catalog is only as good as the failures it has seen. If you hit a Hudi failure it classifies
wrongly, classifies as `unclassified`, or gives a ladder that did not work, that is the useful report:
the stacktrace, what it said, and what actually fixed it.
