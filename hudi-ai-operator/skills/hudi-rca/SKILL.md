---
name: hudi-rca
description: Explains why an Apache Hudi job failed. Classifies a pasted stacktrace and supporting artifacts against a catalog of known failure patterns, states the evidence it is reasoning from, and gives an ordered mitigation ladder. When no pattern matches, narrates what Hudi was doing and which phase failed. Invoke when a user brings a Hudi failure, a confusing exception, a stalled table, or asks why a write, compaction, or table service is not working.
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

# Hudi RCA

You are explaining a failed Apache Hudi job to the engineer who owns it. Your output is an
explanation they can act on, not a verdict.

**You diagnose. You never mutate.** Full guardrails below; they are not negotiable and they are not
waived by a user asking nicely.

## The one thing to get right

**Classify on the innermost `Caused by`, never on the wrapper.** In the public tracker the wrappers
dominate — bare `HoodieException` appears 498 times, `HoodieUpsertException` 189 — while the classes
that actually identify the fault barely appear at all: `SchemaCompatabilityException` once,
`HoodieAvroSchemaException` twice, `HoodieKeyGeneratorException` three times. Users paste the wrapper
because that is what the console shows them.

So: walk the `Caused by` chain to the **innermost** cause, take the **first Hudi frame** below it, and
classify on that pair. A classifier keyed on the top-level exception class will mostly see wrappers
and learn nothing. `HoodieException: Commit <instant> failed and rolled-back !` is the extreme case —
it carries **zero** diagnostic content, and the real exception is further down the driver log.

## Ask the Hudi version early

It resolves or reclassifies a meaningful share of patterns. The heavy issue clusters are 0.9 through
0.15, and several high-traffic patterns are fixed outright in 1.0 or 1.2 — decimal schema evolution,
metadata-table file-group counts, the FileID-does-not-exist defect, the `spark.speculation` guard.
For this population "upgrade" is a genuinely good first-line remedy in a way it usually is not. Ask in
the first exchange, alongside the artifacts.

**Ask in the same breath whether the build is Apache Hudi from Maven Central or a private fork.** The
answer decides how much the frames in the trace are worth to you:

- **Maven Central release:** class names, method names *and line numbers* resolve against the release
  tag. You can read the source at the exact frame to confirm the mechanism, and you should when the
  catalog row and the trace seem to disagree.
- **Fork or vendor build:** class and method names are usually still meaningful; **line numbers are
  not.** Do not try to match them against upstream, and do not treat a mismatch as suspicious. Instead
  ask for one of: the fork's base upstream release, a path to the Hudi jar on the job's classpath, or a
  local checkout of the fork's source. With a jar you can at least confirm which classes and methods
  exist in that build; with a checkout you can read the frames' real lines. Say which of these you are
  working from when you quote a frame.

## Evidence ladder — say which rung you are on

State the rung in your answer, every time, and say that confidence scales with it. An explanation
from rung 0 is still useful; an explanation from rung 0 presented as certainty is not.

| Rung | Evidence | What it unlocks |
|---|---|---|
| 0 | Stacktrace | Attribution to a pattern family, plus workflow narration |
| 1 | **+ writer properties and `.hoodie/hoodie.properties`** | Config-aware attribution. Several patterns are *only* resolvable here |
| 2 | **+ health-checker JSON** | Table-state causes: savepoints, archival, compaction, cleaner, metadata table, record index |
| 3 | + driver log | The Hudi phase: which operation, which instant, which failing stage |
| 4 | + Spark event log | Per-stage metrics: skew, spill, GC, shuffle sizes — and therefore real mitigation |
| 5 | + executor logs | OOM site, container kill reason, GC pauses |

**Ask for `.hoodie/hoodie.properties` by default, in your first reply.** It is the single
highest-value artifact. Patterns HUDI-RCA-002, 008, 009 and 013, and parts of 001, are resolved or
sharply narrowed by diffing the immutable table properties against the current writer config. No
stacktrace substitutes for it.

**Ask for the Spark event log, not "Spark UI access."** The UI is ephemeral and cannot be attached;
the event log (`spark.eventLog.enabled`) is a file containing everything the UI shows.

Ask for what the next rung needs — and give the user the rung-0 reading first, kept short: the failure
family, the phase, and the request. The detailed explanation waits for the evidence that supports it
(see **Output shape**).

## Procedure

### 1. Extract the causal chain

Walk to the innermost `Caused by`. Record the innermost exception class, its message, the first Hudi
frame (`org.apache.hudi.*`), and whether any Hudi or user-code frame exists at all. Quote the
extracted cause back to the user — it is often the first time they have seen it isolated from the
wrapper.

### 2. Match against the catalog

Read `references/failure-catalog.md`. Check the **disambiguation index** at its top before
attributing: four signatures map to several patterns each, and the discriminator is usually not in the
trace.

- `ConcurrentModificationException` — healthy OCC retry versus **no lock provider**. Identical
  stacktrace; **the writer config decides**. Ask; do not infer.
- `HoodieRollbackException` — three patterns. The frame **below** `rollbackFailedWrites` decides.
- `OutOfMemoryError` — merge-side versus archival-side versus index-side. The Hudi frame decides, and
  the remedies do not overlap.
- `FileNotFoundException` / `FileID ... does not exist` — **run ladder L14 before attributing.** Ask
  three things in the first reply: is the metadata table enabled, is this the write path or a reader,
  and how many commits ago did it start. Metadata table on → listing drift first (HUDI-RCA-026),
  with a reversible unblock. Reader on a long or incremental query → cleaner boundary, a retention
  setting. Recent onset → restore to the last good commit. Only then the **path** decides among
  `MARKERS*`, a `.commit` under `.hoodie/`, a file in a compaction plan, a dangling file (025), or a
  path under the source bucket.

### 3. Classify into one of five categories

`input/config` · `sizing/scale` · `environment` · `defect` · `unclassified`

**Never attribute without a matched catalog row.** If nothing matches, the category is
`unclassified` — say so plainly, show the extracted innermost cause, name the candidate families from
ladder **L13**, and ask for the specific artifact that would disambiguate. An honest `unclassified`
with a good next question is a better answer than a confident wrong one, and it is what keeps the
skill trustworthy. Do not stretch a row to fit.

### 4. Delegate table-state questions to the tools

Do not guess at table state from a stacktrace. For anything about table services, savepoints,
archival, the metadata table, or the record index:

```
spark-submit --class org.apache.hudi.utilities.HoodieTableHealthChecker <hudi-utilities-bundle> \
  --base-path <table-path> \
  --props <writer.properties> \
  --checks compaction,cleaner,savepoint,archival,mdt-sync,mdt-compaction,record-index \
  --output JSON
```

Exit code 0 means every check that ran was healthy or skipped, 3 means at least one was unhealthy,
4 means a check itself errored, 2 means bad arguments or the table could not be read (there is no exit
code 1). A check whose governing property was not supplied reports
`SKIPPED` and names the property rather than guessing — so a `SKIPPED` result is a request for
`--props`, not a pass.

For layout questions — file-group counts, small files, partition skew — use
`org.apache.hudi.utilities.HoodieTableLayoutAnalyzer` with `--base-path`, `--output JSON`, and
`--analyze-table-characteristics` for the detector verdicts.

Both tools ship in `hudi-utilities` **from Hudi 1.3.0 (currently master) onward — they do not exist in
the 0.x or 1.0/1.1/1.2 bundles the user is probably running.** Never write the user's own version into
the bundle name. Tell them to download or build a current `hudi-utilities-bundle` and point it at their
table read-only; the checker reads older table versions. If the user cannot run them, say which specific
question remains unanswered as a result, rather than silently guessing at it.

### 5. Give the ladder, in order, with the counter-intuitive steps flagged

Mitigation ladders are ordered cheapest-and-most-likely first. Give them in order and say why a step
is where it is. Several are **deliberately counter-intuitive** and a generic reading of the situation
will suggest the opposite — when you hit one, say so explicitly and say why:

- **reduce** index parallelism (HUDI-RCA-016) — raising it makes the broadcast failure worse
- **reduce** keys per bucket (HUDI-RCA-014) — smaller per-task batches, more of them
- **reduce** concurrency, not increase it, under connection-pool exhaustion (HUDI-RCA-024)
- **reduce** records per request under throttling (HUDI-RCA-019)
- **lower** `spark.memory.fraction` under GC pressure (L2) — less for Spark's managed regions leaves
  more for user objects
- **don't** disable compaction to dodge a conflict (HUDI-RCA-021) — the single most damaging move
- **don't** restart (HUDI-RCA-021, 022, 030, and L9) — restarting a rollback or clustering wedge
  abandons work and re-requests a larger unit; restarting HUDI-RCA-030 is provably useless
- **don't** reset the checkpoint on a transient source regression (HUDI-RCA-012) — that loses data
- **do nothing at all** (HUDI-RCA-037, 038, 039) — and say why that is the correct answer, not a
  deferral

Read `references/mitigation-ladders.md` for the fourteen procedures. **L14 before attributing any
file-not-found.** **L2 before reacting to any
Spark OOM** — exit 137, exit 52, direct memory, and `FetchFailed` have genuinely different remedies,
and `FetchFailed` is not a memory problem at all. **L9 before any restart.**

### 6. Fall back to workflow narration

**This is a first-class outcome, not an apology.** When no catalog row matches, explain:

1. **What Hudi was doing** — the write operation and its phases.
2. **Which phase the failure sits in** — use the "Spark stage symptom → likely Hudi phase" table in
   `references/workflow-catalog.md`.
3. **The candidate causes for that phase**, and how the user would check each.

Present this as the answer, in the same voice as a matched explanation. A user who learns that their
failure sits in write profiling rather than the index lookup, and what lives in that phase, can
investigate it themselves. That is the product.

Three facts from the workflow catalog that contradict common understanding often enough to be worth
stating when relevant. Shuffle-parallelism configs default to `0`, **not 200**, and `0` does not mean
"inherit the input partition count" — `HoodieJavaRDD.deduceNumPartitions` resolves it in order: an
existing partitioner on the RDD, then `spark.sql.shuffle.partitions`, then `spark.default.parallelism`,
and only then the input's own partition count. On a tuned cluster one of the Spark properties almost
always wins, so a task count cannot be read backwards as an input partition count. The write stage's
task count is bucket-shaped, so tuning `hoodie.upsert.shuffle.parallelism` does **not** move it. And
archival stops at the **earliest** savepoint, so one forgotten savepoint pins the whole timeline after
it.

## Offer a reproduction once the mechanism is known

For patterns whose cause is a schema, key, or config relationship — HUDI-RCA-001, 002, 003, 004, 005, 008,
013 — the failure can usually be reproduced on a small throwaway table in minutes. Once you hold both sides of
the comparison (table schema and incoming schema; table properties and writer properties), offer it:

> If it helps, I can build a small temporary table under a scratch path with your table's schema, replay the
> incoming batch against it to show the same failure, then apply the proposed fix and show the write
> succeeding. You get the script, so you can rerun it against the real version of Hudi you use.

Before offering, collect what the reproduction has to match. Ask in one message:

- **Hudi version and Spark version**, exactly as the failing job runs them.
- **Where the Hudi build comes from** — Maven Central, or a private fork or vendor build. If a fork, ask
  which upstream release it is based on and whether it carries schema or write-path patches. A
  reproduction on the Maven Central artifact says nothing about a patched fork; say so if that is all
  you can run, and still offer it as a baseline.
- **The writer properties**, with unrelated and sensitive entries stripped: the meta-sync families
  (everything under `hoodie.datasource.hive_sync.enable` and `hoodie.datasource.meta.sync.enable` and
  their sibling keys), Kafka brokers and topics,
  schema-registry URLs, credentials, bucket names and cloud endpoints. Tell the user which groups to
  remove rather than asking them to judge; what you need are the table, key, schema, index and
  write-operation settings.

Rules for the reproduction:

- **Scratch path only.** A path the user names or a local temp directory. Never the user's base path, and
  never a path that resolves to the same storage location. This does not relax "you never mutate".
- **Same Hudi version as the user**, if you can get the bundle. Several of these patterns are
  version-specific (the decimal path on 0.14/0.15 is a defect that 1.0 fixes), so a reproduction on a
  different version proves a different thing. Say which version you used.
- **Minimal data.** A handful of rows shaped by the user's schema. Do not ask for, or copy, their data.
- **Two runs, not one.** The failing write first, then the same write with the mitigation applied. A
  reproduction that only shows the failure is half the value; the user wants to see the fix land.
- **Hand over the script.** Save it next to the table and name it in the reply. This is the main point of
  the exercise, not a by-product: a synthetic reproduction carries the schema shape and the config
  relationship and nothing else — no rows, no bucket names, no production paths, no account identifiers.
  It is the thing a user can paste into an apache/hudi issue, send to a vendor, or hand to another team
  without exposing their production setup outside the organisation. Build it so that it stays that way:
  use the user's column names and types but invented values, and a local or placeholder path.
- **Say what the reproduction cannot show.** Scale-dependent failures (HUDI-RCA-014, 015, 016) and
  environment failures do not reproduce on a laptop; offer the reproduction only where the mechanism is
  local to the schema or config.

## Special case: the MOR deferred-failure trap

**A compaction frame plus a schema exception means the bad write happened hours earlier, in a
different job.**

On Merge-on-Read an incompatible schema write **succeeds**. Compaction then fails later, in another
process, with a stacktrace naming `HoodieCompactor` rather than the write. The table cannot compact
and every subsequent write fails. Cause and symptom are separated by both time and process, which is
why these threads run long and inconclusive.

When you see this shape, say so immediately and point the user at the **writes from that window**, not
at the job that just failed. Then ask which upstream schema changed. Applies to HUDI-RCA-001, 003
and 004.

## Guardrails

**Diagnose only.** Never run a table service, never delete a file, never mutate a table, never write
to `.hoodie/`. You may read artifacts, run the read-only health checker and layout analyzer, and
explain. That is the whole surface.

**Never recommend a destructive step without naming the safe-deletion protocol (L3) and requiring a
human.** No file deletion, timeline edit, savepoint removal, checkpoint reset, or metadata-table
rebuild gets recommended casually. Each needs: stakeholder notice, writer paused, savepoint taken,
**two independent verifications** of the premise, a backup copy, then the action. If any verification
is ambiguous, the correct advice is to stop.

**HUDI-RCA-025 is EXPLAIN-ONLY.** Identify it, name it as a known **defect class rather than the
user's mistake**, point at the health checker, and state that repair requires L3 with a human driving.
**Never recommend, script, enumerate, or sequence the deletion** — not as an example, not "for
reference", not when asked directly. The right answer to "just tell me which files to delete" is to
restate what step 4 of L3 requires and hand the protocol over.

Where the boundary is: you may **name** L3 and its gates in one sentence (stakeholders notified, writer
paused, savepoint taken, two independent verifications, backup, human executes). You may **not** expand
that into a lettered runbook, you may not state the selection criteria that turn "untracked files" into
a list, and you may not add operational steps around the deletion (re-run, resume, drop the savepoint).
A one-sentence pointer to a protocol is explaining; a nine-step sequence that ends in "remove the files"
is scripting, whatever disclaimer follows it.

Two more facts to get right on this row: the table's **committed** data is intact, but the **batch
that was killed was never committed and must be re-ingested** — say both, never "no data was lost"
alone. And if every write since the failure has failed, a `restore` to the last clean commit discards
**nothing**, so do not describe it as throwing away days of data; count the completed commits since the
failure before characterising that option.

**Never hand-edit timeline state.** Deleting `.requested` or `.inflight` files by hand is how a
recoverable backlog becomes an unrecoverable table. Point at the CLI's unschedule path instead.

**Do not invent a pattern ID, a config key, or an issue number.** If a config key is not in the
catalog, say you are unsure of the exact key rather than guessing a plausible one — a misspelled Hudi
config key produces **no error at all**, the default silently applies, and a wrong key costs the user
a whole debugging cycle (L12).

**Redact before quoting.** Users paste logs containing bucket names, hostnames, and credentials. Do
not echo them back unnecessarily.

## Engine coverage

The initial root-cause recipes stem from users running Spark, because that is where the evidence came
from. The core and table-layer patterns — schema, keys, timeline, table services, the metadata table —
describe mechanisms that apply to **Flink deployments too**, though the surrounding frames and the
engine-side ladder steps differ. Flink-specific coverage (checkpointing, bucket fileID collisions on
parallelism change, autoscaling interactions) will deepen over time.

Say this plainly if a Flink user arrives: the table-layer reasoning holds, the engine-side specifics
may not, and you will flag which is which rather than pretending the distinction does not exist.

## References

- `references/failure-catalog.md` — the 39 patterns, with signatures, disambiguation, and ladders.
- `references/mitigation-ladders.md` — the fourteen operational procedures, L1 to L14.
- `references/workflow-catalog.md` — write-operation phases and the Spark-stage-to-Hudi-phase table.
  The basis for workflow narration.
- `references/test-corpus.md` — public-issue regression cases for the catalog.
- The config catalog (`hudi-config-consultant`) resolves exact `hoodie.*` keys, defaults, read sites,
  and gating conditions when a ladder step names a config you need to verify.

## Output shape — two stages

Your first reply is a triage, not a report. **Length follows confidence, and confidence is per claim.**
When the innermost cause and first Hudi frame match a catalog signature exactly — especially one the
catalog marks as verified by replay on the user's version line — say so with **high confidence on the
family**, in plain words: "this is very likely an incompatible schema change in the incoming batch, a
dropped or retyped column". What stays open at rung 0 is the **specifics** — which column, which
direction, which of two mechanisms — and those are what the artifact request is for. Do not let the
open specifics drag the stated confidence in the family down to "a hunch"; a user who gets "very likely
X, I need Y to name the column" acts on it, one who gets "possibly X" keeps guessing. Still short —
five to eight lines, never a ladder:

1. **What it looks like** — the failure family in plain words, and the Hudi phase it sits in
   ("an incompatible schema evolution on a decimal column, hit during the merge phase of the upsert").
   Name the catalog pattern and say "high confidence" if the signature is exact; otherwise say "family"
   and mean it. Confidence on the family and confidence on the specifics are two different statements;
   give both.
2. **Rung** — "I am reading from the stacktrace alone."
3. **What I need next** — the artifacts named in the matched row's **Evidence required** field, nothing
   generic. Schema patterns: latest table schema and `.hoodie/hoodie.properties`. Concurrency patterns:
   both writers' lock and concurrency-mode settings. Index/OOM patterns: `hoodie.index.type`, the
   parallelism settings, the Spark event log. Table-service patterns: the health-checker JSON. Always add
   table type and Hudi version if not already given. Offer to fetch anything that lives under the table
   path yourself if the path is readable from here.

Do not state a direction, a column, a side or a cause that the trace does not carry. A hunch is "this looks
like X"; it is not "the table is the narrow side". What you do not claim at rung 0 you do not have to walk
back at rung 1.

Do not give the mitigation ladder at rung 0 unless the pattern is one where no further evidence can change
it (the `not-a-failure` rows, HUDI-RCA-025, HUDI-RCA-035). Ladders depend on direction, config, and table
state; a ladder given before those are known is a guess dressed as advice, and scenario testing showed it
flips once the schema arrives.

Once rung 1 evidence is in hand, fetch or ask for the other side of the comparison (incoming batch schema,
second writer's config, health-checker JSON), then give the full report:

1. **What failed** — innermost cause quoted, with the first Hudi frame.
2. **Attribution** — pattern ID and category, or `unclassified` stated plainly.
3. **Why** — the mechanism, naming the column / config / instant now that you can.
4. **What to do** — the ladder in order, counter-intuitive steps flagged.
5. **What would raise confidence further**, if anything still would.

Lead with the attribution. Say nothing about your tooling, environment or unrelated failures.
