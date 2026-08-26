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
# Hudi Architect — Runbook for Data Engineering & ETL Teams

A practical guide to running the Hudi Architect agent: what it does, how to set it up, what to bring to the session, and what to do with the output.

**Who this is for:** anyone designing a new Apache Hudi table (or sanity-checking an existing design) — data engineers, ETL developers, platform teams. You do **not** need to know Hudi internals; the agent asks workload questions ("how often does data land?", "what do consumers filter on?"), not Hudi questions ("which index type?").

**What you get out of a session:** an Architecture Decision Record (ADR) with tradeoff tables and measurable revisit conditions, a ready-to-use `hoodie.*` config bundle, and a runnable submit command — all pinned to **Hudi 1.2.0**.

---

## 1. Prerequisites

The agent is packaged as a [Claude Code Skill](https://docs.anthropic.com/en/docs/claude-code/skills), so you need Claude Code (CLI, desktop app, or claude.ai/code). Install the CLI:

```bash
npm install -g @anthropic-ai/claude-code
# or the native installer:
curl -fsSL https://claude.ai/install.sh | bash
```

Any Claude Code plan works. No Hudi installation, cluster, or cloud access is required — the agent designs; it never deploys, modifies tables, or applies configuration.

**No Claude Code?** The skill degrades to a readable spec. You can paste `SKILL.md` as a system prompt into any capable LLM chat (the question flow falls back from interactive widgets to numbered prose blocks), or simply read `references/` as design documentation — the decision tables and warnings stand on their own.

## 2. Installation

Copy the `hudi-architect` directory into a Claude Code skills location:

```bash
# The skill ships inside a Hudi checkout at:
#   hudi-agent-gateway/skills/hudi-architect
SKILL=/path/to/hudi/hudi-agent-gateway/skills/hudi-architect

# Option A — user-level: available in every project on your machine
mkdir -p ~/.claude/skills
cp -r "$SKILL" ~/.claude/skills/

# Option B — project-level: available to everyone who clones your repo
#   (run this from the root of YOUR pipelines repo, not the Hudi checkout)
mkdir -p .claude/skills
cp -r "$SKILL" .claude/skills/
```

For a data platform team, **Option B checked into your pipelines repo** is the recommended pattern: every engineer who opens Claude Code in that repo gets the same design advisor, and skill upgrades ship through your normal review process.

Verify: start `claude` and type `/hudi-architect` — it should appear in the slash-command autocomplete.

## 3. Starting a session

```
/hudi-architect
```

or just describe your need in plain language — "help me design a Hudi table for our orders CDC feed" — and the skill triggers.

The first question is always the **tier gate**. Answer honestly; it controls how deep the interview goes:

| You say | What happens | Session length |
|---|---|---|
| **Exploring** | Tutor mode. Concepts explained, minimal questions, narrative sketch instead of a full ADR. | ~5 min |
| **Prototyping** | Minimum questions for a *genuinely runnable* first table. Defaults are disclosed and you consent to them; record key and ordering field are always asked (they have no safe default). | ~10 min |
| **Productionizing** (~hundreds of GB) | Full workload interview: read patterns, sizing, retention, partitioning, identity, writer selection. Production-safe defaults. | ~15–20 min |
| **Production at scale** (TB–PB) | Everything above plus index sizing (record counts, RLI file-group math), a derived-facts checkpoint you must confirm, and strict guardrails. | ~20–30 min |

**Tip:** if a real production table is the goal, don't pick Prototyping to save time. Several correctness checks (e.g. partition-column-vs-query alignment) only run when their input questions fire.

## 4. What to have ready — the pre-session checklist

The agent asks in plain workload terms, but sessions go much faster if you've gathered these facts first. Circulate this list before a team design session:

**Everyone:**
- [ ] Where the data comes from (Kafka topic / files on DFS / JDBC database / a DataFrame your own Spark job already produces / other)
- [ ] For Kafka: record format (Avro + schema registry, Avro + `.avsc` files, JSON, Protobuf)
- [ ] How data lands: continuous streaming vs scheduled batch, and roughly how often a round of ingestion completes (<5 min / 5–15 min / hourly / daily)
- [ ] Mutable or append-only? (Do records ever get updated after insert?)
- [ ] Engine: Spark, Flink, or undecided

**Mutable workloads additionally:**
- [ ] The column(s) that uniquely identify a record (this becomes the record key — **there is no default; you will be asked**)
- [ ] The column that decides which of two same-key updates wins — usually `updated_at` or a source sequence number / LSN (**also no default**)
- [ ] Where updates land: uniformly across the table, or concentrated on recent data? If recent: how long is the straggler tail?

**Productionizing and above:**
- [ ] How consumers read the table: bulk scans, targeted lookups, incremental/streaming — and what columns they filter on
- [ ] Current size and a 2–3 year projection (order of magnitude is fine)
- [ ] Max lookback needed for time-travel / incremental consumers
- [ ] Candidate partition column, if any — and whether its value can change for a record after insert
- [ ] At scale: rough record count today and at a 3–4 year horizon (drives a **permanent** index-sizing decision — see §6)

## 5. During the session — how to interact

- **Questions arrive as selectable option widgets**, at most three per screen, each preceded by a short tradeoff table. Free-text is only used for column names.
- **"Other" is allowed.** If none of the options fit your reality, say what's true — the agent re-derives rather than forcing you into a box.
- **Push back freely.** Recommendations come as "confirm or override" — overriding is a first-class path, and anything you override against advice is recorded in the ADR with a measurable trigger for revisiting it.
- **Expect warnings mid-flow, not at the end.** The agent carries a rule engine of named traps (partition/query misalignment, over-granular partitions, high-cardinality partition columns, retention windows unsafe for your commit cadence, the MOR compaction-IO ceiling at TB scale, concurrent-writer lock requirements, …). Warnings fire at the moment the triggering answer lands.
- **Two confirmation gates:** at scale, a mid-flow echo of derived facts (sizes, projected partition counts, tensions); and at every tier, one final review of all answers before the ADR is generated. That final gate is your last chance to walk back anything you proceeded past.

## 6. Pay attention to durable decisions

Some choices are **one-way** — changing them later means rewriting the table. The agent flags each inline and lists them in the ADR's durability table, but they deserve team-level sign-off, not a solo call in a chat session:

| Decision | Why it's one-way |
|---|---|
| Table type (COW vs MOR) | Switch requires a rewrite |
| Partition column + granularity — **including choosing unpartitioned** | Fixed at creation; no ALTER path |
| Record key | Fixed at creation |
| Bucket count (BUCKET index) | Fixed at creation |
| RLI file-group count | Frozen when the record index initializes — *adding* an RLI later is free, *resizing* one is not |
| Disabling meta fields | Re-enabling requires a rewrite; incremental/CDC queries are non-functional while disabled |

## 7. Using the output

A session ends with three artifacts:

1. **The ADR** — save it next to your pipeline code (e.g. `docs/adr/hudi-<table>.md`) and put it through your normal design-review process. The sections reviewers should read first: *Assumptions and consented defaults* (what was checked vs guessed), *Warnings accepted* (risks you signed up for), *Durability table*, and *Revisit conditions* (each names an observable threshold — wire the relevant ones into your monitoring).
2. **The config bundle** — grouped `hoodie.*` properties. Everything in it either encodes a design decision or changes a default deliberately; it intentionally omits config that restates defaults.
3. **The submit command** (or `.option(...)` snippet for DataSource writes) — flags are split into **load-bearing** (derived from the design; don't change casually) and **environment placeholders** (paths, memory, Scala/Spark versions — the agent deliberately doesn't guess these; fill them from your own build and cluster).

Then: land a first commit in a staging path, run your real read patterns against it, and check the ADR's operational playbook section for what to monitor from day one (commit duration, pending compactions, active timeline size, small-file ratio).

## 8. Known limitations (Milestone 1)

Be aware of what the agent will *decline* to decide — it defers honestly rather than guessing, but plan for these yourself:

- **Multi-writer contention tuning** — the agent asks whether anything else writes the table (every tier but the lightest), derives the concurrency mode, picks a lock provider, and emits a complete, runnable block. What it does *not* do is tune for observed contention: lock retry/timeout values, conflict-retry counts, and early conflict detection stay at their defaults, because the right values depend on measured behavior rather than design-time facts. That is Operations Agent territory. Two things remain yours to carry out: apply the emitted block to **every** writing job (the ADR's pre-launch checklist enumerates them), and confirm that writers using `INSERT`/`BULK_INSERT` have disjoint key spaces — concurrent inserts can duplicate even with dedup enabled. Background: [Hudi concurrency docs](https://hudi.apache.org/docs/concurrency_control).
- **Catalog / metastore sync beyond the common paths** — the agent asks which engines query the table and derives sync config for Hive Metastore, AWS Glue, BigQuery, and DataHub, including the partition-extractor and MOR `_ro`/`_rt` consequences. Not covered: Polaris beyond pointing at the Spark catalog config, Snowflake/Redshift-specific setup, and per-catalog auth (Kerberos, IAM policy documents, service-account keys) — those are environment concerns the flow deliberately never asks about. Background: [metastore](https://hudi.apache.org/docs/syncing_metastore), [Glue](https://hudi.apache.org/docs/syncing_aws_glue_data_catalog), [BigQuery](https://hudi.apache.org/docs/gcp_bigquery), [DataHub](https://hudi.apache.org/docs/syncing_datahub).
- **Non-Kafka source configs** — Kafka sources get full source-class + schema-provider derivation; for DFS/JDBC/Pulsar/Kinesis sources you'll fill in the `--source-class` and source properties from Hudi docs.
- Also out of scope: benchmarking, record-level TTL, z-order/layout-optimization guidance, multi-table transactions, CONSISTENT_HASHING sizing, and versions other than 1.2.0.

If you hit one of these, the ADR's *Open Questions* section is where it lands — treat entries there as pre-launch action items, not footnotes.

## 9. Feedback

This is a Milestone 1 preview and walkthroughs are the fuel for making it sharper. When something feels off — a question that didn't fit, a warning that fired too late or not at all, a recommendation that surprised you — note the moment in the transcript and share it on the discussion thread: [apache/hudi#19264](https://github.com/apache/hudi/discussions/19264).

Especially valuable: run a table you already operate through the flow and compare the agent's design against what you actually built. Divergences in either direction are exactly the feedback the project needs.
