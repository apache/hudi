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
# hudi-architect (Milestone 1 preview)

An OSS conversational design advisor for Apache Hudi tables. Packaged as a Claude Code Skill for early review.

## What it does

Turns workload requirements into a validated Hudi table architecture through a tiered conversational flow:

1. **Tier gate** — figures out whether you're exploring, prototyping, productionizing at modest scale, or going all-in at TB/PB.
2. **Rounds 1-3** — asks workload questions (not Hudi jargon questions) gated by tier.
3. **Output** — produces an Architecture Decision Record (ADR) with tradeoff tables, durability
   warnings, and measurable revisit conditions; implemented executable routes also include a
   configuration bundle.

Target Hudi version: **1.2.0**.

### Engine coverage

- **Spark and HoodieStreamer** retain the existing full design flow, configuration bundle, and
  runnable output.
- **Flink** combines the routing and safety foundation with bounded append-only COW, mutable COW,
  and mutable MOR SQL sink paths for Hudi 1.2.0 and Flink 1.20 (1.20.1 fixtures). Mutable eligibility requires
  stable identity, event-time ordering, normalized UPSERT changelog, full-row deletes when present, immutable
  partitions, and the fixed global FLINK_STATE/bootstrap/no-TTL contract. MOR additionally fixes
  same-writer asynchronous compaction, records snapshot/read-optimized freshness, and disables
  clustering. It also provides a read-only, non-executable exact-match reconciliation report for
  an existing table backed by structured evidence. For new tables, those same design schemas can
  compose direct Hive sync, Hudi DFS Catalog, or external Hudi HMS Catalog through five pinned
  profiles. Glue, managed HMS, existing-table catalog mutation, and live connectivity claims
  remain fail-closed. A versioned PR7 Writer Contract can record independent writers and ownership,
  but remains assessment-only: OCC is unverified, current schemas are incompatible with NBCC, and
  no multi-writer SQL is emitted.

## How to invoke

> **Data engineering / ETL teams:** see [RUNBOOK.md](RUNBOOK.md) for a step-by-step guide — setup, a pre-session workload checklist, how to read the ADR output, and current limitations.

### As a Claude Code Skill

Copy this directory into your Claude Code skills location:

```bash
# From the root of a Hudi checkout.
SKILL=hudi-ai-operator/skills/hudi-architect

# User-level (available in every project)
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/

# Or project-level, in the repo where your pipelines live
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then in Claude Code:

```
/hudi-architect
```

Claude will drive the design flow.

### As a reference

Even without invoking as a Skill, the files in `references/` are readable design references:

- `question-flow.md` — round-by-round question list with conditional gating.
- `decision-tables.md` — derivation tables for each design domain (engine, writer, table type, index, partitioning, retention, services, meta-fields, record key).
- `warnings.md` — rule-engine warnings and when they fire.
- `config-templates.md` — `hoodie.*` property templates per decision + sample bundles for three workload archetypes.
- `adr-template.md` — the structure of the ADR output.
- `flink-question-flow.md` — Flink-only fail-closed gates, loaded after Flink is selected.
- `flink-decision-overrides.md` — Flink statuses, routing invariants, and deterministic cases.
- `flink-warnings.md` — warnings that must never fire for Spark scenarios.
- `flink-1.20-hudi-1.2.0-capabilities.md` — the fixed initial capability baseline.
- `flink-1.20-hudi-1.2.0-capabilities.toml` — the machine-readable manifest pinned to the Hudi
  1.2.0 source revision.
- `flink-config-templates.md` — non-executable and bounded executable Flink output envelopes.
- `flink-existing-table-reconciliation.md` — evidence authority, comparison, and writer-transition
  rules for the assessment-only existing-table path.
- `flink-catalog-composition.md` — the five PR6 profiles, lifecycle/path rules, Hive sync ownership,
  MOR naming, freshness boundary, and deployment checks.
- `flink-writer-contract.md` — the PR7 writer inventory, ownership, observed-concurrency, evidence,
  and non-executable capability rules.

The Skill itself is defined in `SKILL.md`.

### Config-key validation

The validation utilities require Python 3.11 or newer, matching the Agent Gateway project
baseline.

Every `hoodie.*` key mentioned in `SKILL.md` and `references/` is checked against the actual `ConfigProperty` definitions in the Hudi source tree:

```bash
python3 hudi-ai-operator/skills/hudi-architect/validate_config_keys.py
```

Run it after any edit to the reference files (exit 1 lists unknown keys). Intentional exceptions — e.g. future-version keys the references discuss but never emit — live in `validate_config_keys_allowlist.txt` with a comment each.

Validate the Flink baseline manifest without consulting the current checkout:

```bash
python3 hudi-ai-operator/skills/hudi-architect/validate_flink_capabilities.py
```

Maintainers with the pinned release commit available locally can additionally verify every source
hash with `--verify-source`. Use `--emit-evidence` to produce the baseline evidence included in a
Flink assessment.

Validate and render a complete bounded Flink design contract:

```bash
python3 hudi-ai-operator/skills/hudi-architect/validate_flink_design.py \
  --input hudi-agent-gateway/tests/fixtures/hudi_architect/flink_pr2/stable_key.json \
  --emit-sql
```

The validator emits SQL only after all safety and load-bearing checks pass. The exact SQL is also
planned in CI against the released Hudi 1.2.0 Flink bundle and Flink 1.20.1.

The mutable schema-2 fixture is
`hudi-agent-gateway/tests/fixtures/hudi_architect/flink_pr3/mutable_cow.json`.
The mutable MOR schema-3 fixture is
`hudi-agent-gateway/tests/fixtures/hudi_architect/flink_pr4/mutable_mor.json`.
PR6 profile inputs are recorded in
`hudi-agent-gateway/tests/fixtures/hudi_architect/flink_pr6/catalog_profiles.toml`.
PR7 source-backed concurrency fixtures are implemented in
`hudi-agent-gateway/fixtures/hudi-architect-flink-1.20/src/test/java/org/apache/hudi/agent/architect/TestFlinkArchitectWriterContractFixtures.java`.

Validate a structured existing-table reconciliation contract without rendering SQL:

```bash
python3 hudi-ai-operator/skills/hudi-architect/validate_flink_existing_table.py \
  --input hudi-agent-gateway/tests/fixtures/hudi_architect/flink_pr5/compatible_mutable_cow.json
```

Credential-bearing evidence can be sanitized without executing or parsing it as configuration:

```bash
python3 hudi-ai-operator/skills/hudi-architect/redact_sensitive_values.py < evidence.txt
```

## What to look for during review

This is **Milestone 1 of a longer arc** — meant to be shareable and playable, not final. Things worth stress-testing:

1. **Walk a real workload through the flow.** Pick a Hudi table you know (existing or planned) and run `/hudi-architect`. Note where the questions don't fit, where the tradeoffs feel wrong, where a decision surprises you.
2. **Check the warnings fire when they should.** Try configurations that should trigger Vice 1/2/3, the high-cardinality-partition trap, or the compaction-target-IO trap. Do the warnings surface at the right moment?
3. **Try the tier gate at all four levels.** The `EXPLORATION` mode should feel like a Hudi tutor, not a design advisor. The `PRODUCTION_AT_SCALE` mode should feel rigorous. If either feels wrong, that's a signal.
4. **Read the ADR output.** Are the revisit conditions actually measurable? Do the durability tables cover the one-way decisions relevant to your workload?
5. **Declare a second writer.** On the Spark route, check the selected lock provider, emitted block,
   and pre-launch checklist. On the Flink route, check that the result is `REVIEW_REQUIRED` and
   that the Writer Contract is normalized without emitting concurrency-sensitive configuration.

## What's out of scope in Milestone 1

- Flink append-only MOR, standalone compaction, detailed compaction tuning, clustering,
  existing-table SQL generation or catalog takeover, AWS Glue, managed/embedded HMS, live catalog
  probing, executable multi-writer operation, alternative indexes, custom
  mergers, retract normalization, partition movement, key-only deletes, full CDC history,
  DataStream API, `HoodieFlinkStreamer`, source connector generation, and submit/deploy commands.
  Those paths do not reuse the bounded SQL output as if it were generally applicable.

- Multi-writer contention tuning — the Spark route may derive its established bundle; the Flink
  Writer Contract records only observed mode/provider facts. Retry/timeout tuning and early
  conflict detection depend on observed behavior, not design-time facts.
- Benchmarking / scale-characterization — different flow shape, future revision.
- CONSISTENT_HASHING bucket recommendations at design time.
- Partial-update MERGE nudging (Spark SQL only).
- Version-awareness across Hudi releases (V1 pins to 1.2.0).
- Session persistence for tier upgrades.

## Longer arc

- **M1 (this)** — Skill-shaped shareable version for colleague/community review.
- **M2** — Sort out pending items (benchmarking, session persistence, etc.).
- **M3** — Feedback incorporation from reviewers.
- **M4** — Integration into Hudi's Agentic Lakehouse (`hudi-agent-gateway`) — see [discussion #19264](https://github.com/apache/hudi/discussions/19264).

## Feedback welcome

Play with the Skill, note what breaks or feels off, and share back. Every walkthrough that surfaces a gap makes the design engine sharper before it becomes real code.

Full proposal design document lives in the parent directory as `hudi_architect_agent_proposal_chatgpt.md`.
