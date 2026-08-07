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
3. **Output** — produces an Architecture Decision Record (ADR) with tradeoff tables, durability warnings, config bundle, and measurable revisit conditions.

Target Hudi version: **1.2.0**.

## How to invoke

> **Data engineering / ETL teams:** see [RUNBOOK.md](RUNBOOK.md) for a step-by-step guide — setup, a pre-session workload checklist, how to read the ADR output, and current limitations.

### As a Claude Code Skill

Copy this directory into your Claude Code skills location:

```bash
# From the root of a Hudi checkout.
SKILL=hudi-agent-gateway/skills/hudi-architect

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

The Skill itself is defined in `SKILL.md`.

### Config-key validation

Every `hoodie.*` key mentioned in `SKILL.md` and `references/` is checked against the actual `ConfigProperty` definitions in the Hudi source tree:

```bash
python3 hudi-agent-gateway/skills/hudi-architect/validate_config_keys.py
```

Run it after any edit to the reference files (exit 1 lists unknown keys). Intentional exceptions — e.g. future-version keys the references discuss but never emit — live in `validate_config_keys_allowlist.txt` with a comment each.

## What to look for during review

This is **Milestone 1 of a longer arc** — meant to be shareable and playable, not final. Things worth stress-testing:

1. **Walk a real workload through the flow.** Pick a Hudi table you know (existing or planned) and run `/hudi-architect`. Note where the questions don't fit, where the tradeoffs feel wrong, where a decision surprises you.
2. **Check the warnings fire when they should.** Try configurations that should trigger Vice 1/2/3, the high-cardinality-partition trap, or the compaction-target-IO trap. Do the warnings surface at the right moment?
3. **Try the tier gate at all four levels.** The `EXPLORATION` mode should feel like a Hudi tutor, not a design advisor. The `PRODUCTION_AT_SCALE` mode should feel rigorous. If either feels wrong, that's a signal.
4. **Read the ADR output.** Are the revisit conditions actually measurable? Do the durability tables cover the one-way decisions relevant to your workload?

## What's out of scope in Milestone 1

- Multi-writer / concurrency (OCC, NBCC, lock providers) — deferred to a later pipeline-modeling rubric.
- Benchmarking / scale-characterization — different flow shape, future revision.
- CONSISTENT_HASHING bucket recommendations at design time.
- Partial-update MERGE nudging (Spark SQL only).
- Version-awareness across Hudi releases (V1 pins to 1.2.0).
- Session persistence for tier upgrades.

## Longer arc

- **M1 (this)** — Skill-shaped shareable version for colleague/community review.
- **M2** — Sort out pending items (concurrency, benchmarking, session persistence, etc.).
- **M3** — Feedback incorporation from reviewers.
- **M4** — Integration into Hudi's Agentic Lakehouse (`hudi-agent-gateway`) — see [discussion #19264](https://github.com/apache/hudi/discussions/19264).

## Feedback welcome

Play with the Skill, note what breaks or feels off, and share back. Every walkthrough that surfaces a gap makes the design engine sharper before it becomes real code.

Full proposal design document lives in the parent directory as `hudi_architect_agent_proposal_chatgpt.md`.
