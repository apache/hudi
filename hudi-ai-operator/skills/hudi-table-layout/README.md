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

# Hudi Table Layout

A Claude Code Skill that runs `HoodieTableLayoutAnalyzer` against a Hudi table and turns its JSON
report into a diagnosis: how bytes and files are spread across partitions, how skewed that spread
is, whether the table is micro-partitioned or piling up small files, which partitions are hot, and
what to do about it.

It pairs with the utility of the same name in `hudi-utilities`
(`org.apache.hudi.utilities.HoodieTableLayoutAnalyzer`); see that class's `.md` for the tool's own
runbook and full flag reference. The Skill adds the interpretation layer — choosing the flags for
the question asked, reading the report, explaining detector verdicts, and recommending the next
step — and is careful about the one thing that makes runs expensive: it warns before requesting
row counts on tables without metadata column stats.

## Install

```bash
SKILL=hudi-ai-operator/skills/hudi-table-layout

# user-level
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/
# or project-level
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then `/hudi-table-layout` in Claude Code. You will be asked for the table base path and what you
want to know about it.

## Requirements

- `spark-submit` on the path (Spark 3.x). The analyzer is a Spark application; there is no plain
  Java entry point.
- The `hudi-utilities-bundle` jar from the same Hudi release as this Skill, reachable from wherever
  Claude Code runs commands.
- Read access to the table's base path. The tool is read-only and never modifies the table.

## Files

- `SKILL.md` — the Skill itself.
- `references/detectors.md` — the JSON envelope, and per detector: what it measures, the flags
  that set its thresholds, what each status means, and the remedy.

## Scope

This Skill covers data layout: file and partition sizes, skew, micro-partitions, small files, hot
partitions. Table-service cadence (compaction, cleaning, archival, savepoints) is a separate tool
with its own Skill.
