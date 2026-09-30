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

A Claude Code Skill that runs the metadata-table checks of `HoodieTableHealthChecker` against a
Hudi table and turns the JSON report into a diagnosis: whether the metadata table is caught up
with the data table, whether its file listing can be trusted, whether its compaction is keeping
up, whether the record-level index is sized for the table, and what to do next.

It pairs with the utility in `hudi-utilities`
(`org.apache.hudi.utilities.HoodieTableHealthChecker`); see that class's `.md` for the tool's own
runbook. The Skill adds the interpretation layer — reading the report, explaining findings, and
recommending the next command — and is careful about the one recommendation that is easy to get
wrong: it never proposes rebuilding the metadata table unless a listing inconsistency has been
confirmed, because sync lag and compaction lag recover on their own and a rebuild is disruptive.

## Install

```bash
SKILL=hudi-agent-gateway/skills/hudi-mdt-health

# user-level
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/
# or project-level
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then `/hudi-mdt-health` in Claude Code. You will be asked for the table base path and the writer
properties file; have both ready.

## Requirements

- The `hudi-utilities-bundle` jar from the same Hudi release as this Skill, reachable from wherever
  Claude Code runs commands.
- Read access to the table's base path, including `.hoodie/metadata`. The tool is read-only and
  never modifies the table or its metadata table.

## Files

- `SKILL.md` — the Skill itself.
- `references/checks.md` — the JSON envelope, and per check: governing properties, what
  "unhealthy" means, and the remedy.

## Scope

This Skill covers the metadata table: sync with the data table, listing consistency, compaction
cadence, and record-index sizing. Data-table service cadence (compaction, cleaning, archival,
savepoints) and data layout (small files, micro-partitions) are separate tools with their own
Skills.
