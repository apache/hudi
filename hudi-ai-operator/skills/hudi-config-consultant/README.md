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

# Hudi Config Consultant

A Claude Code Skill that answers questions about Hudi `hoodie.*` configuration from a catalog
extracted out of the Hudi source tree, rather than from a documentation page or from memory.

The published configuration reference gives key, default and description. It cannot tell you where
a config is read, what gates that read, or what else is read beside it. Those three things are
where the real questions live:

- *"I set `hoodie.compact.inline.max.delta.commits` to 20 and compaction still fires every 5
  commits."* The value is only consulted under four of the compaction trigger strategies. The
  catalog shows the `switch` it sits inside, in `ScheduleCompactionActionExecutor.needCompact`, at
  a file and line you can open.
- *"I set `hoodie.cleaner.policy` and nothing changed."* That key is now an alternative of
  `hoodie.clean.policy`. The catalog records the alias.
- *"What do I have to set alongside this?"* The catalog records which other configs are read in the
  same methods.

Every answer the Skill gives points at `file:line` in the Hudi source, so you can check it.

## Files

- `SKILL.md` — the Skill itself.
- `references/catalog-schema.md` — the JSON schema, how to read `readSites` well, and runnable
  query recipes.
- `config-catalog.json` — the generated catalog. A build artifact; do not hand-edit.
- `config-catalog-summary.md` — counts per module and per config group, for a quick sense of scale.

## Install

```bash
SKILL=hudi-ai-operator/skills/hudi-config-consultant

# user-level
mkdir -p ~/.claude/skills && cp -r "$SKILL" ~/.claude/skills/
# or project-level
mkdir -p .claude/skills && cp -r "$SKILL" .claude/skills/
```

Then `/hudi-config-consultant` in Claude Code. No jar, no cluster, no table — the Skill reads only
the catalog file.

## Regenerating the catalog

```bash
python3 scripts/generate_config_catalog.py
```

Stdlib-only Python 3, no dependencies, under a minute on a full checkout. It rewrites
`config-catalog.json` and `config-catalog-summary.md` in place. Output is sorted by config key so
regeneration diffs cleanly. Regenerate whenever the config declarations move — the catalog records
the commit it came from, and the Skill reports that commit in every answer.

`--repo-root` scans a different checkout; `--out-dir` writes elsewhere; `--verbose` reports each
pass on stderr.

## What the generator extracts

Declarations come from `ConfigProperty` builder chains (and Flink `ConfigOptions` chains that name
a literal `hoodie.*` key) across every `src/main` tree, test and generated sources excluded, in
**both Java and Scala**. The Scala pass covers `DataSourceReadOptions` and `DataSourceWriteOptions`
— the `hoodie.datasource.*` keys Spark users set most — resolving their defaults and valid values
through Scala `val` string constants. It resolves keys built from `static final String` prefixes
and from other configs' keys, follows `withDocumentation(SomeEnum.class)` to the enum's
`@EnumDescription` and per-value `@EnumFieldDescription`, and folds re-export aliases onto the
config they point at.

A further pass recovers **engine-specific effective defaults**. `ConfigProperty.defaultValue()` is
the declared default, but a builder's `build()` may override it per engine with
`setDefaultValue(PROP, getDefaultXxx(engineType))`, which applies whenever the key is unset. The
generator follows those helpers' `switch (engineType)` and emits `engineDefaults` alongside the
declared value. Where an engine's default branches on another config or on the runtime instead, it
is recorded as a condition in `conditionalEngineDefaults` rather than flattened to a value.

Code context comes from two further passes: config-class accessor methods whose body reads exactly
one config, and then every call site of those accessors plus every direct mention of a config
constant, attributed to its enclosing method.

## Known limits

- **Gating is a textual heuristic.** The generator records the `if`/`switch` conditions enclosing a
  read. It does not see conditions in callers, guard clauses with early returns, or the derivation
  of a local that a branch tests. Treat a gate as evidence to open the file and check, never as a
  verdict. The Skill is written to present it that way.
- **Scala read sites carry no method or gating.** Scala *declarations* are extracted in full (key,
  default, documentation, valid values, alternatives, since/deprecated). Scala *read sites* are
  not: the generator parses Java method bodies only, so a Scala reference is recorded at file and
  line with no enclosing method, no co-configs and no gating. Those sites carry
  `gatesResolved: false`, and the config carries `readContextResolved: false` with a
  `readContextNote`. For a `hoodie.datasource.*` config an empty `gatingConditions` therefore means
  *not analysed*, not *unconditional*.
- **Engine defaults cover the `setDefaultValue(PROP, helper(engineType))` shape only.** A default
  overridden some other way — an infer function, or a caller passing an explicit value — is not
  reported as an engine default.
- **Read sites are capped at 20 per config**, co-configs at 25. A config at the cap has more
  consumers than the catalog lists.
- **Co-configs are co-occurrence, not dependency.** They tell you what the code reads together,
  which is a good lead, not what Hudi requires.
- A handful of configs resolve no read site at all. Those carry a `parseWarnings` entry saying so.
  That is a gap in extraction, not evidence the config is unused.
