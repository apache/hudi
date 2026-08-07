---
name: explain-hudi
description: Explain any Hudi concept, architecture, or CoW vs MoR tradeoffs from source code. Use when asking "what is", "how does", "explain", or "compare CoW MoR".
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [concept e.g. "compaction", "file groups", "metadata table", "record-level index"]
---

# Explain Hudi Concept

The user wants to understand a Hudi concept: **$ARGUMENTS**

## Instructions

You are an Apache Hudi expert. Explain the concept by actually reading the relevant source code in this repository, not from memory. Ground every explanation in actual code references.

### Step 1: Identify the relevant code
Search the codebase for classes, interfaces, and configs related to the concept. Key locations:
- Core abstractions: `hudi-common/src/main/java/org/apache/hudi/common/`
- Client/write path: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/`
- Spark integration: `hudi-spark-datasource/`
- Configs: classes ending in `Config.java` under `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/` and `hudi-common/src/main/java/org/apache/hudi/common/config/`

### For architecture questions, also cover:
- Module layout: `hudi-io/` (IO), `hudi-common/` (core), `hudi-client/` (write), `hudi-spark-datasource/` (Spark), `hudi-flink-datasource/` (Flink), `hudi-utilities/` (tools), `hudi-sync/` (metastore sync), `hudi-hadoop-common/` (storage), `packaging/` (bundles)
- Key abstractions and interfaces, extension points, design patterns
- Layering rules: `hudi-common` cannot depend on engine-specific code

### For CoW vs MoR comparisons, ground in:
- CoW commit: `BaseCommitActionExecutor` (rewrites full Parquet files)
- MoR append: `HoodieAppendHandle` (appends to log files)
- File group reader: `HoodieFileGroupReader.java` (merges base + logs)
- Table type enum: `HoodieTableType.java`

### Step 2: Build the explanation
Structure your explanation as:
1. **What it is** - 2-3 sentence definition
2. **Why it exists** - The problem it solves
3. **How it works** - Walk through the key classes and methods with file:line references
4. **Key configs** - List the most important configuration knobs (config key, default, what it controls)
5. **CoW vs MoR** - How behavior differs between Copy-on-Write and Merge-on-Read tables (if applicable)
6. **Common gotchas** - Things that trip up production users

### Step 3: Provide actionable examples
- Show relevant Spark SQL or DataFrame API usage
- Show relevant CALL procedures if they exist (check `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/`)
- Show relevant CLI commands if they exist (check `hudi-cli/src/main/java/org/apache/hudi/cli/commands/`)

Keep the tone practical and production-focused. Avoid academic explanations - focus on what a production user needs to know.
