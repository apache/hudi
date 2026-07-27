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
# Question flow — Round 1, 2, 3

Read this file when you're about to ask questions. Each round lists questions in order with conditional gating.

## Round 1 — Source, engine, mutability, distribution, experience (all tiers)

Round 1 is the minimum to specify a PROTOTYPING design. For EXPLORATION, replace hard questions with explanations where possible.

### Q1.1 — Engine

> "Which processing engine are you planning to use — Spark, Flink, or undecided?"

- Spark or Flink → proceed to Q1.2.
- Undecided → present Spark vs Flink tradeoff (see decision-tables.md → engine). Recommend based on workload signals.

### Q1.2 — Source

> "Where does the data come from?"

Two families of answer, and the distinction drives writer selection:

**External systems** — something a writer polls or reads: Kafka, DFS (files), JDBC (database), another Hudi table, S3/GCS events, Kinesis, Pulsar, custom.

**An in-job DataFrame** — the user's own Spark job has already produced the data (an ETL query, a join, an aggregation), and the Hudi write is the final step of that job. Common for silver/gold derived tables. Phrase the option as "a Spark DataFrame from upstream ETL" — users describe this as "my ETL job writes it," not as a "source."

Store this. It determines the writer:
- **Kafka** → HoodieStreamer strong default, plus Q1.2b for record format.
- **In-job DataFrame** → **Spark DataSource**, necessarily. HoodieStreamer polls an external source; there isn't one. Skip Q1.2b entirely — no source class, no schema provider.
- **Everything else** → HoodieStreamer and DataSource are co-equal; decided by pipeline shape at Q2.9.

**Knock-on for the in-job DataFrame case:** the writer can't run async compaction in-process. If the design lands on MOR, that means inline compaction or a standalone `HoodieCompactor` job — and the standalone path makes it a concurrent-writer deployment needing lock configuration. Surface `WRITER_COMPACTION_MISMATCH` and `COMPACTOR_CONCURRENCY_REQUIRED` accordingly. Q2.9 becomes partly redundant here: the pipeline shape is already known to be custom application code.

### Q1.2b — Source record format (KAFKA SOURCE ONLY)

Fires when Q1.2 answer is Kafka. Derives the HoodieStreamer source class and schema provider.

> "What format are the records on your Kafka topic?
>
> - **Avro + schema registry** — `AvroKafkaSource` with `SchemaRegistryProvider`. Most common production shape; schema evolution handled centrally.
> - **Avro + schema file** — `AvroKafkaSource` with `FilebasedSchemaProvider`. You supply `.avsc` files for source and target.
> - **JSON** — `JsonKafkaSource`.
> - **Protobuf** — `ProtoKafkaSource`. Requires the proto class on the classpath."

Closed-set → widget. See decision-tables.md → writer for the source-class mapping and config-templates.md for the emitted properties.

Note this question assumes a schema provider rather than asking whether one exists — deliberate for Kafka. Producers and consumers on a topic already need schema coordination to interoperate, so a registry is usually standing infrastructure before Hudi is involved. Ask *which*, not *whether*. Non-Kafka sources get the genuine yes/no at Q1.2c.

### Q1.2c — Schema source (HoodieStreamer + NON-Kafka source)

Fires when the writer is HoodieStreamer and the source is not Kafka (Q1.2b covers Kafka). **Optional** — a schema provider is not required. Many sources infer their own schema, and simple pipelines with no expected schema evolution commonly run without one.

Ask only when schema evolution is in play or the user has an authoritative schema source. Don't force a choice on a simple pipeline.

> "Do you have a schema definition for this data, or should Hudi infer it from the source?
>
> - **Let Hudi infer it** — fine when the schema is stable and evolution isn't expected. Simplest.
> - **Schema files I manage** (`.avsc`) — `FilebasedSchemaProvider`. The usual answer when a schema is managed explicitly.
> - **A Hive metastore table** — `HiveSchemaProvider`.
> - **The upstream database table** — `JdbcbasedSchemaProvider`.
> - **A schema registry** — `SchemaRegistryProvider`. Strongest option for evolving schemas."

Closed-set → widget. Config keys in config-templates.md → Schema providers.

**Default when the user is unsure:** infer, or file-based if they already have `.avsc` files. Reach for a registry only when schema evolution is an actual requirement — recommending one to a pipeline that doesn't need it adds infrastructure for nothing.

**Skip entirely for Spark DataSource writes** — the DataFrame carries its own schema.

### Q1.3 — Landing mode and ingestion cadence

Fires at **all four tiers**, unconditionally.

> "How does data land — a streaming job that runs continuously, or a batch job on a schedule?
>
> And roughly how often does one round of ingestion complete?
>
> - **Under 5 minutes** — near-real-time.
> - **5–15 minutes** — typical streaming cadence.
> - **Around an hour**
> - **Once a day** (or less often)"

Mode in plain language first — a user running a forEachBatch pipeline genuinely may not know which label applies. Cadence in bands, because the decision tables key off bands rather than exact minutes.

Cadence is load-bearing downstream: it drives the retention safe-maximum (decision-tables.md → retention), and triggers `RETENTION_CLAMP` and `SUB_5_MIN_CADENCE_UNSTABLE`. Because it is captured in Round 1, those two warnings can be evaluated as soon as a retention answer arrives — they no longer have to wait for Q2.3.

### Q1.4 — Mutability

> "Is the workload mutable or immutable? (Mutable = records get updated after insert. Immutable = records only ever append, never change.)"

### Q1.5 — Update distribution (MUTABLE ONLY — skip for immutable)

> "When updates arrive, which parts of the table do they touch?
>
> - **Uniformly across the whole table** — any record can be updated at any time (e.g., customer, vendor, product tables).
> - **Concentrated on recent data** — most updates hit records from the last few days, with a tail of stragglers (e.g., trips, orders, sessions).
> - **Unsure.**"

If "recent-concentrated," ask a follow-up in Round 2 about tail length (see Q2.3).

### Q1.6 — Experience with Hudi (MUTABLE ONLY)

**Fires only for mutable workloads.** For immutable, table type is COW with no compaction to run, so the answer changes nothing — skip it rather than ask.

Gets its own widget screen, preceded by the COW/MOR tradeoff as prose:

- **COW**: rewrites base files on updates. Simple ops, nothing extra to run, but write cost grows with update frequency.
- **MOR**: appends updates to logs and compacts periodically. Lower write cost, but compaction becomes a service you keep healthy or read latency suffers.

Adapt the "ops surface" row to the derived writer before showing it. With HoodieStreamer continuous, MOR's async compaction runs in-process and costs no extra job. With Spark DataSource, it means inline compaction or a standalone `HoodieCompactor` — and the standalone path makes it a concurrent-writer deployment. Presenting a generic tradeoff table when the writer is already known misstates the choice.

> "Are you experienced managing and operating Hudi tables?
>
> - **First-time / want fire-and-forget** — one job to run, simple ops.
> - **Some experience** — want MOR benefits without ops complexity.
> - **Experienced operator** — comfortable with standalone async compaction executors, concurrent services, tuning."

Derives table type + compaction posture (see decision-tables.md → table-type).

## Round 1 outputs

- Engine (user-answered).
- Source, and for Kafka: record format → source class + schema provider.
- Commit cadence (user-answered) — feeds retention safe-max and cadence warnings.
- Table type PROVISIONAL (derived from experience + mutability + distribution).
- Compaction posture PROVISIONAL.
- Writer will be derived in Round 2.

**Delivery:** Round 1 batches into widget screens of at most 3 questions each.

- **Screen 1** — engine, source, cadence. Fold in the Kafka record-format question (Q1.2b) when the source is Kafka — a gated follow-up that exists only because of an answer on the same screen may share it as a fourth item (see SKILL.md batching rule).
- **Screen 2** — mutability and update distribution. Keep these together: Q1.4 gates Q1.5, and splitting them strands the gate.
- **Screen 3** — experience (Q1.6), preceded by the COW/MOR tradeoff table.

**Why experience gets its own screen.** The tradeoff table must precede the widget it informs (tradeoff-as-prose rule), but the COW/MOR tradeoff is only meaningful for mutable workloads — an immutable table has no log files to merge, so MOR buys nothing. Putting experience on the same screen as mutability forces the table to be shown before the answer that determines whether it applies, and on an immutable workload it has to be retracted immediately.

**For immutable workloads, screen 3 changes shape entirely:**
- Skip the COW/MOR tradeoff. Table type is COW, derived silently, no dialogue.
- Skip Q1.5 (update distribution) — it's already answered as N/A.
- Ask experience only if something downstream depends on it. For immutable workloads it mostly doesn't: there's no compaction posture to derive, and small-files posture is asked directly at Q2.8. Prefer skipping it over asking a question whose answer changes nothing.

This means an immutable Round 1 is often two screens (engine/source/cadence, then mutability), while a mutable Round 1 is three.

**For EXPLORATION: Round 1 is the entire conversation.** Skip to output — a narrative sketch, not necessarily a full ADR.

### PROTOTYPING — Round 1 plus a bounded Round 2 subset

Round 1 alone cannot produce a runnable table: it never collects a record key, an ordering field, or a partitioning decision. A prototyping user who is handed a config bundle full of placeholders cannot get to a first commit, which defeats the tier. So PROTOTYPING continues past Round 1 with two mechanisms.

**(1) Disclosed-defaults consent block.** State the defaults rather than asking, flag durable items inline, then take a single consent answer:

> "For a prototype I'll apply these defaults rather than walk you through the full design:
>
> - **Table size** — assuming small (prototype scale). Drives index choice.
> - **Partitioning** — none. Simplest thing that works at this scale. ⚠️ Durable: changing this later needs a table rewrite.
> - **Retention** — 48h lookback for time-travel and incremental queries.
>
> Fine for a prototype, or do you want to set any of these yourself?"

The durability flag on partitioning is mandatory. Silently defaulting a rewrite-to-change decision is the failure this block exists to prevent.

**Two warnings can't evaluate at this tier because their inputs live in Round 2. Default, but say so.**

*Table type* — `WORKLOAD_EXPERIENCE_TENSION` needs projected table size (Q2.2) to detect the mutable + uniform-updates + at-scale case that argues for MOR. Without it, go with **COW** and call out the tradeoff explicitly rather than deriving silently:

> "I'm defaulting to Copy-on-Write. It rewrites base files when records update — simplest to operate, nothing extra to run. The alternative, Merge-on-Read, appends updates to log files and compacts them periodically: cheaper writes, but compaction becomes a service to keep healthy.
>
> For a prototype, COW is almost always the right call. It matters at scale: if this table grows large and updates land uniformly across it, COW rewrites base files table-wide on every commit and Merge-on-Read becomes the better fit. ⚠️ Table type is durable — switching later needs a rewrite.
>
> Keep COW, or switch to Merge-on-Read?"

Offer the override. Don't just announce the default.

*Partitioning* — `VICE_1_PARTITION_MISALIGNMENT` needs the read-pattern answer (Q2.1) to check whether a partition column matches consumer filters. Go with the default (unpartitioned at prototype scale), and attach a scale disclaimer that is explicit about irreversibility:

> "⚠️ Unpartitioned is fine at prototype scale, but this is a one-way decision. **A table's partitioning cannot be changed after creation** — going from unpartitioned to partitioned means building a new table and rewriting all the data into it. There's no ALTER for this.
>
> So if there's a real chance this design carries into production at meaningfully larger volume, decide partitioning now rather than later. Past roughly 500GB an unpartitioned table starts producing file-layout problems, and by then the fix is a migration.
>
> I also haven't asked what your consumers filter on, so I can't check that a partition column would align with their queries. If you want to partition now, tell me and I'll ask the couple of questions needed to pick a column properly."

Both call-outs must also appear in the ADR — the table-type one under key decisions, the partitioning one as a revisit condition. A verbal mention that doesn't survive into the document isn't a call-out.

**(2) Hard-ask the non-defaultable facts.** For mutable workloads, ask the record key using the concept-anchored phrasing in Q2.6, and the ordering field per Q2.6b. Neither has a default. For immutable workloads, auto-generated keys are a legitimate default and can live in the disclosed-defaults block instead.

**Override branch — "let me set them".** Do not emit a wall of prose asking for several values at once. Present a multi-select router:

> "Which of these do you want to set yourself? I'll apply defaults for anything you leave unchecked."
> Options: `Record key + ordering field` (pre-selected and non-optional when mutable) · `Partitioning` · `Table size` · `Retention`

Then work the claimed items in this fixed order, so two runs of the same workload don't diverge:

**Step 1 — closed-set values, one widget screen.** Table size and retention band, whichever were claimed. Both have derivable option sets, so they batch together. Use the Q2.2 and Q2.3 phrasing.

**Step 2 — partitioning, if claimed.** Conversational, because it needs an explanation preamble and branches on the answer. Lead with what partitioning buys, then the shape question (date-based / business dimension / none / you pick) as a widget, then granularity. Defer the column name to step 4.

**Step 3 — validation questions for anything claimed in step 2.** If the user opted into partitioning, ask partition-column stability (Q2.5) and enough of the read pattern (Q2.1) to run the Vice 1 check. These batch into one widget screen. Skipping them means accepting a durable decision the flow can't check.

**Step 4 — free-text column names, batched into a single turn.** Ask for every name at once — partition column, record key, ordering field — rather than one per turn. By this point each has been scoped by a shape question, so the user is supplying names against decisions already made.

**Step 5 — apply defaults for everything unclaimed**, and state which defaults were applied. An unclaimed item is still a decision; the user just delegated it. It belongs in the ADR under consented defaults, not silently in the config.

Then proceed to the final revisit gate as normal.

**Validation travels with the decision** (the principle behind step 3). Partition-column stability gates index scope — partition-scoped versus global — and the read pattern gates whether the chosen column earns its keep at all. Both are correctness forks, and neither can fire if left in a round PROTOTYPING never reaches. Any time a tier lets a user make a decision early, the questions that validate that decision come with it.

## Round 2 — Reads, layout, identity, writer (PRODUCTIONIZING_INITIAL, PRODUCTION_AT_SCALE)

### Q2.1 — Consumer read pattern (three-axis question)

> "How will downstream consumers use this table? A few things to help me understand:
>
> - **General shape** — do they run bulk analytical queries (scan large portions of the table), targeted lookups (look up specific records), or streaming/incremental consumers that only need what changed?
> - **Latency sensitivity** — do reads need to be fast, or is some read latency acceptable if it buys cheaper writes?
> - **Time travel or incremental query needs** — do any consumers need to query the table as-of a past commit, or read only changes since their last checkpoint?"

Rule engine maps answers to Hudi query types internally. See decision-tables.md → read-behavior.

### Q2.2 — Table size

> "What's the current table size, and what do you expect over the next 2-3 years?"

Used for partitioning threshold, unpartitioned viability, index selection, retention limits.

### Q2.3 — Retention lookback

> "What's the maximum lookback window you'd need for time-travel or incremental queries? For example, do downstream consumers need to query the table as-of a past commit, or read only changes since their last checkpoint (last day, last week, last month)?"

- "No time-travel or incremental needs" → default to safe max for commit cadence.
- Otherwise → clamp user-desired to safe max (surface the clamp explicitly). See decision-tables.md → retention.

**If Round 1 update distribution was "recent-concentrated", also ask:**

> "You said updates concentrate on recent data. Roughly how far back do late-arriving updates trickle in — hours, days, weeks, or longer?"

Cross-check tail vs retention (see warnings.md → UPDATE_TAIL_VS_RETENTION).

### Q2.4 — Partitioning

Lead with explanation, then delegation-friendly ask:

> "Partitioning splits the table into subdirectories keyed by a column value. It gives Hudi two things:
> - **Fast reads for queries that filter on the partition column.** Instead of scanning the whole table, Hudi reads only the matching partitions. On multi-TB tables this can be orders of magnitude faster.
> - **Manageable file layout at scale.** As tables grow into TBs, unpartitioned tables produce very large or very many files that stress writers, readers, and downstream jobs like silver bootstrap.
>
> Do you have a partition column in mind (a date-like column, a business dimension), or would you like to skip partitioning for now?"

Follow-up phrasing depends on answer:

**User names a column:**
> "Is it a date-like column, or a business dimension? Roughly how many distinct values will it have across the retention window?"

Rule engine computes projected partition count and runs Vice 1/2/3 checks. See warnings.md.

**User says "you pick":**
Architect defaults to date-based (ingestion time for raw/immutable; event time only if user has strong event-time semantic downstream). Daily granularity. Report the choice with rationale.

**User says "no partitioning / unpartitioned":**
> "That's fine as long as the table stays manageable. Two concerns to flag now, based on what you've told me:
> - If your projected table size at 2-3 years crosses ~500GB and this table feeds downstream silver pipelines that will ever need to bootstrap from it, unpartitioned makes that bootstrap job much harder (small files, whole-table snapshot reads in one commit).
> - If this is a raw layer that only feeds append-only silver consumers who are always caught up, unpartitioned is more forgiving.
>
> Given your workload, my read is: [surface recommendation]. Proceed unpartitioned, or reconsider?"

### Q2.5 — Partition-column stability (PARTITIONED ONLY)

> "Can the partition column's value change for a given record across updates, or is it stable once inserted? Does the source data always contain the correct partition value for each record at update time?"

- Stable + source has correct value → partition-scoped index viable.
- Value can change OR source doesn't always have partition value → global-scope index required.

### Q2.6 — Record key (concept-anchored)

> "Hudi is rooted in database design, so it treats a **primary key** — Hudi calls it a record key — as first-class. A record key is a column (or combination of columns) that uniquely identifies a record.
>
> The record key powers a lot of what Hudi does: upserts, index lookups, compaction, concurrency control, dedup, change tracking.
>
> When a new record arrives with a key that already exists, Hudi updates the existing record. Different key → new record.
>
> Which column (or columns) uniquely identify a record in your workload?"

**For IMMUTABLE workloads, add auto-gen alternative:**

> "For append-only workloads with no downstream identity requirements, Hudi can also auto-generate keys efficiently (roughly 3-10x lighter than UUIDs).
>
> - **Auto-generate keys for me** — Hudi creates keys internally, no key config needed.
> - **I have a natural key column** — event_id, session_id, transaction_id, etc.
>
> Which fits?"

Auto-gen incompatible with disabling meta fields (see Q2.7).

Answer routing:
- Single field → SimpleKeyGenerator.
- Multi-field → ComplexKeyGenerator.
- Auto-gen (immutable only) → no key generator, no `recordkey.field` config.

### Q2.6b — Ordering / precombine field (MUTABLE ONLY)

Fires immediately after Q2.6 for any mutable workload. No default exists — this must be asked.

> "When two updates for the same record land in the same batch, Hudi needs to know which one wins. That's usually a timestamp like `updated_at`, or a version number from the source.
>
> Which column should decide precedence?"

Conversational — needs a free-text column name.

Emits `hoodie.table.ordering.fields`. Without it, precedence among same-key records in a batch is undefined, and the config bundle cannot be completed for a mutable table. For CDC sources this is typically the source's commit timestamp or log sequence number.

### Q2.7 — Meta-fields prompt (IMMUTABLE + record size below 1KB ONLY)

Skip entirely for mutable workloads (silently keep all meta fields).

Skip for immutable + record size >1KB (rounding error).

For immutable + ≤1KB:

> "Hudi adds 5 meta fields to every record. They enable incremental queries, uniqueness checks, and other features. Meta-fields add roughly 50-100 bytes per record — on records above ~1KB they're rounding error, but on smaller records they can add meaningful storage overhead.
>
> Do you know roughly how big each of your records is?
> - **Above ~1KB / not sure** → keep all meta fields (default, safest).
> - **Around 200B–1KB** → the saving from disabling is small but real. Only worth it if you're certain no consumer will ever need incremental reads.
> - **Below ~200B** → meta-fields overhead is significant (25%+ of record size). Two options:
>   - Keep all meta fields — safest, all features work.
>   - Disable meta fields entirely — saves ~50-100 bytes/record, but **incremental queries stop working** and CDC is unavailable.
>
> Storage saving at scale: at 10B records × 200B, disabling saves ~50-100GB total. Worth it only for append-only batch data with no incremental consumers, now or later.
>
> Which trade fits?"

**`hoodie.populate.meta.fields` is a boolean — all or nothing.** There is no selective / commit-time-only mode at 1.2.0; partial inclusion of meta fields in the schema is explicitly not supported. Do not offer one.

**Do not soften what disabling costs.** Hudi's own config documentation states: "When disabled, no meta fields are populated and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing." Earlier revisions of this rubric described incremental as merely "slower, falls back to snapshot-read + filter" — that is wrong. Treat incremental as unavailable.

**Gating rule:** if the workload has *any* incremental or streaming consumer, **don't ask this question at all.** At 1.2.0 the answer is determined — keep all meta fields — because incremental queries need `_hoodie_commit_time` and the config is all-or-nothing. Asking implies a choice the user doesn't have. State the decision and note the storage cost in the ADR.

Selective population (`hoodie.meta.fields.mode`) would make this a real tradeoff for small-record tables, but it targets 1.3.0 and hasn't merged — see decision-tables.md → meta-fields.

### Q2.8 — Small-files posture (IMMUTABLE ONLY — after partitioning resolved)

Skip for mutable (inline small-file handling via `insert`/`upsert` is Hudi default; no user question).

> "How should Hudi handle small files?
>
> - **Don't worry about small files** — fastest ingest. Uses `bulk_insert`, no clustering. Files stay whatever size the batch produces.
> - **Handle small files without slowing ingestion** — `bulk_insert` + async clustering. Writer stays fast; a separate service compacts small files in the background.
> - **Keep files well-sized even if ingestion takes longer** — `insert` inline. Slight per-batch latency cost, no separate clustering service to run."

Recommendation prose adapts based on:
- Partition cardinality (low-card date vs high-card business dim).
- Future-consumers axis (closed universe = fixed consumers; open universe = future silver pipelines may bootstrap-read).

See decision-tables.md → small-files-posture.

### Q2.9 — Pipeline shape

> "How is the pipeline expressed?
>
> - **Config-driven ingestion** — property-file-driven source → transform → Hudi wiring. Most common shape.
> - **Custom application code** — Scala/Java/Python that reads sources, transforms with DataFrame ops, writes to Hudi. Includes streaming-source consumers using forEachBatch.
> - **SQL-centric** — you write INSERT/MERGE/UPDATE/DELETE statements.
> - **True streaming-sink writes** — you use `writeStream.format('hudi')` directly, and you need stateful streaming primitives (windows, watermarks, joins across streams)."

**Structured Streaming disambiguation follow-up (if user picks "streaming"):**

> "Do you use `writeStream.format('hudi')` as the actual sink, or do you consume from a stream and call `.write.format('hudi')` inside a forEachBatch callback?"

- forEachBatch → route to DataSource path.
- writeStream sink → true Structured Streaming. Ask about stateful primitives:

> "Do you need stateful stream operations (windows, watermarks, joins with another stream)?"

- Yes → Structured Streaming.
- No → nudge toward HoodieStreamer.

**Kafka source override note:** If source is Kafka, default is HoodieStreamer regardless of user's pipeline_shape answer — surface HoodieStreamer's Kafka-specific advantages (schema registry, exactly-once, error routing, in-process async services). Only route to DataSource for Kafka when the pipeline has multi-source complexity, ML DataFrame-native library work, or one-off backfills.

**First-time user nudge:** if experience is EXPLORATION/PROTOTYPING and pipeline_shape is SQL-centric or streaming-with-primitives:

> "For a first Hudi table, HoodieStreamer or Spark DataSource are the two most-deployed paths. Spark SQL / Structured Streaming work but have smaller production footprints. Do you want to reconsider, or proceed with the specialized writer?"

## §8.3 — Derived-fact synthesis checkpoint

**Fires between Round 2 and Round 3.** Echo computed facts back to user:

```
Confirmed workload: <categorical facts from Rounds 1-2>

Derived:
- Steady-state table size: <computed>
- Projected partition count: <computed>
- Files/day at target file size: <computed>
- Small-file risk: <assessment>

Tensions surfaced (if any):
- <update-tail vs retention window>
- <partition-column stability vs cross-partition update>
- <three-concurrent-services warning>
- <table-type refinement, only if the writer was undetermined until Q2.9 and now unlocks free async>

Please confirm before I generate the full config.
```

User confirms before Round 3 fires.

**Refinement moment (only when the writer wasn't known earlier):** if writer selection at Q2.9 landed on HoodieStreamer continuous mode AND table type is MOR, async compaction becomes free regardless of experience level. State it as a bonus upgrade at the checkpoint.

**This is not the primary place the upgrade fires.** When the source is Kafka the writer is derived in Round 1, so the upgrade applies there and the upgraded table type should be stated immediately — see decision-tables.md → table type. Waiting for this checkpoint would hide it from PROTOTYPING and PRODUCTIONIZING_INITIAL users, since §8.3 only runs at PRODUCTION_AT_SCALE.

Only reach this moment when the writer was genuinely undetermined until Q2.9.

## Round 3 — Scale, concurrency, index (PRODUCTION_AT_SCALE ONLY)

### Q3.1 — Writers

> "Does anything else ever write this table — another pipeline, a backfill job, a GDPR/cleanup job, a standalone compactor? Even occasionally counts."

**Single writer** → emit `hoodie.write.concurrency.mode=SINGLE_WRITER`.

**User declares any second writer** → `SINGLE_WRITER` is unsafe: two OCC-less processes writing one table is the silent corruption path named in config-templates.md → Concurrency. Do **not** proceed assuming single. Instead:

- Emit the OCC skeleton (`OPTIMISTIC_CONCURRENCY_CONTROL` + `hoodie.write.lock.provider=<lock provider class>` + `hoodie.clean.failed.writes.policy=LAZY`), with the identical-in-every-writing-job requirement stated.
- Record a **blocking open question** in ADR §13: a lock provider must be chosen and configured in every writing job before go-live. Provider selection is V2+ scope — point to the Hudi concurrency-control docs.
- Record the declared writers in ADR §2 Confirmed Facts. A declared multi-writer deployment is a confirmed fact, never an "assumption."

Full multi-writer rubric (provider choice, OCC vs NBCC, conflict resolution) remains deferred — V1 owes the user the requirement and a bundle that is safe as emitted, not the decision tree.

### Q3.1b — Record count and growth (fires when the derived index is RLI)

Required before emitting RLI config. The file-group count is durable at index initialization — see decision-tables.md → RLI file-group sizing.

> "Roughly how many records does the table hold today, and what do you expect in 3-4 years?"

Offer a closed-set for each (under 100M / 100M-1B / 1B-10B / over 10B), with an "I don't know" path.

**If the user can project:** compute the file-group count from the sizing formula and emit `min == max`. Use table-wide count for global RLI, per-partition count for partitioned RLI.

**If the user cannot project:** recommend landing the first commit / bulk load with RLI disabled, then enabling it async via `HoodieIndexer` so Hudi's estimator sees real volume. State plainly that this fixes the bootstrap problem but not the growth problem — the estimator still applies growth factor 2.0 to whatever exists at initialization. Record a measurable revisit condition in the ADR.

Also ask, or state as an assumption, whether record keys are UUID-shaped: the 50-bytes-per-RLI-record constant assumes they are, and long keys inflate it.

### Q3.2 — Index

Derived from decision-tables.md → index. Architect presents recommendation with rationale from the decision table:

> "For your workload — <mutable/immutable, partitioned/unpartitioned, partition-stable, projected size, key characteristics> — I recommend `<index type>` because <one reason>.
>
> [Tradeoff table showing options]
>
> Confirm, or override?"

### Q3.3 — Derived services confirmation

Architect states what services will run:
- Cleaner + archival: inline autopilot, config emitted per retention answer.
- Compaction: derived from writer + table type (see decision-tables.md → compaction).
- Clustering: off by default; only surface as recommendation if immutable + small-files posture (b).

Ask user to confirm.

## Final revisit gate

**Fires once, immediately before ADR generation, at every tier.** Not per round.

Show every answer collected across all rounds as one numbered table, then:

> "Ready to generate the design, or change an answer first?"
> - `Generate the design`
> - `Change an answer`

Closed-set → widget. If the user amends, take the change and re-show the table before proceeding.

Per-round answer echoes remain **informational** — they do not block. The §8.3 checkpoint is a separate thing with a different job (confirming *derived* facts, not raw answers); don't collapse the two or a PRODUCTION_AT_SCALE user gets two consecutive confirmations.

**If any warning fired during the flow and the user chose to proceed past it, restate that in the review table** — e.g. "partition column: `customer_id` (high-cardinality trap acknowledged)". The gate is the last opportunity to reverse one.

## Retired from V1 dialogue

- Ops-appetite question (three-way hands-off/standard/tuned) — retired. Experience question replaced it.
- Visibility-interval as first-class Round 1 question — retired. Moves to ADR-level fact (sizing note).
- **Compaction** cadence question at design time — retired. Emitted silently at default. (Distinct from **ingestion** cadence, which Q1.3 now asks at every tier — the two are different facts that share a word.)
- Cleaner cadence question — retired. Autopilot.
- Col-stats decision at design time — retired. Operations Agent territory.
- Clustering push — clustering is off by default; only surfaces on strong workload signals.
- Point-lookup column follow-up (record key vs other column) — retired. Secondary index becomes ADR flag.

## Question ordering rationale

Reads first (Q2.1) → physical layout (Q2.2-2.5) → identity (Q2.6-2.7) → writer (Q2.9). Writer selection benefits from knowing the workload profile, so it comes last.

Q2.7 meta-fields prompt fires immediately after Q2.6 record key when applicable — they're coupled by the auto-gen / virtual-keys mutual exclusion.

Q2.8 small-files posture fires only for immutable, only after partitioning is resolved (recommendation depends on partition cardinality).
