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
# Rule engine warnings

Named warnings that fire on specific workload signals. Consult when driving the flow — surface warnings at the right point in the conversation, not all at once at the end.

Format for each: name, trigger condition, message template, when it fires.

## Partitioning warnings

### VICE_1_PARTITION_MISALIGNMENT

**Triggered when:** user chooses a partition column that doesn't match consumer read filters.

**Message:**
> "You partitioned by `<column>`, but you said consumers filter primarily on `<other_column>`. Partition pruning won't help these queries. Options: (i) repartition by the filtered column, (ii) enable column stats + data skipping (deferred to Operations Agent), (iii) keep the current partitioning if there are other filters you didn't mention."

**When fires:** immediately after user names a partition column in Q2.4, once read pattern (Q2.1) is known.

**When it can't fire (PROTOTYPING — no Q2.1).** Apply the default and state the limitation. Be explicit that partitioning — including the decision to stay unpartitioned — is fixed at table creation: changing it means a new table and a full rewrite, with no ALTER path. Offer to ask the read-pattern questions if the user wants to partition now. Carry the disclaimer into the ADR as a revisit condition tied to projected size.

### VICE_2_OVERLY_GRANULAR_PARTITIONING

**Triggered when:** `partition_size = table_size / partition_count < 100MB` at projected steady state.

**Message:**
> "At ~`<partition_size>`MB per partition, average partition well below healthy 100MB+ range. Options: coarsen granularity, drop composite dimension, or drop partitioning entirely if total table stays under ~500GB."

**When fires:** after Q2.4 + Q2.2 (table size known + partition count projected).

### VICE_3_PARTITION_EVOLUTION

**Triggered when:** user mentions "start hourly, coarsen later" or similar evolution plan.

**Message:**
> "Don't. Queries spanning evolution boundary hit mixed file sizes and see straggler tasks. Pick the coarsest scheme that meets your freshness needs upfront, even if current volume seems to justify finer grain."

**When fires:** during Q2.4 partitioning discussion.

### HIGH_CARDINALITY_PARTITION_TRAP

**Triggered when:** partition col looks like business ID (column name contains `_id`, `_uuid`, or matches known-ID patterns).

**Message:**
> "You're proposing `<column>` as the partition column. This looks like a business ID with potentially high cardinality. Concerns:
> - At 100K+ partitions, Hudi's in-memory file system view grows large enough to cause elevated read and write latencies due to memory pressure.
> - Even if queries filter on `<column>` (pruning to one partition), writer-side and reader-side metadata handling is expensive.
> - Composite `<business_id>/<date>` patterns compound this rapidly.
>
> Alternatives:
> - Partition by date and rely on data-skipping / secondary index for `<column>`-based reads.
> - Bucket index on `<column>` if lookups on this column are dominant — provides direct file-group routing without partition-count blowup.
> - Keep the partition choice if cardinality stays bounded and you have strong reason — validate against the projected-count guardrails."

**When fires:** immediately after user names a partition column in Q2.4.

### PROJECTED_PARTITION_COUNT_YELLOW

**Triggered when:** projected partition count in 10K-50K band.

**Message:**
> "Warning: at ~`<count>` partitions, MDT `files` partition grows large, listing operations get expensive. Consider coarsening granularity or dropping composite dimension."

**When fires:** at the §8.3 derived-fact synthesis checkpoint (PRODUCTION_AT_SCALE). At PRODUCTIONIZING_INITIAL §8.3 never runs — fire during the Q2.4 partitioning discussion, as soon as table size (Q2.2) and the partition scheme make the projection computable.

### PROJECTED_PARTITION_COUNT_RED

**Triggered when:** projected partition count > 50K.

**Message:**
> "Reject: ~`<count>` partitions at projected steady state. This is a known failure mode — 270K partitions with 3MB per partition is documented as disaster territory. Coarsen granularity or drop composite dimension before proceeding."

**When fires:** during Q2.4 partitioning discussion. Blocks proceeding until user changes partitioning.

## Design tension warnings

### WORKLOAD_EXPERIENCE_TENSION

**Triggered when:** mutable + uniform-distribution updates + large projected table AND user picks fire-and-forget experience.

**Message:**
> "Your workload signals point toward MOR (mutable + uniform updates at scale = high write amp for COW), but you picked fire-and-forget which typically means COW. Three reconciliations:
> - (a) Accept COW; ADR flags concrete revisit conditions if write amp materializes.
> - (b) Step up to MOR with inline compaction. Slightly more per-batch latency, no separate service to deploy.
> - (c) Keep the workload smaller and rely on the Operations Agent to flag if COW hits a wall.
>
> Which matches your priorities?"

**When fires:** after Q1.6 (experience) when Q1.4 + Q1.5 + Q2.2 signals are available. Ask user to reconcile.

**When it can't fire (PROTOTYPING — no Q2.2).** Don't derive table type silently. Default to COW, state the COW/MOR tradeoff plainly, name the condition that would change the answer (large table + uniform updates), flag that table type is durable, and offer the override. The user should leave knowing they made a choice, not knowing a default was applied. Carry the call-out into the ADR's key-decisions section.

### UPDATE_TAIL_VS_RETENTION

**Triggered when:** update-tail estimate (from Q2.3 follow-up) > retention window (Q2.3 main answer).

**Message:**
> "Your update pattern has a tail extending ~`<tail>`, but your retention window is `<retention>`. Late-arriving updates land correctly on current records — Hudi handles that fine. But downstream consumers with incremental checkpoints older than `<retention>` cannot reconcile against intermediate historical states.
>
> Options:
> - (a) Widen retention (if commit cadence allows).
> - (b) Keep retention and expect consumers to check in more frequently.
> - (c) Reduce commit cadence to allow wider safe retention."

**When fires:** at the §8.3 derived-fact synthesis checkpoint (PRODUCTION_AT_SCALE). At PRODUCTIONIZING_INITIAL §8.3 never runs — fire at the end of Round 2, before the final revisit gate, as soon as both inputs (tail estimate, retention window) exist. A warning whose only firing point is a checkpoint the tier skips is a warning that never fires.

## Config trap warnings

### RETENTION_CLAMP

**Triggered when:** `user_desired_lookback > safe_max_retention` (computed from commit cadence).

**Message:**
> "You asked for `<desired>` of lookback, but at `<commit_cadence>`-minute commit cadence that would push the active timeline past its healthy range and degrade latency. Clamping to `<safe_max>`. To widen retention, either reduce commit cadence (e.g., 15-min instead of 5-min → ~5 days safe) or accept the shorter window."

**When fires:** during Q2.3 retention question, immediately when user's desired value exceeds safe max.

**If the user responds by changing commit cadence, recompute the safe max against the new cadence and restate it before moving on.** The warning offers slowing the cadence as a remedy, so the Architect must follow through — don't leave the original clamped figure standing after the input it derived from has changed.

### SUB_5_MIN_CADENCE_UNSTABLE

**Triggered when:** commit cadence < 5 min AND computed safe retention < 1 day.

**Message:**
> "At `<cadence>`-minute cadence, safe retention drops below 1 day (~`<computed>`). As a best practice, first stabilize a 5-minute cadence pipeline before attempting sub-5-minute commits. If sub-minute is a hard requirement, expect a very tight retention window and plan operational monitoring accordingly."

**When fires:** during Q2.3 retention question. Not a hard block.

### COMPACTION_TARGET_IO_TRAP

**Triggered when:** MOR + projected table size ≥ 1TB.

**Message:**
> "`hoodie.compaction.target.io` is a per-round IO ceiling, defaulting to 500GB. It exists to bound how long inline compaction can block a commit. At your scale that ceiling throttles compaction below the rate log files accumulate — file groups never catch up, log files grow without bound, and snapshot read latency degrades. Setting it effectively uncapped and letting the compaction strategy decide what to compact."

**Config note:** the value is in **megabytes, not bytes** (default `512000` = 500GB). Emit a large finite MB value such as `104857600` (100TB). Do not derive a point value from table size — that just re-creates the same ceiling at a higher threshold as the table grows.

**When fires:** after table type + projected size are known. Add to ADR as explicit tuning knob.

### COMPACTOR_CONCURRENCY_REQUIRED

**Triggered when:** the design lands on a standalone `HoodieCompactor` job (MOR + a writer that can't run async compaction in-process — Spark DataSource, Spark SQL).

**Message:**
> "A separate compactor job means two processes write to this table, so `SINGLE_WRITER` is unsafe. Both jobs need `OPTIMISTIC_CONCURRENCY_CONTROL` and the same lock provider, configured identically on both sides. On AWS that's DynamoDB, and Hudi creates the lock table for you; if you already run ZooKeeper or a Hive Metastore, use that. If setting up locking isn't practical at all, use inline compaction instead and accept the per-batch latency."

**When fires:** immediately when the standalone-compactor path is selected or recommended. Must appear in the ADR's operational playbook, not only in dialogue.

Emit the concrete OCC block from config-templates.md → Concurrency for the deployment, in **both** jobs. Provider selection is decision-tables.md → Concurrency Step 3.

### NBCC_INELIGIBLE

**Triggered when:** the user asks for `NON_BLOCKING_CONCURRENCY_CONTROL` but the derived design fails the eligibility gate — table is not MOR, index is not BUCKET, table version < 8, or clustering is required.

**Message:**
> "NBCC needs a MOR table with simple bucket index, on table version 8 or later — Hudi's config validation rejects any other combination outright, so this would fail at write time rather than degrade. Your design is `<table_type>` + `<index>`. I'd recommend OCC here, which has no such restriction. I'm deliberately not reshaping the table to fit NBCC: bucket count is fixed at table creation, while the concurrency mode can be changed on a live table — trading a reversible decision for an irreversible one is the wrong way round."

**When fires:** the moment NBCC is requested and the gate fails. Blocks the NBCC request, not the session — emit OCC and record the reason in the ADR.

### NBCC_CLUSTERING_CONFLICT

**Triggered when:** NBCC is otherwise eligible (MOR + BUCKET) AND clustering is wanted.

**Message:**
> "NBCC doesn't support clustering yet — the non-blocking path resolves conflicts between ingestion and compaction, but not with a clustering writer. Since clustering is part of this design, OCC is the right mode. If clustering is optional here, NBCC becomes available again."

**When fires:** at the concurrency derivation, before emitting a mode. Not a hard block — the user may drop clustering.

### STORAGE_LOCK_MATURITY

**Triggered when:** `StorageBasedLockProvider` is selected — whether the Architect surfaced it as the only option without existing lock infrastructure, or the user asked for it by name.

**Message:**
> "One thing to know about this choice: the storage-based lock provider arrived in Hudi 1.0.2, which makes it years younger than the DynamoDB, ZooKeeper, and Hive Metastore providers. It isn't a thin wrapper either — it does lease renewal with a heartbeat and its own per-cloud lock clients, which is the kind of code whose edge cases surface under real contention and clock skew rather than in tests. There's no known defect here; it's an age argument. The zero-infrastructure story is genuinely the nicest of the options, so this is worth revisiting as it accumulates mileage. If you'd rather stay on well-trodden ground today, DynamoDB on AWS is the usual alternative and Hudi creates the lock table for you."

**When fires:** at provider selection, once. **Not a hard block** — it is a maturity judgement, not a defect, and the user may reasonably accept it. Record the choice and the caveat in the ADR so a later reviewer sees it was deliberate. Do not re-raise it after the user has decided.

### LOCK_PROVIDER_MISMATCH

**Triggered when:** an **explicit** lock provider is chosen over its implicit variant — `ZookeeperBasedLockProvider` (needs `base_path` + `lock_key`) or `DynamoDBBasedLockProvider` (needs `partition_key`).

**Message:**
> "This provider takes its lock identity from config rather than deriving it from the table path, so every writing job has to set the same values by hand. If two jobs disagree — a different `lock_key`, a different `partition_key`, even `s3a://` on one side and `s3://` on the other — each takes out its own lock, every writer succeeds, and the table corrupts with nothing in any log. The implicit variant (`<implicit class>`) derives the identity from `hoodie.base.path` and makes that failure impossible. Use it unless you specifically need several tables to share one lock, or must match an existing deployment's lock identity."

**When fires:** the moment an explicit provider is selected. Not a hard block — sharing a lock across tables is a legitimate reason — but the choice must land in the ADR with its rationale, and the pre-launch checklist must call for verifying the values match across every writing job.

### FILESYSTEM_LOCK_UNSAFE

**Triggered when:** `FileSystemBasedLockProvider` is selected and the table path is cloud storage (s3, s3a, gs, abfs, abfss, wasb, wasbs), or the deployment is production.

**Message:**
> "The filesystem lock provider isn't supported on cloud storage and isn't intended for production — it needs atomic filesystem semantics that object stores don't provide, so it can appear to work while granting the same lock twice. On cloud storage use `StorageBasedLockProvider` instead: same zero-infrastructure story, built for object-store semantics."

**When fires:** immediately on selection. **Hard block** for cloud storage — this one silently appears to work, which is worse than failing.

### OCC_INSERT_DUPLICATES

**Triggered when:** OCC is derived AND any writer uses `INSERT` or `BULK_INSERT` (rather than `UPSERT`).

**Message:**
> "One caveat specific to multi-writer: with concurrent `INSERT`/`BULK_INSERT`, the table can end up with duplicates **even with dedup enabled**. Dedup applies within a writer's own batch; OCC's conflict detection is at file-group granularity and doesn't catch two writers inserting the same key into different file groups. If these writers can produce overlapping keys, use `UPSERT`, or partition the key space so no two writers ever touch the same key."

**When fires:** at concurrency derivation, once the operation type per writer is known. Not a hard block — many multi-writer deployments have disjoint key spaces by construction — but it must be answered rather than assumed.

### PARTITION_EXTRACTOR_MISMATCH

**Triggered when:** catalog sync is enabled AND the key generator is `TimestampBasedKeyGenerator` producing a `yyyy/MM/dd`-style partition path.

**Message:**
> "Your partition paths look like `2026/08/25` — three path segments from one logical date. The default partition extractor reads those as three separate partition values, so the catalog gets a table partitioned by three columns instead of one date. Queries filtering on a date then don't prune correctly. Set `hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SinglePartPartitionValueExtractor`, which turns the path back into a single `2026-08-25` value."

**When fires:** at catalog-sync derivation, once the key generator and partition scheme are known. Not a hard block, but it must reach the emitted config — this is a sync that *succeeds* and then produces a table nobody can query correctly, which is worse than a failure.

### GLUE_SYNC_VERSION_CHURN

**Triggered when:** the Glue sync tool is selected AND commit cadence is hourly or faster.

**Message:**
> "Glue sync writes a new catalog version on every commit by default. At your cadence that's roughly `<commits/day>` versions a day for a table whose schema mostly isn't changing. Setting `hoodie.datasource.meta_sync.condition.sync=true` syncs only when the schema or partitions actually change, which is what you want for a steady pipeline."

**When fires:** immediately on selecting Glue, with the commits-per-day figure computed from the answered cadence rather than left abstract. Not a hard block.

### CATALOG_SYNC_SILENT_STALENESS

**Triggered when:** any catalog sync is enabled.

**Message:**
> "One operational note: a sync failure doesn't fail the write. The commit succeeds and the catalog quietly falls behind, so readers see a stale schema — or miss new partitions — with nothing in the writer's logs pointing at it. Worth an alert on sync errors, and worth knowing that 'the table looks wrong in Athena' usually means the catalog, not the data."

**When fires:** once, at catalog-sync derivation. Informational — it belongs in the ADR's operational playbook rather than the dialogue's critical path.

### BIGQUERY_MOR_READ_OPTIMIZED

**Triggered when:** BigQuery sync is selected AND the table type is MERGE_ON_READ.

**Message:**
> "BigQuery sync will accept this MOR table, but its manifest lists base files only — so BigQuery won't merge your log files, and queries there see read-optimized data rather than a current snapshot. Updates land in logs and only become visible to BigQuery after compaction. If BigQuery consumers need current data, either shorten the compaction interval or reconsider MOR for this table. Spark and Flink readers are unaffected."

**When fires:** at catalog-sync derivation. Not a hard block — read-optimized is a legitimate choice — but it must be an explicit one, since nothing errors and the data merely looks stale.

### CLOUD_BUNDLE_REQUIRED

**Triggered when:** the design names a class outside the Spark and utilities bundles — the DynamoDB lock provider, the storage-based lock provider, or a Glue / BigQuery / DataHub sync tool.

**Message:**
> "`<class>` lives in its own module, so `<bundle>` has to be on the classpath of every job that writes this table — in `--packages` for `spark-submit`, `spark-shell`, or `spark-sql`, or as a compile-time dependency for a packaged application. The config is correct without it, which is exactly the problem: the job submits fine, starts, and then fails at the first commit with a ClassNotFoundException on a class your properties file names."

**When fires:** the moment such a class is derived — at lock-provider selection and at sync-tool selection, whichever comes first. Fire it **once per bundle, not once per decision**: a design with DynamoDB locking *and* Glue sync needs `hudi-aws-bundle` one time, and saying it twice invites the reader to think they need two jars.

Must appear in **the emitted submit command**, not only in dialogue — see config-templates.md → "Cloud bundles are load-bearing". Also in the ADR's pre-launch checklist. Bundle mapping is in that same section.

### XTABLE_MOR_UNSUPPORTED

**Triggered when:** the user raises Apache XTable, or names an Iceberg or Delta consumer, AND the derived table type is MERGE_ON_READ.

**Message:**
> "One conflict worth checking before you commit to this: XTable's FAQ lists MOR tables as unsupported — Copy-on-Write only, for both Hudi and Iceberg. Your design is MOR. I'd verify that against their current docs rather than take my word for it, since XTable is incubating and its support matrix moves. If it still holds, table type is fixed at creation, so it's a decision for now rather than later: keep MOR and read this table only through Hudi, or switch to CoW to keep Iceberg and Delta interoperability open. The MOR case here was `<the reason MOR was derived>` — worth weighing that against how much cross-format access matters."

**When fires:** the moment XTable or a cross-format consumer comes up, if the design is MOR. **Do not silently reshape the table type** — surface the tension with both options, as with any durable decision. Record whichever way it goes in the ADR, since a future reader will want to know the interop question was considered rather than missed.

### THREE_CONCURRENT_SERVICES

**Triggered when:** writer is HoodieStreamer continuous AND table type is MOR AND clustering enabled.

**Message:**
> "Three concurrent services in one Spark job: ingestion + async compaction + async clustering. Default 1:1:1 resource split works for balanced workloads. If ingestion falls behind, shift weight toward `--delta-sync-scheduling-weight`. If compaction backlog grows, shift toward `--compact-scheduling-weight`. Operations Agent territory."

**When fires:** at the §8.3 derived-fact synthesis checkpoint (PRODUCTION_AT_SCALE). At tiers where §8.3 doesn't run, fire at the moment the third concurrent service is enabled.

### WRITER_COMPACTION_MISMATCH

**Triggered when:** user wants MOR async compaction AND writer that doesn't support async in-process (DataSource or SQL).

**Message:**
> "Your writer choice (`<writer>`) means async compaction requires deploying a separate `HoodieCompactor` job — an advanced deployment pattern. Alternative: switch writer to HoodieStreamer continuous mode, and get async compaction for free in-process. Which fits?"

**When fires:** after Q2.9 (pipeline shape → writer) is known, only if MOR + experienced signals were captured earlier. For in-job DataFrame sources the writer is known in Round 1 — fire there instead.

**In-job DataFrame exception:** when the source is a DataFrame produced inside the user's own Spark job, the "switch to HoodieStreamer continuous" alternative does not exist — HoodieStreamer polls an external source, and there isn't one (question-flow.md Q1.2). Drop that clause and offer only the real choices: inline compaction, or the standalone `HoodieCompactor` with its concurrency requirements.

## Growing set

More warnings will emerge during Path A implementation. When you encounter a workload where the current warnings don't fire but something feels wrong, flag it explicitly — that's the signal for a new warning to add.
