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
# Config templates

Grouped `hoodie.*` properties emitted per decision. Consult when generating the final config bundle.

## Grouping (per §9.3 of the proposal)

1. **Durable table properties** — set at creation, cannot change without rewrite.
2. **Writer properties** — writer-side runtime config.
3. **Reader properties** — reader-side config (per query engine — different keys per engine).
4. **Platform-managed properties** — MDT, target Hudi version, other fixed platform standards.
5. **Workload-dependent tuning variables** — cadences, target sizes. Not shuffle parallelism: Hudi derives that from the incoming DataFrame's partition count, so setting it explicitly overrides a value that is usually already correct.

## Durable table properties

### Table type
```
hoodie.table.type=COPY_ON_WRITE   # or MERGE_ON_READ
```

### Record key
For SimpleKeyGenerator (single field):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator
```

For ComplexKeyGenerator (composite):
```
hoodie.datasource.write.recordkey.field=<col1>,<col2>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.ComplexKeyGenerator
```

For auto-gen (immutable only) — omit `recordkey.field` and keygenerator entirely.

For TimestampBasedKeyGenerator (timestamp-derived partition):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.TimestampBasedKeyGenerator
hoodie.keygen.timebased.timestamp.type=<UNIX_TIMESTAMP|DATE_STRING|MIXED|EPOCHMILLISECONDS|SCALAR>
hoodie.keygen.timebased.output.dateformat=<format>
hoodie.keygen.timebased.timezone=UTC
```

For CustomKeyGenerator (mixed):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.CustomKeyGenerator
```
Additional config for CustomKeyGenerator partition-path spec: `<field1:type1,field2:type2>` where type is SIMPLE or TIMESTAMP.

For NonpartitionedKeyGenerator (unpartitioned):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
hoodie.datasource.write.partitionpath.field=
```

### Partition path
```
hoodie.datasource.write.partitionpath.field=<column>
# Empty string for unpartitioned
```

For Hive-style partitioning (column=value folder naming):
```
hoodie.datasource.write.hive_style_partitioning=true
```

### Meta fields

Boolean — all meta fields or none. **There is no selective / commit-time-only mode at 1.2.0.**

For keep all (default):
```
hoodie.populate.meta.fields=true
```

For disable entirely — append-only batch data with no incremental or CDC consumers, ever:
```
hoodie.populate.meta.fields=false
```

Disabling makes incremental queries **non-functional** (not merely slower) and CDC unavailable. Durable: changing it later requires a table rewrite.

**If the workload has any incremental or streaming consumer, emit `true` and don't present a choice** — at 1.2.0 there is no way to keep incremental queries while dropping meta fields.

Selective population via `hoodie.meta.fields.mode` (apache/hudi#19205) targets 1.3.0 and is not merged. When it ships it will be CoW-only, Spark-only, and immutable at table creation.

## Writer properties

### Operation (per §7.7.5 mapping)
```
hoodie.datasource.write.operation=<upsert|insert|bulk_insert|delete|insert_overwrite|insert_overwrite_table>
```

### Ordering / precombine
```
hoodie.table.ordering.fields=<column>
# Used for resolving record precedence when multiple versions of a key exist in a batch
```

### Small-file handling
Default inline for insert/upsert (mutable). For immutable + posture (a):
```
hoodie.datasource.write.operation=bulk_insert
hoodie.parquet.small.file.limit=0   # disable small-file handling
```

For immutable + posture (b):
```
hoodie.datasource.write.operation=bulk_insert
# Add async clustering — see clustering section
```

For immutable + posture (c):
```
hoodie.datasource.write.operation=insert
hoodie.parquet.small.file.limit=104857600   # 100MB, default
hoodie.parquet.max.file.size=125829120       # 120MB, default
```

### Bulk-insert sort mode
```
hoodie.bulkinsert.sort.mode=<NONE|GLOBAL_SORT|PARTITION_SORT|PARTITION_PATH_REPARTITION|PARTITION_PATH_REPARTITION_AND_SORT>
```

## Reader properties (per engine)

**Reader-side MDT is per-engine — do not assume readers inherit writer MDT config.**

Spark:
```
hoodie.metadata.enable=true
hoodie.enable.data.skipping=true
```

Flink:
```
metadata.enabled=true
read.data.skipping.enabled=true
```

Presto:
```
hudi.metadata-table-enabled=true
```

Athena:
```
hudi.metadata-listing-enabled=true
```

Emit reader config per query engine named in the workload.

## Platform-managed properties

Always emit, don't ask:
```
hoodie.metadata.enable=true                              # MDT on
```

**Do NOT emit these — they already carry the desired default, so setting them is noise:**
- `hoodie.metadata.index.column.stats.enable=false` — col stats (and partition stats, which is coupled to the same knob) are off by default. The design guidance that col stats is Operations Agent territory still stands; it just doesn't need a config line.
- `hoodie.metadata.index.bloom.filter.enable=false` — already false by default. Bloom-via-MDT remains experimental at 1.2.0 and should not be recommended at design time.

General rule: emit config that *changes* behavior. A bundle full of redundant defaults obscures the handful of properties that actually encode the design.

Target Hudi 1.2.0 (implied by dependency version, not a runtime config).

## Source properties (HoodieStreamer)

Derived from source + record format (question-flow.md Q1.2b).

**The source class and schema provider are CLI flags, not properties.** They travel on the submit command as `--source-class` and `--schemaprovider-class` (see Sample submit commands). There are **no** `hoodie.streamer.source.class` / `hoodie.streamer.schemaprovider.class` properties — such lines are silently ignored, and a job that relies on them falls back to HoodieStreamer's default source class and reads the wrong source type. Each block below lists the real properties and notes the matching CLI flags in a comment.

### Kafka + Avro + schema registry
```
# CLI flags: --source-class org.apache.hudi.utilities.sources.AvroKafkaSource
#            --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider
hoodie.streamer.schemaprovider.registry.url=<SCHEMA_REGISTRY_URL>/subjects/<TOPIC>-value/versions/latest
hoodie.streamer.source.kafka.topic=<TOPIC>
bootstrap.servers=<KAFKA_BOOTSTRAP>
auto.offset.reset=latest
```

### Kafka + Avro + schema file
```
# CLI flags: --source-class org.apache.hudi.utilities.sources.AvroKafkaSource
#            --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
hoodie.streamer.schemaprovider.source.schema.file=<PATH>/source.avsc
hoodie.streamer.schemaprovider.target.schema.file=<PATH>/target.avsc
hoodie.streamer.source.kafka.topic=<TOPIC>
bootstrap.servers=<KAFKA_BOOTSTRAP>
auto.offset.reset=latest
```

### Kafka + JSON
```
# CLI flag: --source-class org.apache.hudi.utilities.sources.JsonKafkaSource
hoodie.streamer.source.kafka.topic=<TOPIC>
bootstrap.servers=<KAFKA_BOOTSTRAP>
auto.offset.reset=latest
```
Schema provider optional — pass `--schemaprovider-class` for `FilebasedSchemaProvider` or `SchemaRegistryProvider` if the schema is managed rather than inferred.

### Kafka + Protobuf
```
# CLI flag: --source-class org.apache.hudi.utilities.sources.ProtoKafkaSource
hoodie.streamer.source.kafka.topic=<TOPIC>
hoodie.streamer.source.kafka.proto.value.deserializer.class=<PROTO_CLASS>
bootstrap.servers=<KAFKA_BOOTSTRAP>
auto.offset.reset=latest
```

### Schema providers

Schema providers are **optional and orthogonal to source type** — any of them can pair with any HoodieStreamer source. Many sources infer their schema, and simple pipelines that don't expect schema evolution often set no provider at all. That's a legitimate configuration, not an omission.

**Kafka is the practical exception.** Kafka producers and consumers already need schema coordination to interoperate, so a registry is usually standing infrastructure before Hudi enters the picture. For Kafka sources, assume a schema provider is present and ask *which* one rather than *whether* — you're discovering existing infrastructure, not proposing new. For file, JDBC, and other sources, treat it as a genuine yes/no.

Emit one when the user has an authoritative schema source or expects evolution. Pick by where the schema lives:

| Schema lives in | Provider class |
|---|---|
| Confluent / compatible registry | `SchemaRegistryProvider` |
| `.avsc` files you manage | `FilebasedSchemaProvider` |
| A Hive metastore table | `HiveSchemaProvider` |
| The upstream JDBC table itself | `JdbcbasedSchemaProvider` |
| A proto class on the classpath | `ProtoClassBasedSchemaProvider` |

All property keys use the `hoodie.streamer.schemaprovider.` prefix. **The provider class itself is the `--schemaprovider-class` CLI flag, not a property.**

**File-based** — the common default for DFS sources:
```
# CLI flag: --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
hoodie.streamer.schemaprovider.source.schema.file=<PATH>/source.avsc
hoodie.streamer.schemaprovider.target.schema.file=<PATH>/target.avsc
```
Target defaults to source when omitted — set both only when the transformer changes the schema.

**Hive metastore** — when a synced table already defines the schema:
```
# CLI flag: --schemaprovider-class org.apache.hudi.utilities.schema.HiveSchemaProvider
hoodie.streamer.schemaprovider.source.schema.hive.database=<DB>
hoodie.streamer.schemaprovider.source.schema.hive.table=<TABLE>
# target.schema.hive.database / .table if the target differs
```

**JDBC** — derive from the upstream table:
```
# CLI flag: --schemaprovider-class org.apache.hudi.utilities.schema.JdbcbasedSchemaProvider
hoodie.streamer.schemaprovider.source.schema.jdbc.connection.url=<JDBC_URL>
hoodie.streamer.schemaprovider.source.schema.jdbc.driver.type=<DRIVER_CLASS>
hoodie.streamer.schemaprovider.source.schema.jdbc.username=<USER>
hoodie.streamer.schemaprovider.source.schema.jdbc.password=<PASSWORD>
hoodie.streamer.schemaprovider.source.schema.jdbc.dbtable=<TABLE>
```

**When no provider is set**, the source infers the schema. Appropriate for stable schemas with no expected evolution — don't add a provider (or the infrastructure behind it) to a pipeline that doesn't need one.

**Not applicable to Spark DataSource writes** — the DataFrame already carries its schema. Schema providers exist for HoodieStreamer only.

## Cleaner + archival (inline autopilot)

```
# Automatic inline cleaning and archival are Hudi defaults — emit no on/off switches.
# (hoodie.clean.automatic=true, hoodie.clean.async.enabled=false, hoodie.archive.automatic=true,
#  hoodie.archive.async=false, hoodie.commits.archival.batch=10 are all defaults already.)
hoodie.clean.policy=<KEEP_LATEST_BY_HOURS or KEEP_LATEST_COMMITS>
hoodie.clean.hours.retained=<derived>          # if KEEP_LATEST_BY_HOURS
hoodie.clean.commits.retained=<derived>        # if KEEP_LATEST_COMMITS

hoodie.keep.min.commits=<derived from cadence — see below>
hoodie.keep.max.commits=<keep.min.commits × 1.2>
```

**Derive the archival window from commit cadence — never emit a constant.**

```
commits_per_day  = 1440 / commit_cadence_minutes
cleaner_commits  = commits_per_day × cleaner_retention_days
keep.min.commits = max(100, ceil(cleaner_commits × 1.1))
keep.max.commits = ceil(keep.min.commits × 1.2)
```

At a 48h cleaner window: 5-min → **634 / 761**. 15-min → **211 / 253**. Hourly → **100 / 120** (floor).

**At daily cadence or slower, emit no cleaner or archival config at all** — leave Hudi's defaults. A table committing once a day accumulates timeline entries far too slowly for the active timeline to be at risk, so overriding adds config surface for nothing.

Archival must outlast the cleaner, hence the +10% margin. See decision-tables.md → Cleaner + archival for the full table and rationale.

**Do not emit 1000 / 1200.** The active-timeline target is ~1000 entries; an archival floor of 1000 means archival can't reclaim until the timeline already sits at the number it's meant to protect.

**Do NOT emit:** `hoodie.clean.fileversions.retained` — file-versions policy not recommended.

## Index

### SIMPLE
```
hoodie.index.type=SIMPLE
```

### Global SIMPLE
```
hoodie.index.type=GLOBAL_SIMPLE
hoodie.simple.index.update.partition.path=true
```

### BLOOM
```
hoodie.index.type=BLOOM
hoodie.bloom.index.prune.by.ranges=true
# Do NOT set hoodie.bloom.index.use.metadata=true — experimental at 1.2.0
```

### Global BLOOM
```
hoodie.index.type=GLOBAL_BLOOM
hoodie.bloom.index.update.partition.path=true
```

### Record Level Index (partitioned)
```
hoodie.index.type=RECORD_LEVEL_INDEX
hoodie.metadata.record.level.index.enable=true

# File-group count is PER PARTITION and DURABLE once the index initializes.
# Set min == max to pin it; a range lets Hudi estimate instead.
# Size from projected PER-PARTITION record count — see decision-tables.md → RLI file-group sizing.
hoodie.metadata.record.level.index.min.filegroup.count=<computed>
hoodie.metadata.record.level.index.max.filegroup.count=<same as min>
```

### Global Record Level Index
```
hoodie.index.type=GLOBAL_RECORD_LEVEL_INDEX
hoodie.metadata.global.record.level.index.enable=true

# File-group count is TABLE-WIDE and DURABLE once the index initializes.
# Set min == max to pin it; a range lets Hudi estimate instead.
# Size from projected TABLE-WIDE record count — see decision-tables.md → RLI file-group sizing.
hoodie.metadata.global.record.level.index.min.filegroup.count=<computed>
hoodie.metadata.global.record.level.index.max.filegroup.count=<same as min>
```

**Do NOT emit** `hoodie.metadata.record.index.{min,max}.filegroup.count` — these are deprecated aliases for the **global** properties. Using them under a partitioned-RLI config silently sets global knobs.

**Optional, when the user cannot project growth** (see decision-tables.md → RLI file-group sizing):
```
hoodie.metadata.record.index.growth.factor=<above 2.0 to buy headroom>
```

### BUCKET (SIMPLE)
```
hoodie.index.type=BUCKET
hoodie.index.bucket.engine=SIMPLE
hoodie.bucket.index.num.buckets=<derived>
```

### BUCKET (CONSISTENT_HASHING — MOR only)
Not recommended at design time. Escape hatch for skewed-partition BUCKET workloads.

## Compaction (MOR only)

### Inline (default for DataSource/SQL)
```
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5
hoodie.compact.inline.trigger.strategy=NUM_COMMITS
```

### Async via HoodieStreamer continuous
No config emitted. On by default; disable via `--disable-compaction` CLI flag (not recommended).

### Async via Spark Structured Streaming
```
hoodie.datasource.compaction.async.enable=true
```

### Compaction target IO trap

**This config is denominated in MEGABYTES, not bytes.** The default is `512000` (= 500 GB). Emitting a byte count here is off by a factor of ~10^6.

It is a per-round IO *ceiling*, not a sizing target — "amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy... helps bound ingestion latency." Capping it low is what creates the backlog: compaction is throttled below the rate at which log files accumulate, so file groups never catch up and read latency degrades.

Rather than deriving a point value from table size (which just re-creates the same trap at a higher threshold as the table grows), set it high enough that it never binds and let the compaction strategy decide what to compact:

```
hoodie.compaction.target.io=104857600   # 100 TB expressed in MB — effectively uncapped
```

Include in the ADR with the rationale: the ceiling exists to bound inline-compaction latency, and any workload where compaction must keep pace with ingestion wants it out of the way.

### Compaction selection strategy
Default (LogFileSizeBasedCompactionStrategy):
```
hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy
```

## Clustering (off by default)

Only emit when enabled (immutable + posture (b), or explicit user request).

### Inline
```
hoodie.clustering.inline=true
hoodie.clustering.inline.max.commits=4
```

### Async
```
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=5
```

### Plan strategy
```
hoodie.clustering.plan.strategy.class=org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy
hoodie.clustering.plan.strategy.small.file.limit=314572800    # 300MB
hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824  # 1GB
```

### Execution strategy
Default (SparkSortAndSizeExecutionStrategy):
```
hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy
```

### Sort columns (if layout optimization enabled)
```
hoodie.clustering.plan.strategy.sort.columns=<column>
hoodie.layout.optimize.strategy=LINEAR    # or ZORDER or HILBERT
```

### Incremental table services (1.2.0 default, keep on)
```
hoodie.table.services.incremental.enabled=true
```

## Concurrency (V1 default = SINGLE_WRITER)

```
hoodie.write.concurrency.mode=SINGLE_WRITER
```

Multi-writer configs (OCC, NBCC, lock providers, LAZY failed writes policy) are deferred to V2+ per §7.6 — **with one exception that V1 must handle**, below.

### Standalone HoodieCompactor implies concurrent writers

Whenever the design lands on a **separate `HoodieCompactor` job** (the recommended async path for MOR + Spark DataSource / Spark SQL), two processes now write to the same table. `SINGLE_WRITER` is unsafe in that deployment — emitting it alongside a two-job recommendation is a silent corruption path.

When this path is chosen, the Architect must surface:

- A write-concurrency mode appropriate to concurrent writers (`OPTIMISTIC_CONCURRENCY_CONTROL`).
- **The same lock provider, configured identically in BOTH jobs** — the ingestion writer and the compactor. Mismatched or absent lock config across the two is the failure mode.
- Matching failed-writes cleanup policy across both jobs.

```
# Must be set IDENTICALLY in the ingestion job and the compactor job
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=<lock provider class>
hoodie.clean.failed.writes.policy=LAZY   # modern key — hoodie.cleaner.policy.failed.writes is a deprecated alias
# ... plus the provider-specific lock config (ZK quorum, DynamoDB table, HMS URI, etc.),
#     identical on both sides
```

Full multi-writer rubric (choosing a provider, OCC vs NBCC, conflict resolution) remains V2+. What V1 owes the user is the warning that this deployment requires it, not the full decision tree. If the user is not prepared to set up locking, recommend inline compaction instead and record the latency tradeoff.

## Sample bundles per archetype

### Immutable event stream (EVENT-shape) — Kafka source, small records
```
# Table
hoodie.table.type=COPY_ON_WRITE
hoodie.datasource.write.recordkey.field=            # empty (auto-gen)
hoodie.datasource.write.partitionpath.field=event_ingest_date
hoodie.populate.meta.fields=false                    # only valid with no incremental/CDC consumers
hoodie.datasource.write.hive_style_partitioning=true

# Writer
hoodie.datasource.write.operation=bulk_insert       # posture (b) — see clustering
hoodie.bulkinsert.sort.mode=NONE

# Index
hoodie.index.type=SIMPLE

# Services
hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=<derive from commit cadence — see decision-tables.md → retention>
# NOT a fixed 48. At 15-min cadence the safe max is ~5 days (120h); at hourly, ~20 days.
# A hardcoded 48 silently under-retains on slower cadences.
#
# Example below assumes 5-min cadence + 48h cleaner window.
# Recompute both cleaner and archival for the actual answered cadence.

hoodie.keep.min.commits=634        # 5-min cadence: 288 commits/day × 2 days × 1.1
hoodie.keep.max.commits=761        # min × 1.2

hoodie.clustering.async.enabled=true                 # posture (b)
hoodie.clustering.async.max.commits=5

# Platform
hoodie.metadata.enable=true

# Concurrency — valid ONLY while exactly one process writes this table, counting
# backfills, GDPR/cleanup jobs, and any standalone compactor. Any second writer
# means the OCC block above, not this line (see Concurrency section).
hoodie.write.concurrency.mode=SINGLE_WRITER
```

### Mutable dimension table (DIM-shape) — Kafka CDC source, unpartitioned, uniform updates
```
# Table
hoodie.table.type=MERGE_ON_READ                     # experience = some
hoodie.datasource.write.recordkey.field=customer_id
hoodie.datasource.write.partitionpath.field=       # empty (unpartitioned)
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
# meta fields: kept — hoodie.populate.meta.fields defaults to true; emit only when disabling

# Writer
hoodie.datasource.write.operation=upsert
hoodie.table.ordering.fields=updated_at

# Index
hoodie.index.type=GLOBAL_RECORD_LEVEL_INDEX
hoodie.metadata.global.record.level.index.enable=true
# DURABLE at index initialization. min == max pins the count; a range lets Hudi estimate.
# Size from projected TABLE-WIDE record count — decision-tables.md → RLI file-group sizing.
hoodie.metadata.global.record.level.index.min.filegroup.count=<computed>
hoodie.metadata.global.record.level.index.max.filegroup.count=<same as min>

# Services
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5

hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=<derive from commit cadence — see decision-tables.md → retention>
# NOT a fixed 48. At 15-min cadence the safe max is ~5 days (120h); at hourly, ~20 days.
# A hardcoded 48 silently under-retains on slower cadences.
#
# Example below assumes 5-min cadence + 48h cleaner window.
# Recompute both cleaner and archival for the actual answered cadence.

hoodie.keep.min.commits=634        # 5-min cadence: 288 commits/day × 2 days × 1.1
hoodie.keep.max.commits=761        # min × 1.2

# Platform
hoodie.metadata.enable=true

# Concurrency — valid ONLY while exactly one process writes this table, counting
# backfills, GDPR/cleanup jobs, and any standalone compactor. Any second writer
# means the OCC block above, not this line (see Concurrency section).
hoodie.write.concurrency.mode=SINGLE_WRITER
```

### Sample submit commands

Emit alongside the properties bundle. Parameterize on the derived writer, table type, source class, and operation. **Always distinguish load-bearing flags (derived from design decisions) from environment-specific placeholders (paths, memory, engine/Scala versions) that the flow never asked about.**

#### HoodieStreamer — continuous mode (MOR with free async compaction)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_<SCALA>:<HUDI_VERSION>,org.apache.hudi:hudi-spark<SPARK_VERSION>-bundle_<SCALA>:<HUDI_VERSION> \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
  --executor-memory <MEM> \
  --num-executors <N> \
  /path/to/hudi-utilities-slim-bundle_<SCALA>-<HUDI_VERSION>.jar \
  --props <PATH_TO_PROPERTIES> \
  --target-base-path <TABLE_BASE_PATH> \
  --target-table <TABLE_NAME> \
  --table-type MERGE_ON_READ \
  --source-class <DERIVED_SOURCE_CLASS> \
  --schemaprovider-class <DERIVED_SCHEMA_PROVIDER> \
  --source-ordering-field <ORDERING_FIELD> \
  --op UPSERT \
  --continuous \
  --min-sync-interval-seconds <CADENCE_SECONDS>
```

**Load-bearing:** `--continuous` is what provides in-process async compaction; without it you get run-once batch semantics and MOR log files grow unbounded. `--table-type`, `--op`, `--source-class`, `--schemaprovider-class`, `--source-ordering-field` all come from derived decisions. `--min-sync-interval-seconds` reflects the answered cadence.

**Never suggest `--disable-compaction` on MOR.**

**Environment-specific — leave these to the user, deliberately.** Master and deploy mode, memory and executor counts, `<SCALA>` (2.12 or 2.13), `<SPARK_VERSION>` in the bundle artifact name, `<HUDI_VERSION>`, and all paths.

The flow does not ask about these and shouldn't: they're facts about the user's build and cluster, not design decisions, and asking would add several infrastructure questions to a design conversation for no design benefit. Emit them as clearly-marked placeholders and say plainly that the user fills them in from their own environment. Don't guess concrete versions — a wrong Scala suffix produces a confusing classpath failure, and a placeholder that looks like a recommendation is worse than one that looks like a blank.

#### HoodieStreamer — run-once (scheduled batch)

Same as above, minus `--continuous` and `--min-sync-interval-seconds`. Schedule externally (cron, Airflow). On MOR without continuous mode, add inline compaction config to the properties file.

#### Spark DataSource

No submit template — the user's own application carries the write. Emit the properties bundle as `.option(...)` calls, plus the required session config.

Batch job:
```scala
df.write.format("hudi")
  .options(hudiOpts)          // the properties bundle
  .mode(SaveMode.Append)
  .save("<TABLE_BASE_PATH>")
```

Continuous job writing from a stream via `forEachBatch`:
```scala
sourceStream.writeStream
  .foreachBatch { (batchDF: DataFrame, _: Long) =>
    batchDF.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save("<TABLE_BASE_PATH>")
  }
  .option("checkpointLocation", "<CHECKPOINT_PATH>")
  .trigger(Trigger.ProcessingTime("<CADENCE>"))
  .start()
```

Required session config either way:
```
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
```

Note that `forEachBatch` is the DataSource path from Hudi's perspective, not the Structured Streaming sink — the writer derivation is the same for both snippets.

### Mutable fact table (FACT-shape) — Kafka CDC source, date-partitioned, recent-heavy updates, TB-scale
```
# Table
hoodie.table.type=MERGE_ON_READ                     # experienced → async via HoodieStreamer
hoodie.datasource.write.recordkey.field=trip_id
hoodie.datasource.write.partitionpath.field=trip_date
# meta fields: kept — hoodie.populate.meta.fields defaults to true; emit only when disabling

# Writer
hoodie.datasource.write.operation=upsert
hoodie.table.ordering.fields=updated_at

# Index
hoodie.index.type=RECORD_LEVEL_INDEX
hoodie.metadata.record.level.index.enable=true
# DURABLE at index initialization, and PER PARTITION. min == max pins the count.
# Size from projected PER-PARTITION record count (total / active partitions).
hoodie.metadata.record.level.index.min.filegroup.count=<computed>
hoodie.metadata.record.level.index.max.filegroup.count=<same as min>

# Services
# No compaction config — HoodieStreamer continuous handles async automatically

# COMPACTION TARGET IO TRAP — value is in MB, not bytes
hoodie.compaction.target.io=104857600                # 100TB in MB — effectively uncapped

hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=<derive from commit cadence — see decision-tables.md → retention>
# NOT a fixed 48. At 15-min cadence the safe max is ~5 days (120h); at hourly, ~20 days.
# A hardcoded 48 silently under-retains on slower cadences.
#
# Example below assumes 5-min cadence + 48h cleaner window.
# Recompute both cleaner and archival for the actual answered cadence.

hoodie.keep.min.commits=634        # 5-min cadence: 288 commits/day × 2 days × 1.1
hoodie.keep.max.commits=761        # min × 1.2

# Platform
hoodie.metadata.enable=true

# Concurrency — valid ONLY while exactly one process writes this table, counting
# backfills, GDPR/cleanup jobs, and any standalone compactor. Any second writer
# means the OCC block above, not this line (see Concurrency section).
hoodie.write.concurrency.mode=SINGLE_WRITER
```
