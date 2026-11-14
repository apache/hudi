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

## Concurrency

Default is single writer. Emit only the mode line:

```
hoodie.write.concurrency.mode=SINGLE_WRITER
```

`SINGLE_WRITER` is correct — and required for maximum throughput — whenever exactly one
process commits to the table. Inline table services, and async services running **in the
writer's own process** (HoodieStreamer continuous, Flink streaming), do not make a second
writer. A *standalone* service job does.

Mode selection, NBCC eligibility, and provider choice are derived in
decision-tables.md → Concurrency. This section holds the blocks to emit once that derivation
has run.

### Rule: every block below goes in EVERY writing job

A lock only serializes writers that agree on it. A block applied to the ingestion job but not
the compactor job, or applied with a different provider on each side, gives each writer its own
lock: every writer succeeds, and the table corrupts silently with nothing in any log. State
this every time a multi-writer bundle is emitted — it is the single most common way these
deployments fail. See warnings.md → `LOCK_PROVIDER_MISMATCH`.

### OCC — DynamoDB (default on AWS)

Uses the **implicit** partition-key provider: the lock's partition key is derived from
`hoodie.base.path`, so it cannot drift between jobs.

```
# ---- apply IDENTICALLY in every job that writes this table ----
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.aws.transaction.lock.DynamoDBBasedImplicitPartitionKeyLockProvider
hoodie.write.lock.dynamodb.table=<lock table name>
hoodie.write.lock.dynamodb.region=<aws region>
hoodie.clean.failed.writes.policy=LAZY
```

The lock table is created automatically if absent, so setup is a table name, a region, and IAM
permissions on that table for **every** writing job. It defaults to `PAY_PER_REQUEST` billing
(`hoodie.write.lock.dynamodb.billing_mode`), which is the right mode for a table that sees one
short lock per commit — switching it to `PROVISIONED` means paying for reserved capacity this
workload will not use.

`hoodie.write.lock.dynamodb.partition_key` is **not** set — that is the point of the implicit
provider. (Published docs list
`hoodie.write.lock.dynamodb.endpoint_url` as required; it is optional. The keys actually
validated are `table` and `region`.)

**Requires `hudi-aws-bundle` in `--packages` on every writing job** — see "Cloud bundles are
load-bearing" under Sample submit commands. Without it the job fails at the first commit with a
`ClassNotFoundException` on the lock provider. If this design also syncs to Glue, the same
bundle covers both.

### OCC — ZooKeeper (existing quorum)

Uses the **implicit** base-path provider: ZK base path and lock key are both derived from
`hoodie.base.path`.

```
# ---- apply IDENTICALLY in every job that writes this table ----
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedImplicitBasePathLockProvider
hoodie.write.lock.zookeeper.url=<zk connect string>
hoodie.write.lock.zookeeper.port=<zk port>
hoodie.clean.failed.writes.policy=LAZY
```

Neither `hoodie.write.lock.zookeeper.base_path` nor `hoodie.write.lock.zookeeper.lock_key` is
set — both are derived. The derived ZK base path looks like `/tmp/<hash>`; that is a **znode
path, not a filesystem path**, it is not on disk, and it is not tunable. Mention it in the ADR
so it is not "fixed" later by switching to the explicit provider.

### OCC — Hive Metastore

```
# ---- apply IDENTICALLY in every job that writes this table ----
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider
hoodie.write.lock.hivemetastore.database=<db>
hoodie.write.lock.hivemetastore.table=<table>
hoodie.clean.failed.writes.policy=LAZY
```

Metastore URIs are picked up from the Hadoop configuration at runtime; set
`hoodie.write.lock.hivemetastore.uris` only when that is not the case.

### OCC — storage-based (cloud storage, no existing lock infrastructure)

**Not the default — see decision-tables.md → Concurrency Step 3 for the maturity caution.**
Available since **1.0.2**, years younger than every other provider, and it implements lease
renewal with a heartbeat plus per-cloud lock clients. Emit it when the user is on cloud storage
with no existing lock infrastructure and would otherwise stand up ZooKeeper for one table, or
when they ask for it directly — and state the maturity point once, plainly, so the choice is
made knowingly.

The appeal is real: it locks under the table's own path, so there is no infrastructure to stand
up and no lock identity to keep in sync.

```
# ---- apply IDENTICALLY in every job that writes this table ----
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.StorageBasedLockProvider
# Inferred automatically for multi-writer modes; emitted to document intent.
# EAGER here is a hard config-validation failure.
hoodie.clean.failed.writes.policy=LAZY
```

Optional, only if lock renewal needs tuning (validity must be >= 10x renew interval, and >= 10s):

```
hoodie.write.lock.storage.validity.timeout.secs=300
hoodie.write.lock.storage.renew.interval.secs=30
```

`hoodie.write.lock.storage.heartbeat.poll.secs` is a **deprecated alias** for
`renew.interval.secs` — set one, never both.

Requires the cloud bundle matching the storage scheme on the classpath of every writing job.

### NBCC — MOR + simple bucket index only

Emit **only** when decision-tables.md → Concurrency Step 2 passes: MOR, BUCKET index, table
version ≥ 8, no clustering. Never adjust table type or index to reach this block.

```
# ---- apply IDENTICALLY in every job that writes this table ----
hoodie.write.concurrency.mode=NON_BLOCKING_CONCURRENCY_CONTROL
hoodie.clean.failed.writes.policy=LAZY
```

No lock provider is required — writers append to their own log files and conflicts are resolved
by the reader and the compactor. `hoodie.write.lock.conflict.resolution.strategy` is **not**
emitted: it auto-infers to the bucket-index strategy from the index type, and setting it by
hand is how that gets broken.

### Standalone HoodieCompactor implies concurrent writers

Whenever the design lands on a **separate `HoodieCompactor` job** (the async path for MOR +
Spark DataSource / Spark SQL), two processes write the same table. `SINGLE_WRITER` is unsafe
there — emitting it alongside a two-job recommendation is a silent corruption path.

Emit the OCC block matching the deployment (DynamoDB on AWS; an existing ZooKeeper quorum or
Hive Metastore otherwise) in **both** the ingestion job and the compactor job. See warnings.md → `COMPACTOR_CONCURRENCY_REQUIRED`.

If the user is not prepared to run a lock provider at all, recommend inline compaction instead
and record the latency tradeoff in the ADR.

### Not emitted, and why

| Config | Why not |
|---|---|
| `hoodie.write.lock.conflict.resolution.strategy` | Auto-infers from index type; hand-setting breaks bucket-index conflict handling. |
| `hoodie.write.concurrency.early.conflict.detection.enable` | Experimental, OCC-only, default false. Offer for high-contention OCC; do not emit. |
| `hoodie.write.num.retries.on.conflict.failures` | Default 0. Contention tuning — Operations Agent territory. |
| Lock retry and timeout keys (the `wait_time_ms` / `num_retries` family) | Defaults are sound. Tune on observed contention, not at design time. |
| ZooKeeper session and connection timeouts | Defaults are sound; same reasoning. |
| DynamoDB capacity keys (`read_capacity`, `write_capacity`, `table_creation_timeout`) | Only apply under `PROVISIONED` billing, which this workload should not use. |
| `hoodie.write.lock.app_id` | Identifies the lock holder for debugging. Environment-specific, not a design decision. |
| `hoodie.write.lock.dynamodb.endpoint_url` | Local-development override for pointing at a DynamoDB emulator. |

## Catalog / metastore sync

Off by default — emit nothing unless a consumer needs it. Derivation and constraints are in
decision-tables.md → Catalog / metastore sync. Spark- or Flink-only pipelines reading by path
need none.

### Hive Metastore (the default)

```
hoodie.datasource.meta.sync.enable=true
hoodie.datasource.hive_sync.mode=hms
hoodie.datasource.hive_sync.metastore.uris=thrift://<host>:9083
hoodie.datasource.hive_sync.database=<db>
hoodie.datasource.hive_sync.table=<table>
hoodie.datasource.hive_sync.partition_fields=<partition column(s), comma-separated>
```

No `hoodie.meta.sync.client.tool.class` — `HiveSyncTool` is the default. The partition
extractor is inferred for the common cases; set
`hoodie.datasource.hive_sync.partition_extractor_class` explicitly only for the timestamp-based
`yyyy/MM/dd` case (see decision-tables.md, and warnings.md →
`PARTITION_EXTRACTOR_MISMATCH`).

Omit `partition_fields` entirely for an unpartitioned table — it is what tells Hudi to use the
non-partitioned extractor.

**In Spark SQL with a Hive catalog** (`spark.sql.catalogImplementation=hive`), add:

```
hoodie.datasource.hive_sync.use_spark_catalog=true
```

That uses Spark's own catalog client and avoids the classloader conflicts otherwise seen in
Hive-on-Spark setups.

`hive-site.xml` must be on the classpath (and under `$SPARK_HOME/conf` for spark-shell or
spark-sql). Only set `username` / `password` / `jdbcurl` when the mode is `jdbc`.

### AWS Glue Data Catalog

Reuses every Hive sync config above — only the tool class changes.

```
hoodie.datasource.meta.sync.enable=true
hoodie.meta.sync.client.tool.class=org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
hoodie.datasource.hive_sync.database=<glue database>
hoodie.datasource.hive_sync.table=<table>
hoodie.datasource.hive_sync.partition_fields=<partition column(s)>
# Sync only on schema or partition change. Default is false, which writes a new
# Glue catalog version on EVERY commit.
hoodie.datasource.meta_sync.condition.sync=true
```

`hudi-aws-bundle` must be on the classpath of every writing job. No metastore URI — the tool
talks to Glue directly, and the AWS region comes from the environment.

Optional, for large partitioned tables where Glue partition reads become the bottleneck:

```
hoodie.datasource.meta.sync.glue.partition_index_fields.enable=true
hoodie.datasource.meta.sync.glue.partition_index_fields=<subset of partition fields>
```

The Glue read/write parallelism keys
(`...glue.all_partitions_read_parallelism`, `...glue.changed_partitions_read_parallelism`,
`...glue.partition_change_parallelism`) have working defaults — tune on observed sync latency,
not at design time.

### BigQuery

Separate config namespace, and different constraints — read decision-tables.md before emitting.

```
hoodie.datasource.meta.sync.enable=true
hoodie.meta.sync.client.tool.class=org.apache.hudi.gcp.bigquery.BigQuerySyncTool
hoodie.gcp.bigquery.sync.project_id=<gcp project>
hoodie.gcp.bigquery.sync.dataset_name=<dataset>
hoodie.gcp.bigquery.sync.dataset_location=<region>
hoodie.gcp.bigquery.sync.table_name=<table>
hoodie.gcp.bigquery.sync.source_uri=gs://<bucket>/<path>/dt=*
hoodie.gcp.bigquery.sync.source_uri_prefix=gs://<bucket>/<path>/
# Manifest-based sync — preferred over the legacy view-over-files approach
hoodie.gcp.bigquery.sync.use_bq_manifest_file=true
# BigQuery sync requires hive-style partitioning
hoodie.datasource.write.hive_style_partitioning=true
```

There is **no** `hoodie.gcp.bigquery.sync.base_path` — published docs list one, but the table
location comes from the standard base-path config. `hudi-gcp-bundle` on the classpath.

Optional: `hoodie.gcp.bigquery.sync.require_partition_filter=true` forces queries to filter on
a partition column, which prevents accidental full scans;
`hoodie.gcp.bigquery.sync.billing.project.id` when billing differs from the data project.

### DataHub (discovery, not queries)

Additive — pair it with HMS or Glue, never instead of one, when a query engine is involved.

```
hoodie.datasource.meta.sync.enable=true
hoodie.meta.sync.client.tool.class=org.apache.hudi.hive.HiveSyncTool,org.apache.hudi.sync.datahub.DataHubSyncTool
hoodie.meta.sync.datahub.emitter.server=http://<datahub-gms-host>:8080
hoodie.meta.sync.datahub.emitter.token=<token>
```

`hudi-datahub-sync-bundle` on the classpath. For a custom emitter or dataset URN, see
`hoodie.meta.sync.datahub.emitter.supplier.class` and
`hoodie.meta.sync.datahub.dataset.identifier.class`.

### Polaris

Configured as a Spark catalog rather than through the sync-tool mechanism. Only when the user
already runs Polaris — it is newer than the other options (Hudi 1.1.1 / Polaris 1.3.0, and
Polaris is incubating).

```
--conf spark.sql.catalog.<catalog name>=org.apache.polaris.spark.SparkCatalog
```

Hudi detects the Polaris catalog and delegates table registration to it; the default is already
`org.apache.polaris.spark.SparkCatalog`, so `hoodie.spark.polaris.catalog.class` needs setting
only for a custom implementation.

### MOR registers two tables

A MOR table with catalog sync produces `<table>_ro` (base files only — cheaper, staler) and
`<table>_rt` (merges log files — current, slower). Name **both** in the ADR and say which
consumers should use which; pointing a consumer at the wrong suffix gives stale data or
unexpected latency with no error either way. `--skip-ro-suffix` on the standalone sync tool
suppresses the `_ro` suffix, which exists for backward compatibility and should not be a
default choice.

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
# means this line is replaced by the OCC (or NBCC) block for the deployment —
# see Concurrency section — applied identically in every writing job.
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
# means this line is replaced by the OCC (or NBCC) block for the deployment —
# see Concurrency section — applied identically in every writing job.
hoodie.write.concurrency.mode=SINGLE_WRITER
```

### Sample submit commands

Emit alongside the properties bundle. Parameterize on the derived writer, table type, source class, and operation. **Always distinguish load-bearing flags (derived from design decisions) from environment-specific placeholders (paths, memory, engine/Scala versions) that the flow never asked about.**

#### Cloud bundles are load-bearing — put them in `--packages`

Some derived decisions need a class that is **not** in the Spark or utilities bundle. The config
is valid without the jar, so nothing complains at submit time: the job starts, runs, and dies at
the first commit with a `ClassNotFoundException` on a class the properties file names. That is
the worst shape of failure this skill can emit — correct config that cannot possibly work — so
the bundle belongs in the emitted `--packages` line, not in prose the user may skim past.

| Derived decision | Class that needs the jar | Add to `--packages` |
|---|---|---|
| DynamoDB lock provider (OCC on AWS) | `DynamoDBBasedImplicitPartitionKeyLockProvider` | `org.apache.hudi:hudi-aws-bundle:<HUDI_VERSION>` |
| Glue catalog sync | `AwsGlueCatalogSyncTool` | `org.apache.hudi:hudi-aws-bundle:<HUDI_VERSION>` |
| BigQuery sync | `BigQuerySyncTool` | `org.apache.hudi:hudi-gcp-bundle:<HUDI_VERSION>` |
| DataHub sync | `DataHubSyncTool` | `org.apache.hudi:hudi-datahub-sync-bundle:<HUDI_VERSION>` |
| Storage-based lock provider | the per-cloud lock client | the bundle matching the storage scheme |

**`hudi-aws-bundle` covers both AWS cases**, so a design with DynamoDB locking *and* Glue sync
needs it once, not twice. Say that when both apply — a reader who sees the same jar justified by
two different decisions may wonder whether they need two.

Two rules when emitting:

- **Every writing job needs it**, exactly like the concurrency block. A backfill job that writes
  without `hudi-aws-bundle` cannot take the DynamoDB lock — and it will fail rather than write
  unsafely, which is the good outcome, but only if the operator knows why.
- Version it with the same `<HUDI_VERSION>` placeholder as the other bundles. Never pin a
  concrete version; a mismatched bundle version is its own confusing classpath failure.

Also list these in the ADR's pre-launch checklist → warnings.md → `CLOUD_BUNDLE_REQUIRED`.

#### HoodieStreamer — continuous mode (MOR with free async compaction)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_<SCALA>:<HUDI_VERSION>,org.apache.hudi:hudi-spark<SPARK_VERSION>-bundle_<SCALA>:<HUDI_VERSION><CLOUD_BUNDLE> \
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

`<CLOUD_BUNDLE>` is **not** a placeholder for the user to fill in — substitute it when emitting,
from the table above, or drop it entirely when no cloud bundle is needed. For a design with
DynamoDB locking or Glue sync it becomes
`,org.apache.hudi:hudi-aws-bundle:<HUDI_VERSION>`. Leaving a literal `<CLOUD_BUNDLE>` in the
emitted command is a defect: unlike `<MEM>` or `<SCALA>`, this one is derived, and the user has
no way to know what belongs there.

**Environment-specific — leave these to the user, deliberately.** Master and deploy mode, memory and executor counts, `<SCALA>` (2.12 or 2.13), `<SPARK_VERSION>` in the bundle artifact name, `<HUDI_VERSION>`, and all paths.

The flow does not ask about these and shouldn't: they're facts about the user's build and cluster, not design decisions, and asking would add several infrastructure questions to a design conversation for no design benefit. Emit them as clearly-marked placeholders and say plainly that the user fills them in from their own environment. Don't guess concrete versions — a wrong Scala suffix produces a confusing classpath failure, and a placeholder that looks like a recommendation is worse than one that looks like a blank.

#### HoodieStreamer — run-once (scheduled batch)

Same as above, minus `--continuous` and `--min-sync-interval-seconds`. Schedule externally (cron, Airflow). On MOR without continuous mode, add inline compaction config to the properties file.

#### Spark DataSource

No submit template — the user's own application carries the write. Emit the properties bundle as `.option(...)` calls, plus the required session config.

**The cloud bundle still applies, and is easier to miss here** precisely because there is no
submit command to hang it on. The user's own `spark-submit` (or `spark-shell` / `spark-sql`)
invocation needs it, and the skill never sees that command. So state it explicitly rather than
assuming it carries over from the properties bundle:

```bash
# Whatever launches your job needs the cloud bundle on the classpath.
# spark-submit, spark-shell, and spark-sql all take --packages:
spark-shell \
  --packages org.apache.hudi:hudi-spark<SPARK_VERSION>-bundle_<SCALA>:<HUDI_VERSION>,org.apache.hudi:hudi-aws-bundle:<HUDI_VERSION> \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
```

Substitute the bundle from the table above; drop it when the design needs none. For a
long-running application, the equivalent is a compile-time dependency on the same artifact
rather than `--packages`.

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
# means this line is replaced by the OCC (or NBCC) block for the deployment —
# see Concurrency section — applied identically in every writing job.
hoodie.write.concurrency.mode=SINGLE_WRITER
```
