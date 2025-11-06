---
title: "Release 0.13.0"
sidebar_position: 11
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.13.0](https://github.com/apache/hudi/releases/tag/release-0.13.0)

Apache Hudi 0.13.0 release introduces a number of new features including [Metaserver](#metaserver),
[Change Data Capture](#change-data-capture), [new Record Merge API](#optimizing-record-payload-handling),
[new sources for Deltastreamer](#new-source-support-in-deltastreamer) and more.  While there is no table version upgrade
required for this release, users are expected to take actions by following the [Migration Guide](#migration-guide-overview)
down below on relevant [breaking changes](#migration-guide-breaking-changes) and
[behavior changes](#migration-guide-behavior-changes) before using 0.13.0 release.

## Migration Guide: Overview

This release keeps the same table version (`5`) as [0.12.0 release](/releases/release-0.12.0), and there is no need for
a table version upgrade if you are upgrading from 0.12.0.  There are a few
[breaking changes](#migration-guide-breaking-changes) and [behavior changes](#migration-guide-behavior-changes) as
described below, and users are expected to take action accordingly before using 0.13.0 release.

:::caution
If migrating from an older release (pre 0.12.0), please also check the upgrade instructions from each older release in
sequence.
:::

## Migration Guide: Breaking Changes

### Bundle Updates

#### Spark bundle Support

From now on, [`hudi-spark3.2-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.2-bundle) works
with Apache Spark 3.2.1 and newer versions for Spark 3.2.x.  The support for Spark 3.2.0 with
[`hudi-spark3.2-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.2-bundle) is
dropped because of the Spark implementation change of `getHive` method of `HiveClientImpl` which is incompatible between
Spark version 3.2.0 and 3.2.1.

#### Utilities Bundle Change

The AWS and GCP bundle jars are separated from
[`hudi-utilities-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-utilities-bundle). The user would need
to use [**`hudi-aws-bundle`**](https://mvnrepository.com/artifact/org.apache.hudi/hudi-aws-bundle) or
[**`hudi-gcp-bundle`**](https://mvnrepository.com/artifact/org.apache.hudi/hudi-gcp-bundle) along with
[`hudi-utilities-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-utilities-bundle) while using the
cloud services.

#### New Flink Bundle

Hudi is now supported on Flink 1.16.x with the new
[`hudi-flink1.16-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-flink1.16-bundle).

### Lazy File Index in Spark

Hudi's File Index in Spark is switched to be listed lazily ***by default***: this entails that it would **only** be listing
partitions that are requested by the query (i.e., after partition-pruning) as opposed to always listing the whole table
before this release. This is expected to bring considerable performance improvement for large tables.

A new configuration property is added if the user wants to change the listing behavior:
`hoodie.datasource.read.file.index.listing.mode` (now default to **`lazy`**). There are two possible values that you can
set:

- **`eager`**: This lists all partition paths and corresponding file slices within them eagerly, during initialization.
  This is the default behavior prior 0.13.0.
    - If a Hudi table has 1000 partitions, the eager mode lists the files under all of them when constructing the file index.

- **`lazy`**: The partitions and file-slices within them will be listed lazily, allowing partition pruning predicates to
  be pushed down appropriately, therefore only listing partitions after these have already been pruned.
    - The files are not listed under the partitions when the File Index is initialized. The files are listed only under
      targeted partition(s) after partition pruning using predicates (e.g., `datestr=2023-02-19`) in queries.

:::tip
To preserve the behavior pre 0.13.0, the user needs to set `hoodie.datasource.read.file.index.listing.mode=eager`.
:::

:::danger Breaking Change
The **breaking change** occurs only in cases when the table has **BOTH**: multiple partition columns AND partition
values contain slashes that are not URL-encoded.
:::

For example let's assume we want to parse two partition columns - `month` (`2022/01`) and `day` (`03`), from the
partition path `2022/01/03`. Since there is a mismatch between the number of partition columns (2 here - `month` and
`day`) and the number of components in the partition path delimited by `/` (3 in this case - month, year and day) it
causes ambiguity. In such cases, it is not possible to recover the partition value corresponding to each partition column.

There are two ways to **avoid** the breaking changes:

- The first option is to change how partition values are constructed. A user can switch the partition value of column
  `month` to avoid slashes in any partition column values, such as `202201`, then there is no problem parsing the
  partition path (`202201/03`).

- The second option is to switch the listing mode to `eager`.  The File Index would "gracefully regress" to assume the
  table is non-partitioned and just sacrifice partition-pruning, but would be able to process the query as if the
  table was non-partitioned (therefore potentially incurring performance penalty), instead of failing the queries.

### Checkpoint Management in Spark Structured Streaming

If you are using [Spark streaming](https://spark.apache.org/docs/3.3.2/structured-streaming-programming-guide.html) to
ingest into Hudi, Hudi self-manages the checkpoint internally. We are now adding support for multiple writers, each
ingesting into the same Hudi table via streaming ingest. In older versions of hudi, you can't have multiple streaming
ingestion writers ingesting into the same hudi table (one streaming ingestion writer with a concurrent Spark datasource
writer works with lock provider; however, two Spark streaming ingestion writers are not supported). With 0.13.0, we are
adding support where multiple streaming ingestions can be done to the same table. In case of a single streaming ingestion,
users don't have to do anything; the old pipeline will work without needing any additional changes. But, if you are
having multiple streaming writers to same Hudi table, each table has to set a unique value for the config,
`hoodie.datasource.write.streaming.checkpoint.identifier`. Also, users are expected to set the usual multi-writer
configs. More details can be found [here](/docs/concurrency_control).

### ORC Support in Spark

The [ORC](https://orc.apache.org/) support for Spark 2.x is removed in this release, as the dependency of
`orc-core:nohive` in Hudi is now replaced by  `orc-core`, to be compatible with Spark 3.  [ORC](https://orc.apache.org/)
support is now available for Spark 3.x, which was broken in previous releases.

### Mandatory Record Key Field

The configuration for setting the record key field, `hoodie.datasource.write.recordkey.field`, is now required to be set
and has no default value. Previously, the default value is `uuid`.

## Migration Guide: Behavior Changes

### Schema Handling in Write Path

Many users have requested using Hudi for CDC use cases that they want to have schema auto-evolution where existing
columns might be dropped in a new schema. As of 0.13.0 release, Hudi now has this functionality. You can permit schema
auto-evolution where existing columns can be dropped in a new schema.

Since dropping columns in the target table based on the source schema constitutes a considerable behavior change, this
is disabled by default and is guarded by the following config: `hoodie.datasource.write.schema.allow.auto.evolution.column.drop`.
To enable automatic dropping of the columns along with new evolved schema of the incoming batch, set this to **`true`**.

:::tip
This config is **NOT** required to evolve schema manually by using, for example, `ALTER TABLE … DROP COLUMN` in Spark.
:::

### Removal of Default Shuffle Parallelism

This release changes how Hudi decides the shuffle parallelism of [write operations](/docs/write_operations) including
`INSERT`, `BULK_INSERT`, `UPSERT` and `DELETE` (**`hoodie.insert|bulkinsert|upsert|delete.shuffle.parallelism`**), which
can ultimately affect the write performance.

Previously, if users did not configure it, Hudi would use `200` as the default shuffle parallelism. From 0.13.0 onwards
Hudi by default automatically deduces the shuffle parallelism by either using the number of output RDD partitions as
determined by Spark when available or by using the `spark.default.parallelism` value.  If the above Hudi shuffle
parallelisms are explicitly configured by the user, then the user-configured parallelism is still used in defining the
actual parallelism.  Such behavior change improves the out-of-the-box performance by 20% for workloads with reasonably
sized input.

:::caution
If the input data files are small, e.g., smaller than 10MB, we suggest configuring the Hudi shuffle parallelism
(`hoodie.insert|bulkinsert|upsert|delete.shuffle.parallelism`) explicitly, such that the parallelism is at least
total_input_data_size/500MB, to avoid potential performance regression (see [Tuning Guide](/docs/tuning-guide) for more
information).
:::

### Simple Write Executor as Default

For the execution of insert/upsert operations, Hudi historically used the notion of an executor, relying on in-memory
queue to decouple ingestion operations (that were previously often bound by I/O operations fetching shuffled blocks)
from writing operations. Since then, Spark architectures have evolved considerably making such writing architecture
redundant. Towards evolving this writing pattern and leveraging the changes in Spark, in 0.13.0 we introduce a new,
simplified version of the executor named (creatively) as **`SimpleExecutor`** and also make it out-of-the-box default.

The **`SimpleExecutor`** does not have any internal buffering (i.e., does not hold records in memory), which internally
implements simple iteration over provided iterator (similar to default Spark behavior).  It provides **~10%**
out-of-the-box performance improvement on modern Spark versions (3.x) and even more when used with Spark's native
**`SparkRecordMerger`**.

### `NONE` Sort Mode for Bulk Insert to Match Parquet Writes

This release adjusts the parallelism for `NONE` sort mode (default sort mode) for `BULK_INSERT` write operation. From
now on, by default, the input parallelism is used instead of the shuffle parallelism (`hoodie.bulkinsert.shuffle.parallelism`)
for writing data, to match the default parquet write behavior. This does not change the behavior of clustering using the
`NONE` sort mode.

Such behavior change on `BULK_INSERT` write operation improves the write performance out of the box.

:::tip
If you still observe small file issues with the default `NONE` sort mode, we suggest sorting the input data based on the
partition path and record key before writing to the Hudi table. You can also use `GLOBAL_SORT` to ensure the best file
size.
:::

### Meta Sync Failure in Deltastreamer

In earlier versions, we used a fail-fast approach where syncing to remaining catalogs is not attempted if any
[catalog sync](/docs/syncing_metastore) fails. In 0.13.0, syncing to all configured catalogs is attempted before failing
the operation on any catalog sync failure.  In the case of sync failure for one catalog, the sync to other catalogs can
still succeed, so the user only needs to retry the failed one now.

### No Override of Internal Metadata Table Configs

Since misconfiguration could lead to possible data integrity issues, in 0.13.0, we have put in efforts to make the
metadata table configuration much simpler for the users. Internally, Hudi determines the best choices for these
configurations for optimal performance and stability of the system.

The following metadata-table-related configurations are made internal; you can no longer configure these configs
explicitly:
```
hoodie.metadata.clean.async
hoodie.metadata.cleaner.commits.retained
hoodie.metadata.enable.full.scan.log.files
hoodie.metadata.populate.meta.fields
```

### Spark SQL CTAS Performance Fix

Previously, CTAS write operation was incorrectly set to use `UPSERT` due to misconfiguration. In 0.13.0 release, we fix
this to make sure CTAS uses **`BULK_INSERT`** operation to boost the write performance of the first batch to a Hudi table
(there's no real need to use `UPSERT` for it, as the table is being created).

### Flink CkpMetadata

Before 0.13.0, we bootstrapped the ckp metadata (checkpoint related metadata) by cleaning all the messages. Some corner
cases were not handled correctly. For example:

- The write task can not fetch the pending instant correctly when restarting a job.

- If a checkpoint succeeds and the job crashes suddenly, the instant hasn't had time to commit. The data is lost because
  the last pending instant was rolled back; however, the Flink engine still thinks the checkpoint/instant is successful.

Q: Why did we clean the messages prior to the 0.13.0 release?

A: To prevent inconsistencies between timeline and the messages.

Q: Why are we retaining the messages in the 0.13.0 release?

A: There are two cases for the inconsistency:

1. The timeline instant is complete but the ckp message is inflight (for committing instant).

2. The timeline instant is pending while the ckp message does not start (for starting a new instant).

For case 1, there is no need to re-commit the instant, and it is fine if the write task does not get any pending instant
when recovering.

For case 2, the instant is basically pending. The instant would be rolled back (as expected). Thus, keeping the ckp
messages as is can actually maintain correctness.

## Release Highlights

### Metaserver

In 0.13.0, we introduce Metaserver, a centralized metadata management service. This is one of the first platform service
components we introduce from many more to come. Metaserver helps users to easily manage a large number of tables in a
data lake platform.

:::caution
This is an ***EXPERIMENTAL*** feature.
:::

To set up the metaserver in your environment, use
[`hudi-metaserver-server-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-metaserver-server-bundle) and
run it as a java server application, like `java -jar hudi-metaserver-server-bundle-<HUDI_VERSION>.jar`. On the client
side, add the following options to integrate with the metaserver:
```
hoodie.metaserver.enabled=true
hoodie.metaserver.uris=thrift://<server url>:9090
```

The Metaserver stores Hudi tables' metadata like table name, database, owner; and the timeline's metadata like commit
instants, actions, states, etc. In addition, the Metaserver supports the Spark writer and reader through Hudi Spark
bundles.

### Change Data Capture

In cases where Hudi tables are used as streaming sources, we want to be aware of all changes for the records that belong
to a single commit.  For instance, we want to know which records were inserted, deleted and updated. For updated records,
the subsequent pipeline may want to get the old values before the update and the new ones after. Prior to 0.13.0, the
incremental query does not contain the hard-delete records, and users need to use soft deletes to stream deletes, which
may not meet the GDPR requirements.

The Change-Data-Capture (CDC) feature enables Hudi to show how records are changed by producing the changes and
therefore to handle CDC query use cases.

:::caution
CDC is an ***EXPERIMENTAL*** feature and is supported to work for COW tables with Spark and Flink engines. MOR tables
are not supported by CDC query yet.
:::

To use CDC, users need to enable it first while writing to a table to log extra data, which are returned by CDC
incremental queries.

For writing, set `hoodie.table.cdc.enabled=true` and specify CDC logging mode through `hoodie.datasource.query.incremental.format`,
to control the data being logged. There are 3 modes to choose from:

- **`data_before_after`**: This logs the changed records' operations and the whole record before and after the change. This
  mode incurs the most CDC data on storage and has the least computing efforts for querying CDC results.

- **`data_before`**: This logs the changed records' operations and the whole record before the change.

- **`op_key_only`**: This only logs the changed records' operations and key. This mode incurs the least CDC data on
  storage, and requires most computing efforts for querying CDC results.

The default value is **`data_before_after`**.

For reading, set:
```
hoodie.datasource.query.type=incremental
hoodie.datasource.query.incremental.format=cdc
```
and other usual [incremental query](/docs/quick-start-guide#incremental-query)'s options like begin and end instant
times, and CDC results are returned.

:::caution
Note that `hoodie.table.cdc.enabled` is a table configuration. Once it is enabled, it is not allowed to be turned off
for that table. Similarly, you cannot change `hoodie.table.cdc.supplemental.logging.mode`, once it's saved as a table
configuration.
:::

### Optimizing Record Payload handling

This release introduces the long-awaited support for handling records as their engine-native representations, therefore
avoiding the need to convert them to an intermediate one (Avro).

:::caution
This feature is in an ***EXPERIMENTAL*** mode and is currently only supported for Spark.
:::

This was made possible through RFC-46 by introducing a new
[`HoodieRecordMerger`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java)
abstraction. The `HoodieRecordMerger` is the core
and a source of truth for implementing any merging semantics in Hudi going forward. In this capacity, it replaces the
[`HoodieRecordPayload`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java)
hierarchy previously used for implementing custom merging semantics. By relying on the unified
component in the form of a `HoodieRecordMerger` allows us to handle records throughout the lifecycle of the write operation
in a uniform manner. This substantially reduces latency because the records are now held in the engine-native
representation, avoiding unnecessary copying, deserialization and conversion to the intermediate representation (Avro).
In our benchmarks, upsert performance improves in the ballpark of 10% against 0.13.0 default state and 20% when compared
to 0.12.2.

To try it today, you'd need to specify the configs differently for each Hudi table:

- For COW, specify `hoodie.datasource.write.record.merger.impls=org.apache.hudi.HoodieSparkRecordMerger`
- For MOR, specify `hoodie.datasource.write.record.merger.impls=org.apache.hudi.HoodieSparkRecordMerger` and
  `hoodie.logfile.data.block.format=parquet`

:::caution
Please note, that the current
[`HoodieSparkRecordMerger`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/HoodieSparkRecordMerger.java)
implementation only supports merging semantic equivalent to the
[`OverwriteWithLatestAvroPayload`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-common/src/main/java/org/apache/hudi/common/model/OverwriteWithLatestAvroPayload.java)
class, which is the default `HoodieRecordPayload` implementation used for merging records
currently (set as “hoodie.compaction.payload.class”). Therefore, if you're using any other `HoodieRecordPayload`
implementation, unfortunately, you'd need to wait until it is replaced by the corresponding `HoodieRecordMerger` implementation.
:::

### New Source Support in Deltastreamer

Deltastreamer is a fully-managed incremental ETL utility that supports a wide variety of
sources. In this release, we have added three new sources to its repertoire.

#### Proto Kafka Source

Deltastreamer already supports exactly-once ingestion of new events from Kafka using JSON and Avro formats.
[`ProtoKafkaSource`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/ProtoKafkaSource.java)
extends this support to Protobuf class-based schemas as well. With just one additional config, one
can easily set up this source.

#### GCS Incremental Source

Along the lines of [S3 events source](https://hudi.apache.org/blog/2021/08/23/s3-events-source), we now have a reliable
and fast way of ingesting from objects in Google Cloud Storage (GCS) through
[`GcsEventsHoodieIncrSource`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/GcsEventsHoodieIncrSource.java).
Check out the docs on how to set up this source.

#### Pulsar Source
[Apache Pulsar](https://pulsar.apache.org/) is an open-source, distributed messaging and streaming platform built for
the cloud. [`PulsarSource`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/PulsarSource.java)
supports ingesting from Apache Pulsar through the Deltastreamer.

### Support for Partial Payload Update

Partial update is a frequently asked use case from the community that requires the ability to update only certain fields
and not replace the whole record. Previously, we recommended users satisfy this use case by bringing in their own custom
record payload implementation.  With the popularity of this, in the 0.13.0 release, we added a new record payload
implementation,
[`PartialUpdateAvroPayload`](https://github.com/apache/hudi/blob/release-0.13.0/hudi-common/src/main/java/org/apache/hudi/common/model/PartialUpdateAvroPayload.java),
to support this out-of-the-box so users can use this implementation instead of having to write their own custom implementation.

### Consistent Hashing Index

We introduce the Consistent Hashing Index as yet another indexing option for your writes with Hudi. This is an
enhancement to [Bucket Index](/releases/release-0.11.0#bucket-index) which is added in the 0.11.0 release. With Bucket
Index, buckets/file groups per partition are statically allocated, whereas with Consistent Hashing Index, buckets can
grow dynamically and so users don't need to sweat about data skews. Buckets will expand and shrink depending on the load
factor for each partition. You can find the [RFC](https://github.com/apache/hudi/blob/master/rfc/rfc-42/rfc-42.md) for
the design of this feature.

Here are the configs of interest if you wish to give it a try.
```
hoodie.index.type=bucket
hoodie.index.bucket.engine=CONSISTENT_HASHING
hoodie.bucket.index.max.num.buckets=128
hoodie.bucket.index.min.num.buckets=32
hoodie.bucket.index.num.buckets=4
## do split if the bucket size reach 1.5 * max_file_size
hoodie.bucket.index.split.threshold=1.5
## do merge if the bucket size smaller than 0.2 * max_file_size
hoodie.bucket.index.merge.threshold=0.1 
```

To enforce shrinking or scaling up of buckets, you need to enable clustering using the following configs
```
## check resize for every 4 commit
hoodie.clustering.inline=true
hoodie.clustering.inline.max.commits=4
hoodie.clustering.plan.strategy.class=org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy
hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy
## for supporting concurrent write & resizing
hoodie.clustering.updates.strategy=org.apache.hudi.client.clustering.update.strategy.SparkConsistentBucketDuplicateUpdateStrategy
```

:::caution
Consistent Hashing Index is still an evolving feature and currently there are some limitations to use it as of 0.13.0:

- This index is supported only for Spark engine using a MOR table.
- It does not work with metadata table enabled.
- To scale up or shrink the buckets, users have to manually trigger clustering using above configs (at some cadence), but
  they cannot have compaction concurrently running.
- So, if compaction is enabled with your regular write pipeline, please follow this recommendation: You can choose to
  trigger the scale/shrink once every 12 hours. In such cases, once every 12 hours, you might need to disable compaction,
  stop your write pipeline and enable clustering. You should take extreme care to not run both concurrently because it
  might result in conflicts and a failed pipeline. Once clustering is complete, you can resume your regular write pipeline,
  which will have compaction enabled.


We are working towards automating these and making it easier for users to leverage the Consistent Hashing Index. You can
follow the ongoing work on the Consistent Hashing Index [here](https://issues.apache.org/jira/browse/HUDI-3000).

### Early Conflict Detection for Multi-Writer

Hudi provides Optimistic Concurrency Control (OCC) to allow multiple writers to concurrently write and atomically commit
to the Hudi table if there is no overlapping data file to be written, to guarantee data consistency, integrity and
correctness. Prior to the 0.13.0 release, such conflict detection of overlapping data files is performed before commit
metadata and after the data writing is completed. If any conflict is detected in the final stage, it could have wasted
compute resources because the data writing is finished already.

To improve the concurrency control, the 0.13.0 release introduces a new feature, early conflict detection in OCC, to
detect the conflict during the data writing phase and abort the writing early on once a conflict is detected, using
Hudi's marker mechanism.  Hudi can now stop a conflicting writer much earlier because of the early conflict detection
and release computing resources necessary to cluster, improving resource utilization.
  :::
  
:::caution
The early conflict detection in OCC is ***EXPERIMENTAL*** in 0.13.0 release.
:::

By default, this feature is turned off. To try this out, a user needs to set
`hoodie.write.concurrency.early.conflict.detection.enable` to **`true`**, when using OCC for concurrency control
(see the [Concurrency Control](/docs/concurrency_control) page for more details).

### Lock-Free Message Queue in Writing Data

In previous versions, Hudi writes incoming data into a table via a bounded in-memory queue using a producer-consumer
model. In this release, we added a new type of queue, leveraging [Disruptor](https://lmax-exchange.github.io/disruptor/user-guide/index.html),
which is lock-free. This increases the write throughput when data volume is large.
[The benchmark](https://github.com/apache/hudi/pull/5416) writing 100 million records to 1000 partitions in a Hudi table
on cloud storage shows **20%** performance improvement compared to the existing executor type of bounded in-memory queue.

:::caution
`DisruptorExecutor` is supported for Spark insert and Spark bulk insert operations as an ***EXPERIMENTAL*** feature
:::

Users can set `hoodie.write.executor.type=DISRUPTOR_EXECUTOR` to enable this feature. There are other configurations
like `hoodie.write.wait.strategy` and `hoodie.write.buffer.size` to tune the performance further.

### Hudi CLI Bundle

We introduce a new Hudi CLI Bundle, [`hudi-cli-bundle_2.12`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-cli-bundle),
for Spark 3.x to make Hudi CLI easier and more usable.  A user can now use this single bundle jar
(published to Maven repository) along with Hudi Spark bundle to start the script
to launch the Hudi-CLI shell with Spark. This brings ease of deployment for the Hudi-CLI as the user does not need to
compile the Hudi CLI module locally, upload jars and address any dependency conflict if there is any, which was the
case before this release. Detailed instructions can be found on the [Hudi CLI](/docs/cli) page.

### Support for Flink 1.16
Flink 1.16.x is integrated with Hudi, using profile param `-Pflink1.16` when compiling the source code to activate the
version. Alternatively, use [`hudi-flink1.16-bundle`](https://mvnrepository.com/artifact/org.apache.hudi/hudi-flink1.16-bundle).
Flink 1.15, Flink 1.14 and Flink 1.13 will continue to be supported. Please check the [migration guide](#new-flink-bundle)
for bundle updates.

### Json Schema Converter

For DeltaStreamer users who configure schema registry, a JSON schema converter is added to help convert JSON schema into
AVRO for the target Hudi table. Set `hoodie.deltastreamer.schemaprovider.registry.schemaconverter` to
`org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter` to use this feature. Users can also implement
this interface `org.apache.hudi.utilities.schema.SchemaRegistryProvider.SchemaConverter` to provide custom conversion
from the original schema to AVRO.

### Providing Hudi Configs via Spark SQL Config

Users can now provide Hudi configs via Spark SQL conf, for example, setting
```
spark.sql("set hoodie.sql.bulk.insert.enable = true")
```
to make sure Hudi is able to use `BULK_INSERT` operation when executing `INSERT INTO` statement.


## Known Regressions

We discovered a regression in Hudi 0.13 release related to metadata table and timeline server interplay with streaming ingestion pipelines.

The FileSystemView that Hudi maintains internally could go out of sync due to a occasional race conditions when table services are involved
(compaction, clustering) and could result in updates and deletes routed to older file versions and hence resulting in missed updates and deletes.

Here are the user-flows that could potentially be impacted with this.

- This impacts pipelines using Deltastreamer in **continuous mode** (sync once is not impacted), Spark streaming, or if you have been directly
  using write client across batches/commits instead of the standard ways to write to Hudi. In other words, batch writes should not be impacted.
- Among these write models, this could have an impact only when table services are enabled.
    - COW: clustering enabled (inline or async)
    - MOR: compaction enabled (by default, inline or async)
- Also, the impact is applicable only when metadata table is enabled, and timeline server is enabled (which are defaults as of 0.13.0)

Based on some production data, we expect this issue might impact roughly < 1% of updates to be missed, since its a race condition
and table services are generally scheduled once every N commits. The percentage of update misses could be even less if the
frequency of table services is less.

[Here](https://issues.apache.org/jira/browse/HUDI-5863) is the jira for the issue of interest and the fix has already been landed in master.
Next minor release(0.13.1) should have the [fix](https://github.com/apache/hudi/pull/8079). Until we have a next minor release with the fix, we recommend you to disable metadata table
(`hoodie.metadata.enable=false`) to mitigate the issue.

We also discovered a regression for Flink streaming writer with the hive meta sync which is introduced by HUDI-3730, the refactoring to `HiveSyncConfig`
causes the Hive `Resources` config objects leaking, which finally leads to an OOM exception for the JobManager if the streaming job runs continuously for weeks.
Next minor release(0.13.1) should have the [fix](https://github.com/apache/hudi/pull/8050). Until we have a 0.13.1 release, we recommend you to cherry-pick the fix to local
if hive meta sync is required.

Sorry about the inconvenience caused.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12352101).
