---
title: "Older Releases"
sidebar_position: 6
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---

This page contains older release information, for bookkeeping purposes. It's recommended that you upgrade to one of the 
more recent releases listed [here](/releases/download)

## [Release 0.12.1](https://github.com/apache/hudi/releases/tag/release-0.12.1) ([docs](/docs/0.12.1/quick-start-guide))

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  The latest 0.12 release is [0.12.2](/releases/release-0.12.2).

## Migration Guide

* This release (0.12.1) does not introduce any new table version, thus no migration is needed if you are on 0.12.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/older-releases#release-060-docs),
  [0.9.0](/releases/older-releases#release-090-docs), [0.10.0](/releases/older-releases#release-0100-docs),
  [0.11.0](/releases/older-releases#release-0110-docs), and [0.12.0](/releases/older-releases#release-0120-docs).


## Release Highlights

### Improve Hudi Cli

Add command to repair deprecated partition, rename partition and trace file group through a range of commits.

### Fix invalid record key stats in Parquet metadata

Crux of the problem was that min/max statistics for the record keys were computed incorrectly during (Spark-specific) row-writing
Bulk Insert operation affecting Key Range Pruning flow w/in Hoodie Bloom Index tagging sequence, resulting into updated records being incorrectly tagged
as "inserts" and not as "updates", leading to duplicated records in the table.

If all of the following is applicable to you:

1. Using Spark as an execution engine
2. Using Bulk Insert (using row-writing
   <https://hudi.apache.org/docs/next/configurations#hoodiedatasourcewriterowwriterenable>,
   enabled *by default*)
3. Using Bloom Index (with range-pruning
   <https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges>
   enabled, enabled *by default*) for "UPSERT" operations

Recommended to upgrading to 0.12.1 to avoid getting duplicate records in your pipeline.

### Bug fixes

0.12.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including
* DeltaStreamer
* Table config
* Table services
* Metadata table
* Spark SQL support
* Presto support
* Hive Sync
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions

We discovered a regression in Hudi 0.12.1 release related to metadata table and timeline server interplay with streaming ingestion pipelines.

The FileSystemView that Hudi maintains internally could go out of sync due to a occasional race conditions when table services are involved
(compaction, clustering) and could result in updates and deletes routed to older file versions and hence resulting in missed updates and deletes.

Here are the user-flows that could potentially be impacted with this.

- This impacts pipelines using Deltastreamer in **continuous mode** (sync once is not impacted), Spark streaming, or if you have been directly
  using write client across batches/commits instead of the standard ways to write to Hudi. In other words, batch writes should not be impacted.
- Among these write models, this could have an impact only when table services are enabled.
  - COW: clustering enabled (inline or async)
  - MOR: compaction enabled (by default, inline or async)
- Also, the impact is applicable only when metadata table is enabled, and timeline server is enabled (which are defaults as of 0.12.1)

Based on some production data, we expect this issue might impact roughly < 1% of updates to be missed, since its a race condition
and table services are generally scheduled once every N commits. The percentage of update misses could be even less if the
frequency of table services is less.

[Here](https://issues.apache.org/jira/browse/HUDI-5863) is the jira for the issue of interest and the fix has already been landed in master.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8079). Until we have a 0.12.3 release, we recommend you to disable metadata table
(`hoodie.metadata.enable=false`) to mitigate the issue.

We also discovered a regression for Flink streaming writer with the hive meta sync which is introduced by HUDI-3730, the refactoring to `HiveSyncConfig`
causes the Hive `Resources` config objects leaking, which finally leads to an OOM exception for the JobManager if the streaming job runs continuously for weeks.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8050). Until we have a 0.12.3 release, we recommend you to cherry-pick the fix to local
if hive meta sync is required.

Sorry about the inconvenience caused.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12352182)

## [Release 0.12.0](https://github.com/apache/hudi/releases/tag/release-0.12.0) ([docs](/docs/0.12.0/quick-start-guide))

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  The latest 0.12 release is [0.12.2](/releases/release-0.12.2).

## Migration Guide

In this release, there have been a few API and configuration updates listed below that warranted a new table version.
Hence, the latest [table version](https://github.com/apache/hudi/blob/bf86efef719b7760ea379bfa08c537431eeee09a/hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java#L41)
is `5`. For existing Hudi tables on older version, a one-time upgrade step will be executed automatically. Please take
note of the following updates before upgrading to Hudi 0.12.0.

### Configuration Updates

In this release, the default value for a few configurations have been changed. They are as follows:

- `hoodie.bulkinsert.sort.mode`: This config is used to determine mode for sorting records for bulk insert. Its default value has been changed from `GLOBAL_SORT` to `NONE`, which means no sorting is done and it matches `spark.write.parquet()` in terms of overhead.
- `hoodie.datasource.hive_sync.partition_value_extractor`: This config is used to extract and transform partition value during Hive sync. Its default value has been changed from `SlashEncodedDayPartitionValueExtractor` to `MultiPartKeysValueExtractor`. If you relied on the previous default value (i.e., have not set it explicitly), you are required to set the config to `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor`. From this release, if this config is not set and Hive sync is enabled, then partition value extractor class will be **automatically inferred** on the basis of number of partition fields and whether or not hive style partitioning is enabled.
- The following configs will be inferred, if not set manually, from other configs' values:
  - `META_SYNC_BASE_FILE_FORMAT`: infer from `org.apache.hudi.common.table.HoodieTableConfig.BASE_FILE_FORMAT`

  - `META_SYNC_ASSUME_DATE_PARTITION`: infer from `org.apache.hudi.common.config.HoodieMetadataConfig.ASSUME_DATE_PARTITIONING`

  - `META_SYNC_DECODE_PARTITION`: infer from `org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING`

  - `META_SYNC_USE_FILE_LISTING_FROM_METADATA`: infer from `org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE`

### API Updates

In `SparkKeyGeneratorInterface`, return type of the `getRecordKey` API has been changed from String to UTF8String.
```java
// Before
String getRecordKey(InternalRow row, StructType schema); 


// After
UTF8String getRecordKey(InternalRow row, StructType schema); 
```

### Fallback Partition

If partition field value was null, Hudi has a fallback mechanism instead of failing the write. Until 0.9.0,
`__HIVE_DEFAULT_PARTITION__`  was used as the fallback partition. After 0.9.0, due to some refactoring, fallback
partition changed to `default`. This default partition does not sit well with some of the query engines. So, we are
switching the fallback partition to `__HIVE_DEFAULT_PARTITION__`  from 0.12.0. We have added an upgrade step where in,
we fail the upgrade if the existing Hudi table has a partition named `default`. Users are expected to rewrite the data
in this partition to a partition named [\_\_HIVE_DEFAULT_PARTITION\_\_](https://github.com/apache/hudi/blob/0d0a4152cfd362185066519ae926ac4513c7a152/hudi-common/src/main/java/org/apache/hudi/common/util/PartitionPathEncodeUtils.java#L29).
However, if you had intentionally named your partition as `default`, you can bypass this using the config `hoodie.skip.default.partition.validation`.

### Bundle Updates

- `hudi-aws-bundle` extracts away aws-related dependencies from hudi-utilities-bundle or hudi-spark-bundle. In order to use features such as Glue sync, Cloudwatch metrics reporter or DynamoDB lock provider, users need to provide hudi-aws-bundle jar along with hudi-utilities-bundle or hudi-spark-bundle jars.
- Spark 3.3 support is added; users who are on Spark 3.3 can use `hudi-spark3.3-bundle` or `hudi-spark3-bundle` (legacy bundle name).
- Spark 3.2 will continue to be supported via `hudi-spark3.2-bundle`.
- Spark 3.1 will continue to be supported via `hudi-spark3.1-bundle`.
- Spark 2.4 will continue to be supported via `hudi-spark2.4-bundle` or `hudi-spark-bundle` (legacy bundle name).
- Flink 1.15 support is added; users who are on Flink 1.15 can use `hudi-flink1.15-bundle`.
- Flink 1.14 will continue to be supported via `hudi-flink1.14-bundle`.
- Flink 1.13 will continue to be supported via `hudi-flink1.13-bundle`.

## Release Highlights

### Presto-Hudi Connector

Since version 0.275 of PrestoDB, users can now leverage native Hudi connector to query Hudi table.
It is on par with Hudi support in the Hive connector. To learn more about the usage of the connector,
please checkout [prestodb documentation](https://prestodb.io/docs/current/connector/hudi.html).

### Archival Beyond Savepoint

Hudi supports savepoint and restore feature that is useful for backup and disaster recovery scenarios. More info can be
found [here](https://hudi.apache.org/docs/disaster_recovery). Until 0.12.0, archival for a given table will not make
progress beyond the first savepointed commit. But there has been ask from the community to relax this constraint so that
some coarse grained commits can be retained in the active timeline and execute point in time queries. So, with 0.12.0,
users can now let archival proceed beyond savepoint commits by enabling `hoodie.archive.beyond.savepoint` write
configuration. This unlocks new opportunities for Hudi users. For example, one can retain commits for years, by adding
one savepoint per day for older commits (lets say > 30 days). And query hudi table using "as.of.instant" with any older
savepointed commit. By this, Hudi does not need to retain every commit in the active timeline for older commits.

:::note
However, if this feature is enabled, restore cannot be supported. This limitation would be relaxed in a future release
and the development of this feature can be tracked in [HUDI-4500](https://issues.apache.org/jira/browse/HUDI-4500).
:::

### File system based Lock Provider

For multiple writers using optimistic concurrency control, Hudi already supports lock providers based on
Zookeeper, Hive Metastore or Amazon DynamoDB. In this release, there is a new file system based lock provider. Unlike the
need for external systems in other lock providers, this implementation acquires/releases a lock based on atomic
create/delete operations of the underlying file system. To use this lock provider, users need to set the following
minimal configurations (please check the [lock configuration](/docs/configurations#Locks-Configurations) for a few
other optional configs that can be used):
```
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
```

### Deltastreamer Termination Strategy

Users can now configure a post-write termination strategy with deltastreamer `continuous` mode if need be. For instance,
users can configure graceful shutdown if there is no new data from source for 5 consecutive times. Here is the interface
for the termination strategy.
```java
/**
 * Post write termination strategy for deltastreamer in continuous mode.
 */
public interface PostWriteTerminationStrategy {

  /**
   * Returns whether deltastreamer needs to be shutdown.
   * @param scheduledCompactionInstantAndWriteStatuses optional pair of scheduled compaction instant and write statuses.
   * @return true if deltastreamer has to be shutdown. false otherwise.
   */
  boolean shouldShutdown(Option<Pair<Option<String>, JavaRDD<WriteStatus>>> scheduledCompactionInstantAndWriteStatuses);

}
```

Also, this might help in bootstrapping a new table. Instead of doing one bulk load or bulk_insert leveraging a large
cluster for a large input of data, one could start deltastreamer on continuous mode and add a shutdown strategy to
terminate, once all data has been bootstrapped. This way, each batch could be smaller and may not need a large cluster
to bootstrap data. We have one concrete implementation out of the box, [NoNewDataTerminationStrategy](https://github.com/apache/hudi/blob/0d0a4152cfd362185066519ae926ac4513c7a152/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/NoNewDataTerminationStrategy.java).
Users can feel free to implement their own strategy as they see fit.

### Spark 3.3 Support

Spark 3.3 support is added; users who are on Spark 3.3 can use `hudi-spark3.3-bundle` or `hudi-spark3-bundle`. Spark 3.2,
Spark 3.1 and Spark 2.4 will continue to be supported. Please check the migration guide for [bundle updates](#bundle-updates).

### Spark SQL Support Improvements

- Support for upgrade, downgrade, bootstrap, clean, rollback and repair through `Call Procedure` command.
- Support for `analyze table`.
- Support for `Create/Drop/Show/Refresh Index` syntax through Spark SQL.

### Flink 1.15 Support

Flink 1.15.x is integrated with Hudi, use profile param `-Pflink1.15` when compiling the codes to adapt the version.
Alternatively, use `hudi-flink1.15-bundle`. Flink 1.14 and Flink 1.13 will continue to be supported. Please check the
migration guide for [bundle updates](#bundle-updates).

### Flink Integration Improvements

- **Data skipping** is supported for batch mode read, set up SQL option `metadata.enabled`, `hoodie.metadata.index.column.stats.enable`  and `read.data.skipping.enabled` as true to enable it.
- A **HMS-based Flink catalog** is added with catalog identifier as `hudi`. You can instantiate the catalog through API directly or use the `CREATE CATALOG`  syntax to create it. Specifies catalog option `'mode' = 'hms'`  to switch to the HMS catalog. By default, the catalog is in `dfs` mode.
- **Async clustering** is supported for Flink `INSERT` operation, set up SQL option `clustering.schedule.enabled` and `clustering.async.enabled` as true to enable it. When enabling this feature, a clustering sub-pipeline is scheduled asynchronously continuously to merge the small files continuously into larger ones.

### Performance Improvements

This version brings more improvements to make Hudi the most performant lake storage format. Some notable improvements are:
- Closed the performance gap in writing through Spark datasource vs sql. Previously, datasource writes were faster.
- All built-in key generators implement more performant Spark-specific APIs.
- Replaced UDF in bulk insert operation with RDD transformation to cut down serde cost.
- Optimized column stats index performance in data skipping.

We recently benchmarked Hudi against TPC-DS workload.
Please check out [our blog](/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks) for more details.

## Known Regressions:

We discovered a regression in Hudi 0.12 release related to Bloom
Index metadata persisted w/in Parquet footers [HUDI-4992](https://issues.apache.org/jira/browse/HUDI-4992).

Crux of the problem was that min/max statistics for the record keys were
computed incorrectly during (Spark-specific) [row-writing](https://hudi.apache.org/docs/next/configurations#hoodiedatasourcewriterowwriterenable)
Bulk Insert operation affecting [Key Range Pruning flow](https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges)
w/in [Hoodie Bloom Index](https://hudi.apache.org/docs/next/faq/#how-do-i-configure-bloom-filter-when-bloomglobal_bloom-index-is-used)
tagging sequence, resulting into updated records being incorrectly tagged
as "inserts" and not as "updates", leading to duplicated records in the
table.

[PR#6883](https://github.com/apache/hudi/pull/6883) addressing the problem is incorporated into
Hudi 0.12.1 release.*

If all of the following is applicable to you:

1. Using Spark as an execution engine
2. Using Bulk Insert (using [row-writing](https://hudi.apache.org/docs/next/configurations#hoodiedatasourcewriterowwriterenable),
   enabled *by default*)
3. Using Bloom Index (with [range-pruning](https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges)
   enabled, enabled *by default*) for "UPSERT" operations
  - Note: Default index type is SIMPLE. So, unless you have over-ridden the index type, you may not hit this issue.

Please consider one of the following potential remediations to avoid
getting duplicate records in your pipeline:

- [Disabling Bloom Index range-pruning](https://hudi.apache.org/docs/next/basic_configurations/#hoodiebloomindexprunebyranges)
  flow (might
  affect performance of upsert operations)
- Upgrading to 0.12.1.
- Making sure that the [fix](https://github.com/apache/hudi/pull/6883) is
  included in your custom artifacts (if you're building and using ones)

We also found another regression related to metadata table and timeline server interplay with streaming ingestion pipelines.

The FileSystemView that Hudi maintains internally could go out of sync due to a occasional race conditions when table services are involved
(compaction, clustering) and could result in updates and deletes routed to older file versions and hence resulting in missed updates and deletes.

Here are the user-flows that could potentially be impacted with this.

- This impacts pipelines using Deltastreamer in **continuous mode** (sync once is not impacted), Spark streaming, or if you have been directly
  using write client across batches/commits instead of the standard ways to write to Hudi. In other words, batch writes should not be impacted.
- Among these write models, this could have an impact only when table services are enabled.
  - COW: clustering enabled (inline or async)
  - MOR: compaction enabled (by default, inline or async)
- Also, the impact is applicable only when metadata table is enabled, and timeline server is enabled (which are defaults as of 0.12.0)

Based on some production data, we expect this issue might impact roughly < 1% of updates to be missed, since its a race condition
and table services are generally scheduled once every N commits. The percentage of update misses could be even less if the
frequency of table services is less.

[Here](https://issues.apache.org/jira/browse/HUDI-5863) is the jira for the issue of interest and the fix has already been landed in master.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8079). Until we have a 0.12.3 release, we recommend you to disable metadata table
(`hoodie.metadata.enable=false`) to mitigate the issue.

We also discovered a regression for Flink streaming writer with the hive meta sync which is introduced by HUDI-3730, the refactoring to `HiveSyncConfig`
causes the Hive `Resources` config objects leaking, which finally leads to an OOM exception for the JobManager if the streaming job runs continuously for weeks.
0.12.3 should have the [fix](https://github.com/apache/hudi/pull/8050). Until we have a 0.12.3 release, we recommend you to cherry-pick the fix to local
if hive meta sync is required.

Sorry about the inconvenience caused.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351209).

## [Release 0.11.0](https://github.com/apache/hudi/releases/tag/release-0.11.0) ([docs](/docs/0.11.0/quick-start-guide))

## Migration Guide

### Bundle usage updates

- Spark bundle for 3.0.x is no longer officially supported. Users are encouraged to upgrade to Spark 3.2 or 3.1.
- Users are encouraged to use bundles with specific Spark version in the name (`hudi-sparkX.Y-bundle`) and move away
  from the legacy bundles (`hudi-spark-bundle` and `hudi-spark3-bundle`).
- Spark or Utilities bundle no longer requires additional `spark-avro` package at runtime; the
  option `--package org.apache.spark:spark-avro_2.1*:*` can be dropped.

### Configuration updates

- For MOR tables, `hoodie.datasource.write.precombine.field` is required for both write and read.
- Only set `hoodie.datasource.write.drop.partition.columns=true` when work
  with [BigQuery integration](/docs/gcp_bigquery).
- For Spark readers that rely on extracting physical partition path,
  set `hoodie.datasource.read.extract.partition.values.from.path=true` to stay compatible with existing behaviors.
- Default index type for Spark was changed from `BLOOM`
  to `SIMPLE` ([HUDI-3091](https://issues.apache.org/jira/browse/HUDI-3091)). If you currently rely on the default `BLOOM`
  index type, please update your configuration accordingly.

## Release Highlights

### Multi-Modal Index

In 0.11.0, we enable the [metadata table](/docs/metadata) with synchronous updates and metadata-table-based file listing
by default for Spark writers, to improve the performance of partition and file listing on large Hudi tables. On the
reader side, users need to set it to `true` benefit from it. The metadata table and related file listing functionality
can still be turned off by setting `hoodie.metadata.enable=false`. Due to this, users deploying Hudi with async table
services need to configure a locking service. If this feature is not relevant for you, you can set
`hoodie.metadata.enable=false` additionally and use Hudi as before.

We introduce a multi-modal index in metadata table to drastically improve the lookup performance in file index and query
latency with data skipping. Two new indices are added to the metadata table

1. bloom filter index containing the file-level bloom filter to facilitate key lookup and file pruning as a part of
   bloom index during upserts by the writers
2. column stats index containing the statistics of all/interested columns to improve file pruning based on key and
   column value range in both the writer and the reader, in query planning in Spark for example.

They are disabled by default. You can enable them by setting `hoodie.metadata.index.bloom.filter.enable`
and `hoodie.metadata.index.column.stats.enable` to `true`, respectively.

*Refer to the [metadata table guide](/docs/metadata#deployment-considerations) for detailed instructions on upgrade and
deployment.*

### Data Skipping with Metadata Table

With the added support for Column Statistics in metadata table, Data Skipping is now relying on the metadata table's
Column Stats Index (CSI) instead of its own bespoke index implementation (comparing to Spatial Curves added in 0.10.0),
allowing to leverage Data Skipping for all datasets regardless of whether they execute layout optimization procedures (
like clustering) or not. To benefit from Data Skipping, make sure to set `hoodie.enable.data.skipping=true` on both
writer and reader, as well as enable metadata table and Column Stats Index in the metadata table.

Data Skipping supports standard functions (as well as some common expressions) allowing you to apply common standard
transformations onto the raw data in your columns within your query's filters. For example, if you have column "ts" that
stores timestamp as string, you can now query it using human-readable dates in your predicate like
following: `date_format(ts, "MM/dd/yyyy" ) < "04/01/2022"`.

*Note: Currently Data Skipping is only supported in COW tables and MOR tables in read-optimized mode. The work of full
support for MOR tables is tracked in [HUDI-3866](https://issues.apache.org/jira/browse/HUDI-3866)*

*Refer to the [performance](/docs/performance#read-path) guide for more info.*

### Async Indexer

In 0.11.0, we added a new asynchronous service for indexing to our rich set of table services. It allows users to create
different kinds of indices (e.g., files, bloom filters, and column stats) in the metadata table without blocking
ingestion. The indexer adds a new action `indexing` on the timeline. While the indexing process itself is asynchronous
and non-blocking to writers, a lock provider needs to be configured to safely co-ordinate the process with the inflight
writers.

*See the [indexing guide](/docs/metadata_indexing) for more details.*

### Spark DataSource Improvements

Hudi's Spark low-level integration got considerable overhaul consolidating common flows to share the infrastructure and
bring both compute and data throughput efficiencies when querying the data.

- MOR queries with no log files (except for incremental queries) tables are now leveraging Vectorized Parquet reader while reading
  the data, meaning that Parquet reader is now able to leverage modern processors vectorized instructions to further
  speed up decoding of the data. Enabled by default.
- When standard Record Payload implementation is used (e.g., `OverwriteWithLatestAvroPayload`), MOR table will only
  fetch *strictly necessary* columns (primary key, pre-combine key) on top of those referenced by the query,
  substantially reducing wasted data throughput as well as compute spent on decompressing and decoding the data. This is
  significantly beneficial to "wide" MOR tables with 1000s of columns, for example.

*See the [migration guide](#migration-guide) for the relevant configuration updates.*

### Schema-on-read for Spark

In 0.11.0, users can now easily change the current schema of a Hudi table to adapt to the evolving data schema over
time. Spark SQL DDL support (experimental) was added for Spark 3.1.x and Spark 3.2.1 via `ALTER TABLE` syntax.

*Please refer to the [schema evolution guide](/docs/schema_evolution) for more details.*

### Spark SQL Improvements

- Users can update or delete records in Hudi tables using non-primary-key fields.
- Time travel query is now supported via `timestamp as of` syntax. (Spark 3.2+ only)
- `CALL` command is added to support invoking more actions on Hudi tables.

*Please refer to the [Quick Start - Spark Guide](/docs/quick-start-guide) for more details and examples.*

### Spark Versions and Bundles

- Spark 3.2 support is added; users who are on Spark 3.2 can use `hudi-spark3.2-bundle` or `hudi-spark3-bundle` (legacy bundle name).
- Spark 3.1 will continue to be supported via `hudi-spark3.1-bundle`.
- Spark 2.4 will continue to be supported via `hudi-spark2.4-bundle` or `hudi-spark-bundle` (legacy bundle name).

*See the [migration guide](#migration-guide) for usage updates.*

### Slim Utilities Bundle

In 0.11.0, a new `hudi-utilities-slim-bundle` is added to exclude dependencies that could cause conflicts and
compatibility issues with other frameworks such as Spark. `hudi-utilities-slim-bundle` is to work with a chosen Spark
bundle:

- `hudi-utilities-slim-bundle` works with Spark 3.1 and 2.4.
- `hudi-utilities-bundle` continues to work with Spark 3.1 as it does in Hudi 0.10.x.

### Flink Integration Improvements

- In 0.11.0, both Flink 1.13.x and 1.14.x are supported.
- Complex data types such as `Map` and `Array` are supported. Complex data types can be nested in another component data
  type.
- A DFS-based Flink catalog is added with catalog identifier as `hudi`. You can instantiate the catalog through API
  directly or use the `CREATE CATALOG` syntax to create it.
- Flink supports [Bucket Index](#bucket-index) in normal `UPSERT` and `BULK_INSERT` operations. Different from the
  default Flink state-based index, bucket index is in constant number of buckets. Specify SQL option `index.type`
  as `BUCKET` to enable it.

### Google BigQuery Integration

In 0.11.0, Hudi tables can be queried from BigQuery as external tables. Users can
set `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` as the sync tool implementation for `HoodieDeltaStreamer` and make
the target Hudi table discoverable in BigQuery. Please refer to the [BigQuery integration](/docs/gcp_bigquery) guide
page for more details.

*Note: this is an experimental feature and only works with hive-style partitioned Copy-On-Write tables.*

### AWS Glue Meta Sync

In 0.11.0, Hudi tables can be sync'ed to AWS Glue Data Catalog via AWS SDK directly. Users can
set `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` as the sync tool implementation for `HoodieDeltaStreamer` and make
the target Hudi table discoverable in Glue catalog. Please refer
to [Sync to AWS Glue Data Catalog](/docs/syncing_aws_glue_data_catalog) guide page for more details.

*Note: this is an experimental feature.*

### DataHub Meta Sync

In 0.11.0, Hudi table's metadata (specifically, schema and last sync commit time) can be sync'ed
to [DataHub](https://datahubproject.io/). Users can set `org.apache.hudi.sync.datahub.DataHubSyncTool` as the sync tool
implementation for `HoodieDeltaStreamer` and sync the target table as a Dataset in DataHub. Please refer
to [Sync to DataHub](/docs/syncing_datahub) guide page for more details.

*Note: this is an experimental feature.*

### Encryption

In 0.11.0, Spark 3.2 support has been added and accompanying that, Parquet 1.12 has been included, which brings
encryption feature to Hudi (Copy-on-Write tables). Please refer to [Encryption](/docs/encryption) guide page for more
details.

### Bucket Index

Bucket index, an efficient and light-weight index type, is added in 0.11.0. It distributes records to buckets using a
hash function based on the record keys, where each bucket corresponds to a single file group. To use this index, set the
index type to `BUCKET` and set `hoodie.storage.layout.partitioner.class` to `org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner`.
For Flink, set `index.type=BUCKET`.

*For more details, please refer to hoodie.bucket.index.\* in the [configurations page](/docs/configurations).*

### Savepoint & Restore

Disaster recovery is a mission critical feature in any production deployment. Especially when it comes to systems that
store data. Hudi had savepoint and restore functionality right from the beginning for COW tables. In 0.11.0, we have
added support for MOR tables.

*More info about this feature can be found in [Disaster Recovery](/docs/disaster_recovery).*

### Pulsar Write Commit Callback

Hudi users can use `org.apache.hudi.callback.HoodieWriteCommitCallback` to invoke callback function upon successful
commits. In 0.11.0, we add `HoodieWriteCommitPulsarCallback` in addition to the existing HTTP callback and Kafka
callback. Please refer to the [configurations page](/docs/configurations#Write-commit-pulsar-callback-configs) for
detailed settings.

### HiveSchemaProvider

In 0.11.0, `org.apache.hudi.utilities.schema.HiveSchemaProvider` is added for getting schema from user-defined hive
tables. This is useful when tailing Hive tables in `HoodieDeltaStreamer` instead of having to provide avro schema files.

## Known Regression

In 0.11.0 release, with the newly added support for Spark SQL features, the following performance regressions were
inadvertently introduced:
* Partition pruning for some of the COW tables is not applied properly
* Spark SQL query caching (which caches parsed and resolved queries) was not working correctly resulting in additional
* overhead to re-analyze the query every time when it's executed (listing the table contents, etc.)

All of these issues have been addressed in 0.11.1 and are validated to be resolved by benchmarking the set of changes
on TPC-DS against 0.10.1.

In HUDI-2761, HUDI-3576 and HUDI-4279, we did several attempts to optimize the efficiency of embedded timeline server,
but in some cases, these changes would cause silent data loss, the affected table types include both COW and MOR table,
this bug has been addressed in release 0.12.0.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350673)

## [Release 0.10.0](https://github.com/apache/hudi/releases/tag/release-0.10.0) ([docs](/docs/0.10.0/quick-start-guide))

## Migration Guide
- If migrating from an older release, please also check the upgrade instructions for each subsequent release below.
- With 0.10.0, we have made some foundational fix to metadata table and so as part of upgrade, any existing metadata table is cleaned up.
  Whenever Hudi is launched with newer table version i.e 3 (or moving from an earlier release to 0.10.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the hoodie.table.version will be updated in property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - downgrade) is added if in case some users want to downgrade Hudi
  from table version 3 to 2 or move from Hudi 0.10.0 to pre 0.10.0. This needs to be executed from a 0.10.0 hudi-cli binary/script.
- We have made some major fixes to 0.10.0 release around metadata table and would recommend users to try out metadata
  for better performance from optimized file listings. As part of the upgrade, please follow the below steps to enable metadata table.

### Prerequisites for enabling metadata table

Hudi writes and reads have to perform “list files” operation on the file system to get the current view of the system.
This could be very costly in cloud stores which could throttle your requests depending on the scale/size of your dataset.
So, we introduced a metadata table in 0.7.0 to cache the file listing for the table. With 0.10.0, we have made a foundational fix
to the metadata table with synchronous updates instead of async updates to simplify the overall design and to assist in
building future enhancements like multi-modal indexing. This can be turned on using the config hoodie.metadata.enable.
By default, metadata table based file listing feature is disabled.

**Deployment Model** 1 : If your current deployment model is single writer and all table services (cleaning, clustering, compaction) are configured to be **inline**,
then you can turn on the metadata table without needing any additional configuration.

**Deployment Model** 2 : If your current deployment model is [multi writer](https://hudi.apache.org/docs/concurrency_control)
along with [lock providers](https://hudi.apache.org/docs/concurrency_control#enabling-multi-writing) configured,
then you can turn on the metadata table without needing any additional configuration.

**Deployment Model 3** : If your current deployment model is single writer along with async table services (such as cleaning, clustering, compaction) configured,
then it is a must to have the lock providers configured before turning on the metadata table.
Even if you have already had a metadata table turned on, and your deployment model employs async table services,
then it is  a must to have lock providers configured before upgrading to this release.

### Upgrade steps

For deployment mode 1, restarting the Single Writer with 0.10.0 is sufficient to upgrade the table.

For deployment model 2 with multi-writers, you can bring up the writers with 0.10.0 sequentially.
If you intend to use the metadata table, it is a must to have the [metadata config](https://hudi.apache.org/docs/configurations/#hoodiemetadataenable) enabled across all the writers.
Otherwise, it will lead to loss of data from the inconsistent writer.

For deployment model 3 with single writer and async table services, restarting the single writer along with async services is sufficient to upgrade the table.
If async services are configured to run separately from the writer, then it is a must to have a consistent metadata config across all writers and async jobs.
Remember to configure the lock providers as detailed above if enabling the metadata table.

To leverage the metadata table based file listings, readers must have metadata config turned on explicitly while querying.
If not, readers will not leverage the file listings from the metadata table.

### Spark-SQL primary key requirements

Spark SQL in Hudi requires `primaryKey` to be specified by tblproperites or options in sql statement.
For update and delete operations, `preCombineField` is required as well. These requirements align with
Hudi datasource writer’s and the alignment resolves many behavioural discrepancies reported in previous versions.

To specify `primaryKey`, `preCombineField` or other hudi configs, `tblproperties` is a preferred way than `options`.
Spark SQL syntax is detailed [DDL CREATE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html).
In summary, any Hudi table created pre 0.10.0 without a `primaryKey` need to be recreated with a `primaryKey` field with 0.10.0.
We plan to remove the need for primary keys in future versions more holistically.

## Release Highlights

### Kafka Connect

In 0.10.0, we are adding a Kafka Connect Sink for Hudi that provides the ability for users to ingest/stream records from Apache Kafka to Hudi Tables.
While users can already stream Kafka records into Hudi tables using deltastreamer, the Kafka connect sink offers greater flexibility
to current users of Kafka connect sinks such as S3, HDFS, etc to additionally sink their data to a data lake.
It also helps users who do not want to deploy and operate spark.  The sink is currently experimental,
and users can quickly get started by referring to the detailed steps in the [README](https://github.com/apache/hudi/blob/master/hudi-kafka-connect/README.md).
For users who are curious about the internals, you can refer to the [RFC](https://cwiki.apache.org/confluence/display/HUDI/RFC-32+Kafka+Connect+Sink+for+Hudi).

### Z-Ordering, Hilbert Curves and Data Skipping

In 0.10.0 we’re bringing (in experimental mode) support for indexing based on space-filling curves ordering initially
supporting [Z-order](https://en.wikipedia.org/wiki/Z-order_curve) and [Hilbert curves](https://en.wikipedia.org/wiki/Hilbert_curve).

Data skipping is crucial in optimizing query performance. Enabled by the Column Statistics Index containing column level stats (like min, max, number of nulls, etc)
for individual data files, allows to quickly prune (for some queries) the search space by excluding files that are guaranteed
not to contain the values of interest for the query in question. Effectiveness of Data Skipping is maximized
when data is globally ordered by the columns, allowing individual Parquet files to contain disjoint ranges of values,
allowing for more effective pruning.

Using space-filling curves (like Z-order, Hilbert, etc) allows to effectively order rows in your table based on sort-key
comprising multiple columns, while preserving very important property: ordering of the rows using space-filling curve
on multi-column key will preserve within itself the ordering by each individual column as well.
This property comes very handy in use-cases when rows need to be ordered by complex multi-column sort keys,
which need to be queried efficiently by any subset of they key (not necessarily key’s prefix), making space-filling curves stand out
from plain and simple linear (or lexicographical) multi-column ordering. Using space-filling curves in such use-cases might bring order(s)
of magnitude speed-up in your queries' performance by considerably reducing search space, if applied appropriately.

These features are currently experimental, we’re planning to dive into more details showcasing real-world application
of the space-filling curves in a blog post soon.

### Debezium Deltastreamer sources

We have added two new debezium sources to our deltastreamer ecosystem. Debezium is an open source distributed platform for change data capture(CDC).
We have added PostgresDebeziumSource and MysqlDebeziumSource to route CDC logs into Apache Hudi via deltastreamer for postgres and my sql db respectively.
With this capability, we can continuously capture row-level changes that insert, update and delete records that were committed to a database and ingest to hudi.

### External config file support

Instead of directly and sometimes passing configurations to every Hudi job, since 0.10.0 users can now pass in configuration via a configuration file `hudi-default.conf`.
By default, Hudi would load the configuration file under /etc/hudi/conf directory. You can specify a different configuration directory location
by setting the **HUDI_CONF_DIR** environment variable. This can be useful for uniformly enforcing often repeated configs
like Hive sync settings, write/index tuning parameters, across your entire data lake.

### Metadata table

With 0.10.0, we have made a foundational fix to the metadata table with synchronous updates instead of async updates
to simplify the overall design and to assist in building future enhancements. This can be turned on using the config [hoodie.metadata.enable](https://hudi.apache.org/docs/configurations/#hoodiemetadataenable).
By default, metadata table based file listing feature is disabled. We have few following up tasks we are looking to fix by 0.11.0.
You can follow [HUDI-1292](https://issues.apache.org/jira/browse/HUDI-1292) umbrella ticket for further details.
Please refer to the Migration guide section before turning on the metadata table.

### Documentation overhaul

Documentation was added for many pre-existing features that were previously missing docs. We reorganised the documentation
layout to improve discoverability and help new users ramp up on Hudi. We made many doc improvements based on community feedback.
See the latest docs at: [overview](https://hudi.apache.org/docs/overview).

## Writer side improvements

Commit instant time format have been upgraded to ms granularity from secs granularity. Users don’t have to do any special handling in this,
regular upgrade should work smoothly.

Deltastreamer:

- ORCDFSSource has been added to support orc files with DFSSource.
- `S3EventsHoodieIncrSource` can now fan-out multiple tables off a single S3 metadata table.

Clustering:

- We have added support to retain same file groups with clustering to cater to the requirements of external index.
  Incremental timeline support has been added for pending clustering operations.

### DynamoDB based lock provider

Hudi added support for multi-writers in 0.7.0 and as part of it, users are required to configure lock service providers.
In 0.10.0, we are adding DynamoDBBased lock provider that users can make use of. To configure this lock provider, users have to set the below configs:

```java
hoodie.write.lock.provider=org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider
Hoodie.write.lock.dynamodb.table
Hoodie.write.lock.dynamodb.partition_keyhoodie.write.lock.dynamodb.region
```

Also, to set up the credentials for accessing AWS resources, users can set the below property:

```java
hoodie.aws.access.keyhoodie.aws.secret.keyhoodie.aws.session.token
```

More details on concurrency control are covered [here](https://hudi.apache.org/docs/next/concurrency_control).

### Default flips

We have flipped defaults for all shuffle parallelism configs in hudi from 1500 to 200. The configs of interest are [`hoodie.insert.shuffle.parallelism`](https://hudi.apache.org/docs/next/configurations#hoodieinsertshuffleparallelism),
[`hoodie.bulkinsert.shuffle.parallelism`](https://hudi.apache.org/docs/next/configurations#hoodiebulkinsertshuffleparallelism),
[`hoodie.upsert.shuffle.parallelism`](https://hudi.apache.org/docs/next/configurations#hoodieupsertshuffleparallelism) and
[`hoodie.delete.shuffle.parallelism`](https://hudi.apache.org/docs/next/configurations#hoodiedeleteshuffleparallelism).
If you have been relying on the default settings, just watch out after upgrading. We have tested these configs at a reasonable scale though.

We have enabled the rollback strategy to use marker based from listing based. And we have also made timeline server based
markers as default with this release. You can read more on the timeline server based markers [here](https://hudi.apache.org/blog/2021/08/18/improving-marker-mechanism).

Clustering: Default plan strategy changed to `SparkSizeBasedClusteringPlanStrategy`. By default, commit metadata will be preserved when clustering.
It will be useful for incremental query support with replace commits in the timeline.

### Spark SQL improvements

We have made more improvements to spark-sql like adding support for `MERGE INTO` to work with non-primary keys,
and added support for new operations like `SHOW PARTITIONS` and `DROP PARTITIONS`.

## Query side improvements

Hive Incremental query support and partition pruning for snapshot query has been added for MOR table. Support has been added for incremental read with clustering commit.

We have improved the listing logic to gain 65% on query time and 2.8x parallelism on Presto queries against the Hudi tables.

In general, we have made a lot of bug fixes (multi writers, archival, rollbacks, metadata, clustering, etc) and stability fixes in this release.
And have improved our CLI around metadata and clustering commands. Hoping users will have a smoother ride with hudi 0.10.0.

## Flink Integration Improvements

Flink reader now supports incremental read, set `hoodie.datasource.query.type` as `incremental` to enable for batch execution mode.
Configure option `read.start-commit` to specify the reading start commit, configure option `read.end-commit`
to specify the end commit (both are inclusive). Streaming reader can also specify the start offset with the same option `read.start-commit`.

Upsert operation is now supported for batch execution mode, use `INSERT INTO` syntax to update the existing data set.

For non-updating data set like the log data, flink writer now supports appending the new data set directly without merging,
this now becomes the default mode for Copy On Write table type with `INSERT` operation,
by default, the writer does not merge the existing small files, set option `write.insert.cluster` as `true` to enable merging of the small files.

The `write.precombine.field` now becomes optional(not a required option) for flink writer, when the field is not specified,
if there is a field named `ts` in the table schema, the writer use it as the preCombine field,
or the writer compares records by processing sequence: always choose later records.

The small file strategy is tweaked to be more stable, with the new strategy, each bucket assign task manages a subset of filegroups separately,
that means, the parallelism of bucket assign task would affect the number of small files.

The metadata table is also supported for flink writer and reader, metadata table can reduce the partition lookup and file listing obviously for the underneath storage for both writer and reader side.
Set option `metadata.enabled` to `true` to enable this feature.

## Ecosystem

### DBT support

We've made it so much easier to create derived hudi datasets by integrating with the very popular data transformation tool (dbt).
With 0.10.0, users can create incremental hudi datasets using dbt. Please see this PR for details https://github.com/dbt-labs/dbt-spark/issues/187

### Monitoring

Hudi now supports publishing metrics to Amazon CloudWatch. It can be configured by setting [`hoodie.metrics.reporter.type`](https://hudi.apache.org/docs/next/configurations#hoodiemetricsreportertype) to “CLOUDWATCH”.
Static AWS credentials to be used can be configured using [`hoodie.aws.access.key`](https://hudi.apache.org/docs/next/configurations#hoodieawsaccesskey),
[`hoodie.aws.secret.key`](https://hudi.apache.org/docs/next/configurations#hoodieawssecretkey),
[`hoodie.aws.session.token`](https://hudi.apache.org/docs/next/configurations#hoodieawssessiontoken) properties.
In the absence of static AWS credentials being configured, `DefaultAWSCredentialsProviderChain` will be used to get credentials by checking environment properties.
Additional Amazon CloudWatch reporter specific properties that can be tuned are in the `HoodieMetricsCloudWatchConfig` class.

### DevEx

Default maven spark3 version is not upgraded to 3.1 So, `maven profile -Dspark3` will build Hudi against Spark 3.1.2 with 0.10.0. Use `-Dspark3.0.x` for building against Spark 3.0.x versions

### Repair tool for dangling data files

Sometimes, there could be dangling data files lying around due to various reasons ranging from rollback failing mid-way
to cleaner failing to clean up all data files, or data files created by spark task failures were not cleaned up properly.
So, we are adding a repair tool to clean up any dangling data files which are not part of completed commits. Feel free to try out
the tool via spark-submit at `org.apache.hudi.utilities.HoodieRepairTool` in hudi-utilities bundle, if you encounter issues in 0.10.0 release.
The tool has dry run mode as well which would print the dangling files without actually deleting it. The tool is available
from 0.11.0-SNAPSHOT on master.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350285)

## [Release 0.9.0](https://github.com/apache/hudi/releases/tag/release-0.9.0) ([docs](/docs/0.9.0/quick-start-guide))

## Migration Guide for this release
- If migrating from an older release, please also check the upgrade instructions for each subsequent release below.
- With 0.9.0, Hudi is adding more table properties to aid in using an existing hudi table with spark-sql.
  To smoothly aid this transition these properties added to `hoodie.properties` file. Whenever Hudi is launched with
  newer table version i.e 2 (or moving from pre 0.9.0 to 0.9.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the `hoodie.table.version` will be updated in
  property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - `downgrade`) is added if in case some users want to
  downgrade Hudi from table version `2` to `1` or move from Hudi 0.9.0 to pre 0.9.0. This needs to be executed from a
  0.9.0 `hudi-cli` binary/script.
- With this release, we added a new framework to track config properties in code, moving away from string variables that
  hold property names and values. This move helps us automate configuration doc generation and much more. While we still
  support the older configs string variables, users are encouraged to use the new `ConfigProperty` equivalents, as noted
  in the deprecation notices. In most cases, it is as simple as calling `.key()` and `.defaultValue()` on the corresponding
  alternative. e.g `RECORDKEY_FIELD_OPT_KEY` can be replaced by `RECORDKEY_FIELD_NAME.key()`
- If set `URL_ENCODE_PARTITIONING_OPT_KEY=true` *and* `<` and `>` are present in the URL partition paths, users would
  need to migrate the table due to encoding logic changed: `<` (encoded as `%3C`) and `>` (encoded as `%3E`) won't be escaped in 0.9.0.

## Release Highlights

### Spark SQL DML and DDL Support

0.9.0 adds **experimental** support for DDL/DMLs using Spark SQL, taking a huge step towards making Hudi more easily accessible and
operable by all personas (non-engineers, analysts etc). Users can now use `CREATE TABLE....USING HUDI` and `CREATE TABLE .. AS SELECT`
statements to directly create and manage tables in catalogs like Hive. Users can then use `INSERT`, `UPDATE`, `MERGE INTO` and `DELETE`
sql statements to manipulate data. In addition, `INSERT OVERWRITE` statement can be used to overwrite existing data in the table or partition
for existing batch ETL pipelines. For more information, checkout our docs [here](/docs/quick-start-guide) clicking on `SparkSQL` tab.
Please see [RFC-25](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)
for more implementation details.

### Query side improvements

Hudi tables are now registered with Hive as spark datasource tables, meaning Spark SQL on these tables now uses the datasource as well,
instead of relying on the Hive fallbacks within Spark, which are ill-maintained/cumbersome. This unlocks many optimizations such as the
use of Hudi's own [FileIndex](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/hudi/HoodieFileIndex.scala#L46)
implementation for optimized caching and the use of the Hudi metadata table, for faster listing of large tables. We have also added support for
[timetravel query](/docs/quick-start-guide#time-travel-query), for spark datasource.

### Writer side improvements

Virtual keys support has been added where users can avoid adding meta fields to hudi table and leverage existing fields to populate record keys and partition paths.
One needs to disable [this](/docs/configurations#hoodiepopulatemetafields) config to enable virtual keys.

Bulk Insert operations using [row writer enabled](/docs/configurations#hoodiedatasourcewriterowwriterenable) now supports pre-combining,
sort modes and user defined partitioners and now turned on by default for fast inserts.

Hudi performs automatic cleanup of uncommitted data, which has now been enhanced to be performant over cloud storage, even for
extremely large tables. Specifically, a new marker mechanism has been implemented leveraging the timeline server to perform
centrally co-ordinated batched read/write of file markers to underlying storage. You can turn this using this [config](/docs/configurations#hoodiewritemarkerstype) and learn more
about it on this [blog](/blog/2021/08/18/improving-marker-mechanism).

Async Clustering support has been added to both DeltaStreamer and Spark Structured Streaming Sink. More on this can be found in this
[blog post](/blog/2021/08/23/async-clustering). In addition, we have added a new utility class [HoodieClusteringJob](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieClusteringJob.java)
to assist in building and executing a clustering plan together as a standalone spark job.

Users can choose to drop fields used to generate partition paths, using `hoodie.datasource.write.drop.partition.columns=true`, to support
querying of Hudi snapshots using systems like BigQuery, which cannot handle this.

Hudi uses different [types of spillable maps](/docs/configurations#hoodiecommonspillablediskmaptype), for internally handling merges (compaction, updates or even MOR snapshot queries). In 0.9.0, we have added
support for [compression](/docs/configurations#hoodiecommondiskmapcompressionenabled) for the bitcask style default option and introduced a new spillable map backed by rocksDB, which can be more performant for large
bulk updates or working with large base file sizes.

Added a new write operation `delete_partition` operation, with support in spark. Users can leverage this to delete older partitions in bulk, in addition to
record level deletes. Deletion of specific partitions can be done using this [config](/docs/configurations#hoodiedatasourcewritepartitionstodelete).

Support for Huawei Cloud Object Storage, BAIDU AFS storage format, Baidu BOS storage in Hudi.

A [pre commit validator framework](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SparkPreCommitValidator.java)
has been added for spark engine, which can used for DeltaStreamer and Spark Datasource writers. Users can leverage this to add any validations to be executed before committing writes to Hudi.
Three validators come out-of-box
- [org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryEqualityPreCommitValidator.java) can be used to validate for equality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryInequalityPreCommitValidator.java) can be used to validate for inequality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQuerySingleResultPreCommitValidator.java) can be used to validate that a query on the table results in a specific value.

These can be configured by setting `hoodie.precommit.validators=<comma separated list of validator class names>`. Users can also provide their own implementations by extending the abstract class [SparkPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SparkPreCommitValidator.java)
and overriding this method

```java
void validateRecordsBeforeAndAfter(Dataset<Row> before, 
                                   Dataset<Row> after, 
                                   Set<String> partitionsAffected)
```


### Flink Integration Improvements

The Flink writer now supports propagation of CDC format for MOR table, by turning on the option `changelog.enabled=true`. Hudi would then persist all change flags of each record,
using the streaming reader of Flink, user can do stateful computation based on these change logs. Note that when the table is compacted with async compaction service, all the
intermediate changes are merged into one(last record), to only have UPSERT semantics.

Flink writing now also has most feature parity with spark writing, with addition of write operations like `bulk_insert`, `insert_overwrite`, support for non-partitioned tables,
automatic cleanup of uncommitted data, global indexing support, hive style partitioning and handling of partition path updates. Writing also supports a new log append mode, where
no records are de-duplicated and base files are directly written for each flush. To use this mode, set `write.insert.deduplicate=false`.

Flink readers now support streaming reads from COW/MOR tables. Deletions are emitted by default in streaming read mode, the downstream receives the DELETE message as a Hoodie record with empty payload.

Hive sync has been greatly improved by support different Hive versions(1.x, 2.x, 3.x). Hive sync can also now be done asynchronously.

Flink Streamer tool now supports transformers.

### DeltaStreamer

We have enhanced Deltastreamer utility with 3 new sources.

[JDBC source](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java) can take a extraction SQL statement and
incrementally fetch data out of sources supporting JDBC. This can be useful for e.g when reading data from RDBMS sources. Note that, this approach may need periodic re-bootstrapping to ensure data consistency, although being much simpler to operate over CDC based approaches.

[SQLSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/SqlSource.java) takes a Spark SQL statement to fetch data out of existing tables and
can be very useful for easy SQL based backfills use-cases e.g: backfilling just one column for the past N months.

[S3EventsHoodieIncrSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/S3EventsHoodieIncrSource.java) and [S3EventsSource](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/S3EventsSource.java)
assist in reading data from S3 reliably and efficiently ingesting that to Hudi. Existing approach using `*DFSSource` source classes uses last modification time of files as checkpoint to pull in new files.
But, if large number of files have the same modification time, this might miss some files to be read from the source.  These two sources (S3EventsHoodieIncrSource and S3EventsSource) together ensures data
is reliably ingested from S3 into Hudi by leveraging AWS SNS and SQS services that subscribes to file events from the source bucket. [This blog post](/blog/2021/08/23/s3-events-source) presents a model for
scalable, reliable incremental ingestion by using these two sources in tandem.

In addition to pulling events from kafka using regular offset format, we also added support for timestamp based fetches, that can
help with initial backfill/bootstrap scenarios. We have also added support for passing in basic auth credentials in schema registry provider url with schema provider.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350027)

## [Release 0.8.0](https://github.com/apache/hudi/releases/tag/release-0.8.0) ([docs](/docs/0.8.0/quick-start-guide))

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- Specifically check upgrade instructions for 0.6.0. This release does not introduce any new table versions.
- The `HoodieRecordPayload` interface deprecated existing methods, in favor of new ones that also lets us pass properties at runtime. Users are
  encouraged to migrate out of the deprecated methods, since they will be removed in 0.9.0.
- "auto.offset.reset" is a config used for Kafka sources in deltastreamer utility to reset offset to be consumed from such
  sources. We had a bug around this config with 0.8.0 which has been fixed in 0.9.0. Please use "auto.reset.offsets" instead.
  Possible values for this config are "earliest" and "latest"(default). So, would recommend using "auto.reset.offsets" only in
  0.8.0 and for all other releases, you can use "auto.offset.reset".

## Release Highlights

### Flink Integration
Since the initial support for the Hudi Flink Writer in the 0.7.0 release, the Hudi community made great progress on improving the Flink/Hudi integration,
including redesigning the Flink writer pipeline with better performance and scalability, state-backed indexing with bootstrap support,
Flink writer for MOR table, batch reader for COW&MOR table, streaming reader for MOR table, and Flink SQL connector for both source and sink.
In the 0.8.0 release, user is able to use all those features with Flink 1.11+.

Please see [RFC-24](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+24%3A+Hoodie+Flink+Writer+Proposal)
for more implementation details for the Flink writer and follow this [page](/docs/flink-quick-start-guide)
to get started with Flink!

### Parallel Writers Support
As many users requested, now Hudi supports multiple ingestion writers to the same Hudi Table with optimistic concurrency control.
Hudi supports file level OCC, i.e., for any 2 commits (or writers) happening to the same table, if they do not have writes to overlapping files being changed,
both writers are allowed to succeed. This feature is currently experimental and requires either Zookeeper or HiveMetastore to acquire locks.

Please see [RFC-22](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers)
for more implementation details and follow this [page](/docs/concurrency_control) to get started with concurrency control!

### Writer side improvements
- InsertOverwrite Support for Flink writer client.
- Support CopyOnWriteTable in Java writer client.

### Query side improvements
- Support Spark Structured Streaming read from Hudi table.
- Performance improvement of Metadata table.
- Performance improvement of Clustering.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12349423)

## [Release 0.7.0](https://github.com/apache/hudi/releases/tag/release-0.7.0) ([docs](/docs/0.7.0/quick-start-guide))

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- Specifically check upgrade instructions for 0.6.0. This release does not introduce any new table versions.
- The `HoodieRecordPayload` interface deprecated existing methods, in favor of new ones that also lets us pass properties at runtime. Users are
  encouraged to migrate out of the deprecated methods, since they will be removed in 0.9.0.

## Release Highlights

### Clustering

0.7.0 brings the ability to cluster your Hudi tables, to optimize for file sizes and also storage layout. Hudi will continue to
enforce file sizes, as it always has been, during the write. Clustering provides more flexibility to increase the file sizes
down the line or ability to ingest data at much fresher intervals, and later coalesce them into bigger files. [Microbenchmarks](https://gist.github.com/vinothchandar/d7fa1338cddfae68390afcdfe310f94e#gistcomment-3383478)
demonstrate a 3-4x reduction in query performance, for a 10-20x reduction in file sizes.

Additionally, clustering data based on fields that are often used in queries, dramatically
[improves query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance#RFC19Clusteringdataforfreshnessandqueryperformance-PerformanceEvaluation) by allowing many files to be
completely skipped. This is very similar to the benefits of clustering delivered by [cloud data warehouses](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions).
We are proud to announce that such capability is freely available in open source, for the first time, through the 0.7.0 release.

Please see [RFC-19](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance) for more implementation details
and checkout configs [here](/docs/configurations#clustering-configs) for how to use it in your pipelines. At the moment, we support both inline and async clustering modes.

### Metadata Table

Since Hudi was born at Uber, on a HDFS backed data lake, we have since been a tad apathetic to the plight of listing performance on cloud storage (partly in hopes that
cloud providers will fix it over time:)). Nonetheless, 0.7.0 changes this and lays out the foundation for storing more indexes, metadata in an internal metadata table,
which is implemented using a Hudi MOR table - which means it's compacted, cleaned and also incrementally updated like any other Hudi table. Also, unlike similar
implementations in other projects, we have chosen to index the file listing information as HFiles, which offers point-lookup performance to fetch listings for a single partition.

In 0.7.0 release, `hoodie.metadata.enable=true` on the writer side, will populate the metadata table with file system listings
so all operations don't have to explicitly use `fs.listStatus()` anymore on data partitions. We have introduced a sync mechanism that
keeps syncing file additions/deletions on the data timeline, to the metadata table, after each write operation.

In our testing, on a large 250K file table, the metadata table delivers [2-3x speedup](https://github.com/apache/hudi/pull/2441#issuecomment-761742963) over parallelized
listing done by the Hudi spark writer. Please check [RFC-15 (ongoing)](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements)
and the [configurations](/docs/configurations#metadata-config), which offer flags to help adopt this feature safely in your production pipelines.

### Java/Flink Writers

Hudi was originally designed with a heavy dependency on Spark, given it had simply solve specific problems at Uber. But, as we have evolved as an Apache
project, we realized the need for abstracting the internal table format, table services and writing layers of code. In 0.7.0, we have additionally added
Java and Flink based writers, as initial steps in this direction.

Specifically, the `HoodieFlinkStreamer` allows for Hudi Copy-On-Write table to built by streaming data from a Kafka topic.

### Writer side improvements

- **Spark3 Support**: We have added support for writing/querying data using Spark 3. please be sure to use the scala 2.12 hudi-spark-bundle.
- **Parallelized Listing**: We have holistically moved all listings under the `HoodieTableMetadata` interface, which does multi-threaded/spark parallelized list operations.
  We expect this to improve cleaner, compaction scheduling performance, even when the metadata table is not used.
- **Kafka Commit Callbacks**: We have added `HoodieWriteCommitKafkaCallback`, that publishes an event to Apache Kafka, for every commit operation. This can be used to trigger
  derived/ETL pipelines similar to data [sensors](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/index) in Apache Airflow.
- **Insert Overwrite/Insert Overwrite Table**: We have added these two new write operation types, predominantly to help existing batch ETL jobs, which typically overwrite entire
  tables/partitions each run. These operations are much cheaper, than having to issue upserts, given they are bulk replacing the target table.
  Check [here](/docs/quick-start-guide#insert-overwrite-table) for examples.
- **Delete Partition**: For users of WriteClient/RDD level apis, we have added an API to delete an entire partition, again without issuing deletes at the record level.
- The current default `OverwriteWithLatestAvroPayload` will overwrite the value in storage, even if for e.g the upsert was reissued for an older value of the key.
  Added a new `DefaultHoodieRecordPayload` and a new payload config `hoodie.payload.ordering.field` helps specify a field, that the incoming upsert record can be compared with
  the record on storage, to decide whether to overwrite or not. Users are encouraged to adopt this newer, more flexible model.
- Hive sync supports hourly partitions via `SlashEncodedHourPartitionValueExtractor`
- Support for IBM Cloud storage, Open J9 JVM.

### Query side improvements

- **Incremental Query on MOR (Spark Datasource)**: Spark datasource now has experimental support for incremental queries on MOR table. This feature will be hardened and certified
  in the next release, along with a large overhaul of the spark datasource implementation. (sshh!:))
- **Metadata Table For File Listings**: Users can also leverage the metadata table on the query side for the following query paths. For Hive, setting the `hoodie.metadata.enable=true` session
  property and for SparkSQL on Hive registered tables using `--conf spark.hadoop.hoodie.metadata.enable=true`, allows the file listings for the partition to be fetched out of the metadata
  table, instead of listing the underlying DFS partition.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12348721)

## [Release 0.6.0](https://github.com/apache/hudi/releases/tag/release-0.6.0) ([docs](/docs/0.6.0/quick-start-guide))

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- With 0.6.0 Hudi is moving from list based rollback to marker based rollbacks. To smoothly aid this transition a
  new property called `hoodie.table.version` is added to `hoodie.properties` file. Whenever Hudi is launched with
  newer table version i.e 1 (or moving from pre 0.6.0 to 0.6.0), an upgrade step will be executed automatically.
  This automatic upgrade step will happen just once per Hudi table as the `hoodie.table.version` will be updated in property file after upgrade is completed.
- Similarly, a command line tool for Downgrading (command - `downgrade`) is added if in case some users want to downgrade Hudi from table version 1 to 0 or move from Hudi 0.6.0 to pre 0.6.0
- If you were using a user defined partitioner with bulkInsert() RDD API, the base interface has changed to `BulkInsertPartitioner` and will need minor adjustments to your existing implementations.

## Release Highlights

### Writer side improvements:
- Bootstrapping existing parquet datasets :  Adds support for bootstrapping existing datasets into Hudi, via both Spark datasource writer and
  deltastreamer tool, with support for reading from Hive, SparkSQL, AWS Athena (prestoDB support coming soon). See [RFC-15](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements) for technical details.
  Note that this is an experimental feature, which will be improved upon further in the 0.6.x versions.
- Native row writing for bulk_insert : Avoids any dataframe-rdd conversion for bulk_insert path, which can improve performance of initial bulk loads.
  Although, this is typically not the bottleneck for upsert/deletes, subsequent releases in 0.6.x versions will expand this to other write operations
  to make reasoning about schema management easier, avoiding the spark-avro conversion totally.
- Bulk insert sort modes : Hudi bulk_insert sorts the input globally to optimize file sizes and avoid out-of-memory issues encountered when writing parallely to multiple DFS partitions.
  For users who want to prepare the dataframe for writing outside of Hudi, we have made this configurable using `hoodie.bulkinsert.sort.mode`.
- Cleaning can now be run concurrently with writing, using `hoodie.clean.async=true`which can speed up time taken to finish committing.
- Async compaction for spark streaming writes to hudi table, is now self managed by default, controlling `hoodie.datasource.compaction.async.enable`.
- Rollbacks no longer perform full table listings, by leveraging marker files. To enable, set `hoodie.rollback.using.markers=true`.
- Added a new index `hoodie.index.type=SIMPLE` which can be faster than `BLOOM_INDEX` for cases where updates/deletes spread across a large portion of the table.
- Hudi now supports `Azure Data Lake Storage V2` , `Alluxio` and `Tencent Cloud Object Storage` storages.
- [HoodieMultiDeltaStreamer](https://hudi.apache.org/docs/writing_data#multitabledeltastreamer) adds support for ingesting multiple kafka streams in a single DeltaStreamer deployment, effectively reducing operational burden for using delta streamer
  as your data lake ingestion tool (Experimental feature)
- Added a new tool - InitialCheckPointProvider, to set checkpoints when migrating to DeltaStreamer after an initial load of the table is complete.
- Delta Streamer tool now supports ingesting CSV data sources, chaining of multiple transformers to build more advanced ETL jobs.
- Introducing a new `CustomKeyGenerator` key generator class, that provides flexible configurations to provide enable different types of key, partition path generation in  single class.
  We also added support for more time units and date/time formats in `TimestampBasedKeyGenerator`. See [docs](https://hudi.apache.org/docs/writing_data#key-generation) for more.

### Query side improvements:
- Starting 0.6.0, snapshot queries are feasible on MOR tables using spark datasource. (experimental feature)
- In prior versions we only supported `HoodieCombineHiveInputFormat` for CopyOnWrite tables to ensure that there is a limit on the number of mappers spawned for
  any query. Hudi now supports Merge on Read tables also using `HoodieCombineInputFormat`.
- Speedup spark read queries by caching metaclient in HoodieROPathFilter. This helps reduce listing related overheads in S3 when filtering files for read-optimized queries.

### Usability:
- Spark DAGs are named to aid better debuggability.
- Support pluggable metrics reporting by introducing proper abstraction for user defined metrics. Console, JMX, Prometheus and DataDog metric reporters have been added.
- A new utility called Data snapshot exporter has been added. Latest table snapshot as of a certain point in time can be exported as plain parquet files with this tool.
- Introduce write committed callback hooks for incremental pipelines to be notified and act on new commits in the timeline. For e.g, Apache Airflow jobs can be triggered
  as new commits arrive.
- Added support for deleting savepoints via CLI
- Added a new command - `export instants`, to export metadata of instants

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346663)

## [Release 0.5.3](https://github.com/apache/hudi/releases/tag/release-0.5.3) ([docs](/docs/0.5.3/quick-start-guide))

## Migration Guide for this release
* This is a bug fix only release and no special migration steps needed when upgrading from 0.5.2. If you are upgrading from earlier releases “X”, please make sure you read the migration guide for each subsequent release between “X” and 0.5.3
* 0.5.3 is the first hudi release after graduation. As a result, all hudi jars will no longer have "-incubating" in the version name. In all the places where hudi version is referred, please make sure "-incubating" is no longer present.

For example hudi-spark-bundle pom dependency would look like:
```xml
    <dependency>
        <groupId>org.apache.hudi</groupId>
        <artifactId>hudi-spark-bundle_2.12</artifactId>
        <version>0.5.3</version>
    </dependency>
```
## Release Highlights
* Hudi now supports `aliyun OSS` storage service.
* Embedded Timeline Server is enabled by default for both delta-streamer and spark datasource writes. This feature was in experimental mode before this release. Embedded Timeline Server caches file listing calls in Spark driver and serves them to Spark writer tasks. This reduces the number of file listings needed to be performed for each write.
* Incremental Cleaning is enabled by default for both delta-streamer and spark datasource writes. This feature was also in experimental mode before this release. In the steady state, incremental cleaning avoids the costly step of scanning all partitions and instead uses Hudi metadata to find files to be cleaned up.
* Delta-streamer config files can now be placed in different filesystem than actual data.
* Hudi Hive Sync now supports tables partitioned by date type column.
* Hudi Hive Sync now supports syncing directly via Hive MetaStore. You simply need to set hoodie.datasource.hive_sync.use_jdbc
  =false. Hive Metastore Uri will be read implicitly from environment. For example, when writing through Spark Data Source,

```Scala
 spark.write.format(“hudi”)
 .option(…)
 .option(“hoodie.datasource.hive_sync.username”, “<user>”)
 .option(“hoodie.datasource.hive_sync.password”, “<password>”)
 .option(“hoodie.datasource.hive_sync.partition_fields”, “<partition_fields>”)
 .option(“hoodie.datasource.hive_sync.database”, “<db_name>”)
 .option(“hoodie.datasource.hive_sync.table”, “<table_name>”)
 .option(“hoodie.datasource.hive_sync.use_jdbc”, “false”)
 .mode(APPEND)
 .save(“/path/to/dataset”)
```

* Other Writer Performance related fixes:
  - DataSource Writer now avoids unnecessary loading of data after write.
  - Hudi Writer now leverages spark parallelism when searching for existing files for writing new records.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12348256)


## [Release 0.5.2-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.2-incubating) ([docs](/docs/0.5.2/quick-start-guide))

### Migration Guide for this release
* Write Client restructuring has moved classes around ([HUDI-554](https://issues.apache.org/jira/browse/HUDI-554)). Package `client` now has all the various client classes, that do the transaction management. `func` renamed to `execution` and some helpers moved to `client/utils`. All compaction code under `io` now under `table/compact`. Rollback code under `table/rollback` and in general all code for individual operations under `table`. This change only affects the apps/projects depending on hudi-client. Users of deltastreamer/datasource will not need to change anything.

### Release Highlights
* Support for overwriting the payload implementation in `hoodie.properties` via specifying the `hoodie.compaction.payload.class` config option. Previously, once the payload class is set once in `hoodie.properties`, it cannot be changed. In some cases, if a code refactor is done and the jar updated, one may need to pass the new payload class name.
* `TimestampBasedKeyGenerator` supports for `CharSequence`  types. Previously `TimestampBasedKeyGenerator` only supports `Double`, `Long`, `Float` and `String` 4 data types for the partition key. Now, after data type extending, `CharSequence` has been supported in `TimestampBasedKeyGenerator`.
* Hudi now supports incremental pulling from defined partitions via the `hoodie.datasource.read.incr.path.glob` [config option](https://github.com/apache/hudi/blob/master/hudi-spark/src/main/scala/org/apache/hudi/DataSourceOptions.scala#L111). For some use case that users only need to pull the incremental part of certain partitions, it can run faster by only loading relevant parquet files.
* With 0.5.2, hudi allows partition path to be updated with `GLOBAL_BLOOM` index. Previously, when a record is to be updated with a new partition path, and when set to `GLOBAL_BLOOM` as index, hudi ignores the new partition path and update the record in the original partition path. Now, hudi allows records to be inserted into their new partition paths and delete the records in the old partition paths. A configuration (e.g. `hoodie.index.bloom.update.partition.path=true`) can be added to enable this feature.
* A `JdbcbasedSchemaProvider` schema provider has been provided to get metadata through JDBC. For the use case that users want to synchronize data from MySQL, and at the same time, want to get the schema from the database, it's very helpful.
* Simplify `HoodieBloomIndex` without the need for 2GB limit handling. Prior to spark 2.4.0, each spark partition has a limit of 2GB. In Hudi 0.5.1, after we upgraded to spark 2.4.4, we don't have the limitation anymore. Hence removing the safe parallelism constraint we had in` HoodieBloomIndex`.
* CLI related changes:
    - Allows users to specify option to print additional commit metadata, e.g. *Total Log Blocks*, *Total Rollback Blocks*, *Total Updated Records Compacted* and so on.
    - Supports `temp_query` and `temp_delete` to query and delete temp view. This command creates a temp table. Users can write HiveQL queries against the table to filter the desired row. For example,
```
temp_query --sql "select Instant, NumInserts, NumWrites from satishkotha_debug where FileId='ed33bd99-466f-4417-bd92-5d914fa58a8f' and Instant > '20200123211217' order by Instant"
```

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346606)

## [Release 0.5.1-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.1-incubating) ([docs](/docs/0.5.1/quick-start-guide))

### Migration Guide for this release
* Upgrade hudi readers (query engines) first with 0.5.1-incubating release before upgrading writer.
* In 0.5.1, the community restructured the package of key generators. The key generator related classes have been moved from `org.apache.hudi` to `org.apache.hudi.keygen`.

### Release Highlights
* Dependency Version Upgrades
    - Upgrade from Spark 2.1.0 to Spark 2.4.4
    - Upgrade from Avro 1.7.7 to Avro 1.8.2
    - Upgrade from Parquet 1.8.1 to Parquet 1.10.1
    - Upgrade from Kafka 0.8.2.1 to Kafka 2.0.0 as a result of updating spark-streaming-kafka artifact from 0.8_2.11/2.12 to 0.10_2.11/2.12.
* **IMPORTANT** This version requires your runtime spark version to be upgraded to 2.4+.
* Hudi now supports both Scala 2.11 and Scala 2.12, please refer to [Build with Scala 2.12](https://github.com/apache/hudi#build-with-scala-212) to build with Scala 2.12.
  Also, the packages hudi-spark, hudi-utilities, hudi-spark-bundle and hudi-utilities-bundle are changed correspondingly to hudi-spark_{scala_version}, hudi-spark_{scala_version}, hudi-utilities_{scala_version}, hudi-spark-bundle_{scala_version} and hudi-utilities-bundle_{scala_version}.
  Note that scala_version here is one of (2.11, 2.12).
* With 0.5.1, we added functionality to stop using renames for Hudi timeline metadata operations. This feature is automatically enabled for newly created Hudi tables. For existing tables, this feature is turned off by default. Please read this [section](https://hudi.apache.org/docs/deployment#upgrading), before enabling this feature for existing hudi tables.
  To enable the new hudi timeline layout which avoids renames, use the write config "hoodie.timeline.layout.version=1". Alternatively, you can use "repair overwrite-hoodie-props" to append the line "hoodie.timeline.layout.version=1" to hoodie.properties. Note that in any case, upgrade hudi readers (query engines) first with 0.5.1-incubating release before upgrading writer.
* CLI supports `repair overwrite-hoodie-props` to overwrite the table's hoodie.properties with specified file, for one-time updates to table name or even enabling the new timeline layout above. Note that few queries may temporarily fail while the overwrite happens (few milliseconds).
* DeltaStreamer CLI parameter for capturing table type is changed from `--storage-type` to `--table-type`. Refer to [wiki](https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture) with more latest terminologies.
* Configuration Value change for Kafka Reset Offset Strategies. Enum values are changed from LARGEST to LATEST, SMALLEST to EARLIEST for configuring Kafka reset offset strategies with configuration(auto.offset.reset) in deltastreamer.
* When using spark-shell to give a quick peek at Hudi, please provide `--packages org.apache.spark:spark-avro_2.11:2.4.4`, more details would refer to [latest quickstart docs](https://hudi.apache.org/docs/quick-start-guide)
* Key generator moved to separate package under org.apache.hudi.keygen. If you are using overridden key generator classes (configuration ("hoodie.datasource.write.keygenerator.class")) that comes with hudi package, please ensure the fully qualified class name is changed accordingly.
* Hive Sync tool will register RO tables for MOR with a _ro suffix, so query with _ro suffix. You would use `--skip-ro-suffix` in sync config in sync config to retain the old naming without the _ro suffix.
* With 0.5.1, hudi-hadoop-mr-bundle which is used by query engines such as presto and hive includes shaded avro package to support hudi real time queries through these engines. Hudi supports pluggable logic for merging of records. Users provide their own implementation of [HoodieRecordPayload](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java).
  If you are using this feature, you need to relocate the avro dependencies in your custom record payload class to be consistent with internal hudi shading. You need to add the following relocation when shading the package containing the record payload implementation.

  ```xml
  <relocation>
    <pattern>org.apache.avro.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.avro.</shadedPattern>
  </relocation>
  ```

* Better delete support in DeltaStreamer, please refer to [blog](https://cwiki.apache.org/confluence/display/HUDI/2020/01/15/Delete+support+in+Hudi) for more info.
* Support for AWS Database Migration Service(DMS) in DeltaStreamer, please refer to [blog](https://cwiki.apache.org/confluence/display/HUDI/2020/01/20/Change+Capture+Using+AWS+Database+Migration+Service+and+Hudi) for more info.
* Support for DynamicBloomFilter. This is turned off by default, to enable the DynamicBloomFilter, please use the index config `hoodie.bloom.index.filter.type=DYNAMIC_V0`.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346183)

## [Release 0.5.0-incubating](https://github.com/apache/hudi/releases/tag/release-0.5.0-incubating) ([docs](/docs/0.5.0/quick-start-guide))

### Release Highlights
* Package and format renaming from com.uber.hoodie to org.apache.hudi (See migration guide section below)
* Major redo of Hudi bundles to address class and jar version mismatches in different environments
* Upgrade from Hive 1.x to Hive 2.x for compile time dependencies - Hive 1.x runtime integration still works with a patch : See [the discussion thread](https://lists.apache.org/thread/48b3f0553f47c576fd7072f56bb0d8a24fb47d4003880d179c7f88a3@%3Cdev.hudi.apache.org%3E)
* DeltaStreamer now supports continuous running mode with managed concurrent compaction
* Support for Composite Keys as record key
* HoodieCombinedInputFormat to scale huge hive queries running on Hoodie tables

### Migration Guide for this release
This is the first Apache release for Hudi. Prior to this release, Hudi Jars were published using "com.uber.hoodie" maven co-ordinates. We have a [migration guide](https://cwiki.apache.org/confluence/display/HUDI/Migration+Guide+From+com.uber.hoodie+to+org.apache.hudi)

### Raw Release Notes
The raw release notes are available [here](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346087)

## [Release 0.4.7](https://github.com/apache/hudi/releases/tag/hoodie-0.4.7)

### Release Highlights

* Major releases with fundamental changes to filesystem listing & write failure handling
* Introduced the first version of HoodieTimelineServer that runs embedded on the driver
* With all executors fetching filesystem listing via RPC to timeline server, drastically reduced filesystem listing!
* Failing concurrent write tasks are now handled differently to be robust against spark stage retries
* Bug fixes/clean up around indexing, compaction

### PR LIST

- Skip Meta folder when looking for partitions. [#698](https://github.com/apache/hudi/pull/698)
- HUDI-134 - Disable inline compaction for Hoodie Demo. [#696](https://github.com/apache/hudi/pull/696)
- Default implementation for HBase index qps allocator. [#685](https://github.com/apache/hudi/pull/685)
- Handle duplicate record keys across partitions. [#687](https://github.com/apache/hudi/pull/687)
- Fix up offsets not available on leader exception. [#650](https://github.com/apache/hudi/pull/650)
