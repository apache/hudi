---
title: "Release 0.12.0"
sidebar_position: 14
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## [Release 0.12.0](https://github.com/apache/hudi/releases/tag/release-0.12.0)

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  The latest 0.12 release is [0.12.3](/releases/release-0.12.3).

## Migration Guide

In this release, there have been a few API and configuration updates listed below that warranted a new table version.
Hence, the latest [table version](https://github.com/apache/hudi/blob/bf86efef719b7760ea379bfa08c537431eeee09a/hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java#L41)
is `5`. For existing Hudi tables on older version, a one-time upgrade step will be executed automatically. Please take
note of the following updates before upgrading to Hudi 0.12.0.

### Configuration Updates

In this release, the default value for a few configurations have been changed. They are as follows:

- `hoodie.bulkinsert.sort.mode`: This config is used to determine mode for sorting records for bulk insert. Its default value has been changed from `GLOBAL_SORT` to `NONE`, which means no sorting is done and it matches `spark.write.parquet()` in terms of overhead.

- `hoodie.datasource.hive_sync.partition_extractor_class`: This config is used to extract and transform partition value during Hive sync. Its default value has been changed from `SlashEncodedDayPartitionValueExtractor` to `MultiPartKeysValueExtractor`. If you relied on the previous default value (i.e., have not set it explicitly), you are required to set the config to `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor`. From this release, if this config is not set and Hive sync is enabled, then partition value extractor class will be **automatically inferred** on the basis of number of partition fields and whether or not hive style partitioning is enabled.

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
