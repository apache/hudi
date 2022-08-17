---
title: "Release 0.12.0"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2022-08-17T10:30:00+05:30
---
# [Release 0.12.0](https://github.com/apache/hudi/releases/tag/release-0.12.0) ([docs](/docs/quick-start-guide))

## Release Highlights

### Presto-Hudi Connector

Since version 0.275 of PrestoDB, users can now leverage native Hudi connector to query Hudi table. 
It is on par with Hudi support in the Hive connector. To learn more about the usage of the connector, 
please checkout [prestodb documentation](https://prestodb.io/docs/current/connector/hudi.html).

### Archival Beyond Savepoint

Users can now archive Hudi table beyond savepoint commit. Just enable `hoodie.archive.beyond.savepoint` write 
configuration. This unlocks new opportunities for Hudi users. For example, one can retain commits for years, by adding 
one savepoint per day for older commits (say > 30 days old). And they can query hudi using `as.of.instant` semantics for
old data. In previous versions, one would have to retain every commit and let archival stop at the first commit.

:::note
However, if this feature is enabled, restore cannot be supported. This limitation would be relaxed in a future release 
and the development of this feature can be tracked in [HUDI-4500](https://issues.apache.org/jira/browse/HUDI-4500).
:::

### Post Write Termination Strategy with Deltastreamer

Users can now configure a post write termination strategy with deltastreamer `continuous` mode if need be. For instance,
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

### Performance Improvements

While some performance regressions were addressed in [0.11.1 release](/releases/release-0.11.1#addressing-performance-regression-in-0110), 
this version brings more improvements to make Hudi the most performant lake storage format. Some notable improvements are:
- Closed the performance gap in writing through Spark datasource vs sql. Previously, datasource writes were faster.
- All built-in key generators implement more performant Spark-specific APIs.
- Replaced UDF in bulk insert operation with RDD transformation to cut down serde cost.
- Optimized column stats index performance in data skipping.

We recently benchmarked Hudi against TPC-DS workload.
Please check out [our blog](/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks) for more details.

### SQL Support Improvements

- Support for upgrade, downgrade, bootstrap, clean, rollback and repair through `Call Procedure` command.
- Support for `analyze table`.
- Support for `Create/Drop/Show/Refresh Index` syntax through Spark SQL.

### Flink Integration Improvements

- Flink 1.15.x is integrated, use profile param `-Pflink1.15` when compiling the codes to adapt the version. Alternatively, use `hudi-flink1.15-bundle`.
- Data skipping is supported for batch mode read, set up SQL option `metadata.enabled`, `hoodie.metadata.index.column.stats.enable`  and `read.data.skipping.enabled` as true to enable it.
- A HMS-based Flink catalog is added with catalog identifier as `hudi`. You can instantiate the catalog through API directly or use the `CREATE CATALOG`  syntax to create it. Specifies catalog option `'mode' = 'hms'`  to switch to the HMS catalog. By default, the catalog is in `dfs` mode.
- Async clustering is supported for Flink `INSERT` operation, set up SQL option `clustering.schedule.enabled` and `clustering.async.enabled` as true to enable it. When enabling this feature, a clustering sub-pipeline is scheduled asynchronously continuously to merge the small files continuously into larger ones.

### Migration Guide

#### Bundle Updates

- `hudi-aws-bundle` extracts away aws-related dependencies from hudi-utilities-bundle or hudi-spark-bundle. In order to use features such as Glue sync, Cloudwatch metrics reporter or DynamoDB lock provider, users need to provide hudi-aws-bundle jar along with hudi-utilities-bundle or hudi-spark-bundle jars.
- Spark 3.3 support is added; users who are on Spark 3.3 can use `hudi-spark3.3-bundle` or `hudi-spark3-bundle` (legacy bundle name).
- Spark 3.2 will continue to be supported via `hudi-spark3.2-bundle`.
- Spark 3.1 will continue to be supported via `hudi-spark3.1-bundle`.
- Spark 2.4 will continue to be supported via `hudi-spark2.4-bundle` or `hudi-spark-bundle` (legacy bundle name).
- Flink 1.15 support is added; users who are on Flink 1.15 can use `hudi-flink1.15-bundle`.

#### Configuration Updates

In this release, the default value for a few configurations have been changed. They are as follows:

- `hoodie.datasource.hive_sync.partition_value_extractor`: This config is used to extract and transform partition value during Hive sync. Its default value has been changed from `SlashEncodedDayPartitionValueExtractor` to `MultiPartKeysValueExtractor`. If you relied on the previous default value (i.e., have not set it explicitly), you are required to set the config to `org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor`. From this release, if this config is not set and Hive sync is enabled, then partition value extractor class will be **automatically inferred** on the basis of number of partition fields and whether or not hive style partitioning is enabled.
- `hoodie.bulkinsert.sort.mode`: This config is used to determine mode for sorting records for bulk insert. Its default value has been changed from `GLOBAL_SORT` to `NONE`, which means no sorting is done and it matches `spark.write.parquet()` in terms of overhead.
- The following configs will be inferred from other configs' values:
  - `META_SYNC_BASE_FILE_FORMAT`: infer from `org.apache.hudi.common.table.HoodieTableConfig.BASE_FILE_FORMAT`

  - `META_SYNC_ASSUME_DATE_PARTITION`: infer from `org.apache.hudi.common.config.HoodieMetadataConfig.ASSUME_DATE_PARTITIONING`

  - `META_SYNC_DECODE_PARTITION`: infer from `org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING`

  - `META_SYNC_USE_FILE_LISTING_FROM_METADATA`: infer from `org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE`

#### API Updates

In `SparkKeyGeneratorInterface`, return type of the `getRecordKey` API has been changed from String to UTF8String.
```java
// Before
String getRecordKey(InternalRow row, StructType schema); 


// After
UTF8String getRecordKey(InternalRow row, StructType schema); 
```

#### Fallback Partition

If partition field value was null, Hudi has a fallback mechanism instead of failing the write. Until 0.9.0, 
`__HIVE_DEFAULT_PARTITION__`  was used as the fallback partition. After 0.9.0, due to some refactoring, fallback 
partition changed to `default`. This default partition does not sit well with some of the query engines. So, we are 
switching the fallback partition to `__HIVE_DEFAULT_PARTITION__`  from 0.12.0. We have added an upgrade step where in, 
we fail the upgrade if the existing Hudi table has a partition named `default`. Users are expected to rewrite the data 
in this partition to a partition named `__HIVE_DEFAULT_PARTITION__`. [Reference link](https://github.com/apache/hudi/blob/0d0a4152cfd362185066519ae926ac4513c7a152/hudi-common/src/main/java/org/apache/hudi/common/util/PartitionPathEncodeUtils.java#L29). 
However, if you had intentionally named your partition as `default`, you can bypass this using the config `hoodie.skip.default.partition.validation`.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351209).
