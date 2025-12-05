---
title: "Release 0.14.0"
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.14.0](https://github.com/apache/hudi/releases/tag/release-0.14.0)
Apache Hudi 0.14.0 marks a significant milestone with a range of new functionalities and enhancements. 
These include the introduction of Record Level Index, automatic generation of record keys, the `hudi_table_changes` 
function for incremental reads, and more. Notably, this release also incorporates support for Spark 3.4. On the Flink 
front, version 0.14.0 brings several exciting features such as consistent hashing index support, Flink 1.17 support, and 
Update and Delete statement support. Additionally, this release upgrades the Hudi table version, prompting users to consult
the Migration Guide provided below. We encourage users to review the [release highlights](#release-highlights),
[breaking changes](#breaking-changes), and [behavior changes](#behavior-changes) before 
adopting the 0.14.0 release.



## Migration Guide
In version 0.14.0, we've made changes such as the removal of compaction plans from the ".aux" folder and the introduction
of a new log block version. As part of this release, the table version is updated to version `6`. When running a Hudi job 
with version 0.14.0 on a table with an older table version, an automatic upgrade process is triggered to bring the table 
up to version `6`. This upgrade is a one-time occurrence for each Hudi table, as the `hoodie.table.version` is updated in
the property file upon completion of the upgrade. Additionally, a command-line tool for downgrading has been included, 
allowing users to move from table version `6` to `5`, or revert from Hudi 0.14.0 to a version prior to 0.14.0. To use this 
tool, execute it from a 0.14.0 environment. For more details, refer to the 
[hudi-cli](/docs/cli/#upgrade-and-downgrade-table).

:::caution
If migrating from an older release (pre 0.14.0), please also check the upgrade instructions from each older release in
sequence.
:::

### Bundle Updates

#### New Spark Bundles
In this release, we've expanded our support to include bundles for both Spark 3.4 
([hudi-spark3.4-bundle_2.12](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.4-bundle_2.12)) 
and Spark 3.0 ([hudi-spark3.0-bundle_2.12](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.0-bundle_2.12)).
Please note that, the support for Spark 3.0 had been discontinued after Hudi version 0.10.1, but due to strong community 
interest, it has been reinstated in this release.

### Breaking Changes

#### INSERT INTO behavior with Spark SQL
Before version 0.14.0, data ingested through `INSERT INTO` in Spark SQL followed the upsert flow, where multiple versions 
of records would be merged into one version. However, starting from 0.14.0, we've altered the default behavior of 
`INSERT INTO` to utilize the `insert` flow internally. This change significantly enhances write performance as it 
bypasses index lookups.

If a table is created with a *preCombine* key, the default operation for `INSERT INTO` remains as `upsert`. Conversely, 
if no *preCombine* key is set, the underlying write operation for `INSERT INTO` defaults to `insert`. Users have the 
flexibility to override this behavior by explicitly setting values for the config 
[`hoodie.spark.sql.insert.into.operation`](https://hudi.apache.org/docs/configurations#hoodiesparksqlinsertintooperation) 
as per their requirements. Possible values for this config include `insert`, `bulk_insert`, and `upsert`.

Additionally, in version 0.14.0, we have **deprecated** two related older configs:
- `hoodie.sql.insert.mode`
- `hoodie.sql.bulk.insert.enable`.

### Behavior changes

#### Simplified duplicates handling with Inserts in Spark SQL
In cases where the operation type is configured as `insert` for the Spark SQL `INSERT INTO` flow, users now have the 
option to enforce a duplicate policy using the configuration setting 
[`hoodie.datasource.insert.dup.policy`](https://hudi.apache.org/docs/configurations#hoodiedatasourceinsertduppolicy). 
This policy determines the action taken when incoming records being ingested already exist in storage. The available 
values for this configuration are as follows:

- `none`: No specific action is taken, allowing duplicates to exist in the Hudi table if the incoming records contain duplicates.
- `drop`: Matching records from the incoming writes will be dropped, and the remaining ones will be ingested.
- `fail`: The write operation will fail if the same records are re-ingested. In essence, a given record, as determined 
by the key generation policy, can only be ingested once into the target table.

With this addition, an older related configuration setting, 
[`hoodie.datasource.write.insert.drop.duplicates`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriteinsertdropduplicates), 
will be deprecated. The newer configuration will take precedence over the old one when both are specified. If no specific 
configurations are provided, the default value for the newer configuration will be assumed. Users are strongly encouraged 
to migrate to the use of these newer configurations when using Spark SQL. 

:::caution
This is only applicable to Spark SQL writing.
:::


#### Compaction with MOR table
For Spark batch writers (both the Spark datasource and Spark SQL), compaction is automatically enabled by default for 
MOR (Merge On Read) tables, unless users explicitly override this behavior. Users have the option to disable compaction 
explicitly by setting [`hoodie.compact.inline`](https://hudi.apache.org/docs/configurations#hoodiecompactinline) to false. 
In case users do not override this configuration, compaction may be triggered for MOR tables approximately once every 
5 delta commits (the default value for 
[`hoodie.compact.inline.max.delta.commits`](https://hudi.apache.org/docs/configurations#hoodiecompactinlinemaxdeltacommits)).

#### `HoodieDeltaStreamer` renamed to `HoodieStreamer` (Hudi Streamer)

Starting from version 0.14.0, we have renamed [HoodieDeltaStreamer](https://github.com/apache/hudi/blob/84a80e21b5f0cdc1f4a33957293272431b221aa9/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java)
to [`HoodieStreamer`](https://github.com/apache/hudi/blob/84a80e21b5f0cdc1f4a33957293272431b221aa9/hudi-utilities/src/main/java/org/apache/hudi/utilities/streamer/HoodieStreamer.java). 
We have ensured backward compatibility so that existing user jobs remain unaffected. However, in upcoming 
releases, support for Deltastreamer might be discontinued. Hence, we strongly advise users to transition to using 
`HoodieStreamer` instead.


#### MERGE INTO JOIN condition 
Starting from version 0.14.0, Hudi has the capability to automatically generate primary record keys when users do not 
provide explicit specifications. This enhancement enables the `MERGE INTO JOIN` clause to reference any data column for 
the join condition in Hudi tables where the primary keys are generated by Hudi itself. However, in cases where users 
configure the primary record key, the join condition still expects the primary key fields as specified by the user.


## Release Highlights

### Record Level Index
Hudi version 0.14.0, introduces a new index implementation -  
[Record Level Index](https://github.com/apache/hudi/blob/master/rfc/rfc-8/rfc-8.md#rfc-8-metadata-based-record-index). 
The Record level Index significantly enhances write performance for large tables by efficiently storing per-record 
locations and enabling swift retrieval during index lookup operations. It can effectively replace other 
[Global indices](https://hudi.apache.org/docs/next/indexing#global-and-non-global-indexes) like Global_bloom, 
Global_Simple, or Hbase, commonly used in Hudi.

Bloom and Simple Indexes exhibit slower performance for large datasets due to the high costs associated with gathering 
index data from various data files during lookup. Moreover, these indexes do not preserve a one-to-one record-key to 
record file path mapping; instead, they deduce the mapping through an optimized search at lookup time. The per-file 
overhead required by these indexes makes them less effective for datasets with a larger number of files or records.

On the other hand, the Hbase Index saves a one-to-one mapping for each record key, resulting in fast performance that 
scales with the dataset size. However, it necessitates a separate HBase cluster for maintenance, which is operationally 
challenging and resource-intensive, requiring specialized expertise.

The Record Index combines the speed and scalability of the HBase Index without its limitations and overhead. Being a 
part of the HUDI Metadata Table, any future performance enhancements in writes and queries will automatically translate 
into improved performance for the Record Index. Adopting the Record Level Index has the potential to boost index lookup 
performance by 4 to 10 times, depending on the workload, even for extremely large-scale datasets (e.g., 1TB).

With the Record Level Index, significant performance improvements can be observed for large datasets, as latency is 
directly proportional to the amount of data being ingested. This is in contrast to other Global indices where index 
lookup time increases linearly with the table size. The Record Level Index is specifically designed to efficiently 
handle lookups for such large-scale data without a linear increase in lookup times as the table size grows.

To harness the benefits of this lightning-fast index, users need to enable two configurations:
- [`hoodie.metadata.record.index.enable`](https://hudi.apache.org/docs/next/configurations#hoodiemetadatarecordindexenable) must be enabled to write the Record Level Index to the metadata table.
- `hoodie.index.type` needs to be set to `RECORD_INDEX` for the index lookup to utilize the Record Level Index.


### Support for Hudi tables with Autogenerated keys
Since the initial official version of Hudi, the primary key was a mandatory field that users needed to configure for any 
Hudi table. Starting 0.14.0, we are relaxing this constraint. This enhancement addresses a longstanding need within the 
community, where certain use-cases didn't naturally possess an intrinsic primary key. Version 0.14.0 now offers the 
flexibility for users to create a Hudi table without the need to explicitly configure a primary key (by omitting the 
configuration setting -
[`hoodie.datasource.write.recordkey.field`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriterecordkeyfield)). 
Hudi will **automatically generate the primary keys** in such cases. This feature is applicable only for new tables and 
cannot be altered for existing ones.


This functionality is available in all spark writers with certain limitations. For append only type of use cases, Inserts and 
bulk_inserts are allowed with all four writers - Spark Datasource, Spark SQL, Spark Streaming, Hudi Streamer. Updates and 
Deletes are supported only using spark-sql `MERGE INTO` , `UPDATE` and `DELETE` statements. With Spark Datasource, `UPDATE` 
and `DELETE` are supported only when the source dataframe contains Hudi's meta fields. Please check out our 
[quick start guide](https://hudi.apache.org/docs/quick-start-guide) for code snippets on Hudi table CRUD operations where 
keys are autogenerated.

### Spark 3.4 version support

Spark 3.4 support is added; users who are on Spark 3.4 can use 
[hudi-spark3.4-bundle](https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.4-bundle). Spark 3.2, Spark 3.1, 
Spark3.0 and Spark 2.4 will continue to be supported. Please check the migration guide for bundle updates. To quickly get 
started with Hudi and Spark 3.4, you can explore our [quick start guide](https://hudi.apache.org/docs/quick-start-guide).

### Query side improvements:

#### Metadata table support with Athena

Users now have the ability to utilize Hudi’s [Metadata table](https://hudi.apache.org/docs/metadata/) seamlessly with Athena. 
The file listing index removes the need for recursive file system calls like "list files" by retrieving information 
from an index that maintains a mapping of partitions to files. This approach proves to be highly efficient, particularly 
when dealing with extensive datasets. With Hudi 0.14.0, users can activate file listing based on the metadata table when 
performing Glue catalog synchronization for their Hudi tables. To enable this functionality, users can configure 
`hoodie.datasource.meta.sync.glue.metadata_file_listing` and set it to true during the Glue sync process.

#### Leverage Parquet bloom filters w/ read queries
In Hudi 0.14.0, users can now utilize the native 
[Parquet bloom filters](https://github.com/apache/parquet-format/blob/1603152f8991809e8ad29659dffa224b4284f31b/BloomFilter.md), 
provided their compute engine supports Apache Parquet 1.12.0 or higher. This support covers both the writing and reading 
of datasets. Hudi facilitates the use of native Parquet bloom filters through Hadoop configuration. Users are required 
to set a Hadoop configuration with a specific key representing the column for which the bloom filter is to be applied. 
For example, `parquet.bloom.filter.enabled#rider=true` creates a bloom filter for the rider column. Whenever a query 
involves a predicate on the rider column, the bloom filter comes into play, enhancing read performance.

#### Incremental queries with multi-writers
In multi-writer scenarios, there can be instances of gaps in the timeline (requests or inflight instants that are not 
the latest instant) due to concurrent writing activities. These gaps may result in inconsistent outcomes when 
performing incremental queries. To address this issue, Hudi 0.14.0 introduces a new configuration setting, 
[`hoodie.read.timeline.holes.resolution.policy`](https://hudi.apache.org/docs/configurations#hoodiereadtimelineholesresolutionpolicy), 
specifically designed for handling these inconsistencies in incremental queries. The configuration provides three possible policies:
- `FAIL`: This serves as the default policy and throws an exception when such timeline gaps are identified during an incremental query.
- `BLOCK`: In this policy, the results of an incremental query are limited to the time range between the holes in the 
   timeline. For instance, if a gap is detected at instant t1 within the incremental query range from t0 to t2, the 
   query will only display results between t0 and t1 without failing.
- `USE_TRANSITION_TIME`: This policy is experimental and involves using the state transition time, which is based on the 
   file modification time of commit metadata files in the timeline, during the incremental query.

#### Timestamp support with Hive 3.x
For quite some time, Hudi users encountered [challenges](https://issues.apache.org/jira/browse/HUDI-83) regarding reading Timestamp type columns written by Spark and 
subsequently attempting to read them with Hive 3.x. While in Hudi 0.13.x, we introduced a 
[workaround](https://github.com/apache/hudi/commit/cd314b8cfa58c32f731f7da2aa6377a09df4c6f9#diff-cff4dfc264f7abcac63a5ba5db55b38115177fe279ab35807d345c2b8872475e)
to mitigate this issue, version 0.14.0 now ensures full compatibility of HiveAvroSerializer with Hive 3.x to resolve this.

#### Google BigQuery sync enhancements
With 0.14.0, the [BigQuerySyncTool](https://hudi.apache.org/docs/gcp_bigquery) supports syncing table to BigQuery 
using [manifests](https://cloud.google.com/blog/products/data-analytics/bigquery-manifest-file-support-for-open-table-format-queries).
This is expected to have better query performance compared to legacy way. Schema evolution is supported with the manifest approach. 
Partition column no longer needs to be dropped from the files due to new schema handling improvements. To enable this 
feature, users can set 
[`hoodie.gcp.bigquery.sync.use_bq_manifest_file`](https://hudi.apache.org/docs/configurations#hoodiegcpbigquerysyncuse_bq_manifest_file) 
to true.

### Spark read side improvements

#### Snapshot read support for MOR Bootstrap tables
With 0.14.0, MOR snapshot read support is added for Bootstrapped tables. The default behavior has been changed in several 
ways to match the behavior of non-bootstrapped MOR tables. Snapshot reads will now be the default reading mode. Use 
`hoodie.datasource.query.type=read_optimized` for read optimized queries which was previously the default behavior. 
Hive sync for such tables will result in both _ro and _rt suffixed to the table name to signify read optimized and snapshot 
reading respectively.

####  Table-valued function named hudi_table_changes designed for incremental reading through Spark SQL
Hudi offers the functionality to fetch a stream of records changed since a specified commit timestamp through the [incremental query](https://hudi.apache.org/docs/quick-start-guide#incremental-query) type. With the release of Hudi 0.14.0, we've introduced a more straightforward method to access the most recent state or change streams of Hudi datasets. This is achieved using a table-valued function named `hudi_table_changes` in Spark SQL. Here's the syntax and several examples of how to utilize this function:

```text
SYNTAX
hudi_table_changes(table, queryType, beginTime [, endTime]);
-- table: table identifier, example: db.tableName, tableName, or path for the table, example: hdfs://path/to/hudiTable.
-- queryType: incremental query mode, valid values: latest_state, cdc
(for cdc query, first enable cdc for the table by setting cdc.enabled=true),
-- beginTime: instantTime to begin query from, example: earliest, 202305150000,
-- endTime: optional instantTime to end query at, example: 202305160000,

EXAMPLES
-- incrementally query data by table name
-- start from earliest available commit, end at latest available commit.
SELECT * FROM hudi_table_changes('db.table', 'latest_state', 'earliest');

-- start from earliest, end at 202305160000.
SELECT * FROM hudi_table_changes('table', 'latest_state', 'earliest', '202305160000');

-- incrementally query data by path
-- start from earliest available commit, end at latest available commit.
SELECT * FROM hudi_table_changes('path/to/table', 'cdc', 'earliest');
```

Checkout the [quickstart](/docs/quick-start-guide#incremental-query) for more examples.

#### New MOR file format reader in Spark:
Based on a proposal from [RFC-72](https://github.com/apache/hudi/pull/9235) aimed at redesigning Hudi-Spark integration,
we are introducing an experimental file format reader for MOR (Merge On Read) tables. This reader is expected to 
significantly reduce read latencies by 20 to 40% when compared to the older file format, particularly for snapshot and 
bootstrap queries. The goal is to bring the latencies closer to those of the COW (Copy On Write) file format. To utilize 
this new file format, users need to set `hoodie.datasource.read.use.new.parquet.file.format=true`. It's important to note
that this feature is still experimental and comes with a few limitations. For more details and if you're interested in 
contributing, please refer to [this GitHub issue](https://github.com/apache/hudi/issues/16112).

### Spark write side improvements

#### Bulk_Insert and row writer enhancements
The 0.14.0 release provides support for using bulk insert operation while performing SQL operations like `INSERT OVERWRITE TABLE`
and `INSERT OVERWRITE PARTITION`. To enable bulk insert, set config 
[`hoodie.spark.sql.insert.into.operation`](https://hudi.apache.org/docs/configurations#hoodiesparksqlinsertintooperation) 
to value `bulk_insert`. Bulk insert has better write performance compared to insert operation. Row writer support is 
also added for Simple bucket index.

### Hudi Streamer enhancements

#### Dynamic configuration updates
When Hudi Streamer is run in continuous mode, the properties can be refreshed/updated before each sync calls. 
Interested users can implement `org.apache.hudi.utilities.streamer.ConfigurationHotUpdateStrategy` to leverage this.

#### SQL File based source for Hudi Streamer
A new source - [SqlFileBasedSource](https://github.com/apache/hudi/blob/30146d61f5544f06e2100234b9bf9c5e4bc2a97f/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/SqlFileBasedSource.java), 
has been added to Hudi Streamer designed to facilitate one-time backfill scenarios.

### Flink Enhancements
Below are the Flink Engine based enhancements in the 0.14.0 release.

#### Consistent hashing index support
In comparison to the static hashing index (BUCKET index), the consistent hashing index offers dynamic scalability of 
data buckets for the writer. To utilize this feature, configure the option `index.type` as `BUCKET` and set 
`hoodie.index.bucket.engine` to `CONSISTENT_HASHING`.

When enabling the consistent hashing index, it's important to activate clustering scheduling within the writer. 
The clustering plan should be executed through an offline Spark job. During this process, the writer will perform dual writes 
for both the old and new data buckets while the clustering is pending. Although the dual write does not impact correctness, 
it is strongly recommended to execute clustering as quickly as possible.

#### Dynamic partition pruning for streaming read
Before 0.14.0, the Flink streaming reader can not prune the datetime partitions correctly when the queries have 
predicates with constant datetime filtering. Since this release, the Flink streaming queries have been fixed to support 
any pattern of filtering predicates, including but not limited to the datetime filtering.

#### Simple bucket index table query speed up (with index fields)
For a simple bucket index table, if the query takes equality filtering predicates on index key fields, Flink engine 
would optimize the planning to only include the source data files from a very specific data bucket; such queries expect 
to have nearly `hoodie.bucket.index.num.buckets` times performance improvement in average.

#### Flink 1.17 support
Flink 1.17 is supported with a new compile maven profile `flink1.17`, adding profile `-Pflink1.17` in the compile cmd of
Flink Hudi bundle jar to enable the integration with Flink 1.17.

#### Update deletes statement for Flink
`UPDATE` and `DELETE` statements have been integrated since this release for batch queries. Current only table that 
defines primary keys can handle the statement correctly.

```
UPDATE hudi_table SET ... WHERE ...
DELETE FROM hudi_table WHERE ...

EXAMPLES
-- update the specific records with constant age
UPDATE hudi_table SET age=19 WHERE UUID in ('id1', 'id2');
-- delete all the records that with age greater than 23
DELETE FROM hudi_table WHERE age > 23;
```



### Java Enhancements
Lot of write operations have been extended to support Java engine to bring it to parity with other engines. For eg, 
compaction, clustering, and metadata table support has been added to Java Engine with 0.14.0.

## Known Regressions
In Hudi 0.14.0, when querying a table that uses ComplexKeyGenerator or CustomKeyGenerator, partition values are returned
as string. Note that there is no type change on the storage i.e. partition fields are written in the user-defined type 
on storage. However, this is a breaking change for the aforementioned key generators and will be fixed in 0.14.1 -
[tracking issue](https://github.com/apache/hudi/issues/16251)

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12352700).
