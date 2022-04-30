---
title: "Release 0.10.0"
sidebar_position: 4
layout: releases
toc: true
last_modified_at: 2021-12-10T22:07:00+08:00
---
# [Release 0.10.0](https://github.com/apache/hudi/releases/tag/release-0.10.0) ([docs](/docs/quick-start-guide))

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