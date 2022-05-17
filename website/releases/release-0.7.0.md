---
title: "Release 0.7.0"
sidebar_position: 7
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---
# [Release 0.7.0](https://github.com/apache/hudi/releases/tag/release-0.7.0) ([docs](/docs/quick-start-guide))

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