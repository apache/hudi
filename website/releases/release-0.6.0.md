---
title: "Release 0.6.0"
sidebar_position: 22
layout: releases
toc: true
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.6.0](https://github.com/apache/hudi/releases/tag/release-0.6.0)

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
- [HoodieMultiDeltaStreamer](https://hudi.apache.org/docs/writing_data#multitabledeltastreamer) adds support for ingesting multiple kafka streams in a single DeltaStreamer deployment, effectively reducing operational burden for using Hudi Streamer
  as your data lake ingestion tool (Experimental feature)
- Added a new tool - InitialCheckPointProvider, to set checkpoints when migrating to DeltaStreamer after an initial load of the table is complete.
- Hudi Streamer tool now supports ingesting CSV data sources, chaining of multiple transformers to build more advanced ETL jobs.
- Introducing a new `CustomKeyGenerator` key generator class, that provides flexible configurations to provide enable different types of key, partition path generation in  single class.
  We also added support for more time units and date/time formats in `TimestampBasedKeyGenerator`.

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
