---
title: "Release 0.11.0"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2022-01-27T22:07:00+08:00
---
# [Release 0.11.0](https://github.com/apache/hudi/releases/tag/release-0.11.0) ([docs](/docs/quick-start-guide))

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

### Async Indexer

In 0.11.0, we added a new asynchronous service for indexing to our rich set of table services. It allows users to create
different kinds of indices (e.g., files, bloom filters, and column stats) in the metadata table without blocking
ingestion. The indexer adds a new action `indexing` on the timeline. While the indexing process itself is asynchronous
and non-blocking to writers, a lock provider needs to be configured to safely co-ordinate the process with the inflight
writers.

*See the [migration guide](#migration-guide) for more details.*

### Spark DataSource Improvements

Hudi's Spark low-level integration got considerable overhaul consolidating common flows to share the infrastructure and
bring both compute and data throughput efficiencies when querying the data.

- Both COW and MOR (except for incremental queries) tables are now leveraging Vectorized Parquet reader while reading
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

In 0.11.0,

- Spark 3.2 support is added; users can use `hudi-spark3.2-bundle` or `hudi-spark3-bundle` with Spark 3.2.
- Spark 3.1 will continue to be supported via `hudi-spark3.1-bundle`. 
- Spark 2.4 will continue to be supported via `hudi-spark2.4-bundle` or `hudi-spark-bundle`.
- Users are encouraged to use bundles with specific Spark version in the name: `hudi-sparkX.Y-bundle`.
- Spark bundle for 3.0.x is no longer officially supported. Users are encouraged to upgrade to Spark 3.2 or 3.1.
- `spark-avro` package is no longer required to work with Spark bundles.

### Slim Utilities Bundle

In 0.11.0, a new `hudi-utilities-slim-bundle` is added to exclude dependencies that could cause conflicts and
compatibility issues with other frameworks such as Spark.

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

### BigQuery Integration

In 0.11.0, Hudi tables can be queried from BigQuery as external tables. Users can
set `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` as the sync tool implementation for `HoodieDeltaStreamer` and make
the target Hudi table discoverable in BigQuery. Please refer to [Google Cloud BigQuery](/docs/next/gcp_bigquery) guide
page for more details.

*Note: this is an experimental feature and only works with hive-style partitioned Copy-On-Write tables.*

### AWS Glue Meta Sync

In 0.11.0, Hudi tables can be sync'ed to AWS Glue Data Catalog via AWS SDK directly. Users can
set `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` as the sync tool implementation for `HoodieDeltaStreamer` and make
the target Hudi table discoverable in Glue catalog. Please refer
to [Sync to AWS Glue Data Catalog](/docs/next/syncing_aws_glue_data_catalog) guide page for more details.

*Note: this is an experimental feature.*

### DataHub Meta Sync

In 0.11.0, Hudi table's metadata (specifically, schema and last sync commit time) can be sync'ed
to [DataHub](https://datahubproject.io/). Users can set `org.apache.hudi.sync.datahub.DataHubSyncTool` as the sync tool
implementation for `HoodieDeltaStreamer` and sync the target table as a Dataset in DataHub. Please refer
to [Sync to DataHub](/docs/next/syncing_datahub) guide page for more details.

*Note: this is an experimental feature.*

### Bucket Index

Bucket index, an efficient and light-weight index type, is added in 0.11.0. It distributes records to buckets using a
hash function based on the record keys, where each bucket corresponds to a single file group. To use this index, set the
index type to `BUCKET` and set `hoodie.storage.layout.partitioner.class` to `org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner`.
For Flink, set `index.type=BUCKET`.

*For more details, please refer to `HoodieIndexConfig` in the [configurations](/docs/configurations) page.*

### Savepoint & Restore

Disaster recovery is a mission critical feature in any production deployment. Especially when it comes to systems that
store data. Hudi had savepoint and restore functionality right from the beginning for COW tables. In 0.11.0, we have
added support for MOR tables.

*More info about this feature can be found in [Disaster Recovery](/docs/next/disaster_recovery).*

### Write Commit Callback for Pulsar

Hudi users can use `org.apache.hudi.callback.HoodieWriteCommitCallback` to invoke callback function upon successful
commits. In 0.11.0, we add`HoodieWriteCommitPulsarCallback` in addition to the existing HTTP callback and Kafka
callback. Please refer to `org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig` for
configurations to set.

### HiveSchemaProvider

In 0.11.0, `org.apache.hudi.utilities.schema.HiveSchemaProvider` is added for getting schema from user-defined hive
tables. This is useful when tailing Hive tables in `HoodieDeltaStreamer` instead of having to provide avro schema files.

## Migration Guide

### Use async indexer

Enabling metadata table and configuring a lock provider are the prerequisites for using async indexer. The
implementation details were illustrated in [RFC-45](https://github.com/apache/hudi/blob/master/rfc/rfc-45/rfc-45.md). At
the minimum, users need to set the following configurations to schedule and run the indexer:

```shell
# enable async index
hoodie.metadata.index.async=true
# enable specific index type, column stats for example 
hoodie.metadata.index.column.stats.enable=true
# set OCC concurrency mode
hoodie.write.concurrency.mode=optimistic_concurrency_control
# set lock provider configs
hoodie.write.lock.provider=<LockProviderClass>
```

Few points to note from deployment perspective:

1. Files index is created by default as long as the metadata table is enabled.
2. If you intend to build any index asynchronously, say column stats, then be sure to enable the async index and column
   stats index type on the regular ingestion writers as well.
3. In the case of multi-writers, enable async index and specific index config for all writers.
4. While an index can be created concurrently with ingestion, it cannot be dropped concurrently. Please stop all writers
   before dropping an index.

Some of these limitations will be overcome in the upcoming releases. Please
follow [HUDI-2488](https://issues.apache.org/jira/browse/HUDI-2488) for developments on this feature.

### Bundle usage

As we relax the requirement of adding `spark-avro` package in 0.11.0 to work with Spark and Utilities bundle,
the option `--package org.apache.spark:spark-avro_2.1*:*` can be dropped.

### Configuration updates

- For MOR tables, `hoodie.datasource.write.precombine.field` is required for both write and read.
- Only set `hoodie.datasource.write.drop.partition.columns=true` when work
  with [BigQuery integration](/docs/next/gcp_bigquery).
- For Spark readers that rely on extracting physical partition path,
  set `hoodie.datasource.read.extract.partition.values.from.path=true` to stay compatible with existing behaviors.
- Default index type for Spark was change from `BLOOM`
  to `SIMPLE` ([HUDI-3091](https://issues.apache.org/jira/browse/HUDI-3091)). If you currently rely on the default `BLOOM`
  index type, please update your configuration accordingly.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12350673)
