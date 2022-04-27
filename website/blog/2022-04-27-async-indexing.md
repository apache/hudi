---
title: "Asynchronous Indexing using Hudi"
excerpt: "How to setup Hudi for asynchronous indexing"
author: codope 
category: blog
---

In its rich set of asynchronous table services, Hudi has added yet another asynchronous service for indexing. It allows users to create different kinds of indexes (files, bloom
filters, and column stats) on the Hudi table without blocking ingestion.

<!--truncate-->

## Introduction

The metadata table in Hudi is an internal Merge-on-Read (MOR) table that has a single partition called `files` which stores the data partitions to files index that is used for
efficient file listing. In the release version 0.11.0, we added support for multi-modal indexes which include two other types of indexes like `COLUMN_STATS` and `BLOOM_FILTERS`.
Multi-modal indexes will greatly improve the record lookup time. Head over to that [blog](https://todo.add.link/) to learn more about multi-modal indexes. In this blog, we discuss how these indexes
can be created asynchronously without blocking the ongoing ingestion.

## Motivation

For those familiar with `CREATE INDEX` DDL in SQL, you would agree how easy it is to create index without worrying about ongoing writes and then use the index to enhance
performance. Hudi is always at the forefront of bring data lake closer to the database world, and go beyond using the lake simply as a store for unstructured data. Asynchronous
indexing is yet another milestone while marching towards that goal. Being able to index without blocking ingestion has two benefits, improved ingestion latency
(and hence even lesser gap between event time and arrival time), and reduced point of failure on the ingestion path. In the following sections, we will cover the design briefly and
see indexing in action.

## Design & Implementation

We introduce a new Hudi action called `indexing` on the [timeline](/docs/timeline), whose state is handled by the indexer. When indexing is scheduled or
requested, then the indexer picks a base instant i.e. the latest instant on the timelines without any pending actions before it. This is also when the filegroups for the
corresponding metadata index partition are initialized. When the indexing is executed, then the indexer will first index up to that base instant. Meanwhile, ingestion is not
blocked and regular writers continue to write commits. Indexer will try to catch up with the commits that happened after the base instant. Please check
out [RFC-45](https://github.com/apache/hudi/blob/master/rfc/rfc-45/rfc-45.md) for more details on the design and implementation of the indexer.

## Usage

Now, let us see the indexer in action. First, we will generate a continuous workload. In the below example, we are going to start a deltastreamer which will continuously write data
from raw parquet to Hudi table. We used the widely available [NY Taxi dataset](https://registry.opendata.aws/nyc-tlc-trip-records-pds/), whose setup details are as below:
<details>
  <summary>Ingestion write config</summary>
<p>

```
hoodie.datasource.write.recordkey.field=VendorID

hoodie.datasource.write.partitionpath.field=tpep_dropoff_datetime
hoodie.datasource.write.precombine.field=tpep_dropoff_datetime

hoodie.deltastreamer.source.dfs.root=/Users/home/path/to/data/parquet_files/
hoodie.deltastreamer.schemaprovider.target.schema.file=/Users/home/path/to/schema/schema.avsc
hoodie.deltastreamer.schemaprovider.source.schema.file=/Users/home/path/to/schema/schema.avsc

// set lock provider configs
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=<zk_url>
hoodie.write.lock.zookeeper.port=<zk_port>
hoodie.write.lock.zookeeper.lock_key=<zk_key>
hoodie.write.lock.zookeeper.base_path=<zk_base_path>
```

</p>
</details>

<details>
  <summary>Run deltastreamer</summary>
<p>

```
spark-submit \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls /Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.12.0-SNAPSHOT.jar` \
--props `ls /Users/home/path/to/write/config.properties` \
--source-class org.apache.hudi.utilities.sources.ParquetDFSSource  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-ordering-field tpep_dropoff_datetime   \
--table-type COPY_ON_WRITE \
--target-base-path file:///tmp/hudi-ny-taxi/   \
--target-table ny_hudi_tbl  \
--op UPSERT  \
--continuous \
--source-limit 5000000 \
--min-sync-interval-seconds 60
```

</p>
</details>

From version 0.11.0 onwards, Hudi metadata table is enabled by default and the files index will be automatically created. While the deltastreamer is running in continuous mode, let
us schedule the indexing for COLUMN_STATS index. First we need to define a properties file for the indexer.

```
# ensure that both metadata and async indexing is enabled as below two configs
hoodie.metadata.enable=true
hoodie.metadata.index.async=true

# enable column_stats index config
hoodie.metadata.index.column.stats.enable=true

# set concurrency mode and lock configs as this is a multi-writer scenario
# check https://hudi.apache.org/docs/concurrency_control/ for differnt lock provider configs
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=<zk_url>
hoodie.write.lock.zookeeper.port=<zk_port>
hoodie.write.lock.zookeeper.lock_key=<zk_key>
hoodie.write.lock.zookeeper.base_path=<zk_base_path>
```

### Schedule indexing

Now, we can schedule indexing using `HoodieIndexer` in `schedule` mode as follows:

```
spark-submit \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.12.0-SNAPSHOT.jar \
--props /Users/home/path/to/indexer.properties \
--mode schedule \
--base-path /tmp/hudi-ny-taxi \
--table-name ny_hudi_tbl \
--index-types COLUMN_STATS \
--parallelism 1 \
--spark-memory 1g
```

This will write an `indexing.requested` instant to the timeline.

### Execute Indexing

To execute indexing, run the indexer in `execute` mode as below.

```
spark-submit \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.12.0-SNAPSHOT.jar \
--props /Users/home/path/to/indexer.properties \
--mode execute \
--base-path /tmp/hudi-ny-taxi \
--table-name ny_hudi_tbl \
--index-types COLUMN_STATS \
--parallelism 1 \
--spark-memory 1g
```

We can also run the indexer in `scheduleAndExecute` mode to do the above two steps in one shot. Doing it separately gives us better control over when we want to execute.

Let's look at the data timeline.

```
ls -lrt /tmp/hudi-ny-taxi/.hoodie
total 1816
-rw-r--r--  1 sagars  wheel       0 Apr 14 19:53 20220414195327683.commit.requested
-rw-r--r--  1 sagars  wheel  153423 Apr 14 19:54 20220414195327683.inflight
-rw-r--r--  1 sagars  wheel  207061 Apr 14 19:54 20220414195327683.commit
-rw-r--r--  1 sagars  wheel       0 Apr 14 19:54 20220414195423420.commit.requested
-rw-r--r--  1 sagars  wheel     659 Apr 14 19:54 20220414195437837.indexing.requested
-rw-r--r--  1 sagars  wheel  323950 Apr 14 19:54 20220414195423420.inflight
-rw-r--r--  1 sagars  wheel       0 Apr 14 19:55 20220414195437837.indexing.inflight
-rw-r--r--  1 sagars  wheel  222920 Apr 14 19:55 20220414195423420.commit
-rw-r--r--  1 sagars  wheel     734 Apr 14 19:55 hoodie.properties
-rw-r--r--  1 sagars  wheel     979 Apr 14 19:55 20220414195437837.indexing
```

In the data timeline, we can see that indexing was scheduled after one commit completed (`20220414195327683.commit`) and another was requested
(`20220414195423420.commit.requested`). This would have picked `20220414195327683` as the base instant. Indexing was inflight with an inflight writer as well. If we parse the
indexer logs, we would find that it indeed caught up with instant `20220414195423420` after indexing upto the base instant.

```
22/04/14 19:55:22 INFO HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=HFILE) from /tmp/hudi-ny-taxi/.hoodie/metadata
22/04/14 19:55:22 INFO RunIndexActionExecutor: Starting Index Building with base instant: 20220414195327683
22/04/14 19:55:22 INFO HoodieBackedTableMetadataWriter: Creating a new metadata index for partition 'column_stats' under path /tmp/hudi-ny-taxi/.hoodie/metadata upto instant 20220414195327683
...
...
22/04/14 19:55:38 INFO RunIndexActionExecutor: Total remaining instants to index: 1
22/04/14 19:55:38 INFO HoodieTableMetaClient: Loading HoodieTableMetaClient from /tmp/hudi-ny-taxi/.hoodie/metadata
22/04/14 19:55:38 INFO HoodieTableConfig: Loading table properties from /tmp/hudi-ny-taxi/.hoodie/metadata/.hoodie/hoodie.properties
22/04/14 19:55:38 INFO HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=HFILE) from /tmp/hudi-ny-taxi/.hoodie/metadata
22/04/14 19:55:38 INFO HoodieActiveTimeline: Loaded instants upto : Option{val=[20220414195423420__deltacommit__COMPLETED]}
22/04/14 19:55:38 INFO RunIndexActionExecutor: Starting index catchup task
...
```

### Drop Index

To drop an index, just run the index in `dropindex` mode.

```
spark-submit \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.12.0-SNAPSHOT.jar \
--props /Users/home/path/to/indexer.properties \
--mode dropindex \
--base-path /tmp/hudi-ny-taxi \
--table-name ny_hudi_tbl \
--index-types COLUMN_STATS \
--parallelism 1 \
--spark-memory 2g
```

## Caveats

Asynchronous indexing feature is still evolving. There are a few gotchas while running the indexer:

- Stop other writers before dropping any index.
- Trigger indexing for one partition at a time.
- If an index is enabled via async HoodieIndexer, then ensure that index is also enabled in configs corresponding to regular ingestion writers. Otherwise, metadata writer will
  think that particular index was disabled and cleanup the metadata partition.
- Enable async indexing and specific index configs for all writers in multi-writer scenario.

## Conclusion

We saw how we can setup asynchronous indexing without blocking regular ingestion. This greatly enhances both indexing and ingestion experience. We plan to remove some the
limitations we discussed in a future release. Future developments on this feature are being tracked in [HUDI-2488](https://issues.apache.org/jira/browse/HUDI-2488). We look forward
to contributions from the community.
