---
title: Metadata Indexing
summary: "In this page, we describe how to run metadata indexing asynchronously."
toc: true
last_modified_at:
---

Hudi maintains a scalable [metadata](metadata) that has some auxiliary data about the table.
The [pluggable indexing subsystem](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
of Hudi depends on the metadata table. Different types of index, from `files` index for locating records efficiently
to `column_stats` index for data skipping, are part of the metadata table. A fundamental tradeoff in any data system
that supports indices is to balance the write throughput with index updates. A brute-force way is to lock out the writes
while indexing. However, very large tables can take hours to index. This is where Hudi's novel asynchronous metadata
indexing comes into play.

We can now create different metadata indices, including `files`, `bloom_filters`, `column_stats` and `record_index` asynchronously in
Hudi, which are then used by readers and writers to improve performance. Being able to index without blocking writing
has two benefits,

- improved write latency
- reduced resource wastage due to contention between writing and indexing.

In this document, we will learn how to setup asynchronous metadata indexing. To learn more about the design of this
feature, please check out [this blog](https://www.onehouse.ai/blog/asynchronous-indexing-using-hudi).

## Setup Async Indexing

First, we will generate a continuous workload. In the below example, we are going to start a [Hudi Streamer](hoodie_streaming_ingestion#hudi-streamer) which will continuously write data
from raw parquet to Hudi table. We used the widely available [NY Taxi dataset](https://registry.opendata.aws/nyc-tlc-trip-records-pds/), whose setup details are as below:
<details>
  <summary>Ingestion write config</summary>
<p>

```bash
hoodie.datasource.write.recordkey.field=VendorID
hoodie.datasource.write.partitionpath.field=tpep_dropoff_datetime
hoodie.datasource.write.precombine.field=tpep_dropoff_datetime
hoodie.streamer.source.dfs.root=/Users/home/path/to/data/parquet_files/
hoodie.streamer.schemaprovider.target.schema.file=/Users/home/path/to/schema/schema.avsc
hoodie.streamer.schemaprovider.source.schema.file=/Users/home/path/to/schema/schema.avsc
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
  <summary>Run Hudi Streamer</summary>
<p>

```bash
spark-submit \
--class org.apache.hudi.utilities.streamer.HoodieStreamer `ls /Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0.jar` \
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

Hudi metadata table is enabled by default and the files index will be automatically created. While the Hudi Streamer is running in continuous mode, let
us schedule the indexing for COLUMN_STATS index. First we need to define a properties file for the indexer.

### Configurations

As mentioned before, metadata indices are pluggable. One can add any index at any point in time depending on changing
business requirements. Some configurations to enable particular indices are listed below. Currently, available indices under
metadata table can be explored [here](metadata#metadata-table-indices) along with [configs](metadata#enable-hudi-metadata-table-and-multi-modal-index-in-write-side) 
to enable them. The full set of metadata configurations can be explored [here](configurations/#Metadata-Configs).

:::note
Enabling the metadata table and configuring a lock provider are the prerequisites for using async indexer. Checkout a sample
configuration below.
:::

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
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0.jar \
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
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0.jar \
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
/Users/home/path/to/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0.jar \
--props /Users/home/path/to/indexer.properties \
--mode dropindex \
--base-path /tmp/hudi-ny-taxi \
--table-name ny_hudi_tbl \
--index-types COLUMN_STATS \
--parallelism 1 \
--spark-memory 2g
```

## Caveats

Asynchronous indexing feature is still evolving. Few points to note from deployment perspective while running the indexer:

- While an index can be created concurrently with ingestion, it cannot be dropped concurrently. Please stop all writers
  before dropping an index.
- Files index is created by default as long as the metadata table is enabled.
- Trigger indexing for one metadata partition (or index type) at a time.
- If an index is enabled via async HoodieIndexer, then ensure that index is also enabled in configs corresponding to regular ingestion writers. Otherwise, metadata writer will
  think that particular index was disabled and cleanup the metadata partition.
- In the case of multi-writers, enable async index and specific index config for all writers.
- Unlike other table services like compaction and clustering, where we have a separate configuration to run inline, there is no such inline config here.
  For example, if async indexing is disabled and metadata is enabled along with column stats index type, then both files and column stats index will be created synchronously with ingestion.

Some of these limitations will be removed in the upcoming releases. Please
follow [HUDI-2488](https://issues.apache.org/jira/browse/HUDI-2488) for developments on this feature.
