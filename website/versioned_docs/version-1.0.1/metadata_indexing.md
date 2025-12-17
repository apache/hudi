---
title: Indexing
summary: "In this page, we describe how to run metadata indexing asynchronously."
toc: true
last_modified_at:
---

Hudi maintains a scalable [metadata](metadata.md) that has some auxiliary data about the table.
The [pluggable indexing subsystem](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
of Hudi depends on the metadata table. Different types of index, from `files` index for locating records efficiently
to `column_stats` index for data skipping, are part of the metadata table. A fundamental tradeoff in any data system
that supports indices is to balance the write throughput with index updates. A brute-force way is to lock out the writes
while indexing. Hudi supports index creation using SQL, Datasource as well as async indexing. However, very large tables 
can take hours to index. This is where Hudi's novel concurrent indexing comes into play. 

## Concurrent Indexing

Indexes in Hudi are created in two phases and uses a mix of optimistic concurrency control and multi-version concurrency control techniques. The two
phase approach ensures that the other writers are unblocked.

- **Scheduling & Planning** : This is the first phase which schedules an indexing plan and is protected by a lock. Indexing plan considers all the completed commits upto indexing instant.
- **Execution** : This phase creates the index files as mentioned in the index plan. At the end of the phase Hudi ensures the completed commits after indexing instant used already created index plan to add corresponding index metadata. This check is protected by a metadata table lock and in case of failures indexing is aborted.

We can now create different indexes and metadata, including `bloom_filters`, `column_stats`, `partition_stats`, `record_index`, `secondary_index`
and `expression_index` asynchronously in Hudi. Being able to index without blocking writing ensures write performance is unaffected and no 
additional manual maintenance is necessary to add/remove indexes. It also reduces resource wastage by avoiding contention between writing and indexing.

Please refer section [Setup Async Indexing](#setup-async-indexing) to get more details on how to setup
asynchronous indexing. To learn more about the design of asynchronous indexing feature, please check out [this blog](https://www.onehouse.ai/blog/asynchronous-indexing-using-hudi).

## Index Creation Using SQL

Currently indexes like secondary index, expression index and record index can be created using SQL create index command.
For more information on these indexes please refer [metadata section](metadata/#types-of-table-metadata)

:::note
Please note in order to create secondary index:
1. The table must have a primary key and merge mode should be [COMMIT_TIME_ORDERING](record_merger.md#commit_time_ordering).
2. Record index must be enabled. This can be done by setting `hoodie.metadata.record.index.enable=true` and then creating `record_index`. Please note the example below.
:::

**Examples**
```sql
-- Create record index on primary key - uuid
CREATE INDEX record_index ON hudi_indexed_table (uuid);

-- Create secondary index on rider column.
CREATE INDEX idx_rider ON hudi_indexed_table (rider);

-- Create expression index by performing transformation on ts and driver column 
-- The index is created on the transformed column. Here column stats index is created on ts column
-- and bloom filters index is created on driver column.
CREATE INDEX idx_column_ts ON hudi_indexed_table USING column_stats(ts) OPTIONS(expr='from_unixtime', format = 'yyyy-MM-dd');
CREATE INDEX idx_bloom_driver ON hudi_indexed_table USING bloom_filters(driver) OPTIONS(expr='identity');
```

For more information on index creation using SQL refer [SQL DDL](sql_ddl.md#create-index) 

## Index Creation Using Datasource

Indexes like `bloom_filters`, `column_stats`, `partition_stats` and `record_index` can be created using Datasource. 
Below we list the various configs which are needed to create the indexes mentioned.

```sql
-- [Required Configs] Partition stats
hoodie.metadata.index.partition.stats.enable=true
hoodie.metadata.index.column.stats.enable=true
-- [Optional Configs] - list of columns to index on. By default all columns are indexed
hoodie.metadata.index.column.stats.column.list=col1,col2,...

-- [Required Configs] Column stats
hoodie.metadata.index.column.stats.enable=true
-- [Optional Configs] - list of columns to index on. By default all columns are indexed
hoodie.metadata.index.column.stats.column.list=col1,col2,...

-- [Required Configs] Record Level Index
hoodie.metadata.record.index.enable=true

-- [Required Configs] Bloom filter Index
hoodie.metadata.index.bloom.filter.enable=true
```

Here is an example which shows how to create indexes for a table created using Datasource API.

**Examples**
```scala
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord
import spark.implicits._

val tableName = "trips_table_index"
val basePath = "file:///tmp/trips_table_index"

val columns = Seq("ts","uuid","rider","driver","fare","city")
val data =
  Seq((1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179L,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"    ),
    (1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai"));

var inserts = spark.createDataFrame(data).toDF(columns:_*)
inserts.write.format("hudi").
  option("hoodie.datasource.write.partitionpath.field", "city").
  option("hoodie.table.name", tableName).
  option("hoodie.write.record.merge.mode", "COMMIT_TIME_ORDERING").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  mode(Overwrite).
  save(basePath)
  
// Create record index and secondary index for the table
spark.sql(s"CREATE TABLE test_table_external USING hudi LOCATION '$basePath'")
spark.sql(s"SET hoodie.metadata.record.index.enable=true")
spark.sql(s"CREATE INDEX record_index ON test_table_external (uuid)")
spark.sql(s"CREATE INDEX idx_rider ON test_table_external (rider)")
spark.sql(s"SHOW INDEXES FROM hudi_indexed_table").show(false)
spark.sql(s"SELECT * FROM hudi_indexed_table WHERE rider = 'rider-E'").show(false)  
```

## Setup Async Indexing

In the example we will have continuous writing using Hudi Streamer and also create index in parallel. The index creation
in example is done using HoodieIndexer so that schedule and execute phases are clearly visible for indexing. The asynchronous
configurations can be used with Datasource and SQL based configs to create index as well.

First, we will generate a continuous workload. In the below example, we are going to start a [Hudi Streamer](hoodie_streaming_ingestion.md#hudi-streamer) which will continuously write data
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
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
--class org.apache.hudi.utilities.streamer.HoodieStreamer `ls /Users/home/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar` \
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
metadata table can be explored [here](indexes.md#multi-modal-indexing) along with [configs](metadata.md#metadata-tracking-on-writers)
to enable them. The full set of metadata configurations can be explored [here](configurations/#Metadata-Configs).

:::note
Enabling the metadata table and configuring a lock provider are the prerequisites for using async indexer. Checkout a sample
configuration below.
:::

```
# ensure that async indexing is enabled
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
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
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
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
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
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
--class org.apache.hudi.utilities.HoodieIndexer \
/Users/home/path/to/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
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

- Files index is created by default as long as the metadata table is enabled.
- Trigger indexing for one metadata partition (or index type) at a time.
- If an index is enabled via async indexing, then ensure that index is also enabled in configs corresponding to regular ingestion writers. Otherwise, metadata writer will
  think that particular index was disabled and cleanup the metadata partition.

Some of these limitations will be removed in the upcoming releases. Please
follow [this GitHub issue](https://github.com/apache/hudi/issues/14870) for developments on this feature.

## Related Resources
<h3>Videos</h3>

* [Advantages of Metadata Indexing and Asynchronous Indexing in Hudi Hands on Lab](https://www.youtube.com/watch?v=TSphQCsY4pY)
