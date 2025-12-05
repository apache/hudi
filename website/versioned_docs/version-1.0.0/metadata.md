---
title: Table Metadata
keywords: [ hudi, metadata, S3, GCS, file listings, statistics]
---

Hudi tracks metadata about a table to remove bottlenecks in achieving great read/write performance, specifically on cloud storage.

- **Avoid list operations to obtain set of files in a table**: A fundamental need for any engine that wants to read or write Hudi tables is
  to know all the files/objects that are part of the table, by performing listing of table partitions/folders. Unlike many distributed file systems,
  such operation scales poorly on cloud storage taking few seconds or even many minutes on large tables. This is particularly amplified when tables 
  are large and partitioned multiple levels deep. Hudi tracks the file listings so they are readily available for readers/writers without listing the folders 
  containing the data files.

- **Expose columns statistics for better query planning and faster queries**:  Query engines rely on techniques such as partitioning and data skipping
  to cut down on the amount of irrelevant data scanned for query planning and execution. During query planning phase, file footer statistics like column value ranges, 
  null counts are read from all data files to  determine if a particular file needs to be read to satisfy the query. This approach is expensive since reading 
  footers from all files can increase cloud storage API costs and even be subject to throttling issues for larger tables. Hudi enables relevant query predicates to 
  be efficiently evaluated on operate on column statistics without incurring these costs. 
  
## Metadata Table

Hudi employs a special **_metadata table_**, within each table to provide these capabilities. The metadata table implemented as a single 
internal Hudi Merge-On-Read table that hosts different types of table metadata in each partition. This is similar to common practices in databases where metadata
is tracked using internal tables. This approach provides the following advantages. 

- **Scalable**: The table metadata must scale to large sizes as well (see [Big Metadata paper](https://vldb.org/pvldb/vol14/p3083-edara.pdf) from Google). 
  Different types of indexes should be easily integrated to support various use cases with consistent management of metadata. By implementing metadata using the 
  same storage format and engine used for data, Hudi is able to scale to even TBs of metadata with built-in table services for managing metadata. 
 
- **Flexible**: The foundational framework for multi-modal indexing is built to enable and disable new indexes as needed. The
  [async indexing](https://www.onehouse.ai/blog/asynchronous-indexing-using-hudi) protocol index building alongside regular writers without impacting the write latency.

- **transactional updates**: Tables data, metadata and indexes must be upto-date and consistent with each other as writes happen or table services are performed. and table metadata must be always up-to-date and in sync with the data table.
  The data and metadata table's timelines share a parent-child relationship, to ensure they are always in sync with each other. Furthermore, the MoR table storage helps absorb fast changes to metadata from streaming writes without requiring 
  rewriting of all table metadata on each write.

- **Fast lookups**: By employing a SSTable like base file format (HFile) in the metadata table, query engines are able to efficiently perform lookup scans for only specific parts of 
  metadata needed. For e.g. query accessing only 10 out of 100 columns in a table can read stats about only the 10 columns it's interested in, during down planning time and costs.
  Further, these metadata can also be served via a centralized/embedded timeline server which caches the metadata, further reducing the latency of the lookup from executors.


![Metadata Table Mechanics](/assets/images/metadata_table_anim.gif)
<p align = "center">Figure: Mechanics for Metadata Table in Hudi</p>

## Types of table metadata

Following are the different types of metadata currently supported.

- ***[files listings](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=147427331)***: 
  Stored as *files* partition in the metadata table. Contains file information such as file name, size, and active state
  for each partition in the data table, along with list of all partitions in the table. Improves the files listing performance 
  by avoiding direct storage calls such as *exists, listStatus* and *listFiles* on the data table.

- ***[column statistics](https://github.com/apache/hudi/blob/master/rfc/rfc-27/rfc-27.md)***: Stored as *column_stats* 
  partition in the metadata table. Contains the statistics for a set of tracked columns, such as min and max values, total values, 
  null counts, size, etc., for all data files and are used while serving queries with predicates matching interested 
  columns. This is heavily used by techniques like [data skipping](https://www.onehouse.ai/blog/hudis-column-stats-index-and-data-skipping-feature-help-speed-up-queries-by-an-orders-of-magnitude) to speed up queries by orders of magnitude, by skipping 
  irrelevant files.

- ***Partition Statistics***: Partition stats index aggregates statistics at the partition level for the columns tracked by 
  the column statistics for which it is enabled. This helps in efficient partition pruning by skipping entire folders very quickly,
  even without examining column statistics at the file level. The partition stats index is stored in *partition_stats* partition in the metadata table.
  Partition stats index can be enabled using the following configs (note it is required to specify the columns for which stats should be aggregated).

To try out these features, refer to the [SQL guide](sql_ddl#create-partition-stats-index).

## Metadata Tracking on Writers

Following are based basic configs that are needed to enable metadata tracking. For advanced configs please refer 
[here](configurations#Metadata-Configs).

| Config Name                                  | Default                                   | Description                                                                                                                                                                                                                                                                                       |
|----------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.enable                       | true (Optional) Enabled on the write side | Enable the internal metadata table serving file listings. For 0.10.1 and prior releases, metadata table is disabled by default and needs to be explicitly enabled.<br /><br />`Config Param: ENABLE`<br />`Since Version: 0.7.0`                                                                  |
| hoodie.metadata.index.column.stats.enable    | false (Optional)                          | Enable column statistics tracking of files under metadata table. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during data skipping.<br /><br />`Config Param: ENABLE_METADATA_INDEX_COLUMN_STATS`<br />`Since Version: 0.11.0` |
| hoodie.metadata.index.column.stats.columns   | all columns in the table                  | Comma separated list of columns to track column statistics on.                                                                                                                                                                                                                                    |
| hoodie.metadata.index.partition.stats.enable | false (Optional)                          | Enable the partition stats tracking, on the same columns tracked by column stats metadata.                                                                                                                                                                                                        |

For Flink, following are the basic configs of interest to enable metadata tracking. Please refer 
[here](https://hudi.apache.org/docs/next/configurations#Flink-Options) for advanced configs

| Config Name                               | Default          | Description                                                                                                                                                                                                                         |
|-------------------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metadata.enabled                          | true (Optional)  | Enable the internal metadata table which serves table metadata like level file listings, default enabled<br /><br /> `Config Param: METADATA_ENABLED`                                                                               |


:::note
If you turn off the metadata table after enabling, be sure to wait for a few commits so that the metadata table is fully
cleaned up, before re-enabling the metadata table again.
:::

## Leveraging metadata during queries

### files index
Metadata based listing using *files_index* can be leveraged on the read side by setting appropriate configs/session properties
from different engines as shown below:

| Readers                                          | Config                 | Description                                                                                                                   |
|--------------------------------------------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Spark DataSource, Spark SQL, Strucured Streaming | hoodie.metadata.enable | When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br /> |
| Flink DataStream, Flink SQL            | metadata.enabled | When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings                |
| Presto                                           | [hudi.metadata-table-enabled](https://prestodb.io/docs/current/connector/hudi.html)             | When set to `true` fetches the list of file names and sizes from Hudi’s metadata table rather than storage.                   |
| Trino                                            | N/A | Support for reading from the metadata table [has been dropped in Trino 419](https://github.com/apache/hudi/issues/16286). |
| Athena                                           | [hudi.metadata-listing-enabled](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html) | When this table property is set to `TRUE` enables the Hudi metadata table and the related file listing functionality          |

### column_stats index and data skipping
Enabling metadata table and column stats index is a prerequisite to enabling data skipping capabilities. Following are the 
corresponding configs across Spark and Flink readers.

| Readers                                                    | Config                                                                           | Description                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------------------------------------|----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Spark DataSource, Spark SQL, Strucured Streaming | <ul><li>`hoodie.metadata.enable`</li><li>`hoodie.enable.data.skipping`</li></ul> | <ul><li>When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.</li><li>When set to `true` enables data-skipping allowing queries to leverage indexes to reduce the search space by skipping over files <br />`Config Param: ENABLE_DATA_SKIPPING`<br />`Since Version: 0.10.0` <br /></li></ul> |
| Flink DataStream, Flink SQL                      |<ul><li>`metadata.enabled`</li><li>`read.data.skipping.enabled`</li></ul> | <ul><li> When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings</li><li>When set to `true` enables data-skipping allowing queries to leverage indexes to reduce the search space byskipping over files</li></ul>                                                                                         |


## Concurrency Control for Metadata Table

To ensure that metadata table stays up to date and table metadata is tracked safely across concurrent write and 
table operations, there are some additional considerations. If async table services are enabled for the table (i.e. running a separate compaction (`HoodieCompactor`) or 
clustering (`HoodieClusteringJob`) job), even with just a single writer, lock providers 
must be configured. Please refer to [concurrency control](concurrency_control) for more details. 

Before enabling metadata table for the first time, all writers on the same table must and table services must be stopped.
If your current deployment model is [multi-writer](concurrency_control#full-on-multi-writer--async-table-services) along with a lock 
provider and other required configs set for every writer as follows, there is no additional configuration required. You 
can bring up the writers sequentially after stopping the writers for enabling metadata table. Applying the proper 
configurations to only a subset of writers or table services is unsafe and can lead to loss of data. So, please ensure you enable 
metadata table across all writers.

## Related Resources
<h3>Blogs</h3>

* [Table service deployment models in Apache Hudi](https://medium.com/@simpsons/table-service-deployment-models-in-apache-hudi-9cfa5a44addf)
* [Multi Modal Indexing for the Data Lakehouse](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
