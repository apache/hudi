---
title: Table Metadata
keywords: [ hudi, metadata, S3, GCS, file listings, statistics]
---

Hudi tracks metadata about a table to significantly improve read/write performance of the queries by addressing the following key challenges:

- **Avoid list operations to obtain set of files in a table**: When reading and writing data, file listing operations are performed 
  to get the current view of the file system. When tables are large and partitioned multiple levels deep, listing all the files may be a performance bottleneck,
  dut to inefficiencies in cloud storage. More importantly large number of API calls to cloud storage systems like AWS S3 sometimes causes throttling based on pre-defined request limits.

- **Expose columns statistics for better query planning and faster queries**:  Query engines rely on techniques such as partitioning and data skipping
  to cut down on the amount of irrelevant data scanned for query planning and execution. During query planning phase, file footer statistics like column value ranges, 
  null counts are read from all data files to  determine if a particular file needs to be read to satisfy the query. This approach is expensive since it may  
  read footers from all files and even be subject to similar throttling issues for larger tables. Hudi enables relevant query predicates to 
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

## Types of table metadata

Following are the different types of metadata currently supported.

- ***[files index](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+Improvements)***: 
  Stored as *files* partition in the metadata table. Contains file information such as file name, size, and active state
  for each partition in the data table. Improves the files listing performance by avoiding direct file system calls such
  as *exists, listStatus* and *listFiles* on the data table.

- ***[column_stats index](https://github.com/apache/hudi/blob/master/rfc/rfc-27/rfc-27.md)***: Stored as *column_stats* 
  partition in the metadata table. Contains the statistics of interested columns, such as min and max values, total values, 
  null counts, size, etc., for all data files and are used while serving queries with predicates matching interested 
  columns. This index is used along with the [data skipping](https://www.onehouse.ai/blog/hudis-column-stats-index-and-data-skipping-feature-help-speed-up-queries-by-an-orders-of-magnitude) 
  to speed up queries by orders of magnitude.

- ***Partition Stats Index***
  Partition stats index aggregates statistics at the partition level for the columns for which it is enabled. This helps
  in efficient partition pruning even for non-partition fields. The partition stats index is stored in *partition_stats*
  partition under metadata table. Partition stats index can be enabled using the following configs (note it is required
  to specify the columns for which stats should be aggregated):
  ```properties
    hoodie.metadata.index.partition.stats.enable=true
    hoodie.metadata.index.column.stats.columns=<comma-separated-column-names>
  ```


To try out these features, refer to the [SQL guide](sql_ddl#create-partition-stats-index).

## Metadata Tracking on Writers

Following are the Spark based basic configs that are needed to enable metadata tracking. For advanced configs please refer 
[here](https://hudi.apache.org/docs/next/configurations#Metadata-Configs-advanced-configs).

| Config Name                               | Default                                   | Description                                                                                                                                                                                                                                                                                                              |
|-------------------------------------------|-------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.metadata.enable                    | true (Optional) Enabled on the write side | Enable the internal metadata table which serves table metadata like level file listings. For 0.10.1 and prior releases, metadata table is disabled by default and needs to be explicitly enabled.<br /><br />`Config Param: ENABLE`<br />`Since Version: 0.7.0`                                                          |
| hoodie.metadata.index.bloom.filter.enable | false (Optional)                          | Enable indexing bloom filters of user data files under metadata table. When enabled, metadata table will have a partition to store the bloom filter index and will be used during the index lookups.<br /><br />`Config Param: ENABLE_METADATA_INDEX_BLOOM_FILTER`<br />`Since Version: 0.11.0`                          |
| hoodie.metadata.index.column.stats.enable | false (Optional)                          | Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br /><br />`Config Param: ENABLE_METADATA_INDEX_COLUMN_STATS`<br />`Since Version: 0.11.0` |
| hoodie.metadata.record.index.enable       | false (Optional)                          | Create the HUDI Record Index within the Metadata Table<br /><br />`Config Param: RECORD_INDEX_ENABLE_PROP`<br />`Since Version: 0.14.0`                                                                                                                                                                                  |


The metadata table with synchronous updates and metadata-table-based file listing are enabled by default.
There are prerequisite configurations and steps in [Deployment considerations](#deployment-considerations-for-metadata-table) to
safely use this feature.  The metadata table and related file listing functionality can still be turned off by setting
[`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable) to `false`. The 
[multi-modal index](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi) are 
disabled by default and can be enabled in write side explicitly using the above configs.

For flink, following are the basic configs of interest to enable metadata based indexes. Please refer 
[here](https://hudi.apache.org/docs/next/configurations#Flink-Options) for advanced configs

| Config Name                               | Default          | Description                                                                                                                                                                                                                         |
|-------------------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metadata.enabled                          | true (Optional)  | Enable the internal metadata table which serves table metadata like level file listings, default enabled<br /><br /> `Config Param: METADATA_ENABLED`                                                                               |
| hoodie.metadata.index.column.stats.enable | false (Optional) | Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br /> |


:::note
If you turn off the metadata table after enabling, be sure to wait for a few commits so that the metadata table is fully
cleaned up, before re-enabling the metadata table again.
:::

## Leveraging metadata during queries

### files index
Metadata based listing using *files_index* can be leveraged on the read side by setting appropriate configs/session properties
from different engines as shown below:

| Readers                                                                          | Config                 | Description                                                                                                                   |
|----------------------------------------------------------------------------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| <ul><li>Spark DataSource</li><li>Spark SQL</li><li>Strucured Streaming</li></ul> | hoodie.metadata.enable | When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br /> |
|Presto| [hudi.metadata-table-enabled](https://prestodb.io/docs/current/connector/hudi.html)             | When set to `true` fetches the list of file names and sizes from Hudi’s metadata table rather than storage.                   |
|Trino| N/A | Support for reading from the metadata table [has been dropped in Trino 419](https://issues.apache.org/jira/browse/HUDI-7020). |
|Athena| [hudi.metadata-listing-enabled](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html) | When this table property is set to `TRUE` enables the Hudi metadata table and the related file listing functionality          |
|<ul><li>Flink DataStream</li><li>Flink SQL</li></ul> | metadata.enabled | When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings                |

### column_stats index and data skipping
Enabling metadata table and column stats index is a prerequisite to enabling data skipping capabilities. Following are the 
corresponding configs across Spark and Flink readers.

| Readers                                                                                            | Config                                                                           | Description                                                                                                                                                                                                                                                                                                                                                      |
|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <ul><li>Spark DataSource</li><li>Spark SQL</li><li>Strucured Streaming</li></ul>                   | <ul><li>`hoodie.metadata.enable`</li><li>`hoodie.enable.data.skipping`</li></ul> | <ul><li>When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.</li><li>When set to `true` enables data-skipping allowing queries to leverage indexes to reduce the search space by skipping over files <br />`Config Param: ENABLE_DATA_SKIPPING`<br />`Since Version: 0.10.0` <br /></li></ul> |
|<ul><li>Flink DataStream</li><li>Flink SQL</li></ul> |<ul><li>`metadata.enabled`</li><li>`read.data.skipping.enabled`</li></ul> | <ul><li> When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings</li><li>When set to `true` enables data-skipping allowing queries to leverage indexes to reduce the search space byskipping over files</li></ul>                                                                                         |


## Deployment considerations for metadata Table
To ensure that metadata table stays up to date, all write operations on the same Hudi table need additional configurations
besides the above in different deployment models.  Before enabling metadata table, all writers on the same table must
be stopped. Please refer to the different [deployment models](/docs/concurrency_control#deployment-models-with-supported-concurrency-controls) 
for more details on each model. This section only highlights how to safely enable metadata table in different deployment models. 

### Deployment Model A: Single writer with inline table services

In [Model A](/docs/concurrency_control#model-a-single-writer-with-inline-table-services), after setting [`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable) to `true`, restarting
the single writer is sufficient to safely enable metadata table.

### Deployment Model B: Single writer with async table services

If your current deployment model is [Model B](/docs/concurrency_control#model-b-single-writer-with-async-table-services), enabling metadata
table requires adding optimistic concurrency control along with suggested lock provider like below.
```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider
```
:::note
These configurations are required only if metadata table is enabled in this deployment model.
:::

If multiple writers in different processes are present, including one writer with async table services, please refer to
[Deployment Model C: Multi-writer](#deployment-model-c-multi-writer) for configs, with the difference of using a
distributed lock provider.  Note that running a separate compaction (`HoodieCompactor`) or clustering (`HoodieClusteringJob`)
job apart from the ingestion writer is considered as multi-writer deployment, as they are not running in the same
process which cannot rely on the in-process lock provider.

### Deployment Model C: Multi-writer

If your current deployment model is [multi-writer](concurrency_control#full-on-multi-writer--async-table-services) along with a lock 
provider and other required configs set for every writer as follows, there is no additional configuration required. You 
can bring up the writers sequentially after stopping the writers for enabling metadata table. Applying the proper 
configurations to only partial writers leads to loss of data from the inconsistent writer. So, ensure you enable 
metadata table across all writers.

```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=<distributed-lock-provider-classname>
```

Note that there are different external [lock providers available](/docs/concurrency_control#external-locking-and-lock-providers)
to choose from.

## Related Resources
<h3>Blogs</h3>

* [Table service deployment models in Apache Hudi](https://medium.com/@simpsons/table-service-deployment-models-in-apache-hudi-9cfa5a44addf)
