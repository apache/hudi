---
title: Metadata Table
keywords: [ hudi, metadata, S3 file listings]
---

## Metadata Table

Database indices contain auxiliary data structures to quickly locate records needed, without reading unnecessary data 
from storage. Given that Hudi’s design has been heavily optimized for handling mutable change streams, with different 
write patterns, Hudi considers [indexing](#indexing) as an integral part of its design and has uniquely supported 
[indexing capabilities](https://hudi.apache.org/blog/2020/11/11/hudi-indexing-mechanisms/) from its inception, to speed 
up upserts on the [Data Lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/). While Hudi's indices has benefited writers for fast upserts and deletes, Hudi's metadata table 
aims to tap these benefits more generally for both the readers and writers. The metadata table implemented as a single 
internal Hudi Merge-On-Read table hosts different types of indices containing table metadata and is designed to be
serverless and independent of compute and query engines. This is similar to common practices in databases where metadata
is stored as internal views.

The metadata table aims to significantly improve read/write performance of the queries by addressing the following key challenges:
- **Eliminate the requirement of `list files` operation**:<br />
  When reading and writing data, file listing operations are performed to get the current view of the file system.
  When data sets are large, listing all the files may be a performance bottleneck, but more importantly in the case of cloud storage systems
  like AWS S3, the large number of file listing requests sometimes causes throttling due to certain request limits.
  The metadata table will instead proactively maintain the list of files and remove the need for recursive file listing operations
- **Expose columns stats through indices for better query planning and faster lookups by readers**:<br />
  Query engines rely on techniques such as partitioning and file pruning to cut down on the amount of irrelevant data 
  scanned for query planning and execution. During query planning phase all data files are read for metadata on range 
  information of columns for further pruning data files based on query predicates and available range information. This
  approach is expensive and does not scale if there are large number of partitions and data files to be scanned. In
  addition to storage optimizations such as automatic file sizing, clustering, etc that helps data organization in a query
  optimized way, Hudi's metadata table improves query planning further by supporting multiple types of indices that aid 
  in efficiently looking up data files based on relevant query predicates instead of reading the column stats from every 
  individual data file and then pruning. 
   
## Supporting Multi-Modal Index in Hudi

[Multi-modal indexing](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi), 
introduced in [0.11.0 Hudi release](https://hudi.apache.org/releases/release-0.11.0/#multi-modal-index), 
is a re-imagination of what a general purpose indexing subsystem should look like for the lake. Multi-modal indexing is 
implemented by enhancing Hudi's metadata table with the flexibility to extend to new index types as new partitions,
along with an [asynchronous index](https://hudi.apache.org/docs/metadata_indexing/#setup-async-indexing) building 
mechanism and is built on the following core principles:
- **Scalable metadata**: The table metadata, i.e., the auxiliary data about the table, must be scalable to extremely 
  large size, e.g., Terabytes (TB).  Different types of indices should be easily integrated to support various use cases 
  without having to worry about managing the same. To realize this, all indices in Hudi's metadata table are stored as 
  partitions in a single internal MOR table. The MOR table layout enables lightning-fast writes by avoiding synchronous 
  merge of data with reduced write amplification. This is extremely important for large datasets as the size of updates to the 
  metadata table can grow to be unmanageable otherwise. This helps Hudi to scale metadata to TBs of sizes. The 
  foundational framework for multi-modal indexing is built to enable and disable new indices as needed. The 
  [async indexing](https://www.onehouse.ai/blog/asynchronous-indexing-using-hudi) supports index building alongside 
  regular writers without impacting the write latency.
- **ACID transactional updates**: The index and table metadata must be always up-to-date and in sync with the data table. 
  This is designed via multi-table transaction within Hudi and ensures atomicity of writes and resiliency to failures so that 
  partial writes to either the data or metadata table are never exposed to other read or write transactions. The metadata 
  table is built to be self-managed so users don’t need to spend operational cycles on any table services including 
  compaction and cleaning    
- **Fast lookup**: The needle-in-a-haystack type of lookups must be fast and efficient without having to scan the entire 
  index, as index size can be TBs for large datasets. Since most access to the metadata table are point and range lookups,
  the HFile format is chosen as the base file format for the internal metadata table. Since the metadata table stores 
  the auxiliary data at the partition level (files index) or the file level (column_stats index), the lookup based on a 
  single partition path and a file group is going to be very efficient with the HFile format. Both the base and log files 
  in Hudi’s metadata table uses the HFile format and are meticulously designed to reduce remote GET calls on cloud storages.
  Further, these metadata table indices are served via a centralized timeline server which caches the metadata, further 
  reducing the latency of the lookup from executors.

### Metadata table indices

Following are the different indices currently available under the metadata table.

- ***[files index](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=147427331)***: 
  Stored as *files* partition in the metadata table. Contains file information such as file name, size, and active state
  for each partition in the data table. Improves the files listing performance by avoiding direct file system calls such
  as *exists, listStatus* and *listFiles* on the data table.

- ***[column_stats index](https://github.com/apache/hudi/blob/master/rfc/rfc-27/rfc-27.md)***: Stored as *column_stats* 
  partition in the metadata table. Contains the statistics of interested columns, such as min and max values, total values, 
  null counts, size, etc., for all data files and are used while serving queries with predicates matching interested 
  columns. This index is used along with the [data skipping](https://www.onehouse.ai/blog/hudis-column-stats-index-and-data-skipping-feature-help-speed-up-queries-by-an-orders-of-magnitude) 
  to speed up queries by orders of magnitude. 

- ***[bloom_filter index](https://github.com/apache/hudi/blob/46f41d186c6c84a6af2c54a907ff2736b6013e15/rfc/rfc-37/rfc-37.md)***: 
  Stored as *bloom_filter* partition in the metadata table. This index employs range-based pruning on the minimum and 
  maximum values of the record keys and bloom-filter-based lookups to tag incoming records. For large tables, this 
  involves reading the footers of all matching data files for bloom filters, which can be expensive in the case of random 
  updates across the entire dataset. This index stores bloom filters of all data files centrally to avoid scanning the 
  footers directly from all data files.

- ***[record_index](https://cwiki.apache.org/confluence/display/HUDI/RFC-08++Record+level+indexing+mechanisms+for+Hudi+datasets)***: 
  Stored as *record_index* partition in the metadata table. Contains the mapping of the record key to location. Record 
  index is a global index, enforcing key uniqueness across all partitions in the table. Most recently added in 0.14.0 
  Hudi release, this index aids in locating records faster than other existing indices and can provide a speedup orders of magnitude 
  faster in large deployments where index lookup dominates write latencies.

## Enable Hudi Metadata Table and Multi-Modal Index in write side

Following are the Spark based basic configs that are needed to enable metadata and multi-modal indices. For advanced configs please refer 
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
[`hoodie.metadata.enable`](configurations.md#hoodiemetadataenable) to `false`. The 
[multi-modal index](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi) are 
disabled by default and can be enabled in write side explicitly using the above configs.

For flink, following are the basic configs of interest to enable metadata based indices. Please refer 
[here](https://hudi.apache.org/docs/next/configurations#Flink-Options) for advanced configs

| Config Name                               | Default                                   | Description                                                                                                                                                                                                                                                                                                        |
|-------------------------------------------|-------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metadata.enabled                          | false (Optional)                          | Enable the internal metadata table which serves table metadata like level file listings, default disabled<br /><br /> `Config Param: METADATA_ENABLED`                                                                                                                                                             |
| hoodie.metadata.index.column.stats.enable | false (Optional)                          | Enable indexing column ranges of user data files under metadata table key lookups. When enabled, metadata table will have a partition to store the column ranges and will be used for pruning files during the index lookups.<br /> |


:::note
If you turn off the metadata table after enabling, be sure to wait for a few commits so that the metadata table is fully
cleaned up, before re-enabling the metadata table again.
:::

## Use metadata indices for query side improvements

### files index
Metadata based listing using *files_index* can be leveraged on the read side by setting appropriate configs/session properties
from different engines as shown below:

| Readers                                                                          | Config                 | Description                                                                                                                   |
|----------------------------------------------------------------------------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| <ul><li>Spark DataSource</li><li>Spark SQL</li><li>Strucured Streaming</li></ul> | hoodie.metadata.enable | When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br /> |
|Presto| [hudi.metadata-table-enabled](https://prestodb.io/docs/current/connector/hudi.html)             | When set to `true` fetches the list of file names and sizes from Hudi’s metadata table rather than storage.                   |
|Trino| [hudi.metadata-enabled](https://trino.io/docs/current/connector/hudi.html#general-configuration) | When set to `true` fetches the list of file names and sizes from metadata rather than storage.                                |
|Athena| [hudi.metadata-listing-enabled](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html) | When this table property is set to `TRUE` enables the Hudi metadata table and the related file listing functionality          |
|<ul><li>Flink DataStream</li><li>Flink SQL</li></ul> | metadata.enabled | When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings                |

### column_stats index and data skipping
Enabling metadata table and column stats index is a prerequisite to enabling data skipping capabilities. Following are the 
corresponding configs across Spark and Flink readers.

| Readers                                                                                            | Config                                                                           | Description                                                                                                                                                                                                                                                                                                                                                      |
|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <ul><li>Spark DataSource</li><li>Spark SQL</li><li>Strucured Streaming</li></ul>                   | <ul><li>`hoodie.metadata.enable`</li><li>`hoodie.enable.data.skipping`</li></ul> | <ul><li>When set to `true` enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.</li><li>When set to `true` enables data-skipping allowing queries to leverage indices to reduce the search space by skipping over files <br />`Config Param: ENABLE_DATA_SKIPPING`<br />`Since Version: 0.10.0` <br /></li></ul> |
|<ul><li>Flink DataStream</li><li>Flink SQL</li></ul> |<ul><li>`metadata.enabled`</li><li>`read.data.skipping.enabled`</li></ul> | <ul><li> When set to `true` from DDL uses the internal metadata table to serves table metadata like level file listings</li><li>When set to `true` enables data-skipping allowing queries to leverage indices to reduce the search space byskipping over files</li></ul>                                                                                         |


## Deployment considerations for metadata Table
To ensure that metadata table stays up to date, all write operations on the same Hudi table need additional configurations
besides the above in different deployment models.  Before enabling metadata table, all writers on the same table must
be stopped. Please refer to the different [deployment models](concurrency_control.md#deployment-models-with-supported-concurrency-controls) 
for more details on each model. This section only highlights how to safely enable metadata table in different deployment models. 

### Deployment Model A: Single writer with inline table services

In [Model A](concurrency_control.md#model-a-single-writer-with-inline-table-services), after setting [`hoodie.metadata.enable`](configurations.md#hoodiemetadataenable) to `true`, restarting
the single writer is sufficient to safely enable metadata table.

### Deployment Model B: Single writer with async table services

If your current deployment model is [Model B](concurrency_control.md#model-b-single-writer-with-async-table-services), enabling metadata
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

If your current deployment model is [multi-writer](concurrency_control.md#model-c-multi-writer) along with a lock 
provider and other required configs set for every writer as follows, there is no additional configuration required. You 
can bring up the writers sequentially after stopping the writers for enabling metadata table. Applying the proper 
configurations to only partial writers leads to loss of data from the inconsistent writer. So, ensure you enable 
metadata table across all writers.

```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=<distributed-lock-provider-classname>
```

Note that there are different external [lock providers available](concurrency_control.md#external-locking-and-lock-providers)
to choose from.

## Related Resources
<h3>Blogs</h3>

* [Multi-Modal Index for the Lakehouse in Apache Hudi](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
* [Table service deployment models in Apache Hudi](https://medium.com/@simpsons/table-service-deployment-models-in-apache-hudi-9cfa5a44addf)
