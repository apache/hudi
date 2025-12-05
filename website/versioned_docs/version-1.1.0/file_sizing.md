---
title: "File Sizing"
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

Solving the [small file problem](https://hudi.apache.org/blog/2021/03/01/hudi-file-sizing/) is fundamental to ensuring 
great experience on the data lake. If you don’t size the files appropriately, you can slow down the queries and the pipelines. 
Some of the issues you may encounter with small files include the following:

- **Queries slow down**: You’ll have to scan through many small files to retrieve data for a query. It’s a very inefficient 
  way of accessing and utilizing the data. Also, cloud storage, like S3, enforces a rate-limit on how many requests can 
  be processed per second per prefix in a bucket. A higher number of files, i.e., at least one request per file regardless
  of the file size, increases the chance of encountering a rate-limit, as well as additional fixed costs for opening/closing 
  them. All of these causes the queries to slow down.

- **Pipelines slow down**: You can slow down your Spark, Flink or Hive jobs due to excessive scheduling overhead or memory 
  requirements; the more files you have, the more tasks you create.

- **Storage inefficiencies**: When working with many small files, you can be inefficient in using your storage. For example,
  many small files can yield a lower compression ratio, increasing storage costs. If you’re indexing the data, that also
  takes up more storage space to store additional metadata, such as column statistics. If you’re working with a smaller 
  amount of data, you might not see a significant impact with storage. However, when dealing with petabyte and exabyte 
  data, you’ll need to be efficient in managing storage resources.

A critical design decision in the Hudi architecture is to avoid small file creation. Hudi is uniquely designed to write 
appropriately sized files automatically. This page will show you how Apache Hudi overcomes the dreaded small files problem. 
There are two ways to manage small files in Hudi: 

- [Auto-size during writes](#auto-sizing-during-writes)
- [Clustering after writes](#auto-sizing-with-clustering)

Below, we will describe the advantages and trade-offs of each.

:::note
the bulk_insert write operation does not have auto-sizing capabilities during ingestion
:::

## Auto-sizing during writes

You can manage file sizes through Hudi’s auto-sizing capability during ingestion. The default targeted file size for 
Parquet base files is 120MB, which can be configured by `hoodie.parquet.max.file.size`. Auto-sizing may add some write 
latency, but it ensures that the queries are always efficient when a write transaction is committed. It’s important to 
note that if you don’t manage file sizing as you write and, instead, try to run clustering to fix your file sizing 
periodically, your queries might be slow until the point when the clustering finishes. This is only supported for 
**append** use cases only; **mutable** are not supported at the moment. Please refer to the 
[clustering documentation](https://hudi.apache.org/docs/clustering) for more details.



If you need to control the file sizing, i.e., increase the target file size or change how small files are identified, 
follow the instructions below for Copy-On-Write and Merge-On-Read tables.

### File sizing for Copy-On-Write (COW) and Merge-On-Read (MOR) tables
To tune the file sizing for both COW and MOR tables, you can set the small file limit and the maximum Parquet file size. 
Hudi will try to add enough records to a small file at write time to get it to the configured maximum limit.

 - For example, if the `hoodie.parquet.small.file.limit=104857600` (100MB) and `hoodie.parquet.max.file.size=125829120` (120MB), 
   Hudi will pick all files < 100MB and try to get them up to 120MB.

For creating a Hudi table initially, setting an accurate record size estimate is vital to ensure Hudi can adequately 
estimate how many records need to be bin-packed in a Parquet file for the first ingestion batch. Then, Hudi automatically 
uses the average record size for subsequent writes based on previous commits.


### More details about file sizing for Merge-On-Read(MOR) tables 
As a MOR table aims to reduce the write amplification, compared to a COW table, when writing to a MOR table, Hudi limits 
the number of Parquet base files to one for auto file sizing during insert and upsert operation. This limits the number 
of rewritten files. This can be configured through `hoodie.merge.small.file.group.candidates.limit`.

For storage systems that support append operation, in addition to file sizing Parquet base files for a MOR table, you 
can also tune the log files file-sizing with `hoodie.logfile.max.size`. 

MergeOnRead works differently for different INDEX choices so there are few more configs to set:

- Indexes with **canIndexLogFiles = true** : Inserts of new data go directly to log files. In this case, you can configure 
  the [maximum log size](https://hudi.apache.org/docs/configurations#hoodielogfilemaxsize) and a 
  [factor](https://hudi.apache.org/docs/configurations#hoodielogfiletoparquetcompressionratio) that denotes reduction 
  in size when data moves from avro to parquet files.
- Indexes with **canIndexLogFiles = false** : Inserts of new data go only to parquet files. In this case, the same configurations 
  as above for the COPY_ON_WRITE case applies.
**NOTE** : In either case, small files will be auto sized only if there is no PENDING compaction or associated log file 
  for that particular file slice. For example, for case 1: If you had a log file and a compaction C1 was scheduled to 
  convert that log file to parquet, no more inserts can go into that log file. For case 2: If you had a parquet file and 
  an update ended up creating an associated delta log file, no more inserts can go into that parquet file. Only after the 
  compaction has been performed and there are NO log files associated with the base parquet file, can new inserts be sent 
  to auto size that parquet file.

### Configs
Here are the essential configurations for **COW tables**.

**Spark based configs:**

| Config Name                             | Default              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-----------------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.parquet.small.file.limit         | 104857600 (Optional) | During an insert and upsert operation, we opportunistically expand existing small files on storage, instead of writing new files, to keep number of files to an optimum. This config sets the file size limit below which a file on storage  becomes a candidate to be selected as such a `small file`. By default, treat any file &lt;= 100MB as a small file. Also note that if this set &lt;= 0, will not try to get small files and directly write new files<br /><br />`Config Param: PARQUET_SMALL_FILE_LIMIT` |
| hoodie.parquet.max.file.size            | 125829120 (Optional) | Target size in bytes for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br />`Config Param: PARQUET_MAX_FILE_SIZE`                                                                                                                                                                                                                                                                                          |
| hoodie.copyonwrite.record.size.estimate | 1024 (Optional)                                                                             | The average record size. If not explicitly specified, hudi will compute the record size estimate compute dynamically based on commit metadata.  This is critical in computing the insert parallelism and bin-packing inserts into small files.<br /><br />`Config Param: COPY_ON_WRITE_RECORD_SIZE_ESTIMATE`                                                                                                                                                                                                         |

**Flink based configs:**

| Config Name                             | Default              | Description                                                                                                                                                                                                               |
|-----------------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| write.parquet.max.file.size             | 120 (Optional)       | Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br /> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE` |

Here are the essential configurations for **MOR tables**:

**Spark based configs:**

| Config Name                                    | Default               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.parquet.small.file.limit                | 104857600 (Optional)  | During an insert and upsert operation, we opportunistically expand existing small files on storage, instead of writing new files, to keep number of files to an optimum. This config sets the file size limit below which a file on storage  becomes a candidate to be selected as such a `small file`. By default, treat any file &lt;= 100MB as a small file. Also note that if this set &lt;= 0, will not try to get small files and directly write new files<br /><br />`Config Param: PARQUET_SMALL_FILE_LIMIT` |
| hoodie.parquet.max.file.size                   | 125829120 (Optional)  | Target size in bytes for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br />`Config Param: PARQUET_MAX_FILE_SIZE`                                                                                                                                                                                                                                                                                          |
| hoodie.merge.small.file.group.candidates.limit | 1 (Optional)          | Limits number of file groups, whose base file satisfies small-file limit, to consider for appending records during upsert operation. Only applicable to MOR tables<br /><br />`Config Param: MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT`                                                                                                                                                                                                                                                                                |
| hoodie.logfile.max.size                        | 1073741824 (Optional) | LogFile max size in bytes. This is the maximum size allowed for a log file before it is rolled over to the next version. This log rollover limit only works on storage systems that support append operation. Please note that on cloud storage like S3/GCS, this may not be respected<br /><br />`Config Param: LOGFILE_MAX_SIZE`                                                                                                                                                                                   |
| hoodie.logfile.to.parquet.compression.ratio    | 0.35 (Optional)       | Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files &amp; control the size of compacted parquet file.<br /><br />`Config Param: LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION`                                                                                                                                                                               |


**Flink based configs:**

| Config Name                             | Default              | Description                                                                                                                                                                                                               |
|-----------------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| write.parquet.max.file.size             | 120 (Optional)       | Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.<br /><br /> `Config Param: WRITE_PARQUET_MAX_FILE_SIZE` |
| write.log.max.size                      | 1024 (Optional)      | Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB<br /><br /> `Config Param: WRITE_LOG_MAX_SIZE`                                                                        |


## Auto-Sizing With Clustering
Clustering is a service that allows you to combine small files into larger ones while at the same time (optionally) changing 
the data layout by sorting or applying [space-filling curves](https://hudi.apache.org/blog/2021/12/29/hudi-zorder-and-hilbert-space-filling-curves/) 
like Z-order or Hilbert curve. We won’t go into all the details about clustering here, but please refer to the 
[clustering section](https://hudi.apache.org/docs/clustering) for more details. 

Clustering is one way to achieve file sizing, so you can have faster queries. When you ingest data, you may still have a 
lot of small files (depending on your configurations and the data size from ingestion i.e., input batch). In this case, 
you will want to cluster all the small files to larger files to improve query performance. Clustering can be performed 
in different ways. Please check out the [clustering documentation](https://hudi.apache.org/docs/clustering) for more details. 

An example where clustering might be very useful is when a user has a Hudi table with many small files. For example, if 
you're using BULK_INSERT without any sort modes, or you want a different file layout, you can use the clustering service 
to fix all the file sizes without ingesting any new data.

:::note
Clustering in Hudi is not a blocking operation, and writes can continue concurrently as long as no files need to be 
updated while the clustering service is running. The writes will fail if there are updates to the data being clustered 
while the clustering service runs.
:::

:::note
Hudi always creates immutable files on storage. To be able to do auto-sizing or clustering, Hudi will always create a
newer version of the smaller file, resulting in 2 versions of the same file. The [cleaner service](cleaning)
will later kick in and delete the older version small file and keep the latest one.
:::

Here are the critical file sizing configurations:

### Configs

**Spark based configs:**

| Config Name                                           | Default              | Description                                                                                                                                                                                       |
|-------------------------------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.clustering.plan.strategy.small.file.limit      | 314572800 (Optional)  | Files smaller than the size in bytes specified here are candidates for clustering<br /><br />`Config Param: PLAN_STRATEGY_SMALL_FILE_LIMIT`<br />`Since Version: 0.7.0`                          |
| hoodie.clustering.plan.strategy.target.file.max.bytes | 1073741824 (Optional) | Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups<br /><br />`Config Param: PLAN_STRATEGY_TARGET_FILE_MAX_BYTES`<br />`Since Version: 0.7.0` |

**Flink based configs:**

| Config Name                                     | Default              | Description                                                                                                                                                                                    |
|-------------------------------------------------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| clustering.plan.strategy.small.file.limit       | 600 (Optional)       | Files smaller than the size specified here are candidates for clustering, default 600 MB<br /><br /> `Config Param: CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT`                                 |
| clustering.plan.strategy.target.file.max.bytes  | 1073741824 (Optional)| Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups, default 1 GB<br /><br /> `Config Param: CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES` |

## Related Resources
<h3>Videos</h3>

* [Mastering File Sizing in Hudi: Boosting Performance and Efficiency](https://www.youtube.com/watch?v=qg-2aYyvfts)
* ["How do I Ingest Extremely Small Files into Hudi Data lake with Glue Incremental data processing](https://www.youtube.com/watch?v=BvoLVeidd-0)