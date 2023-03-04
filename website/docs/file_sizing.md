---
title: "File Sizing"
toc: true
---
One of the fundamental problems in data lakes during writing is having a lot of small files. This is also known as a small file problem. If you don’t size the files appropriately, you can slow down the query performance and work with stale analytics. Some of the issues you may encounter with small files include the following:

- **Reads slow down**: You’ll have to scan through many small files to retrieve data for a query. It’s a very inefficient way of accessing and utilizing the data. Also, cloud storage, like S3, enforces rate-limiting on how many requests can be processed per second per prefix in a bucket. For a higher number of files, i.e., at least one request per file regardless of the file size, increases the chance of encountering the rate-limiting. This causes the reader to slow down.

- **Processes slow down**: You can slow down your i.e., Spark or Hive jobs; the more files you have, the more tasks you create.

- **Storage use inefficiencies**: When working with a lot of data, you can be inefficient in using your storage. For example, many small files can have a lower compression ratio, leading to more data on storage. If you’re indexing the data, that also takes up more storage space inside the Parquet files. If you’re working with a small amount of data, you might not see a significant impact with storage. However, when dealing with petabyte and exabyte data, you’ll need to be efficient in managing storage resources.

All these challenges inevitably lead to stale analytics and scalability challenges:
- Query performance slows down.
- Jobs could run slower.
- You utilize more resources. 

A critical design decision in the Hudi architecture is to avoid small file creation. Hudi is uniquely designed to write appropriately sized files automatically. This document will show you how Apache Hudi overcomes the dreaded small files problem. There are two ways to manage small files in Hudi: 

- Auto-size during ingestion
- Clustering

Below, we will describe the advantages and trade-offs of each.

## Auto-sizing during ingestion​

You can manage file sizes through Hudi’s auto-sizing capability during ingestion. The default targeted file size for Parquet base files is 120MB, which can be configured by `hoodie.parquet.max.file.size`. Auto-sizing may add some data latency, but it ensures that the read queries are always efficient as soon as a write transaction is committed. It’s important to note that if you don’t manage file sizing as you write and, instead, try to run clustering to fix your file sizing periodically, your queries might be slow until the point when the clustering finishes. This is only supported for **APPEND** use cases only; **UPSERTS** are not supported at the moment. Please refer to the [clustering documentation](https://hudi.apache.org/docs/clustering) for more details. 

:::note 
the bulk_insert write operation does not have auto-sizing capabilities during ingestion
:::

If you need to customize the file sizing, i.e., increase the target file size or change how small files are identified, follow the instructions below for Copy-On-Write and Merge-On-Read.

### Copy-On-Write (COW)​
To tune the file sizing for a COW table, you can set the small file limit and the maximum Parquet file size. Hudi will try to add enough records to a small file at write time to get it to the configured maximum limit.

 - For example, if the `hoodie.parquet.small.file.limit=104857600` (100MB) and `hoodie.parquet.max.file.size=125829120` (120MB), Hudi will pick all files < 100MB and try to get them up to 120MB.

For creating a Hudi table initially, setting an accurate record size estimate is vital to ensure Hudi can adequately estimate how many records need to be bin-packed in a Parquet file for the first ingestion batch. Then, Hudi automatically uses the average record size for subsequent writes based on previous commits.

 Parameter Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `hoodie.parquet.small.file.limit` | 104857600 (100MB) | During an insert and upsert operation, we opportunistically expand existing small files on storage instead of writing new files to keep the number of files optimum. This config sets the file size limit below which a storage file becomes a candidate to be selected as such a `small file`. By default, treat any file <= 100MB as a small file. Also note that if this is set to <= 0, Hudi will not try to get small files and directly write new files. | Write COW, MOR | 0.4.0 |
| `hoodie.parquet.max.file.size` |125829120 (120MB) | This config is the target size in bytes for parquet files produced by the Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.  | Write COW, MOR  | 0.4.0 |
| `hoodie.copyonwrite.record.size.estimate` |1024 (1024B) | The config is the average record size. If it’s not explicitly specified, Hudi will compute the record size estimate compute dynamically based on commit metadata. This is critical in computing the insert parallelism, and bin-packing inserts into small files. | Write COW  | 0.4.0 |  

## Merge-On-Read ​(MOR) 
As a MOR table aims to reduce the write amplification, compared to a COW table, when writing to a MOR table, Hudi limits the number of Parquet base files to one for auto file sizing during insert and upsert operation. This limits the number of rewritten files. This can be configured through `hoodie.merge.small.file.group.candidates.limit`.

In addition to file sizing Parquet base files for a MOR table, you can also tune the log files file-sizing with `hoodie.logfile.max.size`. 

:::note
For the BloomFilter index:  Small files in file groups included in the requested or inflight compaction or clustering under the active timeline, or small files with associated log files are not auto-sized with incoming inserts until the compaction or clustering is complete. For example: 
:::

- In case 1: If you had a log file and a compaction, C1, was scheduled to convert that log file to Parquet, no more inserts can go into the same file slice. 

- In case 2: If the Hudi table has a file group with a Parquet base file and an associated log file from updates, or this file group is under a requested or inflight compaction, no more inserts can go into this file group to automatically size the Parquet file. Only after the compaction has been performed, and there are NO log files associated with the base Parquet file, can new inserts be sent to auto-size that parquet file.

Here are the essential configurations:

| Property Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `hoodie.parquet.small.file.limit` | 104857600 (100MB) | During an insert and upsert operation, we opportunistically expand existing small files on storage instead of writing new files to keep the number of files optimum. This config sets the file size limit below which a storage file becomes a candidate to be selected as such a `small file`. By default, treat any file <= 100MB as a small file. Also note that if this is set to <= 0, Hudi will not try to get small files and directly write new files. | Write COW, MOR | 0.4.0 |
| `hoodie.parquet.max.file.size` |125829120 (120MB) | This config is the target size in bytes for parquet files produced by the Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.  | Write COW, MOR  | 0.4.0 | 
| `hoodie.logfile.max.size` | 1073741824 (1GB) | This is the log file max size in bytes. This is the maximum size allowed for a log file before it is rolled over to the next version. | Write MOR  | 0.4.0 | 
| `hoodie.merge.small.file.group.candidates.limit` | 1 | This limits the number of file groups, whose base file satisfies the small-file limit to be considered for appending records during an upsert operation. This is only applicable for MOR tables. | Write MOR | 0.4.0 |


## Auto-Sizing With Clustering​
Clustering is a service that allows you to combine small files into larger ones while at the same time (optionally) changing the data layout by sorting or applying space-filling curves like Z-order or Hilbert curve. We won’t go into all the details about clustering here, but please refer to the [clustering section](https://hudi.apache.org/docs/clustering) for more details. 

Clustering is one way to achieve file sizing so you can have faster queries. When you ingest data, you may still have a lot of small files (depending on your configurations and the data size from ingestion i.e., input batch). In this case, you will want to cluster all the small files to larger files to improve query performance. Clustering can be performed in different ways. Please check out the [clustering documentation](https://hudi.apache.org/docs/clustering) for more details. 

An example where clustering might be very useful is when a user has a Hudi table with many small files. Then, instead of waiting for multiple ingestion batches to gradually auto-size files, a user can use the clustering service to fix all the file sizes without ingesting any new data.

:::note
Clustering in Hudi is not a blocking operation, and ingestion can continue concurrently as long as no files need to be updated while the clustering service is running. The writes will fail if there are updates to the data being clustered while the clustering service runs.
:::

Here are the critical file sizing configurations:

| Parameter Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `hoodie.clustering.plan.strategy.small.file.limit` | 314572800 (300MB) | Files smaller than the size in bytes specified here are candidates for clustering. | Clustering | 0.7.0 |
| `target.file.max.bytes` |1073741824 (1GB) | This configures the target file size in bytes for clustering.| Clustering  | 0.7.0 |

:::note
Hudi always creates immutable files on disk. To be able to do auto-sizing or clustering, Hudi will always create a newer version of the smaller file, resulting in 2 versions of the same file. The [cleaner service](/docs/next/hoodie_cleaner) will later kick in and delete the older version small file and keep the latest one.
:::
