---
title: "File Sizing"
toc: true
---

This doc will show you how Apache Hudi overcomes the dreaded small files problem. A key design decision in Hudi was to 
avoid creating small files in the first place and always write properly sized files. 
There are 2 ways to manage small files in Hudi and below will describe the advantages and trade-offs of each.

## Auto-Size During ingestion

You can automatically manage size of files during ingestion. This solution adds a little latency during ingestion, but
it ensures that read queries are always efficient as soon as a write is committed. If you don't 
manage file sizing as you write and instead try to periodically run a file-sizing clean-up, your queries will be slow until that resize cleanup is periodically performed.
 
(Note: [bulk_insert](/docs/next/write_operations) write operation does not provide auto-sizing during ingestion)

### For Copy-On-Write 
This is as simple as configuring the [maximum size for a base/parquet file](/docs/configurations#hoodieparquetmaxfilesize) 
and the [soft limit](/docs/configurations#hoodieparquetsmallfilelimit) below which a file should 
be considered a small file. For the initial bootstrap of a Hudi table, tuning record size estimate is also important to 
ensure sufficient records are bin-packed in a parquet file. For subsequent writes, Hudi automatically uses average 
record size based on previous commit. Hudi will try to add enough records to a small file at write time to get it to the 
configured maximum limit. For e.g , with `compactionSmallFileSize=100MB` and limitFileSize=120MB, Hudi will pick all 
files < 100MB and try to get them upto 120MB.

### For Merge-On-Read 
MergeOnRead works differently for different INDEX choices so there are few more configs to set:  

- Indexes with **canIndexLogFiles = true** : Inserts of new data go directly to log files. In this case, you can 
configure the [maximum log size](/docs/configurations#hoodielogfilemaxsize) and a 
[factor](/docs/configurations#hoodielogfiletoparquetcompressionratio) that denotes reduction in 
size when data moves from avro to parquet files.
- Indexes with **canIndexLogFiles = false** : Inserts of new data go only to parquet files. In this case, the 
same configurations as above for the COPY_ON_WRITE case applies.

NOTE : In either case, small files will be auto sized only if there is no PENDING compaction or associated log file for 
that particular file slice. For example, for case 1: If you had a log file and a compaction C1 was scheduled to convert 
that log file to parquet, no more inserts can go into that log file. For case 2: If you had a parquet file and an update 
ended up creating an associated delta log file, no more inserts can go into that parquet file. Only after the compaction 
has been performed and there are NO log files associated with the base parquet file, can new inserts be sent to auto size that parquet file.

## Auto-Size With Clustering
**[Clustering](/docs/next/clustering)** is a feature in Hudi to group 
small files into larger ones either synchronously or asynchronously. Since first solution of auto-sizing small files has 
a tradeoff on ingestion speed (since the small files are sized during ingestion), if your use-case is very sensitive to 
ingestion latency where you don't want to compromise on ingestion speed which may end up creating a lot of small files, 
clustering comes to the rescue. Clustering can be scheduled through the ingestion job and an asynchronus job can stitch 
small files together in the background to generate larger files. NOTE that during this, ingestion can continue to run concurrently.

*Please note that Hudi always creates immutable files on disk. To be able to do auto-sizing or clustering, Hudi will 
always create a newer version of the smaller file, resulting in 2 versions of the same file. 
The [cleaner service](/docs/next/hoodie_cleaner) will later kick in and delete the older version small file and keep the latest one.*
