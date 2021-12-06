---
title: Metadata Table
keywords: [ hudi, metadata, S3 file listings]
---

## Motivation for a Metadata Table

The Apache Hudi Metadata Table can significantly improve read/write performance of your queries. The two main purposes of the 
Metadata Table are:

1. **Eliminate the requirement for the "list files" operation:**
   1. When reading, writing data in HDFS, file listing operations are performed to get the current view of the file system.
      When data sets are large, listing all the files becomes a performance bottleneck and in the case of cloud storage systems
      like AWS S3, sometimes causes throttling due to list operation request limits. The Metadata Table will instead
      proactively maintain the list of files and remove the need for recursive file listing operations on HDFS.
2. **Create Column Indexes for better query planning and faster lookups by readers** 
   1. For a column in the dataset, min/max range per Parquet file can be maintained. 
   Just by reading this index file, the query planning system should be able to get the view of potential Parquet files for a range query.
   Reading Column information from an index file should be faster than reading the individual Parquet Footers.

## Enable Hudi Metadata Table
The Hudi Metadata Table is not enabled by default. If you wish to turn it on you need to enable the following configuration:

[`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable)

## Deployment considerations
Once you turn on the Hudi Metadata Table, ensure that all write and read operations enable the configuration above to 
ensure the Metadata Table stays up to date.

:::note
If your current deployment model is single writer along with async table services (such as cleaning, clustering, compaction) 
configured, then it is a must to have [lock providers configured](/docs/next/concurrency_control#enabling-multi-writing) 
before turning on the metadata table.
:::