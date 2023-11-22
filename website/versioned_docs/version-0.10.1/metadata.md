---
title: Metadata Table
keywords: [ hudi, metadata, S3 file listings]
---

## Motivation for a Metadata Table

The Apache Hudi Metadata Table can significantly improve read/write performance of your queries. The main purpose of the
Metadata Table is to eliminate the requirement for the "list files" operation.

When reading and writing data, file listing operations are performed to get the current view of the file system.
When data sets are large, listing all the files may be a performance bottleneck, but more importantly in the case of cloud storage systems
like AWS S3, the large number of file listing requests sometimes causes throttling due to certain request limits.
The Metadata Table will instead proactively maintain the list of files and remove the need for recursive file listing operations

### Some numbers from a study:
Running a TPCDS benchmark the p50 list latencies for a single folder scales ~linearly with the amount of files/objects:

|Number of files/objects|100|1K|10K|100K|
|---|---|---|---|---|
|P50 list latency|50ms|131ms|1062ms|9932ms|

Whereas listings from the Metadata Table will not scale linearly with file/object count and instead take about 100-500ms per read even for very large tables.
Even better, the timeline server caches portions of the metadata (currently only for writers), and provides ~10ms performance for listings.

## Enable Hudi Metadata Table
The Hudi Metadata Table is not enabled by default. If you wish to turn it on you need to enable the following configuration:

[`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable)

## Deployment considerations
Once you turn on the Hudi Metadata Table, ensure that all write and read operations enable the configuration above to 
ensure the Metadata Table stays up to date.

:::note
If your current deployment model is single writer along with async table services (such as cleaning, clustering, compaction) 
configured, then it is a must to have [lock providers configured](/docs/concurrency_control#enabling-multi-writing) 
before turning on the metadata table.
:::