---
title: Performance
keywords: [ hudi, index, storage, compaction, cleaning, implementation]
toc: false
last_modified_at: 2019-12-30T15:59:57-04:00
---

## Optimized DFS Access

Hudi also performs several key storage management functions on the data stored in a Hudi table. A key aspect of storing data on DFS is managing file sizes and counts
and reclaiming storage space. For e.g HDFS is infamous for its handling of small files, which exerts memory/RPC pressure on the Name Node and can potentially destabilize
the entire cluster. In general, query engines provide much better performance on adequately sized columnar files, since they can effectively amortize cost of obtaining
column statistics etc. Even on some cloud data stores, there is often cost to listing directories with large number of small files.

Here are some ways to efficiently manage the storage of your Hudi tables.

- The [small file handling feature](/docs/configurations/#hoodieparquetsmallfilelimit) in Hudi, profiles incoming workload
  and distributes inserts to existing file groups instead of creating new file groups, which can lead to small files.
- Cleaner can be [configured](/docs/configurations#hoodiecleanercommitsretained) to clean up older file slices, more or less aggressively depending on maximum time for queries to run & lookback needed for incremental pull
- User can also tune the size of the [base/parquet file](/docs/configurations#hoodieparquetmaxfilesize), [log files](/docs/configurations#hoodielogfilemaxsize) & expected [compression ratio](/docs/configurations#hoodieparquetcompressionratio),
  such that sufficient number of inserts are grouped into the same file group, resulting in well sized base files ultimately.
- Intelligently tuning the [bulk insert parallelism](/docs/configurations#hoodiebulkinsertshuffleparallelism), can again in nicely sized initial file groups. It is in fact critical to get this right, since the file groups
  once created cannot be deleted, but simply expanded as explained before.
- For workloads with heavy updates, the [merge-on-read table](/docs/concepts#merge-on-read-table) provides a nice mechanism for ingesting quickly into smaller files and then later merging them into larger base files via compaction.

## Performance Optimizations

In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against
the conventional alternatives for achieving these tasks. 

### Write Path

#### Upserts

Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi table on the copy-on-write storage,
on 5 tables ranging from small to huge (as opposed to bulk loading the tables)

<figure>
    <img className="docimage" src={require("/assets/images/hudi_upsert_perf1.png").default} alt="hudi_upsert_perf1.png"  />
</figure>

Given Hudi can build the table incrementally, it opens doors for also scheduling ingesting more frequently thus reducing latency, with
significant savings on the overall compute cost.

<figure>
    <img className="docimage" src={require("/assets/images/hudi_upsert_perf2.png").default} alt="hudi_upsert_perf2.png"  />
</figure>

Hudi upserts have been stress tested upto 4TB in a single commit across the t1 table. 
See [here](https://cwiki.apache.org/confluence/display/HUDI/Tuning+Guide) for some tuning tips.

#### Indexing

In order to efficiently upsert data, Hudi needs to classify records in a write batch into inserts & updates (tagged with the file group 
it belongs to). In order to speed this operation, Hudi employs a pluggable index mechanism that stores a mapping between recordKey and 
the file group id it belongs to. By default, Hudi uses a built in index that uses file ranges and bloom filters to accomplish this, with
upto 10x speed up over a spark join to do the same. 

Hudi provides best indexing performance when you model the recordKey to be monotonically increasing (e.g timestamp prefix), leading to range pruning filtering
out a lot of files for comparison. Even for UUID based keys, there are [known techniques](https://www.percona.com/blog/2014/12/19/store-uuid-optimized-way/) to achieve this.
For e.g , with 100M timestamp prefixed keys (5% updates, 95% inserts) on a event table with 80B keys/3 partitions/11416 files/10TB data, Hudi index achieves a 
**~7X (2880 secs vs 440 secs) speed up** over vanilla spark join. Even for a challenging workload like an '100% update' database ingestion workload spanning 
3.25B UUID keys/30 partitions/6180 files using 300 cores, Hudi indexing offers a **80-100% speedup**.


### Read Path

#### Data Skipping
 
Data Skipping is a technique (originally introduced in Hudi 0.10) that leverages files metadata to very effectively prune the search space, by 
avoiding reading (even footers of) the files that are known (based on the metadata) to only contain the data that _does not match_ the query's filters.

Data Skipping is leveraging Metadata Table's Column Stats Index bearing column-level statistics (such as min-value, max-value, count of null-values in the column, etc)
for every file of the Hudi table. This then allows Hudi for every incoming query instead of enumerating every file in the table and reading its corresponding metadata 
(for ex, Parquet footers) for analysis whether it could contain any data matching the query filters, to simply do a query against a Column Stats Index 
in the Metadata Table (which in turn is a Hudi table itself) and within seconds (even for TBs scale tables, with 10s of thousands of files) obtain the list 
of _all the files that might potentially contain the data_ matching query's filters with crucial property that files that could be ruled out as not containing such data
(based on their column-level statistics) will be stripped out.

In spirit, Data Skipping is very similar to Partition Pruning for tables using Physical Partitioning where records in the dataset are partitioned on disk
into a folder structure based on some column's value or its derivative (clumping records together based on some intrinsic measure), but instead
of on-disk folder structure, Data Skipping leverages index maintaining a mapping "file &rarr; columns' statistics" for all of the columns persisted 
within that file.

For very large tables (1Tb+, 10s of 1000s of files), Data skipping could 
1. Substantially improve query execution runtime (by avoiding fruitless Compute churn) in excess of **10x** as compared to the same query on the same dataset but w/o Data Skipping enabled.
2. Help avoid hitting Cloud Storages throttling limits (for issuing too many requests, for ex, AWS limits # of requests / sec that could be issued based on the object's prefix which considerably complicates things for partitioned tables)  

If you're interested on learning more details around how Data Skipping is working internally please watch out for a blog-post coming out on this soon!  

To unlock the power of Data Skipping you will need to

1. Enable Metadata Table along with Column Stats Index on the _write path_ (TODO add ref to async indexer)
2. Enable Data Skipping in your queries

To enable Metadata Table along with Column Stats Index on the write path, make sure 
following properties are set to true:
  - `hoodie.metadata.enable` (to enable Metadata Table on the write path, enabled by default)
  - `HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key` (to enable Column Stats Index being populated on the write path, disabled by default)

TODO(alexey) add ref to async indexer docs
> NOTE: If you're planning on enabling Column Stats Index for already existing table, please check out Async Indexer documentation
> on how to build Metadata Table Indexes (such as Column Stats Index) for existing tables


To enable Data Skipping in your queries make sure to set following properties to "true" (on the read path): 
  - `hoodie.enable.data.skipping` (to enable Data Skipping)
  - `hoodie.metadata.enable` (to enable Metadata Table use on the read path)
  - `HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key` (to enable Column Stats Index use on the read path)
