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

- The [small file handling feature](configurations/#hoodieparquetsmallfilelimit) in Hudi, profiles incoming workload
  and distributes inserts to existing file groups instead of creating new file groups, which can lead to small files.
- Cleaner can be [configured](configurations#hoodiecleanercommitsretained) to clean up older file slices, more or less aggressively depending on maximum time for queries to run & lookback needed for incremental pull
- User can also tune the size of the [base/parquet file](configurations#hoodieparquetmaxfilesize), [log files](configurations#hoodielogfilemaxsize) & expected [compression ratio](configurations#hoodieparquetcompressionratio),
  such that sufficient number of inserts are grouped into the same file group, resulting in well sized base files ultimately.
- Intelligently tuning the [bulk insert parallelism](configurations#hoodiebulkinsertshuffleparallelism), can again in nicely sized initial file groups. It is in fact critical to get this right, since the file groups
  once created cannot be changed without re-clustering the table. Writes will simply expand given file groups with new updates/inserts as explained before.
- For workloads with heavy updates, the [merge-on-read table](concepts#merge-on-read-table) provides a nice mechanism for ingesting quickly into smaller files and then later merging them into larger base files via compaction.

## Performance Optimizations

In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against
the conventional alternatives for achieving these tasks.

### Write Path

#### Bulk Insert

Write configurations in Hudi are optimized for incremental upserts by default. In fact, the default write operation type is UPSERT as well.
For simple append-only use case to bulk load the data, following set of configurations are recommended for optimal writing:
```
-- Use “bulk-insert” write-operation instead of default “upsert”
hoodie.datasource.write.operation = BULK_INSERT
-- Disable populating meta columns and metadata, and enable virtual keys
hoodie.populate.meta.fields = false
hoodie.metadata.enable = false
-- Enable snappy compression codec for lesser CPU cycles (but more storage overhead)
hoodie.parquet.compression.codec = snappy
```

For ingesting via spark-sql
```
-- Use “bulk-insert” write-operation instead of default “upsert”
hoodie.sql.insert.mode = non-strict,
hoodie.sql.bulk.insert.enable = true,
-- Disable populating meta columns and metadata, and enable virtual keys
hoodie.populate.meta.fields = false
hoodie.metadata.enable = false
-- Enable snappy compression codec for lesser CPU cycles (but more storage overhead)
hoodie.parquet.compression.codec = snappy
```

We recently benchmarked Hudi against TPC-DS workload. 
Please check out [our blog](/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks) for more details.

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
 

Data Skipping is a technique (originally introduced in Hudi 0.10) that leverages metadata to very effectively prune the search space of a query,
by eliminating files that cannot possibly contain data matching the query's filters. By maintaining this metadata in the internal Hudi metadata table,
Hudi avoids reading file footers to obtain this information, which can be costly for queries spanning tens of thousands of files.

Data Skipping leverages metadata table's `col_stats` partition bearing column-level statistics (such as min-value, max-value, count of null-values in the column, etc)
for every file of the Hudi table. This then allows Hudi for every incoming query instead of enumerating every file in the table and reading its corresponding metadata
(for ex, Parquet footers) for analysis whether it could contain any data matching the query filters, to simply do a query against a Column Stats Index
in the Metadata Table (which in turn is a Hudi table itself) and within seconds (even for TBs scale tables, with 10s of thousands of files) obtain the list
of _all the files that might potentially contain the data_ matching query's filters with crucial property that files that could be ruled out as not containing such data
(based on their column-level statistics) will be stripped out. See [RFC-27](https://github.com/apache/hudi/blob/master/rfc/rfc-27/rfc-27.md) for detailed design.

Partitioning can be considered a coarse form of indexing and data skipping using the col_stats partition can be thought of as a range index, that databases use to identify potential 
blocks of data interesting to a query. Unlike partition pruning for tables using physical partitioning where records in the dataset are organized into a folder structure based 
on some column's value, data skipping using col_stats delivers a logical/virtual partitioning.

For very large tables (1Tb+, 10s of 1000s of files), Data skipping could

1. Substantially improve query execution runtime **10x** as compared to the same query on the same dataset but w/o Data Skipping enabled.
2. Help avoid hitting Cloud Storages throttling limits (for issuing too many requests, for ex, AWS limits # of requests / sec that could be issued based on the object's prefix which considerably complicates things for partitioned tables)

To unlock the power of Data Skipping you will need to

1. Enable Metadata Table along with Column Stats Index on the _write path_ (See [Metadata Indexing](metadata_indexing)), using `hoodie.metadata.enable=true` (to enable Metadata Table on the write path, enabled by default)
2. Enable Data Skipping in your queries, using `hoodie.metadata.index.column.stats.enable=true` (to enable Column Stats Index being populated on the write path, disabled by default)

:::note
If you're planning on enabling Column Stats Index for already existing table, please check out the [Metadata Indexing](metadata_indexing) guide on how to build Metadata Table Indices (such as Column Stats Index) for existing tables.
:::

To enable Data Skipping in your queries make sure to set following properties to `true` (on the read path): 

  - `hoodie.enable.data.skipping` (to control data skipping, enabled by default)
  - `hoodie.metadata.enable` (to enable metadata table use on the read path, enabled by default)
  - `hoodie.metadata.index.column.stats.enable` (to enable column stats index use on the read path)

## Related Resources

<h3>Blogs</h3>
* [Hudi’s Column Stats Index and Data Skipping feature help speed up queries by an orders of magnitude!](https://www.onehouse.ai/blog/hudis-column-stats-index-and-data-skipping-feature-help-speed-up-queries-by-an-orders-of-magnitude)
* [Top 3 Things You Can Do to Get Fast Upsert Performance in Apache Hudi](https://www.onehouse.ai/blog/top-3-things-you-can-do-to-get-fast-upsert-performance-in-apache-hudi)
