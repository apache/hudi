---
title: Table & Query Types
summary: "In this page, we describe the different tables types in Hudi."
toc: true
last_modified_at:
---

## Table and Query Types
Hudi table types define how data is indexed & laid out on the DFS and how the above primitives and timeline activities are implemented on top of such organization (i.e how data is written).
In turn, `query types` define how the underlying data is exposed to the queries (i.e how data is read).

| Table Type    | Supported Query types |
|-------------- |------------------|
| Copy On Write | Snapshot Queries + Incremental Queries  |
| Merge On Read | Snapshot Queries + Incremental Queries + Read Optimized Queries |

### Table Types
Hudi supports the following table types.

- [Copy On Write](#copy-on-write-table) : Stores data using exclusively columnar file formats (e.g parquet). Updates simply version & rewrite the files by performing a synchronous merge during write.
- [Merge On Read](#merge-on-read-table) : Stores data using a combination of columnar (e.g parquet) + row based (e.g avro) file formats. Updates are logged to delta files & later compacted to produce new versions of columnar files synchronously or asynchronously.

Following table summarizes the trade-offs between these two table types

| Trade-off     | CopyOnWrite      | MergeOnRead |
|-------------- |------------------| ------------------|
| Data Latency | Higher   | Lower |
| Query Latency | Lower   | Higher |
| Update cost (I/O) | Higher (rewrite entire parquet) | Lower (append to delta log) |
| Parquet File Size | Smaller (high update(I/0) cost) | Larger (low update cost) |
| Write Amplification | Higher | Lower (depending on compaction strategy) |


### Query types
Hudi supports the following query types

- **Snapshot Queries** : Queries see the latest snapshot of the table as of a given commit or compaction action. In case of merge on read table, it exposes near-real time data(few mins) by merging
  the base and delta files of the latest file slice on-the-fly. For copy on write table,  it provides a drop-in replacement for existing parquet tables, while providing upsert/delete and other write side features.
- **Incremental Queries** : Queries only see new data written to the table, since a given commit/compaction. This effectively provides change streams to enable incremental data pipelines.
- **Read Optimized Queries** : Queries see the latest snapshot of table as of a given commit/compaction action. Exposes only the base/columnar files in latest file slices and guarantees the
  same columnar query performance compared to a non-hudi columnar table.

Following table summarizes the trade-offs between the different query types.

| Trade-off     | Snapshot    | Read Optimized |
|-------------- |-------------| ------------------|
| Data Latency  | Lower | Higher
| Query Latency | Higher (merge base / columnar file + row based delta / log files) | Lower (raw base / columnar file performance)


## Copy On Write Table

File slices in Copy-On-Write table only contain the base/columnar file and each commit produces new versions of base files.
In other words, we implicitly compact on every commit, such that only columnar data exists. As a result, the write amplification
(number of bytes written for 1 byte of incoming data) is much higher, where read amplification is zero.
This is a much desired property for analytical workloads, which is predominantly read-heavy.

Following illustrates how this works conceptually, when data written into copy-on-write table  and two queries running on top of it.


<figure>
    <img className="docimage" src={require("/assets/images/hudi_cow.png").default} alt="hudi_cow.png" />
</figure>


As data gets written, updates to existing file groups produce a new slice for that file group stamped with the commit instant time,
while inserts allocate a new file group and write its first slice for that file group. These file slices and their commit instant times are color coded above.
SQL queries running against such a table (eg: `select count(*)` counting the total records in that partition), first checks the timeline for the latest commit
and filters all but latest file slices of each file group. As you can see, an old query does not see the current inflight commit's files color coded in pink,
but a new query starting after the commit picks up the new data. Thus queries are immune to any write failures/partial writes and only run on committed data.

The intention of copy on write table, is to fundamentally improve how tables are managed today through

- First class support for atomically updating data at file-level, instead of rewriting whole tables/partitions
- Ability to incremental consume changes, as opposed to wasteful scans or fumbling with heuristics
- Tight control of file sizes to keep query performance excellent (small files hurt query performance considerably).


## Merge On Read Table

Merge on read table is a superset of copy on write, in the sense it still supports read optimized queries of the table by exposing only the base/columnar files in latest file slices.
Additionally, it stores incoming upserts for each file group, onto a row based delta log, to support snapshot queries by applying the delta log,
onto the latest version of each file id on-the-fly during query time. Thus, this table type attempts to balance read and write amplification intelligently, to provide near real-time data.
The most significant change here, would be to the compactor, which now carefully chooses which delta log files need to be compacted onto
their columnar base file, to keep the query performance in check (larger delta log files would incur longer merge times with merge data on query side)

Following illustrates how the table works, and shows two types of queries - snapshot query and read optimized query.

<figure>
    <img className="docimage" src={require("/assets/images/hudi_mor.png").default} alt="hudi_mor.png"  />
</figure>

There are lot of interesting things happening in this example, which bring out the subtleties in the approach.

- We now have commits every 1 minute or so, something we could not do in the other table type.
- Within each file id group, now there is an delta log file, which holds incoming updates to records in the base columnar files. In the example, the delta log files hold
  all the data from 10:05 to 10:10. The base columnar files are still versioned with the commit, as before.
  Thus, if one were to simply look at base files alone, then the table layout looks exactly like a copy on write table.
- A periodic compaction process reconciles these changes from the delta log and produces a new version of base file, just like what happened at 10:05 in the example.
- There are two ways of querying the same underlying table: Read Optimized query and Snapshot query, depending on whether we chose query performance or freshness of data.
- The semantics around when data from a commit is available to a query changes in a subtle way for a read optimized query. Note, that such a query
  running at 10:10, wont see data after 10:05 above, while a snapshot query always sees the freshest data.
- When we trigger compaction & what it decides to compact hold all the key to solving these hard problems. By implementing a compacting
  strategy, where we aggressively compact the latest partitions compared to older partitions, we could ensure the read optimized queries see data
  published within X minutes in a consistent fashion.

The intention of merge on read table is to enable near real-time processing directly on top of DFS, as opposed to copying
data out to specialized systems, which may not be able to handle the data volume. There are also a few secondary side benefits to
this table such as reduced write amplification by avoiding synchronous merge of data, i.e, the amount of data written per 1 bytes of data in a batch

