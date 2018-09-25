---
title: Concepts
keywords: concepts
sidebar: mydoc_sidebar
permalink: concepts.html
toc: false
summary: "Here we introduce some basic concepts & give a broad technical overview of Hoodie"
---

Hoodie provides the following primitives over datasets on HDFS

 * Upsert                     (how do I change the dataset?)
 * Incremental consumption    (how do I fetch data that changed?)


In order to achieve this, Hoodie maintains a `timeline` of all activity performed on the dataset, that helps provide `instantaenous` views of the dataset,
while also efficiently supporting retrieval of data in the order of arrival into the dataset.
Such key activities include

 * `COMMITS` - A single commit captures information about an **atomic write** of a batch of records into a dataset.
       Commits are identified by a monotonically increasing timestamp, denoting the start of the write operation.
 * `CLEANS` - Background activity that gets rid of older versions of files in the dataset, that are no longer needed.
 * `DELTA_COMMITS` - A single commit captures information about an **atomic write** of a batch of records into a 
 MergeOnRead storage type of dataset
 * `COMPACTIONS` - Background activity to reconcile differential data structures within Hoodie e.g: moving updates from row based log files to columnar formats.


{% include image.html file="hoodie_timeline.png" alt="hoodie_timeline.png" %}

Example above shows upserts happenings between 10:00 and 10:20 on a Hoodie dataset, roughly every 5 mins, leaving commit metadata on the hoodie timeline, along
with other background cleaning/compactions. One key observation to make is that the commit time indicates the `arrival time` of the data (10:20AM), while the actual data
organization reflects the actual time or `event time`, the data was intended for (hourly buckets from 07:00). These are two key concepts when reasoning about tradeoffs between latency and completeness of data.

When there is late arriving data (data intended for 9:00 arriving >1 hr late at 10:20), we can see the upsert producing new data into even older time buckets/folders.
With the help of the timeline, an incremental query attempting to get all new data that was committed successfully since 10:00 hours, is able to very efficiently consume
only the changed files without say scanning all the time buckets > 07:00.

## Terminologies

 * `Hudi Dataset` 
    A structured hive/spark dataset managed by Hudi. Hudi supports both partitioned and non-partitioned Hive tables. 
 * `Commit` 
    A commit marks a new batch of data applied to a dataset. Hudi maintains  monotonically increasing timestamps to track commits and guarantees that a commit is atomically 
    published.
 * `Commit Timeline`
    Commit Timeline refers to the sequence of Commits that was applied in order on a dataset over its lifetime. 
 * `File Slice` 
    Hudi provides efficient handling of updates by having a fixed mapping between record key to a logical file Id. 
    Hudi uses MVCC to provide atomicity and isolation of readers from a writer. This means that a logical fileId will
    have many physical versions of it. Each of these physical version of a file represents a complete view of the
    file as of a commit and is called File Slice
 * `File Group`
    A file-group is a file-slice timeline. It is a list of file-slices in commit order. It is identified by `file id`


## Storage Types

Hoodie storage types capture how data is indexed & laid out on the filesystem, and how the above primitives and timeline activities are implemented on top of
such organization (i.e how data is written). This is not to be confused with the notion of Read Optimized & Near-Real time tables, which are merely how the underlying data is exposed
to the queries (i.e how data is read).

Hoodie (will) supports the following storage types.

| Storage Type  | Supported Tables |
|-------------- |------------------|
| Copy On Write | Read Optimized   |
| Merge On Read | Read Optimized + Near Real-time |

  - Copy On Write : A heavily read optimized storage type, that simply creates new versions of files corresponding to the records that changed.
  - Merge On Read : Also provides a near-real time datasets in the order of 5 mins, by shifting some of the write cost, to the reads and merging incoming and on-disk data on-the-fly

{% include callout.html content="Hoodie is a young project. merge-on-read is currently underway. Get involved [here](https://github.com/uber/hoodie/projects/1)" type="info" %}

Regardless of the storage type, Hoodie organizes a datasets into a directory structure under a `basepath`,
very similar to Hive tables. Dataset is broken up into partitions, which are folders containing files for that partition.
Each partition uniquely identified by its `partitionpath`, which is relative to the basepath.

Within each partition, records are distributed into multiple files. Each file is identified by an unique `file id` and the `commit` that
produced the file. Multiple files can share the same file id but written at different commits, in case of updates.

Each record is uniquely identified by a `record key` and mapped to a file id forever. This mapping between record key
and file id, never changes once the first version of a record has been written to a file. In short, the
 `file id` identifies a group of files, that contain all versions of a group of records.


## Copy On Write

As mentioned above, each commit on Copy On Write storage, produces new versions of files. In other words, we implicitly compact every
commit, such that only columnar data exists. As a result, the write amplification (number of bytes written for 1 byte of incoming data)
 is much higher, where read amplification is close to zero. This is a much desired property for a system like Hadoop, which is predominantly read-heavy.

Following illustrates how this works conceptually, when  data written into copy-on-write storage  and two queries running on top of it.


{% include image.html file="hoodie_cow.png" alt="hoodie_cow.png" %}


As data gets written, updates to existing file ids, produce a new version for that file id stamped with the commit and
inserts allocate a new file id and write its first version for that file id. These file versions and their commits are color coded above.
Normal SQL queries running against such dataset (eg: select count(*) counting the total records in that partition), first checks the timeline for latest commit
and filters all but latest versions of each file id. As you can see, an old query does not see the current inflight commit's files colored in pink,
but a new query starting after the commit picks up the new data. Thus queries are immune to any write failures/partial writes and only run on committed data.

The intention of copy on write storage, is to fundamentally improve how datasets are managed today on Hadoop through

  - First class support for atomically updating data at file-level, instead of rewriting whole tables/partitions
  - Ability to incremental consume changes, as opposed to wasteful scans or fumbling with heuristical approaches
  - Tight control file sizes to keep query performance excellent (small files hurt query performance considerably).


## Merge On Read

Merge on read storage is a superset of copy on write, in the sense it still provides a read optimized view of the dataset via the Read Optmized table.
But, additionally stores incoming upserts for each file id, onto a `row based append log`, that enables providing near real-time data to the queries
 by applying the append log, onto the latest version of each file id on-the-fly during query time. Thus, this storage type attempts to balance read and write amplication intelligently, to provide near real-time queries.
The most significant change here, would be to the compactor, which now carefully chooses which append logs need to be compacted onto
their columnar base data, to keep the query performance in check (larger append logs would incur longer merge times with merge data on query side)

Following illustrates how the storage works, and shows queries on both near-real time table and read optimized table.

{% include image.html file="hoodie_mor.png" alt="hoodie_mor.png" max-width="1000" %}


There are lot of interesting things happening in this example, which bring out the subleties in the approach.

 - We now have commits every 1 minute or so, something we could not do in the other storage type.
 - Within each file id group, now there is an append log, which holds incoming updates to records in the base columnar files. In the example, the append logs hold
 all the data from 10:05 to 10:10. The base columnar files are still versioned with the commit, as before.
 Thus, if one were to simply look at base files alone, then the storage layout looks exactly like a copy on write table.
 - A periodic compaction process reconciles these changes from the append log and produces a new version of base file, just like what happened at 10:05 in the example.
 - There are two ways of querying the same underlying storage: ReadOptimized (RO) Table and Near-Realtime (RT) table, depending on whether we chose query performance or freshness of data.
 - The semantics around when data from a commit is available to a query changes in a subtle way for the RO table. Note, that such a query
 running at 10:10, wont see data after 10:05 above, while a query on the RT table always sees the freshest data.
 - When we trigger compaction & what it decides to compact hold all the key to solving these hard problems. By implementing a compacting
 strategy, where we aggressively compact the latest partitions compared to older partitions, we could ensure the RO Table sees data
 published within X minutes in a consistent fashion.

{% include callout.html content="Hoodie is a young project. merge-on-read is currently underway. Get involved [here](https://github.com/uber/hoodie/projects/1)" type="info" %}

The intention of merge on read storage, is to enable near real-time processing directly on top of Hadoop, as opposed to copying
data out to specialized systems, which may not be able to handle the data volume.

## Trade offs when choosing different storage types and views

### Storage Types

| Trade-off | CopyOnWrite | MergeOnRead |
|-------------- |------------------| ------------------|
| Data Latency | Higher   | Lower |
| Update cost (I/O) | Higher (rewrite entire parquet) | Lower (append to delta file) |
| Parquet File Size | Smaller (high update(I/0) cost) | Larger (low update cost) |
| Write Amplification | Higher | Lower (depending on compaction strategy) |

### Hudi Views

| Trade-off | ReadOptimized | RealTime |
|-------------- |------------------| ------------------|
| Data Latency | Higher   | Lower |
| Query Latency | Lower (raw columnar performance) | Higher (merge columnar + row based delta) |