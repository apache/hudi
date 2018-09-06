---
title: Implementation
keywords: implementation
sidebar: mydoc_sidebar
toc: false
permalink: implementation.html
---

Hoodie is implemented as a Spark library, which makes it easy to integrate into existing data pipelines or ingestion
libraries (which we will refer to as `hoodie clients`). Hoodie Clients prepare an `RDD[HoodieRecord]` that contains the data to be upserted and
Hoodie upsert/insert is merely a Spark DAG, that can be broken into two big pieces.

 - **Indexing** :  A big part of Hoodie's efficiency comes from indexing the mapping from record keys to the file ids, to which they belong to.
 This index also helps the `HoodieWriteClient` separate upserted records into inserts and updates, so they can be treated differently.
 `HoodieReadClient` supports operations such as `filterExists` (used for de-duplication of table) and an efficient batch `read(keys)` api, that
 can read out the records corresponding to the keys using the index much quickly, than a typical scan via a query. The index is also atomically
 updated each commit, and is also rolled back when commits are rolled back.

 - **Storage** : The storage part of the DAG is responsible for taking an `RDD[HoodieRecord]`, that has been tagged as
 an insert or update via index lookup, and writing it out efficiently onto storage.

## Index

Hoodie currently provides two choices for indexes : `BloomIndex` and `HBaseIndex` to map a record key into the file id to which it belongs to. This enables
us to speed up upserts significantly, without scanning over every record in the dataset. Hoodie Indices can be classified based on
their ability to lookup records across partition. A `global` index does not need partition information for finding the file-id for a record key 
but a `non-global` does.

#### HBase Index (global)

Here, we just use HBase in a straightforward way to store the mapping above. The challenge with using HBase (or any external key-value store
 for that matter) is performing rollback of a commit and handling partial index updates.
 Since the HBase table is indexed by record key and not commit Time, we would have to scan all the entries which will be prohibitively expensive.
 Insteead, we store the commit time with the value and discard its value if it does not belong to a valid commit.

#### Bloom Index (non-global)

This index is built by adding bloom filters with a very high false positive tolerance (e.g: 1/10^9), to the parquet file footers.
The advantage of this index over HBase is the obvious removal of a big external dependency, and also nicer handling of rollbacks & partial updates
since the index is part of the data file itself.

At runtime, checking the Bloom Index for a given set of record keys effectively amounts to checking all the bloom filters within a given
partition, against the incoming records, using a Spark join. Much of the engineering effort towards the Bloom index has gone into scaling this join
by caching the incoming RDD[HoodieRecord] and dynamically tuning join parallelism, to avoid hitting Spark limitations like 2GB maximum
for partition size. As a result, Bloom Index implementation has been able to handle single upserts upto 5TB, in a reliable manner.


## Storage

The implementation specifics of the two storage types, introduced in [concepts](concepts.html) section, are detailed below.


#### Copy On Write

The Spark DAG for this storage, is relatively simpler. The key goal here is to group the tagged hoodie record RDD, into a series of
updates and inserts, by using a partitioner. To achieve the goals of maintaining file sizes, we first sample the input to obtain a `workload profile`
that understands the spread of inserts vs updates, their distribution among the partitions etc. With this information, we bin-pack the
records such that

 - For updates, the latest version of the that file id, is rewritten once, with new values for all records that have changed
 - For inserts, the records are first packed onto the smallest file in each partition path, until it reaches the configured maximum size.
   Any remaining records after that, are again packed into new file id groups, again meeting the size requirements.

In this storage, index updation is a no-op, since the bloom filters are already written as a part of committing data.

In the case of Copy-On-Write, a single parquet file constitutes one `file slice` which contains one complete version of 
the file 

{% include image.html file="hoodie_log_format_v2.png" alt="hoodie_log_format_v2.png" max-width="1000" %}

#### Merge On Read

In the Merge-On-Read storage model, there are 2 logical components - one for ingesting data (both inserts/updates) into the dataset
 and another for creating compacted views. The former is hereby referred to as `Writer` while the later
 is referred as `Compactor`.
 
##### Merge On Read Writer
 
 At a high level, Merge-On-Read Writer goes through same stages as Copy-On-Write writer in ingesting data.
 The key difference here is that updates are appended to latest log (delta) file belonging to the latest file slice 
 without merging. For inserts, Hudi supports 2 modes:

   1. Inserts to Log Files - This is done for datasets that have an indexable log files (for eg global index)
   2. Inserts to parquet files - This is done for datasets that do not have indexable log files, for eg bloom index
      embedded in parquer files. Hudi treats writing new records in the same way as inserting to Copy-On-Write files.

As in the case of Copy-On-Write, the input tagged records are partitioned such that all upserts destined to 
a `file id` are grouped together. This upsert-batch is written as one or more log-blocks written to log-files.
Hudi allows clients to control log file sizes (See [Storage Configs](../configurations))

The WriteClient API is same for both Copy-On-Write and Merge-On-Read writers.
 
With Merge-On-Read, several rounds of data-writes would have resulted in accumulation of one or more log-files.
All these log-files along with base-parquet (if exists) constitute a `file slice` which represents one complete version
of the file. 
  
#### Compactor

Realtime Readers will perform in-situ merge of these delta log-files to provide the most recent (committed) view of
the dataset. To keep the query-performance in check and eventually achieve read-optimized performance, Hudi supports
compacting these log-files asynchronously to create read-optimized views.

Asynchronous Compaction involves 2 steps:

  * `Compaction Schedule` : Hudi Write Client exposes API to create Compaction plans which contains the list of `file slice`
    to be compacted atomically in a single compaction commit. Hudi allows pluggable strategies for choosing
    file slices for each compaction runs. This step is typically done inline by Writer process as Hudi expects
    only one schedule is being generated at a time which allows Hudi to enforce the constraint that pending compaction
    plans do not step on each other file-slices. This constraint allows for multiple concurrent `Compactors` to run at 
    the same time. Some of the common strategies used for choosing `file slice` for compaction are:
    * BoundedIO - Limit the number of file slices chosen for a compaction plan by expected total IO (read + write) 
    needed to complete compaction run 
    * Log File Size - Prefer file-slices with larger amounts of delta log data to be merged
    * Day Based - Prefer file slice belonging to latest day partitions
    ```
        API for scheduling compaction
          /**
           * Schedules a new compaction instant
           * @param extraMetadata
           * @return Compaction Instant timestamp if a new compaction plan is scheduled
           */
           Optional<String> scheduleCompaction(Optional<Map<String, String>> extraMetadata) throws IOException;
     ```
  * `Compactor` : Hudi provides a separate API in Write Client to execute a compaction plan. The compaction
    plan (just like a commit) is identified by a timestamp. Most of the design and implementation complexities for Async
    Compaction is for guaranteeing snapshot isolation to readers and writer when
    multiple concurrent compactors are running. Typical compactor deployment involves launching a separate
    spark application which executes pending compactions when they become available. The core logic of compacting
    file slices in the Compactor is very similar to that of merging updates in a Copy-On-Write table. The only
    difference being in the case of compaction, there is an additional step of merging the records in delta log-files. 
    
    Here are the main API to lookup and execute a compaction plan.
    ```
      Main API in HoodieWriteClient for running Compaction:
       /**
        * Performs Compaction corresponding to instant-time
        * @param compactionInstantTime   Compaction Instant Time
        * @return
        * @throws IOException
        */
        public JavaRDD<WriteStatus> compact(String compactionInstantTime) throws IOException;
    
      To lookup all pending compactions, use the API defined in HoodieReadClient
    
      /**
       * Return all pending compactions with instant time for clients to decide what to compact next.
       * @return
       */
      public List<Pair<String, HoodieCompactionPlan>> getPendingCompactions();
    ```

Refer to  __hoodie-client/src/test/java/HoodieClientExample.java__ class for an example of how compaction
is scheduled and executed.

##### Deployment Models

These are typical Hoodie Writer and Compaction deployment models

  * `Inline Compaction` : At each round, a single spark application ingests new batch to dataset. It then optionally decides to schedule
   a compaction run and executes it in sequence.
  * `Single Dedicated Async Compactor` :  The Spark application which brings in new changes to dataset (writer) periodically
     schedules compaction. The Writer application does not run compaction inline. A separate spark applications periodically
     probes for pending compaction and executes the compaction.
  * ` Multi Async Compactors` : This mode is similar to `Single Dedicated Async Compactor` mode. The main difference being
     now there can be more than one spark application picking different compactions and executing them in parallel.
     In order to ensure compactors do not step on each other, they use coordination service like zookeeper to pickup unique
     pending compaction instants and run them.

The Compaction process requires one executor per file-slice in the compaction plan. So, the best resource allocation
strategy (both in terms of speed and resource usage) for clusters supporting dynamic allocation is to lookup the compaction
plan to be run to figure out the number of file slices being compacted and choose that many number of executors.

## Async Compaction Design Deep-Dive (Optional)

For the purpose of this section, it is important to distinguish between 2 types of commits as pertaining to the file-group: 

A commit which generates a merged and read-optimized file-slice is called `snapshot commit` (SC) with respect to that file-group.
A commit which merely appended the new/updated records assigned to the file-group into a new log block is called `delta commit` (DC) 
with respect to that file-group.

### Algorithm

The algorithm is described with an illustration. Let us assume a scenario where there are commits SC1, DC2, DC3 that have
already completed on a data-set. Commit DC4 is currently ongoing with the writer (ingestion) process using it to upsert data. 
Let us also imagine there are a set of file-groups (FG1 … FGn) in the data-set whose latest version (`File-Slice`) 
contains the base file created by commit SC1 (snapshot-commit in columnar format) and a log file containing row-based 
log blocks of 2 delta-commits (DC2 and DC3). 

{% include image.html file="async_compac_1.png" alt="async_compac_1.png" max-width="1000" %}

 * Writer (Ingestion) that is going to commit "DC4" starts. The record updates in this batch are grouped by file-groups 
   and appended in row formats to the corresponding log file as delta commit. Let us imagine a subset of file-groups has 
   this new log block (delta commit) DC4 added.
 * Before the writer job completes, it runs the compaction strategy to decide which file-group to compact by compactor 
   and creates a new compaction-request commit SC5. This commit file is marked as “requested” with metadata denoting 
   which fileIds to compact (based on selection policy). Writer completes without running compaction (will be run async). 
 
   {% include image.html file="async_compac_2.png" alt="async_compac_2.png" max-width="1000" %}
 
 * Writer job runs again ingesting next batch. It starts with commit DC6. It reads the earliest inflight compaction 
   request marker commit in timeline order and collects the (fileId, Compaction Commit Id “CcId” ) pairs from meta-data. 
   Ingestion DC6 ensures a new file-slice with base-commit “CcId” gets allocated for the file-group. 
   The Writer will simply append records in row-format to the first log-file (as delta-commit) assuming the 
   base-file (“Phantom-Base-File”) will be created eventually by the compactor.
   
   {% include image.html file="async_compac_3.png" alt="async_compac_3.png" max-width="1000" %}
 
 * Compactor runs at some time  and commits at “Tc” (concurrently or before/after Ingestion DC6). It reads the commit-timeline 
   and finds the first unprocessed compaction request marker commit. Compactor reads the commit’s metadata finding the 
   file-slices to be compacted. It compacts the file-slice and creates the missing base-file (“Phantom-Base-File”) 
   with “CCId” as the commit-timestamp. Compactor then marks the compaction commit timestamp as completed. 
   It is important to realize that at data-set level, there could be different file-groups requesting compaction at 
   different commit timestamps.
 
    {% include image.html file="async_compac_4.png" alt="async_compac_4.png" max-width="1000" %}

 * Near Real-time reader interested in getting the latest snapshot will have 2 cases. Let us assume that the 
   incremental ingestion (writer at DC6) happened before the compaction (some time “Tc”’).  
   The below description is with regards to compaction from file-group perspective. 
   * `Reader querying at time between ingestion completion time for DC6 and compaction finish “Tc”`: 
     Hoodie’s implementation will be changed to become aware of file-groups currently waiting for compaction and 
     merge log-files corresponding to DC2-DC6 with the base-file corresponding to SC1. In essence, Hoodie will create 
     a pseudo file-slice by combining the 2 file-slices starting at base-commits SC1 and SC5 to one. 
     For file-groups not waiting for compaction, the reader behavior is essentially the same - read latest file-slice 
     and merge on the fly.
   * `Reader querying at time after compaction finished (> “Tc”)` : In this case, reader will not find any pending 
     compactions in the timeline and will simply have the current behavior of reading the latest file-slice and 
     merging on-the-fly.
     
 * Read-Optimized View readers will query against the latest columnar base-file for each file-groups. 

The above algorithm explains Async compaction w.r.t a single compaction run on a single file-group. It is important
to note that multiple compaction plans can be run concurrently as they are essentially operating on different 
file-groups.

## Performance

In this section, we go over some real world performance numbers for Hoodie upserts, incremental pull and compare them against
the conventional alternatives for achieving these tasks.

#### Upsert vs Bulk Loading

Following shows the speed up obtained for NoSQL ingestion, by switching from bulk loads off HBase to Parquet to incrementally upserting
on a Hoodie dataset, on 5 tables ranging from small to huge.

{% include image.html file="hoodie_upsert_perf1.png" alt="hoodie_upsert_perf1.png" max-width="1000" %}


Given Hoodie can build the dataset incrementally, it opens doors for also scheduling ingesting more frequently thus reducing latency, with
significant savings on the overall compute cost.


{% include image.html file="hoodie_upsert_perf2.png" alt="hoodie_upsert_perf2.png" max-width="1000" %}

Hoodie upserts have been stress tested upto 4TB in a single commit across the t1 table.



#### Copy On Write Regular Query Performance

The major design goal for copy-on-write storage was to achieve the latency reduction & efficiency gains in previous section,
with no impact on queries. Following charts compare the hoodie vs non-hoodie datasets across Hive/Presto/Spark queries.

**Hive**

{% include image.html file="hoodie_query_perf_hive.png" alt="hoodie_query_perf_hive.png" max-width="800" %}

**Spark**

{% include image.html file="hoodie_query_perf_spark.png" alt="hoodie_query_perf_spark.png" max-width="1000" %}

**Presto**

{% include image.html file="hoodie_query_perf_presto.png" alt="hoodie_query_perf_presto.png" max-width="1000" %}

