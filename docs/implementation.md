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
us to speed up upserts significantly, without scanning over every record in the dataset.

#### HBase Index

Here, we just use HBase in a straightforward way to store the mapping above. The challenge with using HBase (or any external key-value store
 for that matter) is performing rollback of a commit and handling partial index updates.
 Since the HBase table is indexed by record key and not commit Time, we would have to scan all the entries which will be prohibitively expensive.
 Insteead, we store the commit time with the value and discard its value if it does not belong to a valid commit.

#### Bloom Index

This index is built by adding bloom filters with a very high false positive tolerance (e.g: 1/10^9), to the parquet file footers.
The advantage of this index over HBase is the obvious removal of a big external dependency, and also nicer handling of rollbacks & partial updates
since the index is part of the data file itself.

At runtime, checking the Bloom Index for a given set of record keys effectively ammonts to checking all the bloom filters within a given
partition, against the incoming records, using a Spark join. Much of the engineering effort towards the Bloom index has gone into scaling this join
by caching the incoming RDD[HoodieRecord] to be able and dynamically tuning join parallelism, to avoid hitting Spark limitations like 2GB maximum
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

#### Merge On Read

Work in Progress .. .. .. .. ..

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
with zero impact on queries. Following charts compare the hoodie vs non-hoodie datasets across Hive/Presto/Spark queries.

{% include image.html file="hoodie_query_perf_hive.png" alt="hoodie_query_perf_hive.png" max-width="800" %}


{% include image.html file="hoodie_query_perf_spark.png" alt="hoodie_query_perf_spark.png" max-width="1000" %}


{% include image.html file="hoodie_query_perf_presto.png" alt="hoodie_query_perf_presto.png" max-width="1000" %}
