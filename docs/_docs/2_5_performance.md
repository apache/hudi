---
title: Performance
keywords: hudi, index, storage, compaction, cleaning, implementation
permalink: /docs/performance.html
toc: false
last_modified_at: 2019-12-30T15:59:57-04:00
---

In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against
the conventional alternatives for achieving these tasks. 

## Upserts

Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi table on the copy-on-write storage,
on 5 tables ranging from small to huge (as opposed to bulk loading the tables)

<figure>
    <img class="docimage" src="/assets/images/hudi_upsert_perf1.png" alt="hudi_upsert_perf1.png" style="max-width: 1000px" />
</figure>

Given Hudi can build the table incrementally, it opens doors for also scheduling ingesting more frequently thus reducing latency, with
significant savings on the overall compute cost.

<figure>
    <img class="docimage" src="/assets/images/hudi_upsert_perf2.png" alt="hudi_upsert_perf2.png" style="max-width: 1000px" />
</figure>

Hudi upserts have been stress tested upto 4TB in a single commit across the t1 table. 
See [here](https://cwiki.apache.org/confluence/display/HUDI/Tuning+Guide) for some tuning tips.

## Indexing

In order to efficiently upsert data, Hudi needs to classify records in a write batch into inserts & updates (tagged with the file group 
it belongs to). In order to speed this operation, Hudi employs a pluggable index mechanism that stores a mapping between recordKey and 
the file group id it belongs to. By default, Hudi uses a built in index that uses file ranges and bloom filters to accomplish this, with
upto 10x speed up over a spark join to do the same. 

Hudi provides best indexing performance when you model the recordKey to be monotonically increasing (e.g timestamp prefix), leading to range pruning filtering
out a lot of files for comparison. Even for UUID based keys, there are [known techniques](https://www.percona.com/blog/2014/12/19/store-uuid-optimized-way/) to achieve this.
For e.g , with 100M timestamp prefixed keys (5% updates, 95% inserts) on a event table with 80B keys/3 partitions/11416 files/10TB data, Hudi index achieves a 
**~7X (2880 secs vs 440 secs) speed up** over vanilla spark join. Even for a challenging workload like an '100% update' database ingestion workload spanning 
3.25B UUID keys/30 partitions/6180 files using 300 cores, Hudi indexing offers a **80-100% speedup**.

## Snapshot Queries

The major design goal for snapshot queries is to achieve the latency reduction & efficiency gains in previous section,
with no impact on queries. Following charts compare the Hudi vs non-Hudi tables across Hive/Presto/Spark queries and demonstrate this.

**Hive**

<figure>
    <img class="docimage" src="/assets/images/hudi_query_perf_hive.png" alt="hudi_query_perf_hive.png" style="max-width: 800px" />
</figure>

**Spark**

<figure>
    <img class="docimage" src="/assets/images/hudi_query_perf_spark.png" alt="hudi_query_perf_spark.png" style="max-width: 1000px" />
</figure>

**Presto**

<figure>
    <img class="docimage" src="/assets/images/hudi_query_perf_presto.png" alt="hudi_query_perf_presto.png" style="max-width: 1000px" />
</figure>
