---
title: "Introducing Hudi's Non-blocking Concurrency Control for streaming, high-frequency writes"
excerpt: "Announcing the Non-blocking Concurrency Control in Apache Hudi"
author: Danny Chan
category: blog
image: /assets/images/blog/non-blocking-concurrency-control/lsm_archive_timeline.png
tags:
- design
- streaming ingestion
- multi-writer
- concurrency-control
- blog
---

## Introduction

In streaming ingestion scenarios, there are plenty of use cases that require concurrent ingestion from multiple streaming sources. 
The user can union all the upstream source inputs into one downstream table to collect the records for unified access across federated queries.
Another very common scenario is multiple stream sources joined together to supplement dimensions of the records to build a wide-dimension table where each source 
stream is taking records with partial table schema fields. Common and strong demand for multi-stream concurrent ingestion has always been there. 
The Hudi community has collected so many feedbacks from users ever since the day Hudi supported streaming ingestion and processing.

Starting from [Hudi 1.0.0](https://hudi.apache.org/releases/release-1.1#release-100), we are thrilled to announce a new general-purpose 
concurrency model for Apache Hudi - the Non-blocking Concurrency Control (NBCC)- aimed at the stream processing or high-contention/frequent writing scenarios. 
In contrast to [Optimistic Concurrency Control](/blog/2021/12/16/lakehouse-concurrency-control-are-we-too-optimistic/), where writers abort the transaction 
if there is a hint of contention, this innovation allows multiple streaming writes to the same Hudi table without any overhead of conflict resolution, while 
keeping the semantics of [event-time ordering](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) found in streaming systems, along with 
asynchronous table service such as compaction, archiving and cleaning.

NBCC works seamlessly without any new infrastructure or operational overhead. In the subsequent sections of this blog, we will give a brief introduction to Hudi's internals 
about the data file layout and TrueTime semantics for time generation, a pre-requisite for discussing NBCC. Following that, we will delve into the design and workflows of NBCC, 
and then a simple SQL demo to show the NBCC related config options. The blog will conclude with insights into future work for NBCC.

## Older Design 

It's important to understand the Hudi [storage layout](/docs/next/storage_layouts) and it evolves/manages data versions. In older release before 1.0.0,
Hudi organizes the data files with units as `FileGroup`. Each file group contains multiple `FileSlice`s. Every compaction on this file group generates a new file slice.
Each file slice may comprise an optional base file(columnar file format like Apache Parquet or ORC) and multiple log files(row file format in Apache Avro or Parquet).

<img src="/assets/images/blog/non-blocking-concurrency-control/legacy_file_layout.png" alt="Legacy file layout" width="800" align="middle"/>

The timestamp in the base file name is the instant time of the compaction that writes it, it is also called as "requested instant time" in Hudi's notion.
The timestamp in the log file name is the same timestamp as the current file slice base instant time. Data files with the same instant time belong to one file slice. 
In effect, a file group represented a linear ordered sequence of base files (checkpoints) followed by logs files (deltas), followed by base files (checkpoints).

The instant time naming convention in log files becomes a hash limitation in concurrency mode. Each log file contains incremental changes from
multiple commits. Each writer needs to query the file layout to get the base instant time and figure out the full file name before flushing the records.
A more severe problem is the base instant time can be variable with the async compaction pushing forward. In order to make the base instant time deterministic for the log writers, Hudi
forces the schedule sequence between a write commit and compaction scheduling: a compaction can be scheduled only if there is no ongoing ingestion into the Hudi table. Without this, a log file
can be written with a wrong base instant time which could introduce data loss. This means a compaction scheduling could block all the writers in concurrency mode.

## NBCC Design

In order to resolve these pains, since 1.0.0, Hudi introduces a new storage layout based on both requested and completion times of actions, viewing them as an interval. 
Each commit in 1.x Hudi has two [important notions of time](/docs/next/timeline): instant time(or requested time) and completion time.
All the generated timestamp are globally monotonically increasing. Instead of putting the base instant time in the log file name, Hudi now just uses the requested instant time
of the write. During file slicing, Hudi queries the completion time for each log file with the instant time, and we have a new rule for file slicing:

*A log file belongs to the file slice with the maximum base requested time smaller than(or equals with) it's completion time.*[^1]

<img src="/assets/images/blog/non-blocking-concurrency-control/new_file_layout.png" alt="New file layout" width="800" align="middle"/>

With the flexibility of the new file layout, the overhead of querying base instant time is eliminated for log writers and a compaction can be scheduled anywhere with any instant time.
See [RFC-66](https://github.com/apache/hudi/blob/master/rfc/rfc-66/rfc-66.md) for more.

### True Time API

In order to ensure the monotonicity of timestamp generation, Hudi introduces the "[TrueTime API](/docs/next/timeline#timeline-components)" since 1.x release.
Basically there are two ways to make the time generation monotonically increasing, inline with TrueTime semantics:

- A global lock to guard the time generation with mutex, along with a wait for an estimated max allowed clock skew on distributed hosts;
- Globally synchronized time generation service, e.g. Google Spanner Time Service, the service itself can ensure the monotonicity.

Hudi now implements the "TrueTime" semantics with the first solution, a configurable max waiting time is supported.

### LSM timeline

The new file layout requires efficient queries from instant time to get the completion time. Hudi re-implements the archived timeline since 1.x, the 
new archived timeline data files are organized as [an LSM tree](/docs/next/timeline#lsm-timeline-history) to support fast time range filtering queries with instant time data-skipping on it.

<img src="/assets/images/blog/non-blocking-concurrency-control/lsm_archive_timeline.png" alt="LSM archive timeline" align="middle"/>


With the powerful new file layout, it is quite straight-forward to implement non-blocking concurrency control. The function is implemented with the simple bucket index on MOR table for Flink.
The bucket index ensures fixed record key to file group mappings for multiple workloads. The log writer writes the records into avro logs and the compaction table service would take care of 
the conflict resolution. Because each log file name contains the instant time and each record contains the event time ordering field, Hudi reader can merge the records either 
with natural order(processing time sequence) or event time order.

The concurrency mode should be configured as `NON_BLOCKING_CONCURRENCY_CONTROL`, you can enable the table services on one job and disable it for the others.

## Flink SQL demo

Here is a demo to show 2 pipelines that ingest into the same downstream table, the two sink table views share the same table path.

```sql
-- NB-CC demo

-- The source table
CREATE TABLE sourceT (
  uuid varchar(20),
  name varchar(10),
  age int,
  ts timestamp(3),
  `partition` as 'par1'
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '200'
);

-- table view for writer1
create table t1(
  uuid varchar(20),
  name varchar(10),
  age int,
  ts timestamp(3),
  `partition` varchar(20)
)
with (
  'connector' = 'hudi',
  'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
  'table.type' = 'MERGE_ON_READ',
  'index.type' = 'BUCKET',
  'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
  'write.tasks' = '2'
);

insert into t1/*+options('metadata.enabled'='true')*/ select * from sourceT;

-- table view for writer2
-- compaction and cleaning are disabled because writer1 has taken care of it.
create table t1_2(
  uuid varchar(20),
  name varchar(10),
  age int,
  ts timestamp(3),
  `partition` varchar(20)
)
with (
  'connector' = 'hudi',
  'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
  'table.type' = 'MERGE_ON_READ',
  'index.type' = 'BUCKET',
  'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
  'write.tasks' = '2',
  'compaction.schedule.enabled' = 'false',
  'compaction.async.enabled' = 'false',
  'clean.async.enabled' = 'false'
);

-- executes the ingestion workloads
insert into t1 select * from sourceT;
insert into t1_2 select * from sourceT;
```

## Future Roadmap

While non-blocking concurrency control is a very powerful feature for streaming users, it is a general solution for multiple writer conflict resolution,
here are some plans that improve the Hudi core features:

- NBCC support for metadata table
- NBCC for clustering
- NBCC for other index type


---

[^1] [RFC-66](https://github.com/apache/hudi/blob/master/rfc/rfc-66/rfc-66.md) well-explained the completion time based file slicing with a pseudocode.