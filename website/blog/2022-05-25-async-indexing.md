---
title: "Asynchronous Indexing using Hudi"
excerpt: "How to setup Hudi for asynchronous indexing"
author: codope 
category: blog
---

In one the previous posts, we discussed the design
of [multi-modal index](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi)
, a serverless and high-performance indexing subsystem to the Lakehouse
architecture to boost the read and write latencies. In this blog, we discuss the
design of asynchronous indexing, a first of its kind, to support index building
alongside regular writers without blocking ingestion.

<!--truncate-->

## Background

Built on top of cheap storage and open file formats, the Hudi stack is more than
just a table format. A core layer of the stack is the transactional database
kernel which coordinates reads and writes to the Hudi table. Indexing is a
subsystem of that kernel. All the indexes (_files_, _column_stats_, and _
bloom_filter_) are stored in an internal Hudi Merge-On-Read (MOR) table, i.e.,
the [metadata table](https://hudi.apache.org/docs/metadata) co-located with the
data table. The metadata table is built to be self-managed so users don’t need
to spend operational cycles on any table services including compaction and
cleaning. For each write, all changes to the data table are translated into
metadata records committing to the metadata table. Hudi uses multi-table
transaction to ensure atomicity and resiliency to failures.

## Motivation

As we add more indexes, updating all the indexes synchronously inline with the
ingestion may not be very scalable. Moreover, the write client and indexing
subsystem are tightly coupled. Being able to index asynchronously has two
benefits, improved ingestion latency (and hence even lesser gap between event
time and arrival time), and reduced point of failure on the ingestion path. For
those familiar with `CREATE INDEX` in database systems, you would agree how easy
it is to create index without worrying about ongoing writes. Adding asynchronous
indexing to Hudi's rich set of table services is an attempt to bring the same
ease of use, reliability and performance to the lakehouse.

## Design

<img src="/assets/images/blog/multi-writer-indexer.png" alt="drawing" width="600"/>

At its core, asynchronous indexing with ongoing ingestion is a problem of
isolation and serialization of writes. One way to deal with this problem is to
employ lock-based concurrency control. However, the probability of conflict
increases with locking in case of long-running transactions. The solution to
this problem relies on three pillars of Hudi's transactional database kernel:

- [Hudi file layout](https://hudi.apache.org/docs/file_layouts)
- Hybrid [concurrency control](https://hudi.apache.org/docs/concurrency_control)
  model
- [Hudi timeline](https://hudi.apache.org/docs/timeline)

### Hudi File Layout

Hudi tables are broken up into partitions. Within each partition, files are
organized into file groups, uniquely identified by a file ID. Each file group
contains several file slices. Each slice contains a base file (.parquet)
produced at a certain commit/compaction instant time, along with set of log
files (.log.*) that contain inserts/updates to the base file since the base file
was produced. Compaction action merges logs and base files to produce new file
slices and cleaning action gets rid of unused/older file slices to reclaim space
on the file system.

### Hybrid concurrency control

Asynchronous indexing uses a mix of optimistic concurrency control and log-based
concurrency control models. The indexing is divided into two phases: scheduling
and execution.

During scheduling, the indexer (an external process other than the regular
ingestion writer)
takes a lock and generates an indexing plan for data files upto last commit
instant `t`. It initializes the metadata partitions corresponding to the index
requested and releases the lock once this phase is complete. No index files are
written at this stage.

During execution, the indexer executes the plan, writing the index base files
(corresponding to data files upto instant `t`) to the metadata partitions.
Meanwhile, the regular inflight writers continue to log updates to the log files
within same file group as the base file in the metadata partition. After writing
the base file, the indexer checks for all completed commit instants after `t` to
ensure each of them added entries per its indexing plan, otherwise simply abort
gracefully.

### Hudi Timeline

Hudi maintains a timeline of all actions performed on the table at different
instants. Think of it as an event log as the central piece for inter process
coordination. Hudi implements fine-grained log-based concurrency protocol on the
timeline. To differentiate indexing from other write operations, we introduced a
new action called
`indexing` on this timeline. The state transition of this action is handled by
the indexer. Scheduling indexing adds a `indexing.requested` instant to the
timeline. Execution phase transitions it to `inflight`
state while executing the plan and then finally to `completed` state after
indexing is complete. Indexer only takes a lock while adding events to the
timeline and not while writing index files.

The advantages of this design are:

- Data nodes and index nodes are decoupled and yet they are aware of each other.
- It is extensible to other types of indexes.
- It is suitable for both batch and streaming workloads.

With the timeline as event log, a hybrid of the two concurrency models provides
great scalability as well as asynchrony for the indexing process to run
concurrently with ingestion in tandem with other table services such as
compaction and clustering.

## Documentation

Please check
out [RFC-45](https://github.com/apache/hudi/blob/master/rfc/rfc-45/rfc-45.md)
for more details on the design and implementation of the indexer. To setup and
see the indexer in action,
follow [asynchronous indexer](https://hudi.apache.org/docs/metadata_indexing)
guide.

## Future Work

Asynchronous indexing feature, a first of its kind in the lakehouse
architecture, is still evolving. One line of work is to enahnce the usability of
indexer; integrate with SQL and other types of indexes. Another line of work
investigates ways to increase the amount of asynchrony in the indexing process,
e.g. drop index without taking any lock. We welcome more ideas and contributions
from the community.

## Conclusion

Hudi's multi-modal index and asynchronous indexing features show that there is
more to transactional data lakes than just a table format and metadata. The
fundamental principles of distributed storage systems apply to lakehouse
architecture as well, and the challenges occur on a different scale.
Asynchronous indexing at this scale would soon become a necessity. We discussed
a design that is not only scalable but also provides great amount of asynchrony.
We will build on this foundation to add more capabilities to the indexer.

