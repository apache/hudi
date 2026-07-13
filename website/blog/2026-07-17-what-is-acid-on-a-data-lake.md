---
title: "What is ACID on a Data Lake?"
excerpt: "What atomicity, consistency, isolation and durability actually mean for tables on object storage, and how lakehouse table formats like Apache Hudi deliver them."
description: "What ACID means for tables on S3, GCS or ADLS: how table formats like Apache Hudi deliver atomic commits, snapshot isolation and safe concurrent writes."
authors: [sivabalan]
category: how-to
image: /assets/images/blog/acid.png
tags:
- acid
- concurrency control
- hudi timeline
- data lakehouse
---

ACID on a data lake means that writes to tables stored on object storage are Atomic, Consistent, Isolated and Durable — the same transactional guarantees a database provides, applied to files in S3, GCS or ADLS. That sentence is easy to say and surprisingly hard to deliver. A database owns its storage engine end to end; a data lake is a pile of Parquet files on an object store that offers durable single-object PUTs and nothing more. There is no multi-file atomic operation, no rename that atomically swaps a directory of files, and no built-in notion of "these 500 files belong to one write." Without extra machinery, a reader can list a directory while a writer is halfway through it and see a partial write, and two jobs writing to the same partition can silently clobber each other's output. ACID on a lake is the machinery — a commit protocol layered over the object store — that closes those gaps.

This post walks through what each of the four letters means concretely at the file level, how lakes worked before table formats existed, how formats like [Apache Hudi](https://hudi.apache.org) deliver these guarantees, and — just as important — what ACID on a lake does *not* promise.

## What A, C, I and D Mean on Object Storage

The textbook definitions of ACID were written for databases processing many small transactions. On a data lake, the transactions are large batch or streaming writes that produce files, so it is worth restating each property in file terms.

**Atomicity** means a write either publishes completely or not at all. Consider a Spark job writing 500 Parquet files into a table and dying at file 320 — an executor is lost, a spot instance is reclaimed, someone kills the job. Atomicity requires that those 320 orphaned files are never visible to any query, and that the failed write can be cleanly rolled back. Without it, every downstream consumer inherits a partially written dataset and no reliable way to detect it.

**Consistency** means every write moves the table from one valid state to another valid state. On a lake this covers schema enforcement (a write with an incompatible schema is rejected rather than quietly producing unreadable files), key constraints like upsert semantics (a record with the same key updates in place rather than duplicating), and the invariant that the table's metadata always describes exactly the set of files that constitute the table.

**Isolation** means concurrent readers and writers do not observe each other's intermediate states. Two failure modes matter here. First, two writers targeting the same partition at the same time — say, a backfill job and a streaming ingest job — must not interleave their files such that the table ends up with a mix of both writes' partial output. Second, a reader that starts listing files mid-write must not see half of a commit; it should see the table exactly as it was at some committed point, even if the query runs for an hour while new commits land.

**Durability** means a committed write survives failures. Object stores already provide excellent durability for individual objects — S3, GCS and ADLS replicate objects across zones. The lake-specific part of durability is that the *commit record itself* is persisted to the same durable storage, so that "was this write committed?" has a crash-proof answer, and a committed transaction is never silently rolled back by a subsequent cleanup process.

## Life Before ACID on Data Lakes

The Hadoop and early cloud-lake era answered these problems with conventions, not guarantees. It is worth remembering how fragile those conventions were, because they explain why table formats exist.

The classic Hive pattern was the **partition swap**: write data into a staging directory, then "atomically" move it into place and update the Hive metastore partition location. On HDFS a directory rename was genuinely atomic, so this mostly worked. On object stores there are no directories and no atomic rename — a "rename" is a copy-then-delete of every object — so the same pattern became a slow, non-atomic window during which readers could see anything.

The **`_SUCCESS` file** convention had MapReduce and Spark drop an empty marker file when a job finished, and downstream jobs would poll for it. This signaled completion but protected nothing: readers that listed the directory directly saw partial data, a rerun of a failed job could mix old and new files, and there was no rollback story at all — cleanup after a failed job meant a human with a delete command deciding which files were orphans.

Layered on top of this, early S3 offered only **eventual consistency** for listings, so even a fully written directory might not be fully *visible* yet. Whole projects (S3Guard, committers with DynamoDB bookkeeping) existed just to paper over that. S3 became strongly consistent in 2020, which removed one class of bugs — but strong single-object consistency still does not give you multi-file atomicity, isolation between writers, or rollback. Those require a transaction log.

## How Table Formats Deliver ACID

The shared insight behind Apache Hudi, Apache Iceberg and Delta Lake is that the table's state should be defined by a **metadata log with an atomic publish step**, not by whatever files happen to be in a directory. Data files are written first, invisibly; then a single, small metadata operation makes them visible all at once. Because that final operation is one atomic object-store action (a single PUT, or a conditional/compare-and-swap write), the entire multi-file write inherits its atomicity.

In Hudi, that log is the [timeline](/docs/timeline) — an ordered event log of every action performed on the table, stored under the table's `.hoodie/timeline` directory. Each write is an action that moves through explicit states: `REQUESTED` (the write is planned), `INFLIGHT` (data files are being produced), and `COMPLETED` (the write is published). The transition to `COMPLETED` is a single atomic object-store operation, and Hudi timestamps every action with monotonically increasing, [TrueTime](/docs/timeline#truetime-generation)-style instants so actions are globally ordered even across independent processes.

![Actions in the Hudi timeline](/assets/images/hudi-timeline-actions.png)
<p align = "center">Figure: Writes and table services recorded as actions on the Hudi timeline</p>

This structure delivers each letter directly:

- **Atomicity**: our 500-file Spark job that dies at file 320 leaves an `INFLIGHT` action on the timeline and 320 uncommitted files that no query will ever read, because queries resolve visible files through completed timeline actions — never by listing directories. A rollback action later removes the orphaned files. Nothing partial is ever served.
- **Consistency**: the timeline is the single source of truth for table state, and schema and key constraints are enforced as part of the write before the commit is published.
- **Isolation**: readers get **snapshot isolation**. A query binds to the set of completed commits as of its start and sees exactly that state for its entire duration, no matter how many writes complete concurrently. Multi-version concurrency control (MVCC) keeps prior file versions around so long-running queries are never yanked out from under.
- **Durability**: the commit metadata lives on the same replicated object storage as the data, so a completed instant is as durable as the data it describes.

For a deeper walkthrough of how the timeline underpins each transaction state transition, see the earlier post [Hoodie Timeline: Foundational pillar for ACID transactions](/blog/2023/07/09/Hoodie-Timeline-Foundational-pillar-for-ACID-transactions).

To be fair to the broader ecosystem: Apache Iceberg and Delta Lake achieve ACID through the same fundamental idea with different mechanics. Iceberg builds immutable snapshot trees of manifest files and commits by atomically swapping a pointer to the new table metadata (typically via a catalog's compare-and-swap). Delta Lake maintains a JSON transaction log (`_delta_log`) where writing log entry *N+1* is the atomic commit point, using conditional writes on the object store. All three give you atomic multi-file commits and snapshot isolation; they differ in how metadata is organized, how conflicts are detected, and what concurrency patterns they optimize for. This transactional layer is a defining ingredient of the [lakehouse architecture](/blog/2024/07/11/what-is-a-data-lakehouse) generally, not a Hudi-only concept.

## Concurrency Control Models

Atomic commits solve the single-writer case. The harder question — and where table formats differ most — is what happens when multiple processes write to the same table. Hudi's [concurrency control](/docs/concurrency_control) documentation lays out a spectrum of models:

**Single writer with table services.** Most pipelines have one writer, plus background table services (compaction, clustering, cleaning) that rewrite data for performance. Hudi distinguishes these by design: table services run lock-free alongside the writer, coordinated through the timeline via MVCC, either inline or fully async in the same process. For this very common case, no external lock infrastructure is needed at all.

**Optimistic concurrency control (OCC).** When genuinely separate jobs write to one table — a streaming ingest plus a batch backfill, say — Hudi supports OCC at file-group granularity. Each writer proceeds without blocking, and at commit time takes a short-lived distributed lock to check whether a concurrently completed commit touched the same files. Non-overlapping writes both succeed; overlapping writes cause one to abort and retry. The lock is held only around the commit-time critical section, not for the duration of the job.

![Optimistic concurrency control](/assets/images/blog/concurrency_control/OCC.png)
<p align = "center">Figure: Optimistic concurrency control — conflicts detected at commit time</p>

OCC works well when conflicts are rare, but it has an honest weakness for streaming: two high-frequency writers that routinely touch the same file groups will repeatedly abort each other, wasting compute and starving progress.

**Non-blocking concurrency control (NBCC).** For multi-writer streaming, Hudi supports a mode where multiple writers can write into the *same* file groups simultaneously without aborting each other. Instead of preventing the conflict, resolution is deferred: commits are ordered by completion time on the timeline, and the query reader and the compactor merge overlapping writes deterministically. A lock is needed only for the instant-metadata write to the timeline itself. NBCC currently targets merge-on-read tables with bucket indexes, and reflects a streaming-native view of concurrency: serialize the log, not the writers.

## What ACID on a Data Lake Does Not Mean

Honesty matters here, because "ACID" invites comparisons to OLTP databases that lakes are not designed to win.

- **It is not OLTP.** Commit latency on a lakehouse is measured in seconds — an object-store round trip plus metadata work — not the milliseconds of a transactional database. Lakehouse transactions are designed for batches and micro-batches of records, not single-row point writes at high QPS.
- **Transactions span one table.** The unit of atomicity in Hudi (and its peers, by and large) is a single table's commit. Multi-table transactions, cross-table foreign keys, and interactive multi-statement transactions with `BEGIN`/`COMMIT` semantics are not part of the model.
- **Isolation is snapshot isolation, not serializability of arbitrary read-write transactions.** Writers are serialized against each other per table, and readers see consistent snapshots — which is exactly what analytical workloads need — but you should not port an OLTP application's locking assumptions onto a lake.
- **ACID does not fix bad pipelines.** Atomic commits guarantee that what was written is either fully visible or invisible; they do not validate that the data itself was correct.

If your workload is high-QPS point reads and writes, use an operational database and land the changes onto the lake via CDC — which, conveniently, is a workload where lake ACID shines.

## Why It Matters

These guarantees are not academic; they are what make several load-bearing patterns possible at all.

**CDC correctness.** Replicating a database table into the lake via change data capture means a continuous stream of upserts and deletes. Without atomic commits and key-based upserts, a mid-batch failure leaves the lake copy in a state the source database never had — and there is no way to reconcile. With ACID, every commit is an all-or-nothing application of a batch of changes, and incremental consumers downstream see change batches in a well-defined order, never partially.

**Concurrent ingestion and table services.** Real tables need continuous compaction, clustering and cleaning to stay queryable. Running those alongside ingestion without transactions means either stopping the world or risking corruption. MVCC on the timeline lets Hudi rewrite data in the background while writers write and readers read, with each process pinned to a consistent snapshot.

**Regulatory deletes.** GDPR and CCPA erasure requests require deleting specific records from immutable files — physically, a read-modify-rewrite of affected files. ACID makes each deletion pass atomic and auditable: the timeline records exactly when the delete committed, queries never see a half-deleted state, and the commit history serves as evidence of compliance.

## Conclusion

ACID on a data lake is a commit protocol over object storage: data files are written invisibly, then published through one atomic metadata operation, with snapshot isolation for readers and explicit concurrency control between writers. Hudi implements this with its [timeline](/docs/timeline) — an ordered, durable event log of every action on the table — plus a spectrum of [concurrency control](/docs/concurrency_control) modes ranging from lock-free single-writer deployments to OCC and non-blocking multi-writer streaming. Iceberg and Delta Lake deliver the same core guarantees through their own log designs. None of this turns a lake into an OLTP database, and it should not be sold that way; what it does is make tables on cheap object storage trustworthy enough to run CDC pipelines, concurrent services and compliance workloads that raw file lakes could never support. That trust is the foundation the rest of the lakehouse stack is built on.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'Does S3 support ACID transactions?',
    answer: 'Not by itself. S3 provides durable, strongly consistent operations on individual objects, but no atomic operation spanning multiple objects and no isolation between concurrent writers. ACID transactions on S3 come from a table format layered on top, such as Apache Hudi, Apache Iceberg or Delta Lake, which publishes multi-file writes through a single atomic metadata commit.'
  },
  {
    question: 'Is a data lake ACID compliant?',
    answer: 'A raw data lake of Parquet or ORC files is not ACID compliant: readers can observe partial writes, concurrent jobs can corrupt each other, and failed jobs leave orphaned files. A data lake becomes ACID compliant when tables are managed by a transactional table format, at which point it is usually called a lakehouse.'
  },
  {
    question: 'How does Hudi guarantee atomicity?',
    answer: 'Hudi records every write as an action on its timeline, moving through requested, inflight and completed states. Data files are written first but remain invisible; the transition to completed is a single atomic object-store operation. Queries only read files referenced by completed actions, so a failed write is never visible and is later rolled back cleanly.'
  },
  {
    question: 'What is snapshot isolation on a data lake?',
    answer: 'Snapshot isolation means every query reads the table as of a specific committed state and sees exactly that state for its entire run, even while new writes commit concurrently. Table formats implement it with multi-version concurrency control, keeping prior file versions available until no reader needs them.'
  },
  {
    question: 'Can multiple jobs write to the same lakehouse table at once?',
    answer: 'Yes, with concurrency control. Hudi supports optimistic concurrency control, where writers proceed in parallel and conflicts on overlapping files are detected at commit time using a short-lived lock, and non-blocking concurrency control, where multiple writers can even target the same file group and conflicts are resolved at read and compaction time.'
  },
  {
    question: 'Is ACID on a data lake the same as ACID in a database like PostgreSQL?',
    answer: 'The guarantees are the same in kind but different in scope and latency. Lakehouse transactions cover batches of records within one table and commit in seconds, whereas an OLTP database offers millisecond, multi-statement, often multi-table transactions. Lakes are built for analytical and streaming workloads, not high-QPS point reads and writes.'
  }
]} />
