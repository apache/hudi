---
title: "What is Upsert on a Data Lake?"
excerpt: "An upsert updates a record if it exists and inserts it if it does not. This post explains why that was historically hard on data lakes, and how open table formats like Apache Hudi make it fast."
description: "What an upsert is, why record updates were historically hard on data lakes, and how table formats like Apache Hudi implement fast, indexed upserts."
authors: [vinoth-chandar]
category: how-to
image: /assets/images/blog/2023-05-10-top-3-things-you-can-do-to-get-fast-upsert-performance-in-apache-hudi.png
tags:
- upsert
- dml
- indexing
- data lakehouse
- cow
- mor
---

An upsert is a write operation that updates a record if it already exists and inserts it if it does not — a single atomic operation that combines *update* and *insert*, keyed on a unique record identifier. Databases have offered this for decades (`MERGE` in SQL, `ON CONFLICT DO UPDATE` in PostgreSQL, `replace into` in MySQL). On a data lake, however, upserts were effectively impossible for most of the technology's history, and making them fast is still what separates data lake storage systems from one another today. This post explains why data lakes couldn't update records, how open table formats changed that, what actually happens inside an upsert, and why indexing determines whether an upsert takes minutes or hours.

## Why Data Lakes Couldn't Update Records

Data lakes are built on files — typically columnar formats like Apache Parquet or ORC — sitting on distributed file systems or cloud object storage such as Amazon S3. Two properties of this foundation make record-level updates hard.

First, the files themselves are immutable. A Parquet file is written once, with column chunks, compression, and statistics computed over the whole file; there is no way to reach into it and change one row in place. Second, the storage layer underneath offers no transactional semantics across files. Early cloud object stores were even only eventually consistent, so a reader listing a directory mid-write could see a partial, corrupt view of the table. There were no primary keys, no notion of "this record already exists," and no atomic way to swap old data for new.

The Hive-era workaround was coarse-grained rewriting: partition the table (say, by day), and when any record in a partition changed, recompute and overwrite the *entire partition*. Pipelines re-read hours or days of data to apply what might be a handful of changed rows, then swapped directories and hoped no query was reading at that moment. This pattern wasted enormous compute, delayed data freshness from minutes to hours or days, and provided no isolation guarantees. It is precisely the batch-overwrite model that incremental write operations were [designed to replace](/docs/write_operations).

## How Open Table Formats Make Upserts Possible

Open table formats — [Apache Hudi](https://hudi.apache.org), Apache Iceberg, and Delta Lake — introduced a transactional metadata layer on top of immutable files, turning a directory of Parquet files into a proper table. That metadata layer is what makes upserts possible, through a few key mechanisms (covered more broadly in [What is a Data Lakehouse?](/blog/2024/07/11/what-is-a-data-lakehouse)):

- **Atomic commits.** Every write produces new file versions, and a commit protocol atomically publishes them. In Hudi, each write is an action on a [timeline](/docs/timeline); queries only ever see fully committed data, so a failed or in-flight write is invisible to readers.
- **Snapshot isolation and versioning.** Because old file versions are retained until cleaned, readers see a consistent snapshot while writers produce the next one. Updating a record no longer risks corrupting a running query.
- **Record keys.** Hudi brings the database notion of a primary key to the lake: every record has a key (record key, plus optionally a partition path) that uniquely identifies it. This is the precondition for "update if exists" to even be well defined.
- **Fine-grained file management.** Instead of rewriting a whole partition, the system can identify exactly which files contain the affected records and rewrite — or log changes against — only those.

With these pieces in place, an upsert on a data lake becomes what it is in a database: hand the system a batch of records, and it transactionally reconciles them against what is already stored. In Hudi, `upsert` is the [default write operation](/docs/write_operations#upsert): incoming records are tagged as inserts or updates via an index lookup, sized into files, written, and committed atomically. The target table never shows duplicates.

## The Anatomy of an Upsert

Understanding the cost of an upsert requires walking through what actually happens. Hudi's [write path](/docs/write_operations#write-path) breaks it into distinct steps.

**1. Key definition and deduplication.** Each incoming record carries a record key. Since an input batch may itself contain multiple versions of the same key (common with change streams), records are first combined by key, keeping the winner according to configured ordering fields.

**2. Index lookup.** The system must answer: *does this key already exist, and if so, in which file?* Hudi maintains a mapping from each record key to a *file group* — the unit of storage that holds all versions of a set of records. The [index lookup](/docs/indexes) tags each incoming record as an update (routed to its existing file group) or an insert (assigned to a new or under-sized file group). This step is the heart of the upsert, and — as the next section shows — the main determinant of its performance.

**3. Merge and write.** Tagged records are then written, and here the [table type](/docs/table_types) determines the strategy:

- **Copy-on-Write (CoW)** rewrites the affected base files: the writer reads the current base file of each touched file group, merges in the updates, and writes a new versioned base file. Queries stay simple and fast — they read pure columnar data with zero merge overhead — but write amplification is high: updating even a few records in a file means rewriting the whole file. CoW suits read-heavy tables with moderate update rates.
- **Merge-on-Read (MoR)** instead appends the changed records to compact delta log files attached to each file group, deferring the expensive base-file rewrite. Writes become lightweight and fast, enabling commits every few minutes; queries merge the logs with base files at read time, and a background [compaction](/docs/compaction) process periodically folds logs into new base files to restore pure-columnar read performance. MoR suits high-frequency updates and streaming ingestion, trading some query-time merge cost for much lower write latency and amplification.

The trade-off is summarized well by the [comparison table](/docs/table_types#comparison) in the Hudi docs: CoW pays `O(file groups written)` in write amplification for zero read amplification; MoR pays `O(records changed)` on both sides.

**4. Commit.** Finally, the write — new base files, new log blocks, and index updates — is committed atomically to the timeline, at which point queries can see it.

## Why Indexing Is the Difference Between Minutes and Hours

Step 2 above deserves its own section, because it is where naive upsert implementations fall down. Without an index, the only way to find which files contain the incoming keys is to *join the input batch against the entire table* — reading key columns from every file, every time you write. For a large table receiving a small trickle of updates, this means the cost of each write is proportional to the size of the table, not the size of the change.

![Comparison of merge cost for updates against base files, with and without an index](/assets/images/blog/hudi-indexes/with_without_index.png)
<p align = "center">Figure: Merge cost for updates (dark blue) against base files (light blue), with and without an index</p>

Hudi treats [indexing](/docs/indexes) as an integral part of its storage engine and offers several index types tuned to different workloads:

- **Bloom index**: stores bloom filters and key ranges (in file footers, or centrally in the metadata table) so most files can be pruned without reading data. Works very well when keys have some ordering — e.g., event tables with timestamp-prefixed keys — where range pruning eliminates most candidate files.
- **Simple index**: a lean join against keys extracted from existing files; a better fit for random-update workloads like dimension tables, where bloom filters would return true for nearly every file anyway.
- **Bucket index**: hashes keys directly to file groups, eliminating the lookup entirely at the cost of a fixed (or consistent-hashed) bucket layout.
- **[Record Level Index](/blog/2023/11/01/record-level-index)**: a scalable, exact key-to-file-group mapping stored in Hudi's metadata table, sharded by hash. Instead of pruning candidate files probabilistically, it answers the location question with a point lookup, delivering order-of-magnitude speedups on large deployments where index lookup dominates write latency.

The practical impact is dramatic. In a table with tens of thousands of files, an untagged upsert must consider all of them; a well-chosen index narrows the work to only the file groups that actually contain updated records. Merge-on-Read benefits doubly: the index bounds how many change records any base file must be merged against at query time, not just at write time. For concrete tuning guidance, see [Top 3 Things You Can Do to Get Fast Upsert Performance in Apache Hudi](/blog/2023/05/10/top-3-things-you-can-do-to-get-fast-upsert-performance-in-apache-hudi).

## What Upserts Unlock

Fast record-level mutation is not a niche feature; it changes what a data lake can be used for.

- **Change data capture (CDC) replication.** Streaming a database's change log (via Debezium, AWS DMS, or similar) into the lake requires applying inserts, updates, and deletes in order, continuously. Upserts make it possible to maintain a query-ready mirror of an operational database with minute-level freshness, instead of nightly full dumps.
- **Compliance deletes (GDPR/CCPA).** "Delete this user's data" is a targeted mutation across potentially every partition of a table. With keyed [deletes](/docs/write_operations#delete) — implemented on the same indexed path as upserts — this becomes a routine operation rather than a table-rewriting project.
- **Deduplication.** Event pipelines deliver duplicates; upserting by event key means the table enforces uniqueness on write, so consumers never need to deduplicate at query time.
- **Mutable fact and dimension tables.** Late-arriving corrections, order status changes, transaction settlements, slowly changing dimensions — all become incremental updates against the affected records, rather than periodic partition rewrites.
- **Incremental pipelines.** Because upserts record exactly what changed and when, downstream jobs can consume just the changes via [incremental queries](/docs/table_types#query-types), replacing full-table rescans with streaming-style processing.

## How Hudi, Iceberg, and Delta Lake Handle Mutation

All three major open table formats support upserts today, but with different designs and different cost profiles. Delta Lake and Iceberg expose mutation primarily through `MERGE INTO`: the engine joins the source batch against the target table to find affected files, then either rewrites them (copy-on-write) or, in newer versions, writes delete files/deletion vectors that mark rows as removed (merge-on-read style reads). Neither maintains a persistent record-level index, so locating affected files generally relies on the join plus file-level statistics pruning. Hudi was built mutation-first with primary keys, [pluggable indexes](/docs/indexes), and both [CoW and MoR](/docs/table_types) writer paths as core design elements, plus managed table services (compaction, clustering, cleaning) to keep continuously mutated tables performant. Which design performs better depends on the workload: for occasional, large batch merges the approaches converge, while for frequent, targeted updates against large tables, an indexed write path avoids repeatedly scanning table state to find what changed. Evaluate against your own update rate, latency requirements, and table sizes.

## Conclusion

An upsert — update if the record exists, insert if it does not — is the operation that turns a data lake from an append-only archive into a live, continuously updated foundation for analytics. It was historically out of reach because lakes stored immutable files with no transactions, keys, or record-level metadata, forcing wasteful partition rewrites. Open table formats supplied the transactional layer; what determines real-world performance is everything layered on top: how records are keyed, how quickly the system can locate existing records (indexing), and how the merge is physically executed (Copy-on-Write versus Merge-on-Read). Apache Hudi's answer — indexed, key-based writes with a choice of merge strategies and automated table services — is documented in depth in the [write operations](/docs/write_operations), [indexes](/docs/indexes), and [table types](/docs/table_types) pages, and is a good starting point for anyone building update-heavy lakehouse workloads.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'What is the difference between upsert and merge?',
    answer: 'They achieve similar outcomes through different interfaces. MERGE is a SQL statement where you write an explicit join condition and specify what happens on match or no match, giving full control including conditional logic. Upsert is a keyed write operation: the system uses the table\'s record key to decide automatically whether each incoming record is an update or an insert. In Apache Hudi, upsert is the default write operation and uses an index to locate existing records, while SQL MERGE INTO is also supported on top of the same machinery.',
  },
  {
    question: 'Can you update a record in a Parquet file?',
    answer: 'Not in place. Parquet files are immutable once written, so there is no way to modify a single row inside one. Table formats work around this by rewriting the affected file with the change applied, or by logging the change separately and merging it with the base file when the data is read. Either way, the update is expressed as new files plus a metadata commit, never as an in-place edit.',
  },
  {
    question: 'Is upsert slow on a data lake?',
    answer: 'It depends almost entirely on how the system finds existing records and how it applies changes. Without an index, every upsert must scan or join against the whole table to locate matching records, so write cost grows with table size. With an index such as Hudi\'s bloom index or record level index, the write only touches file groups that actually contain updated records. Choosing Merge-on-Read further reduces write latency by logging changes instead of rewriting columnar files on every commit.',
  },
  {
    question: 'What is the difference between upsert and insert in Apache Hudi?',
    answer: 'Upsert performs an index lookup to tag each incoming record as an update or an insert, guaranteeing the table never shows duplicate keys. Insert skips the index lookup entirely, which makes it faster but allows duplicates unless the input is pre-deduplicated. Upsert is recommended for change data capture and other update-heavy workloads, while insert suits append-mostly data that can tolerate or separately handle duplicates.',
  },
  {
    question: 'How do GDPR deletes work on a data lake?',
    answer: 'A GDPR or CCPA delete request means removing specific records, identified by key, from potentially every partition of a table. Table formats implement this on the same path as upserts: an index lookup finds the files containing the keys, and the delete is applied either by rewriting those files or by logging delete markers that compaction later reconciles. Hudi supports both soft deletes, which null out non-key fields, and hard deletes, which remove the records entirely.',
  },
  {
    question: 'Do Apache Iceberg and Delta Lake support upserts?',
    answer: 'Yes. Both support row-level mutation, primarily through the SQL MERGE INTO statement, with copy-on-write file rewrites or merge-on-read style delete files and deletion vectors. The main design difference from Apache Hudi is that they do not maintain a persistent record-level index, so finding affected files relies on joining the source data against the table with statistics-based pruning. Hudi instead defines record keys as a table-level concept and maintains indexes that map keys to file groups.',
  },
]} />
