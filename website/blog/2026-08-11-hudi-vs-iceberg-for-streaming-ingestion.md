---
title: "Apache Hudi vs Apache Iceberg for Streaming Ingestion"
excerpt: "Both formats accept streaming writes, but their architectures diverge sharply under continuous ingestion. A mechanism-level comparison of write paths, commit cadence, compaction, and concurrency."
description: "Hudi vs Iceberg for streaming ingestion: write paths, commit cadence, small files, compaction, and multi-writer concurrency compared at the mechanism level."
authors: [sivabalan]
category: deep-dive
image: /assets/images/blog/2025-07-02-Lakehouse-Architecture-apache-hudi-and-apache-iceberg.png
tags:
- comparison
- apache iceberg
- streaming
- data lakehouse
---

Both Apache Hudi and Apache Iceberg can ingest streaming data, but they are built around different assumptions about how often a table is written to. Hudi's architecture is streaming-native: Merge-on-Read tables absorb updates as lightweight log appends, commits land every few minutes with automatic file sizing, compaction and clustering run asynchronously without ever blocking ingestion, and non-blocking concurrency control lets multiple writers land data into the same file groups. Iceberg ingests streams as a sequence of periodic snapshot commits — a design that works well at moderate cadence for append-heavy data, but relies on externally scheduled maintenance jobs to control the small files, delete files, and snapshot metadata that continuous writing produces.

<!--truncate-->

That is the short answer to "Hudi vs Iceberg for streaming." The rest of this post walks through the mechanisms behind it: what continuous ingestion actually stresses in a table format, how each format's write path behaves under it, what minute-level commits do to each format's metadata, and how each keeps readers fast while the writers never stop. The goal is a fair, mechanism-level comparison — both are excellent open table formats, and the differences that matter here are architectural, not a matter of one project being "better" in the abstract.

## What Continuous Ingestion Stresses in a Table Format

Batch and streaming workloads stress a table format in very different places. A nightly batch job commits once, writes well-sized files, and leaves a long quiet window for maintenance. A streaming pipeline inverts every one of those properties:

- **Commit frequency.** A pipeline targeting minute-level freshness commits hundreds of times a day, every day. Every structure the format touches per commit — metadata files, manifests, timeline entries — gets exercised at that rate.
- **Small files.** Each micro-commit writes only a sliver of data. Without countermeasures, a week of minute-level commits leaves behind tens of thousands of files, and query planning and scan performance degrade with file count.
- **Write amplification on updates.** Streams from CDC sources are update-heavy. If applying an update means rewriting the columnar file that contains the old value, a trickle of changes turns into a torrent of rewritten bytes.
- **Concurrent maintenance.** There is no quiet window. Compaction, clustering, and cleaning must run *while* ingestion continues, without blocking it and without being starved by it.
- **Exactly-once delivery.** The format's commit protocol has to compose with Flink checkpoints or Spark Structured Streaming epochs so that failures and retries never duplicate or drop records.

These pressures are exactly what a [streaming data lake](/blog/2026/07/21/what-is-a-streaming-data-lake) has to absorb, and they are where the two formats' designs part ways.

## The Write Path Compared

**Hudi.** Hudi's [Merge-on-Read table type](/docs/table_types) organizes data into file groups, each holding a columnar base file plus a set of row-oriented delta log files. When a streaming update arrives, Hudi's indexes locate the file group that holds the record and *append* the change to that group's log file. No base file is rewritten on the hot path — write amplification for a file group is proportional to the records that changed, not the size of the files they live in. Inserts are bin-packed against existing small file groups by Hudi's automatic file sizing, so the writer itself counteracts small-file buildup on every commit rather than deferring the problem to a cleanup job.

**Iceberg.** Iceberg's write path is snapshot-oriented: every commit adds new immutable data files and produces a new snapshot — a new manifest list, new or rewritten manifests, and a new table metadata file. For appends this is clean and cheap per commit. For streaming upserts, Iceberg's merge-on-read mode writes *delete files* alongside data files: equality deletes (typical for Flink CDC pipelines) or position deletes mark old rows as superseded, and the new values land in fresh data files. Nothing is rewritten on the hot path here either — but each micro-commit contributes both new small data files and new delete files, plus a fresh layer of snapshot metadata, and the format itself does not resize or merge any of it at write time. That work belongs to separately scheduled maintenance procedures such as `rewrite_data_files` and `rewrite_manifests`.

The structural difference: Hudi routes changes *into* an existing file-group layout via its indexes and keeps files sized as it writes; Iceberg accretes new files and metadata per commit and restores layout health through external compaction.

## Commit Cadence and Its Costs

Consider what one day of minute-level commits — roughly 1,440 of them — does to each format's metadata.

In Hudi, each commit adds an entry to the [timeline](/docs/timeline), and file-level statistics and listings land in Hudi's internal metadata table, which is itself an MOR table designed to absorb frequent small mutations cheaply. Older timeline entries are archived automatically into a compact timeline history, so the active timeline that writers and readers consult stays bounded no matter how long the pipeline runs. Frequent commits are the *assumed* operating mode — the mechanisms that keep metadata bounded (archival, metadata-table compaction) are built in and run as part of normal table operation.

In Iceberg, each commit produces a new snapshot: a metadata JSON file, a manifest list, and one or more manifests. At minute-level cadence, that is 1,440 snapshots a day, each retaining pointers to the manifests of its predecessors until `expire_snapshots` runs. Manifests accumulate and fragment, planning cost grows with manifest count, and the catalog's atomic pointer swap becomes a serialization point that every commit contends on. None of this is a correctness problem — Iceberg handles it — but keeping it healthy requires operating a maintenance regimen (snapshot expiry, manifest rewriting, orphan file cleanup) at a tempo matched to the ingestion rate. This is why Iceberg streaming guidance generally steers users toward longer checkpoint intervals: the format's per-commit metadata cost sets a practical floor on cadence.

The honest summary: both formats *can* commit every minute. Hudi's metadata structures were designed around that rate; Iceberg's were designed around fewer, larger commits, and sustained high cadence shifts real work onto the maintenance schedule.

## Keeping Readers Fast While Writing Continuously

Log appends and delete files both defer merge work — the question is who pays for it, and when.

Hudi gives readers an explicit choice via its [query types](/docs/table_types#query-types). *Snapshot queries* merge base files with delta logs at read time and see the freshest data. *Read-optimized queries* read only the compacted columnar base files, trading a bounded amount of freshness for pure-Parquet scan performance. Async compaction runs continuously in the background — scheduled and executed without pausing ingestion, coordinated through MVCC on the timeline — so the merge debt on any file group stays bounded. A compaction strategy that aggressively compacts recent partitions keeps read-optimized queries within a predictable freshness window.

Iceberg readers on an upsert stream pay read amplification from delete files: every scan of a data file must also apply the equality and position deletes that reference it, until a compaction rewrites the affected files. There is no built-in equivalent of the read-optimized query — the practical lever is running `rewrite_data_files` (and delete-file compaction) often enough that delete-file overhead stays tolerable. Format v3's deletion vectors improve the mechanics of position deletes considerably, but the operational shape is unchanged: read performance under continuous upserts is a function of how aggressively you schedule external maintenance. For append-only streams this section mostly doesn't apply to Iceberg — no deletes means no delete files — which is a genuine sweet spot for it.

## Multi-Writer and Table Services Concurrency

Streaming deployments rarely stay single-writer: a backfill job lands next to the live pipeline, or compaction runs as a separate job from ingestion.

Hudi's [concurrency control](/docs/concurrency_control) distinguishes writers from table services. A single writer with async compaction, clustering, and cleaning needs no external locks at all — MVCC coordinates ingestion and services in-process, which covers the most common streaming deployment with essentially zero concurrency configuration. For true multi-writer setups, Hudi offers file-level optimistic concurrency control, and — for streaming semantics — *non-blocking concurrency control* (NBCC), where multiple writers can write into the same file group simultaneously without aborting each other; conflicts are resolved by the reader and the compactor using commit-completion ordering, with a lock held only for the instant of timeline publication. NBCC currently applies to MOR tables with bucket indexes, and clustering concurrent with ingestion still uses OCC — real limitations worth knowing.

Iceberg uses optimistic concurrency for everything: each committer builds its snapshot, then attempts an atomic pointer swap through the catalog; on conflict it re-validates and retries. This is a clean, well-proven model, and for writers touching disjoint files retries are cheap metadata operations. The friction appears under sustained high commit rates: the ingestion job, the compaction job, and the snapshot-expiry job are all competing committers to the same table, and a long-running compaction commit can repeatedly lose races to a fast-committing stream — which is why compacting *hot* partitions of a streaming Iceberg table often needs partial-progress commits and careful scheduling. There is no built-in notion of a table service that is planned around the writer; every process is just another optimistic committer.

## Engine Integrations for Streaming

Both projects integrate with the major streaming engines, and it is worth being factual rather than dismissive on either side.

Iceberg has a mature Flink connector (checkpoint-aligned commits, upserts via equality deletes), a Spark Structured Streaming sink, and a Kafka Connect sink. For append-centric event pipelines at moderate checkpoint intervals, these work well.

Hudi's streaming surface is broader and more opinionated: the Flink writer with async compaction in the same job, Spark Structured Streaming, the Kafka Connect sink, and [Hudi Streamer](/docs/hoodie_streaming_ingestion) — a complete ingestion utility with continuous mode, Kafka/DFS/CDC sources, checkpoint management, and exactly-once delivery built in. The Flink integration in particular has seen sustained recent investment: Hudi 1.1 [rebuilt the Flink write path around native RowData](/blog/2025/12/10/apache-hudi-11-deep-dive-optimizing-streaming-ingestion-with-flink) to cut SerDe and buffer overhead, and Hudi 1.2 added [Record Level Index support for Flink](/blog/2026/06/10/stateless-global-upserts-for-flink-streaming-in-apache-hudi-1-2-0), moving global upsert indexing out of Flink state and into the table itself — so index state no longer grows with table cardinality or ties the index to one job's checkpoints. That last point is a good example of the design-center difference: Hudi treats the *indexing* problem of streaming upserts as the table's job, not the engine's.

## A Decision Framework

Setting aside benchmarks (run your own, on your workload), the architecture suggests a straightforward split:

- **Append-only event streams at moderate cadence** — clickstreams or logs committed every 10–15 minutes, no updates. Either format works; Iceberg is a perfectly good choice here, especially if your ecosystem is already Iceberg-centric. Budget for scheduled compaction and snapshot expiry, and size checkpoint intervals with metadata growth in mind.
- **Upsert-heavy streams (CDC, mutable entities)** — this is where the write-path difference compounds. Hudi's indexed log-append writes, delete handling, and async compaction were built for exactly this pattern.
- **Sub-10-minute freshness targets** — minute-level commit cadence plays to Hudi's bounded timeline and mutation-friendly metadata table; on Iceberg it demands a proportionally aggressive maintenance regimen.
- **Hands-off operations** — if no team will babysit compaction schedules, Hudi's self-managing posture (automatic file sizing at write time, async table services inside the writer process) removes a whole category of orchestration.

## It's Not Either/Or: Interoperability via Apache XTable

The comparison above frames a *writer-side* decision — and it does not have to constrain your readers. [Apache XTable](https://xtable.apache.org) (incubating) translates table metadata between Hudi, Iceberg, and Delta Lake without copying or rewriting data files. A common pattern is to ingest with Hudi — taking the streaming-native write path, indexing, and async table services — and expose the same data as an Iceberg table to catalogs and engines that expect Iceberg, with metadata kept in sync incrementally. We cover this pattern end to end in the companion post on [using Hudi with Apache Iceberg via XTable](/blog/2026/07/28/using-hudi-with-apache-iceberg-via-xtable). If your organization has standardized on Iceberg for consumption, that standard is compatible with choosing Hudi for ingestion.

## Conclusion

Iceberg and Hudi are both capable, openly governed table formats, and both can sit at the end of a streaming pipeline. The difference is what each was designed to assume. Iceberg assumes commits are relatively infrequent and maintenance runs between them; streaming is supported, and works, but small files, delete files, and snapshot metadata become an operational program you run alongside the pipeline. Hudi assumes the writers never stop: MOR log appends keep update costs proportional to change volume, file sizing happens at write time, the timeline and metadata table are built to absorb minute-level commits indefinitely, and compaction, clustering, and multi-writer coordination are designed to proceed *concurrently* with ingestion rather than around it. For append-only streams at relaxed cadence, use whichever fits your stack. For update-heavy, freshness-sensitive, continuously written tables, Hudi's architecture does the streaming work for you — and with XTable, you can make that choice without giving up Iceberg-based consumers.

## FAQ

<PostFAQ heading={null} items={[
  {question: 'Can Apache Iceberg handle streaming writes?', answer: 'Yes. Iceberg has a mature Flink connector, a Spark Structured Streaming sink, and a Kafka Connect sink, and streaming appends work well at moderate commit cadence. The caveat is operational: every commit creates a new snapshot with new metadata and manifest files, and upserts add delete files that readers must merge on every scan, so sustained high-frequency streaming requires regularly scheduled compaction, manifest rewriting, and snapshot expiry jobs to keep the table healthy.'},
  {question: 'Why does Apache Hudi handle small files better than Iceberg?', answer: 'Hudi addresses small files at write time rather than after the fact. The writer automatically bin-packs new inserts against existing under-sized file groups, so every commit works toward well-sized files, and updates are appended to log files within existing file groups instead of creating new small files. Iceberg writes new immutable files on every commit and relies on a separately scheduled rewrite_data_files procedure to merge small files afterward.'},
  {question: 'Which table format works best with Apache Flink for streaming ingestion?', answer: 'Both have solid Flink connectors, but Hudi offers a deeper streaming integration: async compaction runs inside the same Flink job, recent releases rebuilt the write path around Flink-native row types to cut serialization overhead, and Hudi 1.2 added Record Level Index support so global upsert lookups use the table-backed index instead of large Flink keyed state. Iceberg Flink pipelines handle appends well, while upserts rely on equality delete files that must be compacted by external jobs.'},
  {question: 'Does frequent committing hurt Iceberg tables?', answer: 'Frequent commits are safe for correctness but costly operationally. Each Iceberg commit produces a new metadata file, manifest list, and manifests, so minute-level cadence yields well over a thousand snapshots per day whose metadata accumulates until snapshot expiry runs, and concurrent committers contend on the catalog pointer swap. This is why Iceberg streaming guidance typically recommends longer checkpoint intervals, whereas Hudi bounds its active timeline through built-in archival and absorbs frequent commits by design.'},
  {question: 'Can I use Hudi for ingestion and still serve Iceberg readers?', answer: 'Yes. Apache XTable (incubating) translates table metadata between Hudi, Iceberg, and Delta Lake without copying or rewriting data files, either as a one-time conversion or as continuous incremental sync. A common pattern is to ingest with Hudi to get the streaming-native write path and async table services, then expose the same data files as an Iceberg table to catalogs and engines that expect Iceberg.'},
]} />
