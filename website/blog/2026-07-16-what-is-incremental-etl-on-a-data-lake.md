---
title: "What is Incremental ETL on a Data Lake?"
excerpt: "Incremental ETL is a pipeline pattern that processes only the data that changed since the last run. This post explains how it works, what the storage layer must provide, and how it compares to batch and stream processing."
description: "Incremental ETL processes only changed data instead of recomputing entire tables. Learn how it works on a data lake and how Apache Hudi enables it."
authors: [vinoth-chandar]
category: how-to
image: /assets/images/blog/incr-processing/image7.png
tags:
- incremental processing
- etl
- streaming
- data lakehouse
- cdc
---

Incremental ETL is a pipeline pattern that processes only the data that changed since the last run, instead of recomputing entire tables or partitions on every execution. Each run of an incremental pipeline consumes a delta — the records inserted, updated, or deleted after a known checkpoint — applies its transformation logic to just that delta, and merges the results into the target table. The pipeline's work is proportional to how much the data changed, not how large the tables have grown.

That distinction matters because most data pipelines today still run as full-recompute batch jobs: every night (or every hour), they re-scan source tables end to end and rewrite the derived tables downstream, even when only a tiny fraction of the rows actually changed. As data volumes grow, that recomputation gets slower and more expensive at exactly the moment businesses are asking for fresher data.

Incremental ETL occupies the middle ground between classic batch processing and full stream processing. It keeps the familiar economics and tooling of batch — scheduled jobs, SQL or DataFrame transformations, cheap [data lakehouse](/blog/2024/07/11/what-is-a-data-lakehouse) storage — while borrowing the core idea of stream processing: treat data as a continuous flow of changes, and process each change roughly once.

## The Problem with Full Recomputation

Consider a typical medallion-style warehouse or lakehouse: raw data lands in bronze tables, gets cleaned into silver tables, and is aggregated into gold tables for reporting. In a full-recompute design, each layer is rebuilt by scanning its inputs wholesale. Three problems compound as the tables grow:

- **Cost.** A daily job that scans a multi-terabyte fact table to pick up one day's worth of changes does orders of magnitude more I/O and compute than the size of the change justifies. Joins are the worst offenders: rewriting a large joined table means re-shuffling both sides in full, every run. Organizations routinely spend a large share of their data platform budget on pipelines that mostly reprocess data they already processed yesterday.

- **Latency.** If rebuilding a table takes four hours, the data in it can never be fresher than four hours plus the scheduling interval. Teams respond by running the job less often (making data staler) or throwing more compute at it (making it more expensive). Neither addresses the root cause: the work grows with table size rather than change size.

- **Missed SLAs and cascading delays.** Derived pipelines form DAGs. When an upstream full-recompute job runs long, every downstream job either starts late or reads stale inputs. Late-arriving data makes this worse: when records for an old partition show up, the whole partition — and every downstream partition derived from it — must be recomputed, silently blowing through freshness SLAs.

Stream processing solves the latency problem but replaces batch's cost profile with a different one: always-on clusters, state stores to manage, and a second storage system (typically a log store like Kafka) holding another copy of the data. For the large class of pipelines where "fresh within a few minutes" is sufficient, that operational overhead buys latency headroom no one uses.

## What Makes ETL "Incremental"

An ETL pipeline is incremental when both its input and its output are expressed as changes:

1. **Change streams in.** Instead of scanning the full source table, the pipeline asks the storage layer a question a plain file listing cannot answer: *"give me all records that changed since commit X."* The result is a delta — new inserts, the latest values of updated records, and deletes — along with a new checkpoint to resume from next run.

2. **Incremental writes out.** Instead of rewriting the target table, the pipeline applies its transformed delta using record-level operations — upserts and deletes — so only the affected file groups are rewritten. Crucially, this makes the *output* table itself an incremental source: the next pipeline in the DAG can consume its changes the same way.

Because deltas flow in and deltas flow out, the pattern composes. A chain of incremental pipelines behaves like a topology of stream processors — each stage consuming a change stream and emitting one — except the "streams" are ordinary tables on cheap cloud storage, queryable by any engine at any time. This is the [duality between change logs and tables](/blog/2020/08/18/hudi-incremental-processing-on-data-lakes) that stream processing literature has long described, realized directly on the data lake.

The efficiency win is easy to see with numbers. If a 10 TB fact table receives 50 GB of changes per hour, an hourly incremental pipeline processes 50 GB per run instead of 10 TB — a 200x reduction in data scanned, while *improving* freshness from daily to hourly.

## What the Storage Layer Must Provide

Incremental ETL is not primarily a compute-engine feature; it is a storage contract. Plain files in cloud storage — even columnar formats like Parquet — cannot tell a consumer what changed, and cannot accept record-level updates without rewriting whole files or partitions. Open table formats close this gap to varying degrees. Concretely, the storage layer must provide three primitives:

- **Record-level change retrieval.** The table must be able to answer "what changed between time T1 and T2" efficiently, without scanning everything. This comes in two flavors: *incremental queries*, which return the latest value of each changed record (ideal for driving downstream upserts), and *CDC queries*, which return before/after images of every change (needed when the transformation must see each individual mutation). Apache Hudi supports [both query types](/docs/sql_queries#incremental-query) on Copy-on-Write and Merge-on-Read tables.

- **Upserts and deletes to apply changes.** Producing a delta is only useful if the next table can absorb it cheaply. That requires primary keys and indexes to locate which files contain the affected records, so a small change touches a small amount of storage. Hudi's [write operations](/docs/write_operations) are built around indexed upserts and deletes, with merge modes to handle out-of-order and late-arriving data correctly.

- **Ordering and commit points.** Consumers need a reliable notion of "where I left off." Hudi's [timeline](/docs/timeline) records every action on the table as an atomically committed instant; incremental queries use commit completion times to define exact, repeatable change windows, and checkpoints are managed automatically by tools like Hudi Streamer's incremental sources. Hudi additionally tracks record-level metadata (commit time and sequence number on every record), so change history survives background operations like compaction and clustering rather than depending on which snapshots happen to be retained.

These primitives — upsert and incremental consumption — are exactly the pair that defines the pattern: with them, you can maintain one table from a change stream, then derive another table from *its* change stream, end to end. The [use cases documentation](/docs/use_cases) covers how this plays out across ingestion, warehouse offload, and derived pipelines.

### Frequent Table Maintenance Becomes the Norm

There is a fourth requirement that follows directly from the first three: the format must be designed for frequent table maintenance, because incremental processing fundamentally increases how often the table is written. A table that once absorbed one nightly batch now commits every few minutes — producing small files, accumulating deltas in log files, and continuously churning the storage layout. At that write frequency, maintenance work — [compaction](/docs/compaction), clustering, cleaning, file sizing — stops being an occasional chore and becomes the norm, running constantly alongside ingestion.

And it must run *without blocking the writers it serves*. If compacting or clustering a table means pausing ingestion, or if maintenance jobs contend with writers as just another optimistic transaction — conflicting and forcing retries on exactly the hot file groups that change most — then every maintenance pass reintroduces the latency and cost the pipeline went incremental to eliminate. Hudi builds table services into the storage engine for this reason: compaction, clustering, cleaning, and indexing all ship built-in and run [asynchronously](/docs/compaction) — in the same process as the writer or as separate jobs — coordinated through MVCC and [non-blocking concurrency control](/docs/concurrency_control) so writers keep committing while maintenance proceeds in the background.

## Incremental ETL vs. Stream Processing

Incremental ETL and stream processing share semantics but differ sharply in infrastructure and economics:

| | Stream processing (Flink, Kafka Streams) | Incremental ETL (Hudi on a data lake) |
|---|---|---|
| Latency | Sub-second to seconds | Minutes |
| Compute model | Always-on jobs with managed state | Scheduled or continuous mini-batch jobs; state lives in tables |
| Storage | Event log (Kafka) + state stores + eventual sink | One copy: lakehouse tables on cloud object storage |
| Reprocessing/backfill | Replay the log, rebuild state | Re-run the job over a commit range or full table |
| Query access to intermediate results | Requires sinking to another system | Every intermediate table is directly queryable |

A useful way to summarize the trade: **incremental ETL delivers streaming semantics at minute-level batch economics.** If a use case genuinely needs sub-second reactions — fraud blocking, operational alerting — a stream processor is the right tool. But a large majority of analytics pipelines have freshness requirements in the minutes-to-hour range, where paying for always-on stream infrastructure and a second copy of the data buys nothing. The two also compose rather than compete: Flink writing into Hudi tables gives low-latency ingestion upstream, with cheaper incremental pipelines fanning out downstream.

## A Concrete Pipeline Walkthrough

Here is the pattern end to end, using a familiar CDC scenario: an operational database (say, PostgreSQL) whose changes must feed an analytics table.

**Step 1 — CDC into a bronze table.** A capture tool such as Debezium streams the database's change log into Kafka, and an ingestion job — for example [Hudi Streamer](/docs/hoodie_streaming_ingestion) — continuously upserts those changes into a bronze Hudi table. The bronze table is now a queryable, transactional mirror of the source, and every commit on its timeline is a consumable change point.

**Step 2 — incrementally read the bronze table.** The silver pipeline wakes up on its schedule (or runs continuously) and pulls only what changed since its last checkpoint:

```scala
// Read only the records that changed since the last run
val ordersDelta = spark.read.format("hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.read.begin.instanttime", lastCheckpoint).
  load(bronzeOrdersPath)
```

The same read is available in SQL through the `hudi_table_changes` table-valued function:

```sql
SELECT * FROM hudi_table_changes('bronze_orders', 'latest_state', '20260716083000000');
```

**Step 3 — transform and upsert into a silver table.** The transformation runs over just the delta, and the result is applied as an upsert keyed on the record key:

```scala
val silverDelta = ordersDelta.
  filter(col("order_status") =!= "CANCELLED").
  withColumn("order_total", col("quantity") * col("unit_price"))

silverDelta.write.format("hudi").
  option("hoodie.datasource.write.operation", "upsert").
  option("hoodie.table.name", "silver_orders").
  mode("append").
  save(silverOrdersPath)
```

The job then persists the new checkpoint (the latest commit completion time it consumed) for the next run. Because the silver table was written with upserts, it exposes its own change stream — a gold-layer aggregation job can consume `silver_orders` incrementally in exactly the same way, re-aggregating only the keys whose inputs changed.

![Incremental ETL: upserts into an upstream table, incrementally consumed to maintain a derived projected table](/assets/images/blog/incr-processing/image7.png)
<p align = "center">Figure: Changes are upserted into an upstream table and incrementally consumed to maintain a derived table</p>

One honest caveat: not every transformation is trivially incremental. Simple projections, filters, and enrichments map one-to-one onto deltas. Aggregations require merging new partial results into existing ones, and some full-table operations (percentiles, complex window functions) may still warrant periodic full recomputation. A pragmatic design runs the same logic incrementally for freshness and occasionally in full for correction — on the same tables and infrastructure.

## A Brief History: Pioneered at Uber

Incremental processing on the data lake is not a recent idea bolted onto table formats — it is the problem Hudi was created to solve. In 2016, Uber's data team faced exactly the trade-off described above: petabyte-scale Hadoop tables that could only be rewritten wholesale, and business use cases (like driver-partner incentives and trip pricing) that needed data in minutes, not hours. The case for a third processing style — incremental, on top of batch storage — was laid out in the O'Reilly article [The Case for Incremental Processing on Hadoop](/blog/2016/08/04/The-Case-for-incremental-processing-on-Hadoop), and Hudi (originally "Hoodie" — Hadoop Upserts, Deletes and Incrementals) was built at Uber to provide the two missing primitives: upserts and incremental consumption. The project entered the Apache Incubator in 2019 and became a top-level Apache project in 2020; a deeper treatment of the ideas appears in [Incremental Processing on the Data Lake](/blog/2020/08/18/hudi-incremental-processing-on-data-lakes). The name itself encodes the pattern — the "I" in Hudi stands for incrementals.

## Conclusion

Incremental ETL replaces "recompute everything, always" with "process what changed, continuously." It requires a storage layer that can serve record-level change streams, absorb upserts and deletes efficiently, and expose reliable commit points to checkpoint against — the primitives Apache Hudi has provided on data lakes since its inception. The payoff is pipelines whose cost tracks the rate of change in the business rather than the accumulated size of history, with data freshness measured in minutes on the same cheap, open storage batch jobs already use. For most analytics workloads sitting between nightly batch and hard real-time, it is the most economical point on the latency-cost curve.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'What is incremental ETL?',
    answer: 'Incremental ETL is a data pipeline pattern that processes only the records that were inserted, updated, or deleted since the pipeline\'s last run, instead of re-scanning and rewriting entire tables. Each run consumes a delta from a checkpoint, transforms it, and merges the result into the target table with upserts and deletes. This makes pipeline cost proportional to the volume of change rather than total table size.'
  },
  {
    question: 'What is the difference between incremental ETL and streaming?',
    answer: 'Both process data as a flow of changes, but stream processors like Apache Flink run always-on jobs with managed state and typically require a separate event log such as Kafka, delivering sub-second latency. Incremental ETL runs as scheduled or continuous mini-batch jobs directly against lakehouse tables on cloud storage, delivering minute-level freshness at much lower infrastructure cost. A common summary is that incremental ETL provides streaming semantics at batch economics.'
  },
  {
    question: 'How does incremental ETL reduce cost?',
    answer: 'It eliminates repeated scans and rewrites of data that did not change. If a large table receives a small volume of changes between runs, an incremental pipeline reads and processes only that delta instead of the whole table, often reducing data scanned per run by orders of magnitude. Compute clusters also only run for the duration of each small job rather than staying on permanently.'
  },
  {
    question: 'What does the storage layer need to support incremental ETL?',
    answer: 'Four things: the ability to efficiently retrieve record-level changes between two points in time, support for upserts and deletes so downstream tables can absorb deltas cheaply, an ordered log of commits that consumers can checkpoint against, and table maintenance that runs without blocking writers — because incremental processing multiplies write frequency, compaction, clustering, and cleaning become continuous background work rather than occasional jobs. Plain Parquet files on object storage provide none of these, which is why incremental ETL depends on a transactional table format. Apache Hudi provides all four through its timeline, indexes, incremental and CDC queries, and built-in asynchronous table services.'
  },
  {
    question: 'Can every transformation be made incremental?',
    answer: 'No. Projections, filters, enrichments, and key-based joins map naturally onto change streams, and aggregations can be maintained by merging partial results for the affected keys. Some computations, like exact percentiles or complex full-table window functions, are hard to update incrementally, so teams often run those periodically in full while keeping the rest of the pipeline incremental.'
  },
  {
    question: 'Which Apache Hudi features enable incremental ETL?',
    answer: 'Hudi provides incremental queries that return the latest values of changed records after a given commit, CDC queries that return before and after images of each change, and indexed upsert and delete write operations to apply deltas downstream. Its timeline records every commit with completion times that define exact change windows, and record-level metadata preserves change history across compaction and clustering. Ingestion tools like Hudi Streamer manage incremental checkpoints automatically.'
  }
]} />
