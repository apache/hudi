---
title: "Point Lookups on the Lakehouse: How Hudi Indexes Accelerate Read-Heavy Workloads"
excerpt: "Partition pruning and min/max file statistics stop helping when queries filter on high-cardinality columns. Hudi's record-level, secondary, and expression indexes prune point lookups down to the handful of files that actually contain matching rows."
description: "How Apache Hudi's record-level, secondary, and expression indexes prune files at query planning time to accelerate point lookups and selective reads."
authors: [sivabalan]
category: deep-dive
image: /assets/images/blog/2024-03-30-record-level-indexing-apache-hudi-delivers-70-faster-point.png
tags:
- indexing
- performance
- querying
- data skipping
---

Analytical scans are not the only workload a lakehouse table serves. In many production query mixes, a large share of queries are needle-in-haystack reads: fetch one order by `order_id`, pull a user's profile by `user_id`, trace a request by `uuid`, list all events for one `customer_id`. These queries touch a few rows out of billions — and they are exactly where the lake's two standard pruning tools, partition pruning and min/max file statistics, stop helping. Apache Hudi answers this with queryable indexes maintained inside its [metadata table](/docs/metadata): a record-level index that maps each record key to its file group, secondary indexes that map non-key column values to record keys, expression indexes over transformed columns, and centrally stored bloom filters. At query planning time, these indexes prune the scan down to the few files that actually contain matching rows. The effect is not subtle: on a 400 GB table with 20,000 file groups, a record-key lookup published in [Hudi's indexing deep-dive series](/blog/2025/11/12/deep-dive-into-hudis-indexing-subsystem-part-2-of-2) dropped from 977 seconds to 12 seconds once the record index was in play.

This post is the read-side companion to our [write-heavy comparison](/blog/2026/08/12/hudi-vs-delta-lake-for-write-heavy-workloads). Same approach: mechanisms first, and only benchmark numbers that have already been published, with dates and sources.

## Why Selective Queries Are Hard on a Data Lake

Lakehouse tables are laid out for scans: large immutable columnar files, grouped into partitions, described by file-level statistics. Every standard read optimization prunes at one of those granularities, and each one has a blind spot for selective predicates on high-cardinality columns:

- **Partition pruning** only helps if the filter column is the partition column. Nobody partitions by `user_id` or `uuid` — the cardinality is far too high — so a point lookup on such a column matches *every* partition.
- **Min/max file statistics** help when values correlate with file layout. A filter on an ingestion timestamp prunes beautifully, because each file covers a narrow time range. But a random key like a UUID is uniformly spread: every file's min/max range spans nearly the whole keyspace, so every file "might" contain the value and nothing is pruned. Sorting or clustering the data can rescue statistics for *one* column, but a table can only be physically ordered one way.
- **Parquet footer bloom filters and page indexes** operate per file — the engine still has to open every candidate file to consult them, which at thousands of files is itself the bottleneck.

The result is familiar to anyone who has run `SELECT * FROM events WHERE request_id = '...'` on a large table: a full scan of the key column across the table, minutes of compute, and (on scan-priced engines) a bill proportional to table size rather than result size. What the query needed was a database-style answer to "which files contain this value?" — an index.

## How Hudi Indexes Serve Reads

Hudi has maintained indexes since its inception, originally to make upserts and deletes fast — the write side of the same problem, as covered in the [write-heavy post](/blog/2026/08/12/hudi-vs-delta-lake-for-write-heavy-workloads). With the [multi-modal indexing subsystem](/docs/indexes#multi-modal-indexing), those same structures are consulted at *query planning time*. The indexes live as partitions of Hudi's metadata table — itself a Merge-on-Read Hudi table using an HFile format optimized for point lookups — and are updated transactionally with every commit, so index results are always consistent with the data. The read-relevant ones:

- **Record-level index (RLI)** — an exact mapping from record key to file location, hash-sharded across file groups to scale to very large keyspaces. A query with an equality predicate on the record key (`WHERE uuid = '...'`) resolves directly to the file group holding that key; only that file is scanned.
- **Secondary index** — introduced in [Hudi 1.0](/blog/2025/04/02/secondary-index), an index on any non-key column. It maps secondary key values (e.g., `city`, `driver`, `customer_id`) to the record keys that carry them; the record index then maps those keys to file locations. Equality and `IN` predicates on indexed columns prune to exactly the files containing matches.
- **Expression index** — an index on a *function* of a column, in two flavors: column-stats over transformed values (e.g., `from_unixtime(ts)` for date filters on epoch columns) and bloom filters over transformed values for equality matching on high-cardinality columns.
- **Bloom filter index** — bloom filters for all data files stored centrally in the metadata table, so candidate files can be eliminated without touching each file's footer.
- **Column stats and partition stats indexes** — the min/max statistics story, but stored in the scalable metadata table and usable for range predicates and partition-level skipping.

The planning flow for a secondary-index lookup, as described in the [indexing deep dive](/blog/2025/11/12/deep-dive-into-hudis-indexing-subsystem-part-2-of-2): the engine pushes the equality predicate down to Hudi's integration layer, the secondary index returns the matching record keys, the record index returns the enclosing file locations, and the engine plans a scan over just those files. Two point lookups against compact metadata replace a scan over the table — the same shape a database index lookup takes, running over lake storage.

This combination of write-side and read-side indexing is one of the [things that distinguish Hudi architecturally](/blog/2025/03/05/hudi-21-unique-differentiators): the storage format deliberately spends extra space on indexes to serve both record-level mutation and selective reads, rather than optimizing for vanilla scans alone.

## Using It from Spark SQL

Index-accelerated reads are plain SQL. Create a table with the record index enabled (secondary indexes require it, along with a primary key and the `COMMIT_TIME_ORDERING` merge mode), then create indexes with `CREATE INDEX`:

```sql
CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING hudi
OPTIONS (
    primaryKey = 'uuid',
    hoodie.metadata.record.index.enable = 'true',
    hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING'
)
PARTITIONED BY (city);

-- record index first; secondary indexes build on it
CREATE INDEX record_index ON hudi_table (uuid);
-- secondary index on a non-key, high-cardinality column
CREATE INDEX idx_rider ON hudi_table (rider);
```

Queries need no hints — equality predicates on indexed columns are pruned automatically during planning:

```sql
-- point lookup on the record key, served by the record-level index
SELECT * FROM hudi_table
WHERE uuid = 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa';

-- selective filter on a non-key column, served by the secondary index
SELECT * FROM hudi_table WHERE rider = 'rider-B';
```

In the walkthrough in the [SQL queries documentation](/docs/sql_queries), the second query scans one file instead of three after the index is created — on the toy table that is the whole point demonstrated; on production tables the pruning ratio scales with file count. Expression indexes cover predicates with inline transformations, and bloom-filter expression indexes handle equality matching where an exact mapping would be overkill:

```sql
-- date filters on an epoch column
CREATE INDEX idx_column_ts ON hudi_table
  USING column_stats(ts) OPTIONS(expr='from_unixtime', format='yyyy-MM-dd');

-- bloom-filter pruning for equality predicates on driver
CREATE INDEX idx_bloom_driver ON hudi_table
  USING bloom_filters(driver) OPTIONS(expr='identity');
```

`SHOW INDEXES FROM hudi_table` lists what exists, `DROP INDEX` removes one, and session settings such as `SET hoodie.metadata.record.index.enable=true` and `SET hoodie.metadata.column.stats.enable=true` control which indexes the reader consults — see [SQL queries](/docs/sql_queries) for the full set.

## What About Trino?

Honestly stated: index-based pruning through the record-level, secondary, and expression indexes is a **Spark SQL capability today**. When secondary indexes shipped in Hudi 1.0, [support was planned for Flink, Presto, and Trino](/blog/2025/04/02/secondary-index) in a subsequent release; that work rides on the fact that the indexes are engine-neutral storage structures — partitions of the metadata table on disk, not Spark-private state — so an engine integration implements the lookup against index data that is already there.

What Trino supports today, per the [query engine documentation](/docs/sql_queries#trino): Hudi tables are queried through the native Hudi connector (Trino 398 onward) or via the Hive connector with table redirection (Trino 411 onward, using `hive.hudi-catalog-name=hudi`). Both paths support snapshot queries on Copy-on-Write tables and read-optimized queries on Merge-on-Read tables, with MOR snapshot query support in progress in the Trino community. So a Trino-fronted deployment gets Hudi's transactional reads and columnar scan performance now, and a practical pattern for selective workloads is to route the highly selective lookups through Spark SQL (where index acceleration is live) while Trino serves the scan-shaped dashboards and ad-hoc analytics — converging on one engine story as connector-side index support lands.

## What Published Results Show

Two data points, both from Hudi's own published material, both with setups disclosed:

- **Record-level index** ([indexing deep dive, November 2025](/blog/2025/11/12/deep-dive-into-hudis-indexing-subsystem-part-2-of-2)): on a 400 GB synthetic Hudi table with 20,000 file groups, a query filtering on a single record key dropped from 977 seconds to 12 seconds — a 98% reduction — with the record index in use.
- **Secondary index** ([secondary index announcement, April 2025](/blog/2025/04/02/secondary-index)): on the TPC-DS 1 TB dataset (Hudi 1.0.1, Spark 3.5.5 on EMR, 10 executors), a join query with a customer-id lookup on `web_sales` ran ~33% faster on the first run and ~58% faster on a warm second run with a secondary index on `ws_ship_customer_sk`. Data scanned fell ~90% — from 67 GB across 5,000 files to 7 GB across 521 files, and from 719M rows scanned to 75M.

The scan reduction is the number to internalize: latency gains vary with cluster and cache state, but reading 90% fewer bytes is an architectural outcome, and on engines priced per byte scanned it translates directly to cost. For reproducing this class of measurement on your own keys and data distribution, the open-source [LakeLoader](https://github.com/onehouseinc/lake-loader) framework exists precisely to generate controlled, repeatable lakehouse workloads.

## How This Compares Architecturally

Lakehouse table formats broadly take one of two positions on selective reads. One position is *file-statistics-only pruning*: keep per-file min/max statistics (plus partition values) in table metadata, and make them effective by physically clustering data so that values correlate with files. This is metadata that is cheap to maintain and works well when queries filter on the clustering dimensions — but a table can only be clustered one way, and predicates on other high-cardinality columns degrade toward scanning the column across all files.

Hudi's position is *queryable index metadata*: spend additional storage and write-path work maintaining exact value-to-location mappings (record-level and secondary indexes) and auxiliary structures (bloom filters, expression indexes) in a scalable, transactionally-updated metadata table, so that pruning for equality predicates is an index lookup rather than a statistics estimate — on as many columns as you choose to index. The trade is explicit: index storage and maintenance cost in exchange for bounded lookup cost regardless of which column the predicate hits. For read-heavy workloads dominated by selective queries, that bound is what shows up in latency and scan bills; for purely scan-shaped workloads, statistics-based skipping (which Hudi also has, via column stats and partition stats) is sufficient for any format.

## Operational Notes

Two things keep this practical in production. First, indexes are maintained **transactionally with each commit** — a query planned against the secondary index sees results consistent with the latest completed write, not a lagging sidecar. Second, adding an index to a table that is already ingesting does not require stopping it: Hudi's [async indexing](/docs/metadata_indexing) builds a new index in the background while writers keep committing, then reconciles the seam — the mechanics and the architectural reasons it works are covered in [Building Indexes on a Moving Target](/blog/2026/06/25/building-indexes-on-a-moving-target). Start with the record index for key lookups, add secondary indexes for the handful of non-key columns that appear in selective predicates (each adds maintenance cost, so index high-value columns, not everything), and use expression indexes where predicates transform columns inline.

## Conclusion

Point lookups and selective filters on high-cardinality columns are a real, often dominant slice of production query traffic, and they are precisely the queries that partition pruning and min/max statistics cannot save. Hudi's answer is the one databases settled on decades ago — indexes — rebuilt for lake storage as transactional partitions of a scalable metadata table: a record-level index for key equality, secondary indexes for non-key columns, expression indexes for transformed predicates, and centrally stored bloom filters, all consulted at query planning to shrink the scan to files that matter. From Spark SQL this is live today with plain `CREATE INDEX` syntax; on Trino, Hudi serves fast transactional reads through its connector with index acceleration on the roadmap the community has published. Paired with the [write-side indexing story](/blog/2026/08/12/hudi-vs-delta-lake-for-write-heavy-workloads), the same investment — a queryable index subsystem — pays on both halves of the workload: writes that scale with the change, and reads that scale with the result.

## FAQ

<PostFAQ heading={null} items={[
  {
    question: 'Can you do point lookups on a data lake?',
    answer: 'Yes, with a table format that maintains indexes. On a plain columnar lake, a point lookup on a high-cardinality column degrades to scanning that column across all files, because partition pruning and min/max statistics cannot narrow random key values. Apache Hudi maintains a record-level index mapping each record key to its file location and secondary indexes for non-key columns inside its metadata table, so an equality predicate resolves to the few files containing matches. A published example showed a record-key lookup on a 400 GB table dropping from 977 seconds to 12 seconds.',
  },
  {
    question: 'What is a secondary index in Apache Hudi?',
    answer: 'A secondary index, introduced in Hudi 1.0, is an index on any column other than the record key. It stores mappings from secondary key values to record keys in the metadata table; at query time the matched record keys are resolved to file locations through the record-level index, and only those files are scanned. It is created with plain SQL, for example CREATE INDEX idx_city ON hudi_table(city), and requires the record index to be enabled on the table.',
  },
  {
    question: 'Does Trino use Hudi indexes?',
    answer: 'Not yet for record-level and secondary index pruning — that acceleration is available from Spark SQL today. Trino queries Hudi tables through the native Hudi connector or Hive connector redirection, supporting snapshot queries on Copy-on-Write tables and read-optimized queries on Merge-on-Read tables. Because Hudi\'s indexes are engine-neutral structures stored in the metadata table rather than Spark-private state, engine integrations can adopt them, and support for Presto, Trino, and Flink was announced as planned when secondary indexes shipped.',
  },
  {
    question: 'How much faster are queries with Hudi\'s indexes?',
    answer: 'Per Hudi\'s published measurements: a record-key lookup on a 400 GB synthetic table with 20,000 file groups fell from 977 seconds to 12 seconds with the record-level index (a 98% reduction), and a TPC-DS 1 TB join query with a customer-id filter ran about 33-58% faster with a secondary index while scanning roughly 90% less data — 7 GB across 521 files instead of 67 GB across 5,000 files. Actual gains depend on data distribution and cluster setup, so test with your own workload.',
  },
  {
    question: 'Does maintaining indexes for reads slow down ingestion?',
    answer: 'Indexes are updated transactionally with each commit, which adds bounded write-path work per index — one reason to index only the columns that appear in selective predicates. Adding a new index to a live table does not require stopping ingestion: Hudi\'s async indexing service builds the index in the background while writers keep committing, then reconciles concurrent changes, keeping the index timeline-consistent with the table.',
  },
]} />
