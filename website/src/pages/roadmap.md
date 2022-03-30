---
title: Roadmap
last_modified_at: 2019-12-30T15:59:57-04:00
---
# Roadmap

Hudi community strives to deliver major releases every 2-3 months, while offering minor releases every month!
This page captures the forward-looking roadmap of ongoing & upcoming projects and when they are expected to land, broken
down by areas on our [stack](blog/2021/07/21/streaming-data-lake-platform/#hudi-stack).

## H1 2022 Releases

Next major release : [0.11.0](https://issues.apache.org/jira/projects/HUDI/versions/12350673) (Apr 2022)

|Release|Timeline|
|------------|--------|
|[0.10.1](https://issues.apache.org/jira/projects/HUDI/versions/12351135)|Jan 2022|
|[0.11.0](https://issues.apache.org/jira/projects/HUDI/versions/12350673)|Apr 2022|
|[0.12.0](https://issues.apache.org/jira/projects/HUDI/versions/12351209)|Jun 2022|
|[1.0.0](https://issues.apache.org/jira/projects/HUDI/versions/12351210)|Summer 2022|

## Transactions/Database Layer

|Feature|Target Release|Tracking|
|------------|--------|-----------|
|Space-filling curves hardening & perf improvements |0.11|[HUDI-2100](https://issues.apache.org/jira/browse/HUDI-2100)|
|Metadata table update via multi-table transactions, turned on by default |0.11|[HUDI-1292](https://issues.apache.org/jira/browse/HUDI-1292)|
|Metadata Index, as a bloom index alternative, fetching col_stats and bloom_filters from metadata table, improving upsert performance. |0.11|[HUDI-1822](https://issues.apache.org/jira/browse/HUDI-1822), [RFC-37](https://github.com/apache/hudi/pull/3989)|
|Support for Encryption |0.11|[HUDI-2370](https://issues.apache.org/jira/browse/HUDI-2370)|
|Schema-on-read for non-backwards compatible schema evolution |0.11|[HUDI-2429](https://issues.apache.org/jira/browse/HUDI-2429)|
|Improvements to merge-on-read log merging/reading with streaming semantics |0.11|[HUDI-3081](https://issues.apache.org/jira/browse/HUDI-3081)|
|Indexed columns support & elimination of partitioning |0.11|[HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)|
|Record-level index to speed up uuid based upserts/deletes |0.12|[HUDI-53](https://issues.apache.org/jira/browse/HUDI-53)|
|Eager conflict detection for Optimistic Concurrency Control |0.12|[HUDI-1575](https://issues.apache.org/jira/browse/HUDI-1575)|
|Indexed timeline and infinite retention of versions |0.12|RFC coming soon|
|Improvements to streaming read and full CDC data model support |0.12| [HUDI-2749](https://issues.apache.org/jira/browse/HUDI-2749), RFC coming soon|
|Consistent hashing based file distribution over storage to overcome throttling issues for very large tables |0.12|RFC published soon|
|Lock free concurrency control |0.12 -> 1.0.0|[HUDI-3187](https://issues.apache.org/jira/browse/HUDI-3187)|
|Non-blocking/Lock-free updates during clustering |0.12 -> 1.0.0|[HUDI-1042](https://issues.apache.org/jira/browse/HUDI-1042)|
|Time Travel updates, deletes |0.12 -> 1.0.0 ||
|General purpose support for multi-table transactions |0.12 -> 1.0.0||

## Execution Engine Integration

|Feature|Target Release|Tracking|
|------------|--------|-----------|
|Spark SQL DML fixes & enhancements |0.11|[HUDI-1658](https://issues.apache.org/jira/browse/HUDI-1658)|
|Data-skipping for Hive and Spark based on col_stats from metadata table  |0.11|[HUDI-1296](https://issues.apache.org/jira/browse/HUDI-1296), [RFC-27](https://github.com/apache/hudi/pull/4280)|
|Non-keyed tables with updates and deletes |0.11|[HUDI-2968](https://issues.apache.org/jira/browse/HUDI-2968)|
|Trino Connector for Hudi, with read/query support  |0.12|[HUDI-2687](https://issues.apache.org/jira/browse/HUDI-2687), [RFC-38](https://github.com/apache/hudi/pull/3964)|
|Spark Datasource V2|0.12|[HUDI-1297](https://issues.apache.org/jira/browse/HUDI-1297) ,[HUDI-2531](https://issues.apache.org/jira/browse/HUDI-2531)|
|Complete ORC Support across query engines |0.12|[HUDI-57](https://issues.apache.org/jira/browse/HUDI-57)|
|Presto Connector for Hudi, with read/query support |0.12|[PRESTO-17006](https://github.com/prestodb/presto/issues/17006)|
|Multi-Modal indexing full integration across Presto/Trino/Spark queries |0.12 -> 1.0.0|[HUDI-1822](https://issues.apache.org/jira/browse/HUDI-1822)|
|Materialized Views with incremental updates using Flink |1.0.0||
|SQL DML support for Presto/Trino connectors (could be accelerated based on community feedback) |1.0.0||
|Explore other execution engines/runtimes (Ray, native Rust, Python) |1.0.0||

## Platform Services

|Feature|Target Release|Tracking|
|------------|--------|-----------|
|Native support for AWS Glue Metastore   |0.11|[HUDI-2757](https://issues.apache.org/jira/browse/HUDI-2757)|
|BigQuery and Snowflake external table integration   |0.12|[RFC-34](https://github.com/apache/hudi/pull/4503)|
|JDBC Incremental Source GA   |0.12|[HUDI-1859](https://issues.apache.org/jira/browse/HUDI-1859)|
|Mutable, CDC Stream support for Kafka Connect Sink   |0.12|[HUDI-2324](https://issues.apache.org/jira/browse/HUDI-2324)|
|Airbyte integration   |0.12|RFC coming soon|
|Apache Pulsar integration for Delta Streamer (blocked on upstream)   |0.12|[HUDI-246](https://issues.apache.org/jira/browse/HUDI-246)|
|Kinesis deltastreamer source, with DynamoDB CDC   |0.12|[HUDI-1386](https://issues.apache.org/jira/browse/HUDI-1386), [HUDI-310](https://issues.apache.org/jira/browse/HUDI-310)|
|Support for reliable, event based ingestion from cloud stores - GCS, Azure and the others   |1.0.0|[HUDI-1896](https://issues.apache.org/jira/browse/HUDI-1896)|
|Hudi Timeline Metaserver for locks, column status and table listings (could be accelerated based on community feedback)   |1.0.0|[Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_0) |
|Mutable, Transactional caching for Hudi Tables   (could be accelerated based on community feedback) |1.0.0|[Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5)|
