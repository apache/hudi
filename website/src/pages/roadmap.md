---
title: Roadmap
last_modified_at: 2019-12-30T15:59:57-04:00
---
# Roadmap

Hudi community strives to deliver major releases every 3-4 months, while offering minor releases every other month!
This page captures the forward-looking roadmap of ongoing & upcoming projects and when they are expected to land, broken
down by areas on our [stack](blog/2021/07/21/streaming-data-lake-platform/#hudi-stack).

## Future Releases

Next upcoming release : [0.15.0](https://issues.apache.org/jira/projects/HUDI/versions/12353381) (May 2024)


## Transactional Database Layer

| Feature                                                         | Target Release  | Tracking                                                                                                                                                                        |
|-----------------------------------------------------------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Support for primary key-less table                              | 0.14.0          | [HUDI-4699](https://issues.apache.org/jira/browse/HUDI-4699)                                                                                                                    |
| Efficient bootstrap and migration of existing non-Hudi dataset  | 0.14.0          | [HUDI-1265](https://issues.apache.org/jira/browse/HUDI-1265)                                                                                                                    |
| 1.x Storage format                                              | 1.0.0           | [HUDI-6242](https://issues.apache.org/jira/browse/HUDI-6242)                                                                                                                    |
| Writer performance improvements                                 | 1.0.0           | [HUDI-3249](https://issues.apache.org/jira/browse/HUDI-3249)                                                                                                                    |
| Non-blocking concurrency control                                | 1.0.0           | [HUDI-3187](https://issues.apache.org/jira/browse/HUDI-3187), [HUDI-1042](https://issues.apache.org/jira/browse/HUDI-1042), [RFC-66](https://github.com/apache/hudi/pull/7907)  |
| Time Travel updates, deletes                                    | 1.0.0           |                                                                                                                                                                                 |
| General purpose support for multi-table transactions            | 1.0.0           |                                                                                                                                                                                 |
| A more effective HoodieMergeHandler for COW table with parquet  | 1.0.0           | [RFC-68](https://github.com/apache/hudi/blob/f1afb1bf04abdc94a26d61dc302f36ec2bbeb15b/rfc/rfc-68/rfc-68.md)                                                                     |
| Secondary indexes to improve query performance                  | 1.0.0           | [RFC-52](https://github.com/apache/hudi/pull/5370), [HUDI-3907](https://issues.apache.org/jira/browse/HUDI-3907)                                                                |
| Index Function for Optimizing Query Performance                 | 1.0.0           | [RFC-63](https://github.com/apache/hudi/pull/7235), [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)                                                                  |
| Logical partitioning via indexing                               | 1.0.0           | [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)                                                                                                                      |
| Streaming CDC/Incremental read improvement                      | 1.0.0           | [HUDI-2749](https://issues.apache.org/jira/browse/HUDI-2749)                                                                                                                    |
| Supervised table service planning and execution                 | 1.1.0           | [RFC-43](https://github.com/apache/hudi/pull/4309), [HUDI-4147](https://issues.apache.org/jira/browse/HUDI-4147)                                                                |


## Programming APIs

| Feature                                                 | Target Release | Tracking                                                                                                                   |
|---------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------|
| APIs/Abstractions, Record mergers                       | 1.0.0          | [HUDI-6243](https://issues.apache.org/jira/browse/HUDI-6243), [HUDI-3217](https://issues.apache.org/jira/browse/HUDI-3217) |
| New Hudi Table Format APIs for Query Integrations       | 1.0.0          | [RFC-64](https://github.com/apache/hudi/pull/7080), [HUDI-4141](https://issues.apache.org/jira/browse/HUDI-4141)           |
| Snapshot view management                                | 1.0.0          | [RFC-61](https://github.com/apache/hudi/pull/6576), [HUDI-4677](https://issues.apache.org/jira/browse/HUDI-4677)           |
| Optimized storage layout for cloud object stores        | 1.0.0          | [RFC-60](https://github.com/apache/hudi/pull/5113), [HUDI-3625](https://issues.apache.org/jira/browse/HUDI-3625)           |
| Support of verification with multiple event_time fields | 1.0.0          | [RFC-59](https://github.com/apache/hudi/pull/6382), [HUDI-4569](https://issues.apache.org/jira/browse/HUDI-4569)           |


## Execution Engine Integration

| Feature                                                                                        | Target Release | Tracking                                                                                                                                                                                 |
|------------------------------------------------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Integrate column stats index with all query engines                                            | 1.0.0          | [RFC-58](https://github.com/apache/hudi/pull/6345), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552)                                                                         |
| Performance for Spark-SQL writes                                                               | 0.14.0         | [HUDI-6315](https://issues.apache.org/jira/browse/HUDI-6315), [HUDI-6376](https://issues.apache.org/jira/browse/HUDI-6376)                                                               |
| Presto/Trino queries with new format                                                           | 1.0.0          | [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4394](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552) |
| Materialized Views with incremental updates using Flink                                        | 1.0.0          |                                                                                                                                                                                          |
| Explore other execution engines/runtimes (Ray, native Rust, Python)                            | 1.0.0          |                                                                                                                                                                                          |
| Spark datasource V2 read                                                                       | 1.1.0          | [HUDI-4449](https://issues.apache.org/jira/browse/HUDI-4449)                                                                                                                             |


## Platform Services

| Feature                                                                                             | Target Release | Tracking                                                                                                                                                                                           |
|-----------------------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hudi Reverse streamer                                                                               | 1.0.0          | [RFC-70](https://github.com/apache/hudi/pull/9040)                                                                                                                                                 | 
| Partition TTL management                                                                            | 1.1.0          | [RFC-65](https://github.com/apache/hudi/pull/8062)                                                                                                                                                 |
| Diagnostic Reporter                                                                                 | 1.1.0          | [RFC-62](https://github.com/apache/hudi/pull/6600)                                                                                                                                                 |
| Hudi integration with Snowflake                                                                     | 1.1.0          | [RFC-41](https://github.com/apache/hudi/pull/4074), [HUDI-2832](https://issues.apache.org/jira/browse/HUDI-2832)                                                                                   |
| Support for reliable, event based ingestion from cloud stores - GCS, Azure and the others           | 1.1.0          | [HUDI-1896](https://issues.apache.org/jira/browse/HUDI-1896)                                                                                                                                       |
| Mutable, Transactional caching for Hudi Tables (could be accelerated based on community feedback)   | 1.1.0          | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5), [HUDI-6489](https://issues.apache.org/jira/browse/HUDI-6489) |
