---
title: Roadmap
last_modified_at: 2019-12-30T15:59:57-04:00
---
# Roadmap

Hudi community strives to deliver major releases every 3-4 months, while offering minor releases every other month!
This page captures the forward-looking roadmap of ongoing & upcoming projects and when they are expected to land, broken
down by areas on our [stack](blog/2021/07/21/streaming-data-lake-platform/#hudi-stack).

## H2 2022 Releases

Next major release : [0.13.0](https://issues.apache.org/jira/projects/HUDI/versions/12352101) (Mid Q4 2022)

| Release                                                              | Timeline       |
|----------------------------------------------------------------------|----------------|
| [0.12.1](https://issues.apache.org/jira/projects/HUDI/versions/12352182) | Early Q4 2022  |
| [0.13.0](https://issues.apache.org/jira/projects/HUDI/versions/12352101) | Mid Q4 2022    |
| [0.13.1](https://issues.apache.org/jira/projects/HUDI/versions/12352250) | TBD            |

## Transactional Database Layer

| Feature                                                                                | Target Release | Tracking                                                                                                                                                                                                                                                 |
|----------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Metaserver for all metadata                                                            | 0.13.0         | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.ged084e5bf6_0_278), [RFC-36](https://github.com/apache/hudi/pull/4718), [HUDI-3345](https://issues.apache.org/jira/browse/HUDI-3345) |
| Supervised table service planning and execution                                        | 0.13.0         | [RFC-43](https://github.com/apache/hudi/pull/4309), [HUDI-4147](https://issues.apache.org/jira/browse/HUDI-4147)                                                                                                                                         |
| Support of Change Data Capture (CDC) with Hudi change logs                             | 0.13.0         | [RFC-51](https://github.com/apache/hudi/blob/master/rfc/rfc-51/rfc-51.md), [HUDI-3478](https://issues.apache.org/jira/browse/HUDI-3478)                                                                                                                  |
| Record-level index to speed up UUID-based upserts and deletes                          | 0.13.0         | [RFC-08](https://cwiki.apache.org/confluence/display/HUDI/RFC-08++Record+level+indexing+mechanisms+for+Hudi+datasets), [HUDI-53](https://issues.apache.org/jira/browse/HUDI-53)                                                                          |
| Consistent hashing index for dynamic buckets                                           | 0.13.0         | [RFC-42](https://github.com/apache/hudi/blob/master/rfc/rfc-42/rfc-42.md), [HUDI-3000](https://issues.apache.org/jira/browse/HUDI-3000)                                                                                                                  |
| Secondary index to improve query performance                                           | 0.13.0         | [RFC-52](https://github.com/apache/hudi/pull/5370), [HUDI-3907](https://issues.apache.org/jira/browse/HUDI-3907)                                                                                                                                         |
| Reducing write amplification with Log Compaction in MOR                                | 0.13.0         | [RFC-48](https://github.com/apache/hudi/pull/5041), [HUDI-3580](https://issues.apache.org/jira/browse/HUDI-3580)                                                                                                                                         |
| Eager conflict detection for Optimistic Concurrency Control                            | 0.13.0         | [RFC-56](https://github.com/apache/hudi/pull/6003), [HUDI-1575](https://issues.apache.org/jira/browse/HUDI-1575)                                                                                                                                         |
| Efficient bootstrap and migration of existing non-Hudi dataset                         | 0.13.0         | [HUDI-1265](https://issues.apache.org/jira/browse/HUDI-1265)                                                                                                                                                                                             |
| Eliminating physical partitioning with efficient logical partitioning and file pruning | 0.13.0         | [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)                                                                                                                                                                                               |
| Lock-Free message queue to improve writing efficiency                                  | 0.13.0         | [RFC-53](https://github.com/apache/hudi/blob/master/rfc/rfc-53/rfc-53.md), [HUDI-3963](https://issues.apache.org/jira/browse/HUDI-3963)                                                                                                                  |
| Lock free concurrency control                                                          | 1.0.0 onward   | [HUDI-3187](https://issues.apache.org/jira/browse/HUDI-3187)                                                                                                                                                                                             |
| Non-blocking/Lock-free updates during clustering                                       | 1.0.0 onward   | [HUDI-1042](https://issues.apache.org/jira/browse/HUDI-1042)                                                                                                                                                                                             |
| Time Travel updates, deletes                                                           | 1.0.0 onward   ||
| General purpose support for multi-table transactions                                   | 1.0.0 onward   ||

## Programming APIs

|Feature| Target Release |Tracking|
|------------|----------------|-----------|
| Redesign and optimization of record payload abstraction | 0.13.0         | [RFC-46](https://github.com/apache/hudi/blob/master/rfc/rfc-46/rfc-46.md), [HUDI-3217](https://issues.apache.org/jira/browse/HUDI-3217) |
| Optimized storage layout for cloud object stores | 0.13.0         | [RFC-60](https://github.com/apache/hudi/pull/5113), [HUDI-3625](https://issues.apache.org/jira/browse/HUDI-3625) |
| Support of verification with multiple event_time fields | 0.13.0         | [RFC-59](https://github.com/apache/hudi/pull/6382), [HUDI-4569](https://issues.apache.org/jira/browse/HUDI-4569) |

## Execution Engine Integration

| Feature                                                                                        | Target Release | Tracking                                                                                                        |
|------------------------------------------------------------------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------|
| Spark datasource V2 read                                                                       | 0.13.0       | [HUDI-4449](https://issues.apache.org/jira/browse/HUDI-4449)                                                    |
| Integrate column stats index with all query engines                                            | 0.13.0       | [RFC-58](https://github.com/apache/hudi/pull/6345), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552) |
| Upgrade to Spark 3 as the default profile                                                      | 0.13.0       | [HUDI-3431](https://issues.apache.org/jira/browse/HUDI-3431)                                                    |
| Materialized Views with incremental updates using Flink                                        | 1.0.0 onward ||
| SQL DML support for Presto/Trino connectors (could be accelerated based on community feedback) | 1.0.0 onward ||
| Explore other execution engines/runtimes (Ray, native Rust, Python)                            | 1.0.0 onward ||

## Platform Services

| Feature                                                                                             | Target Release | Tracking                                                                                                                                |
|-----------------------------------------------------------------------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| Protobuf source for DeltaStreamer                                                                   | 0.13.0       | [RFC-57](https://github.com/apache/hudi/blob/master/rfc/rfc-57/rfc-57.md), [HUDI-4399](https://issues.apache.org/jira/browse/HUDI-4399) |
| Hudi integration with Snowflake                                                                     | 0.13.0       | [RFC-41](https://github.com/apache/hudi/pull/4074), [HUDI-2832](https://issues.apache.org/jira/browse/HUDI-2832)                        |
| Improving Hudi CLI features and usability                                                           | 0.13.0       | [HUDI-1388](https://issues.apache.org/jira/browse/HUDI-1388)                                                                            |
| Support for reliable, event based ingestion from cloud stores - GCS, Azure and the others           | 1.0.0 onward | [HUDI-1896](https://issues.apache.org/jira/browse/HUDI-1896)                                                                            |
| Mutable, Transactional caching for Hudi Tables   (could be accelerated based on community feedback) | 1.0.0 onward | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5)    |