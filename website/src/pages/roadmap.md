---
title: Roadmap
last_modified_at: 2019-12-30T15:59:57-04:00
---
# Roadmap

Hudi community strives to deliver major releases every 3 months, while offering minor releases every 1-2 months!
This page captures the forward-looking roadmap of ongoing & upcoming projects and when they are expected to land, broken
down by areas on our [stack](/docs/hudi_stack).

## Recent Release
[0.15.0](https://hudi.apache.org/releases/release-0.15.0) (June 2024)

## Future Releases

| Release     | Timeline |
|-------------|----------|
| 1.0.0-beta2 | July 2024 |
| 1.0.0 (GA)  | Q4, 2024 |
 | 1.0.1       | Jan 2025 |
| 1.1.0       | Mar 2025 |
| 1.2.0       | May 2025 |
| 1.3.0       | July 2025 |
| 2.0.0       | Dec 2025 |


## Storage Engine

| Feature                                              | Target Release | Tracking                                                                                                                                                                       |
|------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.x Storage format                                   | 1.0.0          | [HUDI-6242](https://issues.apache.org/jira/browse/HUDI-6242)                                                                                                                   |
| Writer performance improvements                      | 1.0.0          | [HUDI-3249](https://issues.apache.org/jira/browse/HUDI-3249)                                                                                                                   |
| Non-blocking concurrency control                     | 1.0.0          | [HUDI-3187](https://issues.apache.org/jira/browse/HUDI-3187), [HUDI-1042](https://issues.apache.org/jira/browse/HUDI-1042), [RFC-66](https://github.com/apache/hudi/pull/7907) |
| Secondary indexes to improve query performance       | 1.0.0          | [RFC-52](https://github.com/apache/hudi/pull/5370), [HUDI-3907](https://issues.apache.org/jira/browse/HUDI-3907)                                                               |
| Index Function for Optimizing Query Performance      | 1.0.0          | [RFC-63](https://github.com/apache/hudi/pull/7235), [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)                                                                 |
| Logical partitioning via indexing                    | 1.0.0          | [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)                                                                                                                     |
| Storage partition stats index                        | 1.0.0          | [HUDI-7144](https://issues.apache.org/jira/browse/HUDI-7144)                                                                                                                   |
| Non-blocking updates during clustering               | 1.1.0          | [HUDI-1045](https://issues.apache.org/jira/browse/HUDI-1045)                                                                                                                   |
| Track schema in metadata table                       | 1.1.0          | [HUDI-6778](https://issues.apache.org/jira/browse/HUDI-6778)                                                                                                                   |
| Streaming CDC/Incremental read improvement           | 1.1.0          | [HUDI-2749](https://issues.apache.org/jira/browse/HUDI-2749)                                                                                                                   |
| Supervised table service planning and execution      | 1.1.0          | [RFC-43](https://github.com/apache/hudi/pull/4309), [HUDI-4147](https://issues.apache.org/jira/browse/HUDI-4147)                                                               |
| Enable partial updates for CDC workload payload      | 1.1.0          | [HUDI-7229](https://issues.apache.org/jira/browse/HUDI-7229)                                                                                                                   |
| Vector search indexes                                | 1.1.0          |                                                                                                                 |
| General purpose support for multi-table transactions | 1.2.0          ||
| Time Travel updates, deletes                         | 1.3.0          ||
| Unstructured data storage and management             | 1.3.0          ||


## Programming APIs

| Feature                                                 | Target Release | Tracking                                                                                                                   |
|---------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------|
| APIs/Abstractions, Record mergers                       | 1.0.0          | [HUDI-6243](https://issues.apache.org/jira/browse/HUDI-6243), [HUDI-3217](https://issues.apache.org/jira/browse/HUDI-3217) |
| New Hudi Table Format APIs for Query Integrations       | 1.1.0          | [RFC-64](https://github.com/apache/hudi/pull/7080), [HUDI-4141](https://issues.apache.org/jira/browse/HUDI-4141)           |
| Snapshot view management                                | 1.2.0          | [RFC-61](https://github.com/apache/hudi/pull/6576), [HUDI-4677](https://issues.apache.org/jira/browse/HUDI-4677)           |
| Support of verification with multiple event_time fields | 1.2.0          | [RFC-59](https://github.com/apache/hudi/pull/6382), [HUDI-4569](https://issues.apache.org/jira/browse/HUDI-4569)           |


## Query Engine Integration

| Feature                                                 | Target Release | Tracking                                                                                                                                                                                 |
|---------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Presto/Trino queries with new format                    | 1.1.0          | [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4394](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552) |
| Table Valued Function to query Hudi timeline            | 1.0.0	         | [HUDI-7243](https://issues.apache.org/jira/browse/HUDI-7243)                                                                                                                             |
| Default Java 17 support 	                               | 1.1.0	         | [HUDI-6506](https://issues.apache.org/jira/browse/HUDI-6506)                                                                                                                             |
| Spark 4 Support	                                       | 1.1.0	         | [HUDI-7915](https://issues.apache.org/jira/browse/HUDI-7915)                                                                                                                             |
| Spark datasource V2 read                                | 1.1.0          | [HUDI-4449](https://issues.apache.org/jira/browse/HUDI-4449)                                                                                                                             |
| Replace Dataframe write path for Spark                  | 1.1.0	         | [HUDI-4857](https://issues.apache.org/jira/browse/HUDI-4857)                                                                                                                             |


## Platform Components

| Feature                                                                                           | Target Release | Tracking                                                                                                                               |
|---------------------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Support for reliable, event based ingestion from cloud stores - GCS, Azure and the others         | 1.0.0          | [HUDI-1896](https://issues.apache.org/jira/browse/HUDI-1896)                                                                           |
| Hudi Reverse streamer                                                                             | 1.2.0          | [RFC-70](https://github.com/apache/hudi/pull/9040)                                                                                      |
| Diagnostic Reporter                                                                               | 1.2.0          | [RFC-62](https://github.com/apache/hudi/pull/6600)                                                                        |
| Mutable, Transactional caching for Hudi Tables (could be accelerated based on community feedback) | 2.0.0          | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5), [HUDI-6489](https://issues.apache.org/jira/browse/HUDI-6489) |
| Hudi Metaserver (could be accelerated based on community feedback)                                | 2.0.0          |   |
