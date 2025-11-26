---
title: Roadmap
last_modified_at: 2025-02-19T12:00:00-08:00
---

# Roadmap

Hudi community strives to deliver major releases every 3 months, while offering minor releases every 1-2 months!
This page captures the forward-looking roadmap of ongoing & upcoming projects and when they are expected to land, broken
down by areas on our [stack](/docs/hudi_stack).

## Recent Release(s)

[1.1.0](/releases/release-1.1.0) (Nov 2025)

## Future Releases

| Release | Timeline  |
|---------|-----------|
| 1.2.0   | Jan 2026  |
| 2.0.0   | Jun 2026  |

## Storage Engine

| Feature                                              | Target Release | Tracking                                                                                                                                                                       |
|------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Introduce `.abort` state in the timeline             | 1.2.0          | [HUDI-8189](https://issues.apache.org/jira/browse/HUDI-8189) |
| Variant type support on Spark 4                      | 1.2.0          | [HUDI-9046](https://issues.apache.org/jira/browse/HUDI-9046) |
| Non-blocking updates during clustering               | 1.2.0          | [HUDI-1045](https://issues.apache.org/jira/browse/HUDI-1045)                                                                                                                   |
| Enable partial updates for CDC workload payload      | 1.2.0          | [HUDI-7229](https://issues.apache.org/jira/browse/HUDI-7229)                                                                                                                   |
| Schema tracking in metadata table                    | 1.2.0          | [HUDI-6778](https://issues.apache.org/jira/browse/HUDI-6778) |
| NBCC for MDT writes                                  | 1.2.0          | [HUDI-8480](https://issues.apache.org/jira/browse/HUDI-8480) |
| Index abstraction for writer and reader              | 1.2.0          | [HUDI-9176](https://issues.apache.org/jira/browse/HUDI-9176) |
| Vector search index                                  | 1.2.0          | [HUDI-9047](https://issues.apache.org/jira/browse/HUDI-9047) |
| Bitmap index                                         | 1.2.0          | [HUDI-9048](https://issues.apache.org/jira/browse/HUDI-9048) |
| New abstraction for schema, expressions, and filters | 1.2.0          | [RFC-88](https://github.com/apache/hudi/pull/12795) |
| Streaming CDC/Incremental read improvement           | 1.2.0          | [HUDI-2749](https://issues.apache.org/jira/browse/HUDI-2749) |
| Supervised table service planning and execution      | 1.2.0          | [RFC-43](https://github.com/apache/hudi/pull/4309), [HUDI-4147](https://issues.apache.org/jira/browse/HUDI-4147)                                                               |
| General purpose support for multi-table transactions | 1.2.0          | [HUDI-6709](https://issues.apache.org/jira/browse/HUDI-6709) |
| Supporting different updated columns in a single partial update log file | 1.2.0          | [HUDI-9049](https://issues.apache.org/jira/browse/HUDI-9049) |
| CDC format consolidation                             | 1.2.0          | [HUDI-7538](https://issues.apache.org/jira/browse/HUDI-7538) |
| Time Travel updates, deletes                         | 1.3.0          | [HUDI-9050](https://issues.apache.org/jira/browse/HUDI-9050) |
| Unstructured data storage and management             | 1.3.0          | [HUDI-9051](https://issues.apache.org/jira/browse/HUDI-9051)|


## Programming APIs

| Feature                                                 | Target Release | Tracking                                                                                                                   |
|---------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------|
| New Hudi Table Format APIs for Query Integrations       | 1.2.0          | [RFC-64](https://github.com/apache/hudi/pull/7080), [HUDI-4141](https://issues.apache.org/jira/browse/HUDI-4141)           |
| Snapshot view management                                | 1.2.0          | [RFC-61](https://github.com/apache/hudi/pull/6576), [HUDI-4677](https://issues.apache.org/jira/browse/HUDI-4677)           |
| Support of verification with multiple event_time fields | 1.2.0          | [RFC-59](https://github.com/apache/hudi/pull/6382), [HUDI-4569](https://issues.apache.org/jira/browse/HUDI-4569)           |


## Query Engine Integration

| Feature                                                 | Target Release | Tracking                                                                                                                                                                                 |
|---------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Default Java 17 support 	                              | 1.2.0	         | [HUDI-6506](https://issues.apache.org/jira/browse/HUDI-6506)                                                                                                                             |
| Spark datasource V2 read                                | 1.2.0          | [HUDI-4449](https://issues.apache.org/jira/browse/HUDI-4449)                                                                                                                             |
| Simplification of engine integration and module organization | 1.2.0          | [HUDI-9502](https://issues.apache.org/jira/browse/HUDI-9502) |
| End-to-end DataFrame write path on Spark                | 1.2.0          | [HUDI-9019](https://issues.apache.org/jira/browse/HUDI-9019), [HUDI-4857](https://issues.apache.org/jira/browse/HUDI-4857) |
| Support Hudi 1.0 release in Presto Hudi Connector       | Presto Release / Q2 | [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-3210) |
| Support of new indexes in Presto Hudi Connector         | Presto Release / Q3 | [HUDI-4394](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552) |
| MDT support in Trino Hudi Connector                     | Trino Release / Q2 | [HUDI-2687](https://issues.apache.org/jira/browse/HUDI-2687) |
| Support of new indexes in Trino Hudi Connector          | Trino Release / Q3 | [HUDI-4394](https://issues.apache.org/jira/browse/HUDI-4394), [HUDI-4552](https://issues.apache.org/jira/browse/HUDI-4552) |

## Platform Components

| Feature                                                                                           | Target Release | Tracking                                                                                                                               |
|---------------------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Syncing as non-partitoned tables in catalogs             | 1.2.0          | [HUDI-9503](https://issues.apache.org/jira/browse/HUDI-9503) |
| Hudi Reverse streamer                                                                             | 1.2.0          | [RFC-70](https://github.com/apache/hudi/pull/9040)                                                                                      |
| Diagnostic Reporter                                                                               | 1.2.0          | [RFC-62](https://github.com/apache/hudi/pull/6600)                                                                        |
| Mutable, Transactional caching for Hudi Tables (could be accelerated based on community feedback) | 2.0.0          | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5), [HUDI-6489](https://issues.apache.org/jira/browse/HUDI-6489) |
| Hudi Metaserver (could be accelerated based on community feedback)                                | 2.0.0          | [HUDI-3345](https://issues.apache.org/jira/browse/HUDI-3345), [RFC-36](https://github.com/apache/hudi/pull/4718) |


## Developer Experience
| Feature                                                 | Target Release | Tracking                                 |
|---------------------------------------------------------|----------------|------------------------------------------|
| Clean up tech debt and deprecate unused code            | 1.2.0          | [HUDI-9054](https://issues.apache.org/jira/browse/HUDI-9054) |
