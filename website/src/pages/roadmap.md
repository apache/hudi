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
| Introduce `.abort` state in the timeline             | 1.2.0          | [#16609](https://github.com/apache/hudi/issues/16609) |
| Variant type support on Spark 4                      | 1.2.0          | [#16851](https://github.com/apache/hudi/issues/16851) |
| Non-blocking updates during clustering               | 1.2.0          | [#14611](https://github.com/apache/hudi/issues/14611)                                                                                                                   |
| Enable partial updates for CDC workload payload      | 1.2.0          | [#16354](https://github.com/apache/hudi/issues/16354)                                                                                                                   |
| Schema tracking in metadata table                    | 1.2.0          | [#14397](https://github.com/apache/hudi/issues/14397) |
| NBCC for MDT writes                                  | 1.2.0          | [#17305](https://github.com/apache/hudi/issues/17305) |
| Index abstraction for writer and reader              | 1.2.0          | [#16903](https://github.com/apache/hudi/issues/16903) |
| Vector search index                                  | 1.2.0          | [#16852](https://github.com/apache/hudi/issues/16852) |
| Bitmap index                                         | 1.2.0          | [#16853](https://github.com/apache/hudi/issues/16853) |
| New abstraction for schema, expressions, and filters | 1.2.0          | [RFC-88](https://github.com/apache/hudi/pull/12795) |
| Streaming CDC/Incremental read improvement           | 1.2.0          | [#14916](https://github.com/apache/hudi/issues/14916) |
| Supervised table service planning and execution      | 1.2.0          | [RFC-43](https://github.com/apache/hudi/pull/4309), [#15196](https://github.com/apache/hudi/issues/15196)                                                               |
| General purpose support for multi-table transactions | 1.2.0          | [#16181](https://github.com/apache/hudi/issues/16181) |
| Supporting different updated columns in a single partial update log file | 1.2.0          | [#16854](https://github.com/apache/hudi/issues/16854) |
| CDC format consolidation                             | 1.2.0          | [#16429](https://github.com/apache/hudi/issues/16429) |
| Time Travel updates, deletes                         | 1.3.0          | [#16855](https://github.com/apache/hudi/issues/16855) |
| Unstructured data storage and management             | 1.3.0          | [#16856](https://github.com/apache/hudi/issues/16856)|

## Programming APIs

| Feature                                                 | Target Release | Tracking                                                                                                                   |
|---------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------|
| New Hudi Table Format APIs for Query Integrations       | 1.2.0          | [RFC-64](https://github.com/apache/hudi/pull/7080), [#15194](https://github.com/apache/hudi/issues/15194)           |
| Snapshot view management                                | 1.2.0          | [RFC-61](https://github.com/apache/hudi/pull/6576), [#15367](https://github.com/apache/hudi/issues/15367)           |
| Support of verification with multiple event_time fields | 1.2.0          | [RFC-59](https://github.com/apache/hudi/pull/6382), [#15325](https://github.com/apache/hudi/issues/15325)           |

## Query Engine Integration

| Feature                                                 | Target Release | Tracking                                                                                                                                                                                 |
|---------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Default Java 17 support 	                              | 1.2.0	         | [#16082](https://github.com/apache/hudi/issues/16082)                                                                                                                             |
| Spark datasource V2 read                                | 1.2.0          | [#15292](https://github.com/apache/hudi/issues/15292)                                                                                                                             |
| Simplification of engine integration and module organization | 1.2.0          | [#17044](https://github.com/apache/hudi/issues/16857) |
| End-to-end DataFrame write path on Spark                | 1.2.0          | [#16846](https://github.com/apache/hudi/issues/16846), [#15433](https://github.com/apache/hudi/issues/15433) |
| Support Hudi 1.0 release in Presto Hudi Connector       | Presto Release / Q2 | [#14992](https://github.com/apache/hudi/issues/14992) |
| Support of new indexes in Presto Hudi Connector         | Presto Release / Q3 | [#15246](https://github.com/apache/hudi/issues/15246), [#15319](https://github.com/apache/hudi/issues/15319) |
| MDT support in Trino Hudi Connector                     | Trino Release / Q2 | [#14906](https://github.com/apache/hudi/issues/14906) |
| Support of new indexes in Trino Hudi Connector          | Trino Release / Q3 | [#15246](https://github.com/apache/hudi/issues/15246), [#15319](https://github.com/apache/hudi/issues/15319) |

## Platform Components

| Feature                                                                                           | Target Release | Tracking                                                                                                                               |
|---------------------------------------------------------------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Syncing as non-partitoned tables in catalogs             | 1.2.0          | [#17045](https://github.com/apache/hudi/issues/16858) |
| Hudi Reverse streamer                                                                             | 1.2.0          | [RFC-70](https://github.com/apache/hudi/pull/9040)                                                                                      |
| Diagnostic Reporter                                                                               | 1.2.0          | [RFC-62](https://github.com/apache/hudi/pull/6600)                                                                        |
| Mutable, Transactional caching for Hudi Tables (could be accelerated based on community feedback) | 2.0.0          | [Strawman design](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5), [#16072](https://github.com/apache/hudi/issues/16072) |
| Hudi Metaserver (could be accelerated based on community feedback)                                | 2.0.0          | [#15011](https://github.com/apache/hudi/issues/15011), [RFC-36](https://github.com/apache/hudi/pull/4718) |

## Developer Experience

| Feature                                                 | Target Release | Tracking                                 |
|---------------------------------------------------------|----------------|------------------------------------------|
| Clean up tech debt and deprecate unused code            | 1.2.0          | [#16859](https://github.com/apache/hudi/issues/16859) |
