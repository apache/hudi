---
title: "Release 0.8.0"
sidebar_position: 6
layout: releases
toc: true
last_modified_at: 2020-05-28T08:40:00-07:00
---
# [Release 0.8.0](https://github.com/apache/hudi/releases/tag/release-0.8.0) ([docs](/docs/quick-start-guide))

## Migration Guide for this release
- If migrating from release older than 0.5.3, please also check the upgrade instructions for each subsequent release below.
- Specifically check upgrade instructions for 0.6.0. This release does not introduce any new table versions.
- The `HoodieRecordPayload` interface deprecated existing methods, in favor of new ones that also lets us pass properties at runtime. Users are
encouraged to migrate out of the deprecated methods, since they will be removed in 0.9.0.
- "auto.offset.reset" is a config used for Kafka sources in deltastreamer utility to reset offset to be consumed from such 
  sources. We had a bug around this config with 0.8.0 which has been fixed in 0.9.0. Please use "auto.reset.offsets" instead. 
  Possible values for this config are "earliest" and "latest"(default). So, would recommend using "auto.reset.offsets" only in 
  0.8.0 and for all other releases, you can use "auto.offset.reset".

## Release Highlights

### Flink Integration
Since the initial support for the Hudi Flink Writer in the 0.7.0 release, the Hudi community made great progress on improving the Flink/Hudi integration, 
including redesigning the Flink writer pipeline with better performance and scalability, state-backed indexing with bootstrap support, 
Flink writer for MOR table, batch reader for COW&MOR table, streaming reader for MOR table, and Flink SQL connector for both source and sink. 
In the 0.8.0 release, user is able to use all those features with Flink 1.11+.

Please see [RFC-24](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+24%3A+Hoodie+Flink+Writer+Proposal)
for more implementation details for the Flink writer and follow this [page](/docs/flink-quick-start-guide) 
to get started with Flink!

### Parallel Writers Support
As many users requested, now Hudi supports multiple ingestion writers to the same Hudi Table with optimistic concurrency control.
Hudi supports file level OCC, i.e., for any 2 commits (or writers) happening to the same table, if they do not have writes to overlapping files being changed, 
both writers are allowed to succeed. This feature is currently experimental and requires either Zookeeper or HiveMetastore to acquire locks.

Please see [RFC-22](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers)
for more implementation details and follow this [page](/docs/concurrency_control) to get started with concurrency control!

### Writer side improvements
- InsertOverwrite Support for Flink writer client.
- Support CopyOnWriteTable in Java writer client.

### Query side improvements
- Support Spark Structured Streaming read from Hudi table.
- Performance improvement of Metadata table.
- Performance improvement of Clustering.

### Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12349423)