---
title: "Release 0.11.1"
sidebar_position: 6
layout: releases
toc: true
last_modified_at: 2022-06-19T23:30:00-07:00
---
# [Release 0.11.1](https://github.com/apache/hudi/releases/tag/release-0.11.1) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (0.11.1) does not introduce any new table version, thus no migration is needed if you are on 0.11.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
the upgrade instructions in [0.6.0](/releases/older-releases#migration-guide-for-this-release),
[0.9.0](/releases/release-0.9.0#migration-guide-for-this-release), [0.10.0](/releases/release-0.10.0#migration-guide), and 
[0.11.0](/releases/release-0.11.0#migration-guide).


## Release Highlights

### Addressing Performance Regression in 0.11.0

In 0.11.0 release, with the newly added support for Spark SQL features, the following performance regressions were
inadvertently introduced:
* Partition pruning for some of the COW tables is not applied properly
* Spark SQL query caching (which caches parsed and resolved queries) was not working correctly resulting in additional
* overhead to re-analyze the query every time when it's executed (listing the table contents, etc.)

All of these issues have been addressed in 0.11.1 and are validated to be resolved by benchmarking the set of changes
on TPC-DS against 0.10.1.

### Key generator for Spark SQL

Prior to this release, Spark SQL uses a different default key generator compared with data source writers, which brings
in confusion and errors.  In 0.11.1, Spark SQL now aligns with the data source to use the same logic for determining
the key generator.

### Query with Schema evolution

Due to necessary changes in addressing the performance regression in 0.11.0, when reading a Hudi table with Schema
Evolution feature enabled, the query must have the config `hoodie.schema.on.read.enable` to be explicitly set to `true`
to ensure proper schema resolution and data reading.

### Bug fixes

0.11.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including
* DeltaStreamer
* Table config
* Table services
* Metadata table
* Spark SQL support
* Spark, GCP bundles
* Presto support
* Hive Sync and Meta Sync
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions:

We discovered a regression in Hudi 0.11.1 release related to metadata table and timeline server interplay with streaming ingestion pipelines.

The FileSystemView that Hudi maintains internally go out of sync due to a occasional race conditions when table services are involved
(compaction, clustering) and could result in updates routed to older file versions and hence resulting in missed updates.

Here are the user-flows that could potentially be impacted with this.

- This impacts pipelines using Deltastreamer in **continuous mode** (sync once is not impacted), Spark streaming, or if you have been directly
  using write client across batches/commits instead of the standard ways to write to Hudi. In other words, batch writes should not be impacted.
- Among these write models, this could have an impact only when table services are enabled.
    - COW: clustering enabled (inline or async)
    - MOR: compaction enabled (by default, inline or async)
- Also, the impact is applicable only when metadata table is enabled, and timeline server is enabled (which are defaults as of 0.13.0)

Based on some production data, we expect roughly < 1% of updates to be missed, since its a race condition and table services are generally scheduled
once every N commits. This percentage of update misses could be even less if the frequency of table services is less.

[Here](https://issues.apache.org/jira/browse/HUDI-5863) is the jira for the issue of interest and the fix has already been landed in master.
Next minor release should have the fix. Until we have a next minor release with the fix, we recommend you to disable metadata table to mitigate the issue.

Sorry about the inconvenience caused.

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351597)