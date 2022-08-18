---
title: "Release 0.11.1"
sidebar_position: 3
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

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351597)