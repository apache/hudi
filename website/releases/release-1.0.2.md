---
title: "Release 1.0.2"
sidebar_position: 1
layout: releases
toc: true
last_modified_at: 2024-05-02T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.0.2](https://github.com/apache/hudi/releases/tag/release-1.0.2)

## Migration Guide

* This release (1.0.2) does not introduce any new table version, thus no migration is needed if you are on 1.0.1.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0),
  [0.14.0](/releases/release-0.14.0) and [1.0.0](/releases/release-1.0.0)

### Bug fixes

The 1.0.2 release primarily focuses on bug fixes, stability enhancements, and critical improvements, particularly around migration and backwards compatibility. The changes span across various components, including:

* **Metadata Table (MDT):** Numerous fixes and improvements related to validation, writing, reading, compaction, indexing (column stats), and backwards compatibility (especially for table version 6).
* **Spark Integration:** Enhancements and fixes for Spark SQL (MERGE INTO, query behavior), datasource reader/writer, schema handling, performance, and backward compatibility.
* **Backwards Compatibility:** Significant effort ensuring compatibility with older table versions (specifically v6) and smoother upgrades from 0.x versions, including dedicated writers/readers.
* **File Group Reader:** Validation, fixes, and feature completeness improvements, including making it default for table version 6.
* **Flink Engine:** Fixes and improvements related to streamer checkpoints and bundle validation.
* **Compaction and Table Services:** Fixes related to compaction scheduling, execution (especially with global index or RLI), archival, and cleanup.
* **Indexing:** Fixes and enhancements for Column Stats, Record Level Index (RLI), and Bloom Filters.
* **Performance:** Optimizations in areas like log file writing, schema reuse, and metadata initialization.
* **Testing, CI, and Dependencies:** Fixes for flaky tests, improved code coverage, bundle validation, dependency cleanup (HBase removal), and extensive release testing.

## Known Regressions
We have a ComplexKeyGenerator related regression reported [here](release-0.14.1#known-regressions). Please refrain from migrating, if you have single field as record key and mutiple partition fields.

:::tip
Avoid upgrading any existing table to 1.0.2 if you are using ComplexKeyGenerator with single record key configured.
:::

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12355558)

:::tip
1.0.2 release also contains all the new features and bug fixes from 1.0.1, of which the release notes are [here](/releases/release-1.0.1)
:::
