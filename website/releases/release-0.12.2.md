---
title: "Release 0.12.2"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2022-12-27T10:30:00+05:30
---
# [Release 0.12.2](https://github.com/apache/hudi/releases/tag/release-0.12.2) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (0.12.2) does not introduce any new table version, thus no migration is needed if you are on 0.12.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/older-releases#migration-guide-for-this-release),
  [0.9.0](/releases/release-0.9.0#migration-guide-for-this-release), [0.10.0](/releases/release-0.10.0#migration-guide),
  [0.11.0](/releases/release-0.11.0#migration-guide), and [0.12.0](/releases/release-0.12.0#migration-guide).

### Bug fixes

0.12.2 release is mainly intended for bug fixes and stability. The fixes span across many components, including

* DeltaStreamer
* Datatype/schema related bug fixes
* Table services
* Metadata table
* Spark SQL 
* Presto stability/pref fixes
* Trino stability/perf fixes
* Meta Syncs
* Flink engine
* Unit, functional, integration tests and CI

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12352249&styleName=Html&projectId=12322822&Create=Create&atl_token=A5KQ-2QAV-T4JA-FDED_88b472602a0f3c72f949e98ae8087a47c815053b_lin)