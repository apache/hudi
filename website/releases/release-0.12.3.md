---
title: "Release 0.12.3"
sidebar_position: 10
layout: releases
toc: true
last_modified_at: 2023-04-23T10:30:00+05:30
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.12.3](https://github.com/apache/hudi/releases/tag/release-0.12.3)

## Long Term Support

We aim to maintain 0.12 for a longer period of time and provide a stable release through the latest 0.12.x release for
users to migrate to.  This release (0.12.3) is the latest 0.12 release.

## Migration Guide

* This release (0.12.3) does not introduce any new table version, thus no migration is needed if you are on 0.12.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), and [0.12.0](/releases/release-0.12.0).

### Bug fixes

0.12.3 release is mainly intended for bug fixes and stability. The fixes span across many components, including

* DeltaStreamer
* Metadata table and timeline server out of sync issue
* Table services
* Spark SQL 
* Presto stability/pref fixes
* Trino stability/perf fixes
* Meta Syncs
* Flink engine
* Unit, functional, integration tests and CI

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12352934&styleName=Html&projectId=12322822&Create=Create&atl_token=A5KQ-2QAV-T4JA-FDED_88b472602a0f3c72f949e98ae8087a47c815053b_lin)

:::tip
0.12.3 release also contains all the new features and bug fixes from 0.12.2, 0.12.1 and 0.12.0, for which the release notes are
[here](/releases/release-0.12.2), [here](/releases/release-0.12.1) and [here](/releases/release-0.12.0) 
:::
