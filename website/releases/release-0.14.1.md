---
title: "Release 0.14.1"
sidebar_position: 5
layout: releases
toc: true
last_modified_at: 2023-05-25T13:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 0.14.1](https://github.com/apache/hudi/releases/tag/release-0.14.1) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (0.14.1) does not introduce any new table version, thus no migration is needed if you are on 0.14.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0), and
  [0.14.0](/releases/release-0.14.0)

### Bug fixes

0.14.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including

* DeltaStreamer
* Spark SQL
* Spark datasource writer
* Table services
* Meta Syncs
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions
We discovered a regression in Hudi 0.14.1 release related to Complex Key gen when record key consists of one field. 
It can silently ingest duplicates if table is upgraded from previous versions.

:::tip
Avoid upgrading any existing table to 0.14.1 if you are using ComplexKeyGenerator with single field as record key and multiple partition fields.
:::

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12353493)

:::tip
0.14.1 release also contains all the new features and bug fixes from 0.14.0, of which the release notes are [here](/releases/release-0.14.0)
:::
