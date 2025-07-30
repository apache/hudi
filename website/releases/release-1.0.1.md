---
title: "Release 1.0.1"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2024-02-10T13:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.0.1](https://github.com/apache/hudi/releases/tag/release-1.0.1) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (1.0.1) does not introduce any new table version, thus no migration is needed if you are on 1.0.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0),
  [0.14.0](/releases/release-0.14.0) and [1.0.0](/releases/release-1.0.0)

### Bug fixes

1.0.1 release is mainly intended for bug fixes and stability. The fixes span across many components, including

* DeltaStreamer
* Spark SQL
* Spark datasource writer
* Table services
* Backwards compatible writer
* Flink engine
* Unit, functional, integration tests and CI

## Known Regressions
If you use ComplexKeyGenerator with a single field, there is a known regression with key encoding format change that could introduce duplicates. Please refrain from migrating. and raise a GH issue to work through fixes. Note, if you have multiple fields as a part of complex key generator, there there are no issues. You can safely proceed.

:::tip
Avoid upgrading any existing table to 1.0.1 if you are using ComplexKeyGenerator with single record key
configured.
:::

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12355195)

:::tip
1.0.1 release also contains all the new features and bug fixes from 1.0.0, of which the release notes are [here](/releases/release-1.0.0)
:::
