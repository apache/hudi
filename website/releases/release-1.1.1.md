---
title: "Release 1.1.1"
layout: releases
toc: true
last_modified_at: 2025-12-19T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.1.1](https://github.com/apache/hudi/releases/tag/release-1.1.1)

## Migration Guide

* This release (1.1.1) does not introduce any new table version, thus no migration is needed if you are on 1.1.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically
  the upgrade instructions in [0.6.0](/releases/release-0.6.0),
  [0.9.0](/releases/release-0.9.0), [0.10.0](/releases/release-0.10.0),
  [0.11.0](/releases/release-0.11.0), [0.12.0](/releases/release-0.12.0), [0.13.0](/releases/release-0.13.0),
  [0.14.0](/releases/release-0.14.0), [1.0.0](/releases/release-1.0.0) and [1.1.0](/releases/release-1.1.0)

### Bug fixes

The 1.1.1 release primarily focuses on bug fixes, stability enhancements, and critical improvements. 
The changes span across various components, including:

* Hudi Streamer
* Metadata writes
* Spark SQL
* Spark datasource writer
* Flink engine
* Unit, functional, integration tests and CI

## Raw Release Notes

The raw commits that went into 1.1.1 compared to 1.1.0 are available [here](https://github.com/apache/hudi/compare/release-1.1.0...release-1.1.1)
