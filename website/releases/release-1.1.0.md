---
title: "Release 1.1.0"
sidebar_position: 1
layout: releases
toc: true
last_modified_at: 2025-08-12T18:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## [Release 1.1.0](https://github.com/apache/hudi/releases/tag/release-1.1.0) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (1.1.0) introduced a new table version 9. <Migration guide to be done here...>

### Behavior specifications

In table version 9 we introduced index versioning which allows indices to involve with different storage layout without requiring table version upgrades. The new table version default to create any new secondary index with version `V2`. During upgrades, all exiting indices are tagged with version `V1` and will continue to function. During table version downgrade, any indices with index version `V2` or higher will be dropped.

## Bug fixes

The 1.1.0 release primarily focuses on bug fixes, stability enhancements, and critical improvements, particularly around migration and backwards compatibility. The changes span across various components, including:

* **Indexing:** Fixes and enhancements for secondary index. Introduced secondary index V2 which offers better read/write performance. If you are using old secondary indices, it is encouraged to drop and create them after the upgrade to benefit from the performance and functional enhancement.
