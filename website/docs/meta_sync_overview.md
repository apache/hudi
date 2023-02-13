---
title: Meta Sync Overview
keywords: [hudi, sync]
---

Syncing can be enabled for a hudi table to an external meta store or a data catalog. Syncing can be configured
to multiple catalogs. Since version 0.13.0, syncing to all configured catalogs is attempted before failing the operation
on sync failure.

### Common Configurations

#### DeltaStreamer Options

```shell
--enable-sync true
--sync-tool-classes
      Classes (comma-separated) to be used for syncing meta. Shall be used only when --enable-sync or --enable-hive-sync is set to true
      Note: When used with deprecated --enable-hive-sync flag, HiveSyncTool will always be run along with any other classes mentioned in here.
      Default: org.apache.hudi.hive.HiveSyncTool
```

#### DataSource Configs

```shell
hoodie.datasource.meta.sync.enable - true
hoodie.meta.sync.client.tool.class - Comma Separated list of Sync tool class name used to sync to metastore. Defaults to Hive.
```

Further details are available in the catalog specific pages.