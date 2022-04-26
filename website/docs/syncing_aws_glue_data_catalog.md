---
title: Sync to AWS Glue Data Catalog
keywords: [hudi, aws, glue, sync]
---

Hudi tables can sync to AWS Glue Data Catalog directly via AWS SDK. Piggyback on `HiveSyncTool`
, `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` makes use of all the configurations that are taken by `HiveSyncTool`
and send them to AWS Glue.

### Configurations

There is no additional configuration for using `AwsGlueCatalogSyncTool`; you just need to set it as one of the sync tool
classes for `HoodieDeltaStreamer` and everything configured as shown in [Sync to Hive Metastore](syncing_metastore) will
be passed along.

```shell
--sync-tool-classes org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
```
