---
title: AWS Glue Data Catalog
keywords: [hudi, aws, glue, sync]
---

Hudi tables can sync to AWS Glue Data Catalog directly via AWS SDK. Piggyback on `HiveSyncTool`
, `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` makes use of all the configurations that are taken by `HiveSyncTool`
and send them to AWS Glue.

### Configurations

There is no additional configuration for using `AwsGlueCatalogSyncTool`; you just need to set it as one of the sync tool
classes for `HoodieStreamer` and everything configured as shown in [Sync to Hive Metastore](syncing_metastore.md) will
be passed along.

```shell
--sync-tool-classes org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
```

#### Running AWS Glue Catalog Sync for Spark DataSource

To write a Hudi table to Amazon S3 and catalog it in AWS Glue Data Catalog, you can use the options mentioned in the
[AWS documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html#aws-glue-programming-etl-format-hudi-write)

#### Running AWS Glue Catalog Sync from EMR

If you're running HiveSyncTool on an EMR cluster backed by Glue Data Catalog as external metastore, you can simply run the sync from command line like below:

```shell
cd /usr/lib/hudi/bin

./run_sync_tool.sh --base-path s3://<bucket_name>/<prefix>/<table_name> --database <database_name> --table <table_name> --partitioned-by <column_name>
```