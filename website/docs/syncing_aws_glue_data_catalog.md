---
title: AWS Glue Data Catalog
keywords: [hudi, aws, glue, sync]
---

Hudi tables can sync to AWS Glue Data Catalog directly via AWS SDK. Piggyback on `HiveSyncTool`
, `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` makes use of all the configurations that are taken by `HiveSyncTool`
and send them to AWS Glue.

## Configurations

Most of the configurations for `AwsGlueCatalogSyncTool` are shared with `HiveSyncTool`. The example showed in 
[Sync to Hive Metastore](syncing_metastore) can be used as is for sync with Glue Data Catalog, provided that the hive metastore
URL (either JDBC or thrift URI) can proxied to Glue Data Catalog, which is usually done within AWS EMR or Glue job environment.

For Hudi streamer, users can set

```shell
--sync-tool-classes org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
```

For Spark data source writers, users can set

```shell
hoodie.meta.sync.client.tool.class=org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
```

### Avoid creating excessive versions

Tables stored in Glue Data Catalog are versioned. And by default, every Hudi commit triggers a sync operation if enabled, regardless of having relevant metadata changes.
This can lead to too many versions kept in the catalog and eventually failing the sync operation.

Meta-sync can be set to conditional - only sync when there are schema change or partition change. This can avoid creating
excessive versions in the catalog. Users can enable it by setting 

```
hoodie.datasource.meta_sync.condition.sync=true
```

### Glue Data Catalog specific configs

Sync to Glue Data Catalog can be optimized with other configs like

```
hoodie.datasource.meta.sync.glue.all_partitions_read_parallelism
hoodie.datasource.meta.sync.glue.changed_partitions_read_parallelism
hoodie.datasource.meta.sync.glue.partition_change_parallelism
```

[Partition indexes](https://docs.aws.amazon.com/glue/latest/dg/partition-indexes.html) can also be used by setting

```
hoodie.datasource.meta.sync.glue.partition_index_fields.enable
hoodie.datasource.meta.sync.glue.partition_index_fields
```

## Other references

### Running AWS Glue Catalog Sync for Spark DataSource

To write a Hudi table to Amazon S3 and catalog it in AWS Glue Data Catalog, you can use the options mentioned in the
[AWS documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html#aws-glue-programming-etl-format-hudi-write)

### Running AWS Glue Catalog Sync from EMR

If you're running HiveSyncTool on an EMR cluster backed by Glue Data Catalog as external metastore, you can simply run the sync from command line like below:

```shell
cd /usr/lib/hudi/bin

./run_sync_tool.sh --base-path s3://<bucket_name>/<prefix>/<table_name> --database <database_name> --table <table_name> --partitioned-by <column_name>
```