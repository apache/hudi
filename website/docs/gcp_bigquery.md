---
title: Google BigQuery
keywords: [ hudi, gcp, bigquery ]
summary: Introduce BigQuery integration in Hudi.
---

Hudi tables can be queried from [Google Cloud BigQuery](https://cloud.google.com/bigquery) as external tables. As of
now, the Hudi-BigQuery integration only works for hive-style partitioned Copy-On-Write tables.

## Configurations

Hudi uses `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` to sync tables. It works with `HoodieDeltaStreamer` via
setting sync tool class. A few BigQuery-specific configurations are required.

| Config                                       | Notes                                                                                                           |
|:---------------------------------------------|:----------------------------------------------------------------------------------------------------------------|
| `hoodie.gcp.bigquery.sync.project_id`        | The target Google Cloud project                                                                                 |
| `hoodie.gcp.bigquery.sync.dataset_name`      | BigQuery dataset name; create before running the sync tool                                                      |
| `hoodie.gcp.bigquery.sync.dataset_location`  | Region info of the dataset; same as the GCS bucket that stores the Hudi table                                   |
| `hoodie.gcp.bigquery.sync.source_uri`        | A wildcard path pattern pointing to the first level partition; make sure to include the partition key.          |
| `hoodie.gcp.bigquery.sync.source_uri_prefix` | The common prefix of the `source_uri`, usually it's the path to the Hudi table, trailing slash does not matter. |
| `hoodie.gcp.bigquery.sync.base_path`         | The usual basepath config for Hudi table.                                                                       |

Refer to `org.apache.hudi.gcp.bigquery.BigQuerySyncConfig` for the complete configuration list.

In addition to the BigQuery-specific configs, set the following Hudi configs to write the Hudi table in the desired way.

```
hoodie.datasource.write.hive_style_partitioning = 'true'
hoodie.datasource.write.drop.partition.columns  = 'true'
hoodie.partition.metafile.use.base.format       = 'true'
```

## Example

Below shows an example for running `BigQuerySyncTool` with `HoodieDeltaStreamer`.

```shell
spark-submit --master yarn \
--packages com.google.cloud:google-cloud-bigquery:2.10.4 \
--jars /opt/hudi-gcp-bundle-0.11.0.jar \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
/opt/hudi-utilities-bundle_2.12-0.11.0.jar \
--target-base-path gs://my-hoodie-table/path \
--target-table mytable \
--table-type COPY_ON_WRITE \
--base-file-format PARQUET \
# ... other deltastreamer options
--enable-sync \
--sync-tool-classes org.apache.hudi.gcp.bigquery.BigQuerySyncTool \
--hoodie-conf hoodie.deltastreamer.source.dfs.root=gs://my-source-data/path \
--hoodie-conf hoodie.gcp.bigquery.sync.project_id=hudi-bq \
--hoodie-conf hoodie.gcp.bigquery.sync.dataset_name=rxusandbox \
--hoodie-conf hoodie.gcp.bigquery.sync.dataset_location=asia-southeast1 \
--hoodie-conf hoodie.gcp.bigquery.sync.table_name=mytable \
--hoodie-conf hoodie.gcp.bigquery.sync.base_path=gs://rxusandbox/testcases/stocks/data/target/${NOW} \
--hoodie-conf hoodie.gcp.bigquery.sync.partition_fields=year,month,day \
--hoodie-conf hoodie.gcp.bigquery.sync.source_uri=gs://my-hoodie-table/path/year=* \
--hoodie-conf hoodie.gcp.bigquery.sync.source_uri_prefix=gs://my-hoodie-table/path/ \
--hoodie-conf hoodie.gcp.bigquery.sync.use_file_listing_from_metadata=true \
--hoodie-conf hoodie.gcp.bigquery.sync.assume_date_partitioning=false \
--hoodie-conf hoodie.datasource.hive_sync.mode=jdbc \
--hoodie-conf hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://localhost:10000 \
--hoodie-conf hoodie.datasource.hive_sync.skip_ro_suffix=true \
--hoodie-conf hoodie.datasource.hive_sync.ignore_exceptions=false \
--hoodie-conf hoodie.datasource.hive_sync.database=mydataset \
--hoodie-conf hoodie.datasource.hive_sync.table=mytable \
--hoodie-conf hoodie.datasource.write.recordkey.field=mykey \
--hoodie-conf hoodie.datasource.write.partitionpath.field=year,month,day \
--hoodie-conf hoodie.datasource.write.precombine.field=ts \
--hoodie-conf hoodie.datasource.write.keygenerator.type=COMPLEX \
--hoodie-conf hoodie.datasource.write.hive_style_partitioning=true \
--hoodie-conf hoodie.datasource.write.drop.partition.columns=true \
--hoodie-conf hoodie.partition.metafile.use.base.format=true \
--hoodie-conf hoodie.metadata.enable=true \
```

After run, the sync tool will create 2 tables and 1 view in the target dataset in BigQuery. The tables and the view
share the same name prefix, which is taken from the Hudi table name. Query the view for the same results as querying the
Copy-on-Write Hudi table.
