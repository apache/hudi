---
title: Google BigQuery
keywords: [ hudi, gcp, bigquery ]
summary: Introduce BigQuery integration in Hudi.
---

Hudi tables can be queried from [Google Cloud BigQuery](https://cloud.google.com/bigquery) as external tables. As of
now, the Hudi-BigQuery integration only works for hive-style partitioned Copy-On-Write and Read-Optimized Merge-On-Read tables.

## Sync Modes
### Manifest File
As of version 0.14.0, the `BigQuerySyncTool` supports syncing table to BigQuery using [manifests](https://cloud.google.com/blog/products/data-analytics/bigquery-manifest-file-support-for-open-table-format-queries). On the first run, the tool will create a manifest file representing the current base files in the table and a table in BigQuery based on the provided configurations. The tool produces a new manifest file on each subsequent run and will update the schema of the table in BigQuery if the schema changes in your Hudi table.
#### Benefits of using the new manifest approach:
<ol>
	<li>Only the files in the manifest can be scanned leading to less cost and better performance for your queries</li>
	<li>The schema is now synced from the Hudi commit metadata allowing for proper schema evolution</li>
	<li>Lists no longer have unnecessary nesting when querying in BigQuery as list inference is enabled by default</li>
	<li>Partition column no longer needs to be dropped from the files due to new schema handling improvements</li>
</ol>

To enable this feature, set `hoodie.gcp.bigquery.sync.use_bq_manifest_file` to true.

### View Over Files (Legacy)
This is the current default behavior to preserve compatibility as users upgrade to 0.14.0 and beyond.  
After run, the sync tool will create 2 tables and 1 view in the target dataset in BigQuery. The tables and the view
share the same name prefix, which is taken from the Hudi table name. Query the view for the same results as querying the
Copy-on-Write Hudi table.  
**NOTE:** The view can scan all of the parquet files under your table's base path so it is recommended to upgrade to the manifest based approach for improved cost and performance.

## Configurations

Hudi uses `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` to sync tables. It works with Hudi Streamer via
setting sync tool class. A few BigQuery-specific configurations are required.

| Config                                       | Notes                                                                                                           |
|:---------------------------------------------|:----------------------------------------------------------------------------------------------------------------|
| `hoodie.gcp.bigquery.sync.project_id`        | The target Google Cloud project                                                                                 |
| `hoodie.gcp.bigquery.sync.dataset_name`      | BigQuery dataset name; create before running the sync tool                                                      |
| `hoodie.gcp.bigquery.sync.dataset_location`  | Region info of the dataset; same as the GCS bucket that stores the Hudi table                                   |
| `hoodie.gcp.bigquery.sync.source_uri`        | A wildcard path pattern pointing to the first level partition; partition key can be specified or auto-inferred. Only required for partitioned tables |
| `hoodie.gcp.bigquery.sync.source_uri_prefix` | The common prefix of the `source_uri`, usually it's the path to the Hudi table, trailing slash does not matter. |
| `hoodie.gcp.bigquery.sync.base_path`         | The usual basepath config for Hudi table.                                                                       |
| `hoodie.gcp.bigquery.sync.use_bq_manifest_file` | Set to true to enable the manifest based sync                                                                |
| `hoodie.gcp.bigquery.sync.require_partition_filter` | Introduced in Hudi version 0.14.1, this configuration accepts a BOOLEAN value, with the default being false. When enabled (set to true), you must create a partition filter (a WHERE clause) for all queries, targeting the partitioning column of a partitioned table. Queries lacking such a filter will result in an error.        |


Refer to `org.apache.hudi.gcp.bigquery.BigQuerySyncConfig` for the complete configuration list.
### Partition Handling
In addition to the BigQuery-specific configs, you will need to use hive style partitioning for partition pruning in BigQuery. On top of that, the value in partition path will be the value returned for that field in your query. For example if you partition on a time-millis field, `time`, with an output format of `time=yyyy-MM-dd`, the query will return `time` values with day level granularity instead of the original milliseconds so keep this in mind while setting up your tables.

```
hoodie.datasource.write.hive_style_partitioning = 'true'
```

For the view based sync you must also specify the following configurations:
```
hoodie.datasource.write.drop.partition.columns  = 'true'
hoodie.partition.metafile.use.base.format       = 'true'
```

## Example

Below shows an example for running `BigQuerySyncTool` with Hudi Streamer.

```shell
spark-submit --master yarn \
--packages com.google.cloud:google-cloud-bigquery:2.10.4 \
--jars "/opt/hudi-gcp-bundle-0.13.0.jar,/opt/hudi-utilities-slim-bundle_2.12-1.0.1.jar,/opt/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
--class org.apache.hudi.utilities.streamer.HoodieStreamer \
/opt/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
--target-base-path gs://my-hoodie-table/path \
--target-table mytable \
--table-type COPY_ON_WRITE \
--base-file-format PARQUET \
# ... other Hudi Streamer options
--enable-sync \
--sync-tool-classes org.apache.hudi.gcp.bigquery.BigQuerySyncTool \
--hoodie-conf hoodie.streamer.source.dfs.root=gs://my-source-data/path \
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
```
