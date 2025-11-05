---
title: DataHub
keywords: [hudi, datahub, sync]
---

[DataHub](https://datahub.com/) is a rich metadata platform that supports features like data discovery, data
obeservability, federated governance, etc.

Since Hudi 0.11.0, you can now sync to a DataHub instance by setting `DataHubSyncTool` as one of the sync tool classes
for `HoodieStreamer`.

The target Hudi table will be sync'ed to DataHub as a `Dataset`, which will be created with the following properties:

* Hudi table properties and partitioning information
  * This includes: `hudi.table.type`, `hudi.table.version` and `hudi.base.path`
  * As well as: `hudi.partition.fields` (if and only if `hoodie.datasource.hive_sync.partition_fields` is properly set 
    in the Hudi Config)
* Spark-related properties
* User-defined properties (see `hoodie.meta.sync.datahub.table.properties` in the "Configurations" section)
* The last commit and the last commit completion timestamps

Additionally, the `Dataset` object will include the following metadata:

* sub-type as `Table`
* browse path
* parent container
* Avro schema
* optionally, attached with a `Domain` object

Also, the parent database will be sync'ed to DataHub as a `Container`, including the following metadata:

* sub-type as `Database`
* browse paths
* optionally, attached with a `Domain` object

### Configurations

`DataHubSyncTool` makes use of DataHub's Java Emitter to send the metadata via HTTP REST APIs. It is required to
set `hoodie.meta.sync.datahub.emitter.server` to the URL of the DataHub instance for sync.

If needs auth token, set `hoodie.meta.sync.datahub.emitter.token`.

If needs customized creation of the emitter object,
implement `org.apache.hudi.sync.datahub.config.DataHubEmitterSupplier` and supply the implementation's FQCN
to `hoodie.meta.sync.datahub.emitter.supplier.class`.

By default, the sync config's database name and table name will be used to make the target `Dataset`'s URN.
Subclass `HoodieDataHubDatasetIdentifier` and set it to `hoodie.meta.sync.datahub.dataset.identifier.class` to customize
the URN creation.

Optionally, sync'ed `Dataset` and `Container` objects can be attached with a `Domain` object. To do this, set
`hoodie.meta.sync.datahub.domain.name` to a valid `Domain` URN. Also, sync'ed `Dataset` can be attached with 
user defined properties. To do this, set `hoodie.meta.sync.datahub.table.properties` to a comma-separated key-value
string (_eg_ `key1=val1,key2=val2`).

### Example

The following shows an example configuration to run `HoodieStreamer` with `DataHubSyncTool`.

In addition to `hudi-utilities-slim-bundle` that contains `HoodieStreamer`, you also add `hudi-datahub-sync-bundle` to
the classpath.

```shell
spark-submit --master yarn \
--packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.1,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1 \
--jars /opt/hudi-datahub-sync-bundle-1.0.1.jar \
--class org.apache.hudi.utilities.streamer.HoodieStreamer \
/opt/hudi-utilities-slim-bundle_2.12-1.0.1.jar \
--target-table mytable \
# ... other HoodieStreamer's configs
--enable-sync \
--sync-tool-classes org.apache.hudi.sync.datahub.DataHubSyncTool \
--hoodie-conf hoodie.meta.sync.datahub.emitter.server=http://url-to-datahub-instance:8080 \
--hoodie-conf hoodie.datasource.hive_sync.database=mydb \
--hoodie-conf hoodie.datasource.hive_sync.table=mytable \
```
