---
title: DataHub
keywords: [hudi, datahub, sync]
---

[DataHub](https://datahubproject.io/) is a rich metadata platform that supports features like data discovery, data
obeservability, federated governance, etc.

In Hudi 0.11.0, you can now sync to a DataHub instance by setting `DataHubSyncTool` as one of the sync tool classes
for `HoodieDeltaStreamer`.

The target Hudi table will be sync'ed to DataHub as a `Dataset`. The Hudi table's avro schema will be sync'ed, along
with the commit timestamp when running the sync.

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

### Example

The following shows an example configuration to run `HoodieDeltaStreamer` with `DataHubSyncTool`.

In addition to `hudi-utilities-bundle` that contains `HoodieDeltaStreamer`, you also add `hudi-datahub-sync-bundle` to
the classpath.

```shell
spark-submit --master yarn \
--jars /opt/hudi-datahub-sync-bundle-0.11.0.jar \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
/opt/hudi-utilities-bundle_2.12-0.11.0.jar \
--target-table mytable \
# ... other HoodieDeltaStreamer's configs
--enable-sync \
--sync-tool-classes org.apache.hudi.sync.datahub.DataHubSyncTool \
--hoodie-conf hoodie.meta.sync.datahub.emitter.server=http://url-to-datahub-instance:8080 \
--hoodie-conf hoodie.datasource.hive_sync.database=mydb \
--hoodie-conf hoodie.datasource.hive_sync.table=mytable \
```
