---
title: Configurations
keywords: [garbage collection, hudi, jvm, configs, tuning]
summary: This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.
toc: true
last_modified_at: 2021-07-24T00:48:18.710466
---

This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels.

- [**Spark Datasource Configs**](#SPARK_DATASOURCE): These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.
- [**Flink Sql Configs**](#FLINK_SQL): These configs control the Hudi Flink SQL source/sink connectors, providing ability to define record keys, pick out the write operation, specify how to merge records, enable/disable asynchronous compaction or choosing query type to read.
- [**Write Client Configs**](#WRITE_CLIENT): Internally, the Hudi datasource uses a RDD based HoodieWriteClient API to actually perform writes to storage. These configs provide deep control over lower level aspects like file sizing, compression, parallelism, compaction, write schema, cleaning etc. Although Hudi provides sane defaults, from time-time these configs may need to be tweaked to optimize for specific workloads.
- [**Metrics Configs**](#METRICS): These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.
- [**Record Payload Config**](#RECORD_PAYLOAD): This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

## Metrics Configs {#METRICS}
These set of configs are used to enable monitoring and reporting of keyHudi stats and metrics.

### Metrics Configurations for Datadog reporter {#Metrics-Configurations-for-Datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsDatadogConfig<br/>
> #### hoodie.metrics.datadog.metric.tags
> Datadog metric tags (comma-delimited) to be sent along with metrics data.<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_METRIC_TAGS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.api.key.supplier
> Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set.<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_API_KEY_SUPPLIER`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.metric.prefix
> Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended.<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_METRIC_PREFIX`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.api.timeout.seconds
> Datadog API timeout in seconds. Default to 3.<br/>
> **Default Value**: 3 (Optional)<br/>
> `Config Param: DATADOG_API_TIMEOUT_SECONDS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.report.period.seconds
> Datadog reporting period in seconds. Default to 30.<br/>
> **Default Value**: 30 (Optional)<br/>
> `Config Param: DATADOG_REPORT_PERIOD_SECONDS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.metric.host
> Datadog metric host to be sent along with metrics data.<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_METRIC_HOST`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.api.key.skip.validation
> Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.<br/>
> **Default Value**: false (Optional)<br/>
> `Config Param: DATADOG_API_KEY_SKIP_VALIDATION`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.api.site
> Datadog API site: EU or US<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_API_SITE`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.datadog.api.key
> Datadog API key<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: DATADOG_API_KEY`<br/>
> `Since Version: 0.6.0`<br/>

---

### Metrics Configurations for Prometheus {#Metrics-Configurations-for-Prometheus}

Enables reporting on Hudi metrics using Prometheus.  Hudi publishes metrics on every commit, clean, rollback etc.

`Config Class`: org.apache.hudi.config.HoodieMetricsPrometheusConfig<br/>
> #### hoodie.metrics.pushgateway.host
> Hostname of the prometheus push gateway<br/>
> **Default Value**: localhost (Optional)<br/>
> `Config Param: PUSHGATEWAY_HOST`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.pushgateway.delete.on.shutdown
> <br/>
> **Default Value**: true (Optional)<br/>
> `Config Param: PUSHGATEWAY_DELETE_ON_SHUTDOWN`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.prometheus.port
> Port for prometheus server.<br/>
> **Default Value**: 9090 (Optional)<br/>
> `Config Param: PROMETHEUS_PORT`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.pushgateway.random.job.name.suffix
> <br/>
> **Default Value**: true (Optional)<br/>
> `Config Param: PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.pushgateway.report.period.seconds
> Reporting interval in seconds.<br/>
> **Default Value**: 30 (Optional)<br/>
> `Config Param: PUSHGATEWAY_REPORT_PERIOD_SECONDS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.pushgateway.job.name
> Name of the push gateway job.<br/>
> **Default Value**:  (Optional)<br/>
> `Config Param: PUSHGATEWAY_JOB_NAME`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.metrics.pushgateway.port
> Port for the push gateway.<br/>
> **Default Value**: 9091 (Optional)<br/>
> `Config Param: PUSHGATEWAY_PORT`<br/>
> `Since Version: 0.6.0`<br/>

---

### Metrics Configurations {#Metrics-Configurations}

Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.

`Config Class`: org.apache.hudi.config.HoodieMetricsConfig<br/>
> #### hoodie.metrics.jmx.host
> Jmx host to connect to<br/>
> **Default Value**: localhost (Optional)<br/>
> `Config Param: JMX_HOST`<br/>
> `Since Version: 0.5.1`<br/>

---

> #### hoodie.metrics.executor.enable
> <br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: ENABLE_EXECUTOR_METRICS`<br/>
> `Since Version: 0.7.0`<br/>

---

> #### hoodie.metrics.jmx.port
> Jmx port to connect to<br/>
> **Default Value**: 9889 (Optional)<br/>
> `Config Param: JMX_PORT`<br/>
> `Since Version: 0.5.1`<br/>

---

> #### hoodie.metrics.graphite.host
> Graphite host to connect to<br/>
> **Default Value**: localhost (Optional)<br/>
> `Config Param: GRAPHITE_SERVER_HOST`<br/>
> `Since Version: 0.5.0`<br/>

---

> #### hoodie.metrics.on
> Turn on/off metrics reporting. off by default.<br/>
> **Default Value**: false (Optional)<br/>
> `Config Param: METRICS_ON`<br/>
> `Since Version: 0.5.0`<br/>

---

> #### hoodie.metrics.graphite.metric.prefix
> Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: GRAPHITE_METRIC_PREFIX`<br/>
> `Since Version: 0.5.1`<br/>

---

> #### hoodie.metrics.graphite.port
> Graphite port to connect to<br/>
> **Default Value**: 4756 (Optional)<br/>
> `Config Param: GRAPHITE_SERVER_PORT`<br/>
> `Since Version: 0.5.0`<br/>

---

> #### hoodie.metrics.reporter.type
> Type of metrics reporter.<br/>
> **Default Value**: GRAPHITE (Optional)<br/>
> `Config Param: METRICS_REPORTER_TYPE`<br/>
> `Since Version: 0.5.0`<br/>

---

> #### hoodie.metrics.reporter.class
> <br/>
> **Default Value**:  (Optional)<br/>
> `Config Param: METRICS_REPORTER_CLASS`<br/>
> `Since Version: 0.6.0`<br/>

---

## Record Payload Config {#RECORD_PAYLOAD}
This is the lowest level of customization offered by Hudi. Record payloads define how to produce new values to upsert based on incoming new record and stored old record. Hudi provides default implementations such as OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. This can be overridden to a custom class extending HoodieRecordPayload class, on both datasource and WriteClient levels.

### Payload Configurations {#Payload-Configurations}

Payload related configs, that can be leveraged to control merges based on specific business fields in the data.

`Config Class`: org.apache.hudi.config.HoodiePayloadConfig<br/>
> #### hoodie.payload.event.time.field
> Table column/field name to derive timestamp associated with the records. This canbe useful for e.g, determining the freshness of the table.<br/>
> **Default Value**: ts (Optional)<br/>
> `Config Param: PAYLOAD_EVENT_TIME_FIELD_PROP`<br/>

---

> #### hoodie.payload.ordering.field
> Table column/field name to order records that have the same key, before merging and writing to storage.<br/>
> **Default Value**: ts (Optional)<br/>
> `Config Param: PAYLOAD_ORDERING_FIELD_PROP`<br/>

---

## Spark Datasource Configs {#SPARK_DATASOURCE}
These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge records or choosing query type to read.

### Read Options {#Read-Options}

Options useful for reading tables via `read.format.option(...)`


`Config Class`: org.apache.hudi.DataSourceOptions.scala<br/>
> #### hoodie.file.index.enable
> Enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.<br/>
> **Default Value**: true (Optional)<br/>
> `Config Param: ENABLE_HOODIE_FILE_INDEX`<br/>

---

> #### hoodie.datasource.merge.type
> For Snapshot query on merge on read table, control whether we invoke the record payload implementation to merge (payload_combine) or skip merging altogetherskip_merge<br/>
> **Default Value**: payload_combine (Optional)<br/>
> `Config Param: REALTIME_MERGE_OPT_KEY`<br/>

---

> #### hoodie.datasource.read.incr.path.glob
> For the use-cases like users only want to incremental pull from certain partitions instead of the full table. This option allows using glob pattern to directly filter on path.<br/>
> **Default Value**:  (Optional)<br/>
> `Config Param: INCR_PATH_GLOB_OPT_KEY`<br/>

---

> #### hoodie.datasource.query.type
> Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode (obtain latest view, by merging base and (if any) log files)<br/>
> **Default Value**: snapshot (Optional)<br/>
> `Config Param: QUERY_TYPE_OPT_KEY`<br/>

---

> #### hoodie.datasource.write.precombine.field
> Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)<br/>
> **Default Value**: ts (Optional)<br/>
> `Config Param: READ_PRE_COMBINE_FIELD`<br/>

---

> #### hoodie.datasource.read.end.instanttime
> Instant time to limit incrementally fetched data to. New data written with an instant_time &lt; 0.3.1. Will be removed eventually<br/>
> **Default Value**: false (Optional)<br/>
> `Config Param: HOODIE_ASSUME_DATE_PARTITIONING_PROP`<br/>
> `Since Version: 0.3.0`<br/>

---

> #### hoodie.metadata.keep.max.commits
> Controls the archival of the metadata table’s timeline.<br/>
> **Default Value**: 30 (Optional)<br/>
> `Config Param: MAX_COMMITS_TO_KEEP_PROP`<br/>
> `Since Version: 0.7.0`<br/>

---

> #### hoodie.metadata.dir.filter.regex
> Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.<br/>
> **Default Value**:  (Optional)<br/>
> `Config Param: DIRECTORY_FILTER_REGEX`<br/>
> `Since Version: 0.7.0`<br/>

---

> #### hoodie.metadata.validate
> Validate contents of metadata table on each access; e.g against the actual listings from lake storage<br/>
> **Default Value**: false (Optional)<br/>
> `Config Param: METADATA_VALIDATE_PROP`<br/>
> `Since Version: 0.7.0`<br/>

---

> #### hoodie.metadata.clean.async
> Enable asynchronous cleaning for metadata table<br/>
> **Default Value**: false (Optional)<br/>
> `Config Param: METADATA_ASYNC_CLEAN_PROP`<br/>
> `Since Version: 0.7.0`<br/>

---

> #### hoodie.file.listing.parallelism
> Parallelism to use, when listing the table on lake storage.<br/>
> **Default Value**: 1500 (Optional)<br/>
> `Config Param: FILE_LISTING_PARALLELISM_PROP`<br/>
> `Since Version: 0.7.0`<br/>

---

### Bootstrap Configs {#Bootstrap-Configs}

Configurations that control how you want to bootstrap your existing tables for the first time into hudi. The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing  writers and new hudi writers in parallel, to validate the migration.

`Config Class`: org.apache.hudi.config.HoodieBootstrapConfig<br/>
> #### hoodie.bootstrap.partitionpath.translator.class
> Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.<br/>
> **Default Value**: org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator (Optional)<br/>
> `Config Param: BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.keygen.class
> Key generator implementation to be used for generating keys from the bootstrapped dataset<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: BOOTSTRAP_KEYGEN_CLASS`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.mode.selector
> Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped<br/>
> **Default Value**: org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector (Optional)<br/>
> `Config Param: BOOTSTRAP_MODE_SELECTOR`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.keygen.type
> Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE<br/>
> **Default Value**: SIMPLE (Optional)<br/>
> `Config Param: BOOTSTRAP_KEYGEN_TYPE`<br/>
> `Since Version: 0.9.0`<br/>

---

> #### hoodie.bootstrap.mode.selector.regex.mode
> Bootstrap mode to apply for partition paths, that match regex above. METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.<br/>
> **Default Value**: METADATA_ONLY (Optional)<br/>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX_MODE`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.index.class
> Implementation to use, for mapping a skeleton base file to a boostrap base file.<br/>
> **Default Value**: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex (Optional)<br/>
> `Config Param: BOOTSTRAP_INDEX_CLASS_PROP`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.full.input.provider
> Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD<br/>
> **Default Value**: org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider (Optional)<br/>
> `Config Param: FULL_BOOTSTRAP_INPUT_PROVIDER`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.parallelism
> Parallelism value to be used to bootstrap data into hudi<br/>
> **Default Value**: 1500 (Optional)<br/>
> `Config Param: BOOTSTRAP_PARALLELISM`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.mode.selector.regex
> Matches each bootstrap dataset partition against this regex and applies the mode below to it.<br/>
> **Default Value**: .* (Optional)<br/>
> `Config Param: BOOTSTRAP_MODE_SELECTOR_REGEX`<br/>
> `Since Version: 0.6.0`<br/>

---

> #### hoodie.bootstrap.base.path
> Base path of the dataset that needs to be bootstrapped as a Hudi table<br/>
> **Default Value**: N/A (Required)<br/>
> `Config Param: BOOTSTRAP_BASE_PATH_PROP`<br/>
> `Since Version: 0.6.0`<br/>

---

