---
title: "Release 1.1.0"
layout: releases
toc: true
---

## [Release 1.1.0](https://github.com/apache/hudi/releases/tag/release-1.1.0)

Apache Hudi 1.1.0 is a major release that brings significant performance improvements, new features, and important changes to the platform. This release focuses on enhanced table format support, improved indexing capabilities, expanded engine support, and modernized record merging APIs.

## Highlights

- **Pluggable Table Format Framework** - Native integration of multiple table formats with unified metadata management
- **Spark 4.0 and Flink 2.0 Support** - Full support for latest major compute engine versions
- **Enhanced Indexing** - Non-global Record Index, partition-level bucket index, native HFile writer, and Column Stats V2
- **Table Services Optimization** - Parquet file stitching and incremental scheduling for compaction/clustering
- **Storage-based Lock Provider** - Multi-writer concurrency control without external dependencies
- **Record Merging Evolution** - Deprecation of payload classes in favor of merge modes and merger APIs

---

## New Features

### Table Format

#### Pluggable Table Format Support

Hudi 1.1.0 introduces a new Pluggable Table Format framework that enables native integration of multiple table formats within the system. This foundation includes a base interface for pluggable table formats, designed to simplify extension and allow seamless interoperability across different storage backends. The Metadata Table (MDT) integration has been enhanced to support pluggability, ensuring modularity and unified metadata management across all supported table formats.

This release brings native Apache Hudi integration through the new framework, allowing users to leverage Hudi's advanced capabilities directly while maintaining consistent semantics and performance. The configuration `hoodie.table.format` is set to `native` by default, which works as the Hudi table format. **No configuration changes are required** for existing and new Hudi tables. As additional table formats are supported in future releases, users will be able to set this configuration to work natively with other formats.

#### Table Version 9 with Index Versioning

Hudi 1.1.0 introduces table version 9 with support for index versioning. Indexes in the Metadata Table (column stats, secondary index, expression index, etc) now have version tracking. In 1.1.0, these indexes use V2 layouts with enhanced capabilities including comprehensive logical data type support. Tables migrated from older versions will retain V1 index layouts, while new tables created with 1.1.0 use V2. Both versions remain backward compatible, and no action is required when upgrading to 1.1.0.

### Indexing

#### Non-Global Record Index

In addition to the global Record Index introduced in 0.14.0, Hudi 1.1.0 adds a non-global variant that guarantees uniqueness for partition path and record key pairs. This index speeds up lookups in very large partitioned datasets.

**Prior to 1.1.0**, only global Record Index was available, configured as:

```properties
hoodie.metadata.record.index.enable=true
hoodie.index.type=RECORD_INDEX
```

**From 1.1.0 onwards**, both global and non-global variants are available:

For non-global Record Index:

- Metadata table: `hoodie.metadata.record.level.index.enable=true`
- Write index: `hoodie.index.type=RECORD_LEVEL_INDEX`

For global Record Index:

- Metadata table: `hoodie.metadata.global.record.level.index.enable=true`
- Write index: `hoodie.index.type=GLOBAL_RECORD_LEVEL_INDEX`

#### Partition-Level Bucket Index

A new bucket index type that addresses bucket rescaling challenges. Users can set specific bucket numbers for different partitions through a rule engine (regex pattern matching). Existing Bucket Index tables can be upgraded smoothly and seamlessly.

**Key Configurations**:

- `hoodie.bucket.index.partition.rule.type` - Rule parser for expressions (default: regex)
- `hoodie.bucket.index.partition.expressions` - Expression and bucket number pairs
- `hoodie.bucket.index.num.buckets` - Default bucket count for partitions

For more details, see [RFC-89](https://github.com/apache/hudi/pull/12884/files)

#### Native HFile Writer

Hudi now includes a native HFile writer, eliminating dependencies on HBase while ensuring compatibility with both HBase's HFile reader and Hudi's native reader. This significantly reduces the size of Hudi's binary bundle and enhances Hudi's ability to optimize HFile performance.

#### HFile Performance Enhancements

Multiple enhancements to speed up metadata table reads:

- **HFile Block Cache** (enabled by default): Caches HFile data blocks on repeated reads within the same JVM, showing ~4x speedup in benchmarks. Configure with `hoodie.hfile.block.cache.enabled`
- **HFile Prefetching**: For files under 50MB (configurable via `hoodie.metadata.file.cache.max.size.mb`), downloads entire HFile upfront rather than multiple RPC calls
- **Bloom Filter Support**: Speeds up HFile lookups by avoiding unnecessary block downloads. Configure with `hoodie.metadata.bloom.filter.enable`

#### Column Stats V2 with Enhanced Data Type Support

Column Stats V2 significantly improves support for logical data types during writes and statistics collection. Logical types like decimals (with precision/scale) and timestamps are now preserved with proper metadata, improving accuracy for query planning and predicate pushdown.

### Table Services

#### Parquet File Stitching

An optimization that enables direct copying of RowGroup-level data from Parquet files during operations like clustering, bypassing expensive compression/decompression, encoding/decoding, and column-to-row conversions. This optimization supports proper schema evolution and ensures Hudi metadata are collected and aggregated correctly. Experimental results show a **95% reduction in computational workload** for clustering operations.

#### Incremental Table Service Scheduling

Significantly improves performance of compaction and clustering operations on tables with large numbers of partitions. Enabled by default via `hoodie.table.services.incremental.enabled`.

### Concurrency Control

#### Storage-based Lock Provider

A new storage-based lock provider enables Hudi to manage multi-writer concurrency directly using the `.hoodie` directory in the underlying storage, eliminating the need for external lock providers like DynamoDB or ZooKeeper. Currently supports S3 and GCS, with lock information maintained under `.hoodie/.lock`.

### Writers & Readers

#### Multiple Ordering Fields

Support for multiple ordering (pre-combine) fields using comma-separated lists. When records have the same key, Hudi compares the fields in order and keeps the record with the latest values.

**Configuration**: `hoodie.table.ordering.fields = field1,field2,field3`

#### Efficient Streaming Reads for Data Blocks

Support for efficient streaming reads of HoodieDataBlocks (currently for AvroDataBlock) reduces memory usage, improves read stability on HDFS, and lowers the risk of timeouts and OOM errors when reading log files.

#### ORC Support in FileGroupReader

Enhanced support for multiple base file formats (ORC and Parquet) in HoodieFileGroupReader. The 1.1.0 release introduced SparkColumnarFileReader trait and MultipleColumnarFileFormatReader to uniformly handle ORC and Parquet records for both Merge-on-Read (MOR) and Copy-on-Write (COW) tables.

#### Hive Schema Evolution Support

Hive readers can now handle schema evolution when schema-on-write is used.

### Spark

#### Spark 4.0 Support

Apache Spark 4.0 is now supported with necessary compatibility and dependency changes. Available through the new `hudi-spark4.0-bundle_2.13` release artifact.

#### Metadata Table Streaming Writes

Streaming writes to the metadata table enable more efficient metadata record generation by processing data table and metadata table writes in the same execution chain, avoiding on-demand lookups. Enabled by default for Spark via `hoodie.metadata.streaming.write.enabled`.

#### SQL Procedures Enhancements

**New CLEAN Procedures**:

- `show_cleans` - Displays completed cleaning operations with metadata
- `show_clean_plans` - Shows clean operations in all states (REQUESTED, INFLIGHT, COMPLETED)
- `show_cleans_metadata` - Provides partition-level cleaning details

**Enhanced Capabilities**:

- Regex pattern support in `run_clustering` via `partition_regex_pattern` parameter
- Base path and filter parameters for all non-action `SHOW` procedures with advanced predicate expressions

### Flink

#### Flink 2.0 Support

Full support for Flink 2.0 including sink, read, catalog, and new bundle artifact `hudi-flink2.0-bundle`. Includes compatibility fixes for legacy APIs and supports sinkV2 API by default.

**Deprecation**: Removed support for Flink 1.14, 1.15, and 1.16

#### Performance Improvements

- **Engine-native Record Support**: Eliminates Avro transformations, utilizing RowData directly for more efficient serialization/deserialization. Write/read performance improved by **2-3x on average**
- **Async Instant Time Generation**: Significantly improves stability for high throughput workloads by avoiding blocking on instant time generation
- **Meta Fields Control**: Support for `hoodie.populate.meta.fields` in append mode, showing 14% faster writes when disabled

#### New Capabilities

- **In-memory Buffer Sort**: For pk-less tables, enables better compaction ratio for columnar formats (`write.buffer.sort.enabled`)
- **Split-level Rate Limiting**: Configure maximum splits per instant check for streaming reads (`read.splits.limit`)

### Catalogs

#### Apache Polaris Integration

Integration with Apache Polaris catalog by delegating table creation to the Polaris Spark client, allowing Hudi tables to be registered in the Polaris Catalog.

**Configuration**: `hoodie.datasource.polaris.catalog.class` (default: `org.apache.polaris.spark.SparkCatalog`)

#### AWS Glue & DataHub Sync Enhancements

- CatalogId support for cross-catalog scenarios
- Explicit database and table name configuration
- Resource tagging for Glue databases and tables
- TLS/HTTPS support for DataHub with custom CA certificates and mutual-TLS

### Platform Components

#### Enhanced JSON-to-Avro Conversion for HudiStreamer

Improved JSON-to-Avro conversion layer for better reliability of the Kafka JSON source.

#### Prometheus Multi-table Support

Improved PrometheusReporter with reference-counting mechanism to prevent shared HTTP server shutdown when stopping metrics for individual tables.

---

## API Changes & Deprecations

### Deprecation of HoodieRecordPayload

Payload classes are now **deprecated** in favor of merge modes and the merger API. The payload-based approach was closely tied to Avro-formatted records, making it less compatible with native query engine formats like Spark InternalRow.

**Migration Path**:

- **Standard Use Cases**: Use merge mode configurations (`COMMIT_TIME_ORDERING` or `EVENT_TIME_ORDERING`)
- **Custom Logic**: Implement `HoodieRecordMerger` interface instead of custom payloads
- **Automatic Migration**: When upgrading to the latest table version, known payloads are automatically migrated to appropriate merge modes

The merge mode approach supports:

- Commit time and event time ordering
- Partial update strategies (replaces `OverwriteNonDefaultsWithLatestAvroPayload` and `PostgresDebeziumAvroPayload` toasted value handling)
- Consistent merging semantics across engines
- Performance optimizations

**BufferedRecordMerger API**: The `HoodieRecordMerger` interface has been updated to leverage the new `BufferedRecord` class used during record merging.

For details, see [RFC-97](https://github.com/apache/hudi/pull/13499)

---

## Breaking Changes

### Complex Key Generator Regression Fix

A regression affecting Complex Key Generator with a single record key field has been fixed. The regression was introduced in versions 0.14.1, 0.15.0, and 1.0.x releases, causing record key encoding to change from `field_name:field_value` to `field_value`, which could lead to duplicate records during upserts.

**Who Is Affected**: Tables using Complex Key Generator (`ComplexAvroKeyGenerator` or `ComplexKeyGenerator`) with a **single** record key field.

**Default Behavior in 1.1.0**: Reverts to the correct encoding format (`field_name:field_value`) matching 0.14.0 and earlier.

**Migration Path**:

1. **Upgrading from ≤0.14.0**: No action needed, default behavior is correct
2. **New tables created in 0.15.0/1.0.x**: Set `hoodie.write.complex.keygen.new.encoding=true` to maintain current encoding
3. **Upgraded from 0.14.0 → 0.15.0/1.0.x**: If you experienced duplicates, data repair may be needed. A repair tool is expected in 1.1.1

**Validation**: By default, writes will validate table configuration and alert you if affected. Disable with `hoodie.write.complex.keygen.validation.enable=false` if needed (readers are unaffected).

**Note on Table Version 9**: For tables upgraded to version 9, key encoding format is locked to prevent future regressions and ensure correct behavior for Record Index and Secondary Index.

For detailed technical information, see the [PR documentation](https://github.com/apache/hudi/pull/13650/).

### Removal of Auto-Commit Support in WriteClient

For developers using `HoodieWriteClient` directly, auto-commit support has been removed. The `hoodie.auto.commit` configuration is no longer honored.

**Migration Required**:

```java
// Previous (with auto-commit)
HoodieWriteConfig config = // instantiate config
SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, config);
String instantTime = writeClient.startCommit();
writeClient.upsert(JavaRDD<HoodieRecord>, instantTime);

// Now Required (explicit commit)
HoodieWriteConfig config = // instantiate config
SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, config);
String instantTime = writeClient.startCommit();
JavaRDD<WriteStatus> writeStatus = writeClient.upsert(RDD<HoodieRecord>, instantTime);
writeClient.commit(instantTime, writeStatus); // Explicit commit now mandatory
```

This change applies to table services as well.

### INSERT INTO Behavior Change

The default behavior of Spark SQL's `INSERT INTO` command has changed. Previously, it used "upsert" operation for tables with precombine/ordering fields, which caused deduplication. From 1.1.0, `INSERT INTO` now performs "insert" operation by default, ingesting records as-is without deduplication.

**Example**:

```text
Commit1: 
  Partition1, recordKey1, val1, orderingValue1
  Partition1, recordKey1, val2, orderingValue2

Pre-1.1.0: Returns one record (based on ordering field)
From 1.1.0: Returns both records
```

**To Restore Previous Behavior**: Set `hoodie.spark.sql.insert.into.operation = upsert`

### Restriction on Multiple Bulk Inserts for COW with Simple Bucket

Multiple `bulk_insert` operations are now restricted for Copy-On-Write tables when **all** of the following conditions are met:

1. Table type is COW
2. Index type is `BUCKET` with engine set to `SIMPLE`
3. Spark native row writer is disabled (`hoodie.datasource.write.row.writer.enable = false`)

**Resolution**: Use `upsert` operation after the initial `bulk_insert`, or enable Spark native row writer.

### Flink Bucket Index Restriction

Bucket index now supports only UPSERT operations and cannot be used with append write mode in Flink.

---

## Behavior Changes

### Default Merge Mode Changed

The default record merging behavior has changed based on whether a precombine field is set:

- **No Precombine Field**: Defaults to `COMMIT_TIME_ORDERING` using `OverwriteWithLatestAvroPayload` (last-write-wins)
- **Precombine Field Set**: Defaults to `EVENT_TIME_ORDERING` using `DefaultHoodieRecordPayload` (classic event-time based comparison)

The default value for `hoodie.datasource.write.precombine.field` (previously `ts`) has been removed.

**Why This Change**: The previous default required users to have a `ts` field in their schema even for simple upsert cases where last-write-wins was sufficient. The new behavior makes Hudi work out-of-the-box for simple upserts while still supporting event-time ordering when explicitly configured.

**Impact on HoodieStreamer**: The `--source-ordering-field` parameter no longer defaults to `ts`.

- **Before**: Automatically enabled `EVENT_TIME_ORDERING`
- **Now**: Defaults to `COMMIT_TIME_ORDERING` if not specified
- **Action Required**: For new Deltastreamer jobs needing event-time merging, explicitly provide `--source-ordering-field ts` (or your ordering field)

**Validation Changes**: Configuration mismatches now log warnings instead of failing jobs:

- Using `COMMIT_TIME_ORDERING` with a precombine field logs a warning that the field is ignored
- Using `EVENT_TIME_ORDERING` without a precombine field logs a warning instead of failing

### Incremental Query Fallback Behavior

The default value for `hoodie.datasource.read.incr.fallback.fulltablescan.enable` changed from `false` to `true`. Incremental queries that cannot find necessary commit/data files will now automatically fall back to a full table scan instead of failing.

**Performance Impact**: Queries that previously failed fast will now succeed but may run significantly slower due to full table scans.

**To Restore Fail-fast Behavior**: Set `hoodie.datasource.read.incr.fallback.fulltablescan.enable = false`

### Incremental Query Start Time Semantics

The `hoodie.datasource.read.begin.instanttime` configuration behavior has been reverted to be **exclusive** (matching 0.x behavior), correcting the inclusive behavior introduced in 1.0.0.

### Timestamp Logical Type Handling

With Column Stats V2, timestamp fields are now correctly handled as `timestamp_millis` instead of `timestamp_micros` (as in 0.15.0 and earlier 1.x releases). The release maintains backward compatibility when reading tables written with older versions.

### Flink Bucket Index Append Mode Restriction

Bucket index now supports only UPSERT operations and cannot be used with append write mode in Flink.

---

## Upgrade Notes

1. **Review Complex Key Generator Usage**: Users upgrading from 0.14.0 or earlier should review their configurations
2. **Update WriteClient Usage**: If using `HoodieWriteClient` directly, add explicit commit calls
3. **Check INSERT INTO Behavior**: Verify if your pipelines depend on the old upsert behavior
4. **Review Merge Mode Configurations**: Especially for new tables or when using HoodieStreamer
5. **Test Incremental Queries**: Be aware of new fallback behavior and potential performance impacts
6. **Flink Version**: Ensure you're using Flink 1.17+ or 2.0 (1.14-1.16 no longer supported)

---

## Version Support

### End of Life for Versions Prior to 0.14.0

As of this release, Apache Hudi versions prior to 0.14.0 have reached end of life. Users on these older versions should plan to upgrade to 1.1.0 or later to receive ongoing support, bug fixes, and new features. The Hudi community will focus support efforts on versions 0.14.0 and later.

For more details, see the [community discussion](https://github.com/apache/hudi/discussions/13847).

---

## Contributors

Hudi 1.1.0 is the result of contributions from the entire Apache Hudi community. We thank all contributors who made this release possible.
