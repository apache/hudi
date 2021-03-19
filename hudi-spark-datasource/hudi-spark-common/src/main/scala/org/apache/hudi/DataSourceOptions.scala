/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.HiveSyncTool
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config
import org.apache.hudi.keygen.{CustomKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.{DataSourceUtils => SparkDataSourceUtils}

/**
  * List of options that can be passed to the Hoodie datasource,
  * in addition to the hoodie client configs
  */

/**
  * Options supported for reading hoodie tables.
  */
object DataSourceReadOptions {

  private val log = LogManager.getLogger(DataSourceReadOptions.getClass)

  /**
    * Whether data needs to be read, in
    *
    * 1) Snapshot mode (obtain latest view, based on row & columnar data)
    * 2) incremental mode (new data since an instantTime)
    * 3) Read Optimized mode (obtain latest view, based on columnar data)
    *
    * Default: snapshot
    */
  val QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type"
  val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
  val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  val DEFAULT_QUERY_TYPE_OPT_VAL: String = QUERY_TYPE_SNAPSHOT_OPT_VAL

  /**
   * For Snapshot query on merge on read table. Use this key to define the payload class.
   */
  val REALTIME_MERGE_OPT_KEY = "hoodie.datasource.merge.type"
  val REALTIME_SKIP_MERGE_OPT_VAL = "skip_merge"
  val REALTIME_PAYLOAD_COMBINE_OPT_VAL = "payload_combine"
  val DEFAULT_REALTIME_MERGE_OPT_VAL = REALTIME_PAYLOAD_COMBINE_OPT_VAL

  val READ_PATHS_OPT_KEY = "hoodie.datasource.read.paths"

  val READ_PRE_COMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_PROP

  @Deprecated
  val VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
  @Deprecated
  val VIEW_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  @Deprecated
  val VIEW_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  @Deprecated
  val VIEW_TYPE_REALTIME_OPT_VAL = "realtime"
  @Deprecated
  val DEFAULT_VIEW_TYPE_OPT_VAL = VIEW_TYPE_READ_OPTIMIZED_OPT_VAL

  /**
    * This eases migration from old configs to new configs.
    */
  def translateViewTypesToQueryTypes(optParams: Map[String, String]) : Map[String, String] = {
    val translation = Map(VIEW_TYPE_READ_OPTIMIZED_OPT_VAL -> QUERY_TYPE_SNAPSHOT_OPT_VAL,
                          VIEW_TYPE_INCREMENTAL_OPT_VAL -> QUERY_TYPE_INCREMENTAL_OPT_VAL,
                          VIEW_TYPE_REALTIME_OPT_VAL -> QUERY_TYPE_SNAPSHOT_OPT_VAL)
    if (!optParams.contains(QUERY_TYPE_OPT_KEY)) {
      if (optParams.contains(VIEW_TYPE_OPT_KEY)) {
        log.warn(VIEW_TYPE_OPT_KEY + " is deprecated and will be removed in a later release. Please use " + QUERY_TYPE_OPT_KEY)
        optParams ++ Map(QUERY_TYPE_OPT_KEY -> translation(optParams(VIEW_TYPE_OPT_KEY)))
      } else {
        optParams ++ Map(QUERY_TYPE_OPT_KEY -> DEFAULT_QUERY_TYPE_OPT_VAL)
      }
    } else {
      optParams
    }
  }

  /**
    * Instant time to start incrementally pulling data from. The instanttime here need not
    * necessarily correspond to an instant on the timeline. New data written with an
    * `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
    * all new data written after Sep 1, 2017 08:00AM.
    *
    * Default: None (Mandatory in incremental mode)
    */
  val BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"


  /**
    * Instant time to limit incrementally fetched data to. New data written with an
    * `instant_time <= END_INSTANTTIME` are fetched out.
    *
    * Default: latest instant (i.e fetches all new data since begin instant time)
    *
    */
  val END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"

  /**
    * If use the end instant schema when incrementally fetched data to.
    *
    * Default: false (use latest instant schema)
    *
    */
  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.schema.use.end.instanttime"
  val DEFAULT_INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_VAL = "false"

  /**
    * For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies opaque map functions,
    * filters appearing late in the sequence of transformations cannot be automatically pushed down.
    * This option allows setting filters directly on Hoodie Source
    */
  val PUSH_DOWN_INCR_FILTERS_OPT_KEY = "hoodie.datasource.read.incr.filters"
  val DEFAULT_PUSH_DOWN_FILTERS_OPT_VAL = ""

  /**
   * For the use-cases like users only want to incremental pull from certain partitions instead of the full table.
   * This option allows using glob pattern to directly filter on path.
   */
  val INCR_PATH_GLOB_OPT_KEY = "hoodie.datasource.read.incr.path.glob"
  val DEFAULT_INCR_PATH_GLOB_OPT_VAL = ""
}

/**
  * Options supported for writing hoodie tables.
  */
object DataSourceWriteOptions {

  private val log = LogManager.getLogger(DataSourceWriteOptions.getClass)

  /**
    * The write operation, that this write should do
    *
    * Default: upsert()
    */
  val OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
  val BULK_INSERT_OPERATION_OPT_VAL = WriteOperationType.BULK_INSERT.value
  val INSERT_OPERATION_OPT_VAL = WriteOperationType.INSERT.value
  val UPSERT_OPERATION_OPT_VAL = WriteOperationType.UPSERT.value
  val DELETE_OPERATION_OPT_VAL = WriteOperationType.DELETE.value
  val BOOTSTRAP_OPERATION_OPT_VAL = WriteOperationType.BOOTSTRAP.value
  val INSERT_OVERWRITE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE.value
  val INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE_TABLE.value
  val DEFAULT_OPERATION_OPT_VAL = UPSERT_OPERATION_OPT_VAL

  /**
    * The table type for the underlying data, for this write.
    * Note that this can't change across writes.
    *
    * Default: COPY_ON_WRITE
    */
  val TABLE_TYPE_OPT_KEY = "hoodie.datasource.write.table.type"
  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val DEFAULT_TABLE_TYPE_OPT_VAL = COW_TABLE_TYPE_OPT_VAL

  @Deprecated
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  @Deprecated
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  @Deprecated
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  @Deprecated
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  def translateStorageTypeToTableType(optParams: Map[String, String]) : Map[String, String] = {
    if (optParams.contains(STORAGE_TYPE_OPT_KEY) && !optParams.contains(TABLE_TYPE_OPT_KEY)) {
      log.warn(STORAGE_TYPE_OPT_KEY + " is deprecated and will be removed in a later release; Please use " + TABLE_TYPE_OPT_KEY)
      optParams ++ Map(TABLE_TYPE_OPT_KEY -> optParams(STORAGE_TYPE_OPT_KEY))
    } else {
      optParams
    }
  }

  /**
    * Translate spark parameters to hudi parameters
    *
    * @param optParams Parameters to be translated
    * @return Parameters after translation
    */
  def translateSqlOptions(optParams: Map[String, String]): Map[String, String] = {
    var translatedOptParams = optParams
    // translate the api partitionBy of spark DataFrameWriter to PARTITIONPATH_FIELD_OPT_KEY
    if (optParams.contains(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)) {
      val partitionColumns = optParams.get(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(SparkDataSourceUtils.decodePartitioningColumns)
        .getOrElse(Nil)
      val keyGeneratorClass = optParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY,
        DataSourceWriteOptions.DEFAULT_KEYGENERATOR_CLASS_OPT_VAL)

      val partitionPathField =
        keyGeneratorClass match {
          // Only CustomKeyGenerator needs special treatment, because it needs to be specified in a way
          // such as "field1:PartitionKeyType1,field2:PartitionKeyType2".
          // partitionBy can specify the partition like this: partitionBy("p1", "p2:SIMPLE", "p3:TIMESTAMP")
          case c if c == classOf[CustomKeyGenerator].getName =>
            partitionColumns.map(e => {
              if (e.contains(":")) {
                e
              } else {
                s"$e:SIMPLE"
              }
            }).mkString(",")
          case _ =>
            partitionColumns.mkString(",")
        }
      translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD_OPT_KEY -> partitionPathField)
    }
    translatedOptParams
  }

  /**
    * Hive table name, to register the table into.
    *
    * Default:  None (mandatory)
    */
  val TABLE_NAME_OPT_KEY = "hoodie.datasource.write.table.name"

  /**
    * Field used in preCombining before actual write. When two records have the same
    * key value, we will pick the one with the largest value for the precombine field,
    * determined by Object.compareTo(..)
    */
  val PRECOMBINE_FIELD_OPT_KEY = HoodieWriteConfig.PRECOMBINE_FIELD_PROP
  val DEFAULT_PRECOMBINE_FIELD_OPT_VAL = "ts"

  /**
    * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
    * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
    */
  val PAYLOAD_CLASS_OPT_KEY = HoodieWriteConfig.WRITE_PAYLOAD_CLASS
  val DEFAULT_PAYLOAD_OPT_VAL = classOf[OverwriteWithLatestAvroPayload].getName

  /**
    * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
    * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
    * the dot notation eg: `a.b.c`
    *
    */
  val RECORDKEY_FIELD_OPT_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY
  val DEFAULT_RECORDKEY_FIELD_OPT_VAL = "uuid"

  /**
    * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
    * value obtained by invoking .toString()
    */
  val PARTITIONPATH_FIELD_OPT_KEY = KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY
  val DEFAULT_PARTITIONPATH_FIELD_OPT_VAL = "partitionpath"

  /**
    * Flag to indicate whether to use Hive style partitioning.
    * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
    * By default false (the names of partition folders are only partition values)
    */
  val HIVE_STYLE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY
  val DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = KeyGeneratorOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL
  val URL_ENCODE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.URL_ENCODE_PARTITIONING_OPT_KEY
  val DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL = KeyGeneratorOptions.DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL
  /**
    * Key generator class, that implements will extract the key out of incoming record
    *
    */
  val KEYGENERATOR_CLASS_OPT_KEY = HoodieWriteConfig.KEYGENERATOR_CLASS_PROP
  val DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = classOf[SimpleKeyGenerator].getName

  /**
   * When set to true, will perform write operations directly using the spark native `Row` representation.
   * By default, false (will be enabled as default in a future release)
   */
  val ENABLE_ROW_WRITER_OPT_KEY = "hoodie.datasource.write.row.writer.enable"
  val DEFAULT_ENABLE_ROW_WRITER_OPT_VAL = "false"

  /**
    * Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
    * This is useful to store checkpointing information, in a consistent way with the hoodie timeline
    */
  val COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix"
  val DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL = "_"

  /**
    * Flag to indicate whether to drop duplicates upon insert.
    * By default insert will accept duplicates, to gain extra performance.
    */
  val INSERT_DROP_DUPS_OPT_KEY = "hoodie.datasource.write.insert.drop.duplicates"
  val DEFAULT_INSERT_DROP_DUPS_OPT_VAL = "false"

  /**
    * Flag to indicate how many times streaming job should retry for a failed microbatch
    * By default 3
    */
  val STREAMING_RETRY_CNT_OPT_KEY = "hoodie.datasource.write.streaming.retry.count"
  val DEFAULT_STREAMING_RETRY_CNT_OPT_VAL = "3"

  /**
    * Flag to indicate how long (by millisecond) before a retry should issued for failed microbatch
    * By default 2000 and it will be doubled by every retry
    */
  val STREAMING_RETRY_INTERVAL_MS_OPT_KEY = "hoodie.datasource.write.streaming.retry.interval.ms"
  val DEFAULT_STREAMING_RETRY_INTERVAL_MS_OPT_VAL = "2000"

  /**
    * Flag to indicate whether to ignore any non exception error (e.g. writestatus error)
    * within a streaming microbatch
    * By default true (in favor of streaming progressing over data integrity)
    */
  val STREAMING_IGNORE_FAILED_BATCH_OPT_KEY = "hoodie.datasource.write.streaming.ignore.failed.batch"
  val DEFAULT_STREAMING_IGNORE_FAILED_BATCH_OPT_VAL = "true"
  val META_SYNC_CLIENT_TOOL_CLASS = "hoodie.meta.sync.client.tool.class"
  val DEFAULT_META_SYNC_CLIENT_TOOL_CLASS = classOf[HiveSyncTool].getName

  // HIVE SYNC SPECIFIC CONFIGS
  //NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset
  val HIVE_SYNC_ENABLED_OPT_KEY = "hoodie.datasource.hive_sync.enable"
  val META_SYNC_ENABLED_OPT_KEY = "hoodie.datasource.meta.sync.enable"
  val HIVE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.database"
  val HIVE_TABLE_OPT_KEY = "hoodie.datasource.hive_sync.table"
  val HIVE_BASE_FILE_FORMAT_OPT_KEY = "hoodie.datasource.hive_sync.base_file_format"
  val HIVE_USER_OPT_KEY = "hoodie.datasource.hive_sync.username"
  val HIVE_PASS_OPT_KEY = "hoodie.datasource.hive_sync.password"
  val HIVE_URL_OPT_KEY = "hoodie.datasource.hive_sync.jdbcurl"
  val HIVE_PARTITION_FIELDS_OPT_KEY = "hoodie.datasource.hive_sync.partition_fields"
  val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.hive_sync.partition_extractor_class"
  val HIVE_ASSUME_DATE_PARTITION_OPT_KEY = "hoodie.datasource.hive_sync.assume_date_partitioning"
  val HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY = "hoodie.datasource.hive_sync.use_pre_apache_input_format"
  val HIVE_USE_JDBC_OPT_KEY = "hoodie.datasource.hive_sync.use_jdbc"
  val HIVE_AUTO_CREATE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.auto_create_database"
  val HIVE_IGNORE_EXCEPTIONS_OPT_KEY = "hoodie.datasource.hive_sync.ignore_exceptions"
  val HIVE_SKIP_RO_SUFFIX = "hoodie.datasource.hive_sync.skip_ro_suffix"
  val HIVE_SUPPORT_TIMESTAMP = "hoodie.datasource.hive_sync.support_timestamp"

  // DEFAULT FOR HIVE SPECIFIC CONFIGS
  val DEFAULT_HIVE_SYNC_ENABLED_OPT_VAL = "false"
  val DEFAULT_META_SYNC_ENABLED_OPT_VAL = "false"
  val DEFAULT_HIVE_DATABASE_OPT_VAL = "default"
  val DEFAULT_HIVE_TABLE_OPT_VAL = "unknown"
  val DEFAULT_HIVE_BASE_FILE_FORMAT_OPT_VAL = "PARQUET"
  val DEFAULT_HIVE_USER_OPT_VAL = "hive"
  val DEFAULT_HIVE_PASS_OPT_VAL = "hive"
  val DEFAULT_HIVE_URL_OPT_VAL = "jdbc:hive2://localhost:10000"
  val DEFAULT_HIVE_PARTITION_FIELDS_OPT_VAL = ""
  val DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName
  val DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL = "false"
  val DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL = "false"
  val DEFAULT_HIVE_USE_JDBC_OPT_VAL = "true"
  val DEFAULT_HIVE_AUTO_CREATE_DATABASE_OPT_KEY = "true"
  val DEFAULT_HIVE_IGNORE_EXCEPTIONS_OPT_KEY = "false"
  val DEFAULT_HIVE_SKIP_RO_SUFFIX_VAL = "false"
  val DEFAULT_HIVE_SUPPORT_TIMESTAMP = "false"

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE_OPT_KEY = "hoodie.datasource.compaction.async.enable"
  val DEFAULT_ASYNC_COMPACT_ENABLE_OPT_VAL = "true"

  // Avro Kafka Source configs
  val KAFKA_AVRO_VALUE_DESERIALIZER = "hoodie.deltastreamer.source.kafka.value.deserializer.class"

  // Schema provider class to be set to be used in custom kakfa deserializer
  val SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class"

}
