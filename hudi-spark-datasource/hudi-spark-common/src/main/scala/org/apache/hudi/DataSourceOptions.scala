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

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.common.config.{ConfigProperty, HoodieConfig}
import org.apache.hudi.common.fs.ConsistencyGuardConfig
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.util.ConfigUtils
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncTool, MultiPartKeysValueExtractor, NonPartitionedExtractor, SlashEncodedDayPartitionValueExtractor}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.{DataSourceUtils => SparkDataSourceUtils}

import java.util.function.{Function => JavaFunction}
import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * List of options that can be passed to the Hoodie datasource,
 * in addition to the hoodie client configs
 */

/**
  * Options supported for reading hoodie tables.
  */
object DataSourceReadOptions {

  val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
  val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  val QUERY_TYPE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.query.type")
    .defaultValue(QUERY_TYPE_SNAPSHOT_OPT_VAL)
    .withAlternatives("hoodie.datasource.view.type")
    .withDocumentation("Whether data needs to be read, in incremental mode (new data since an instantTime) " +
      "(or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode " +
      "(obtain latest view, by merging base and (if any) log files)")

  val REALTIME_SKIP_MERGE_OPT_VAL = "skip_merge"
  val REALTIME_PAYLOAD_COMBINE_OPT_VAL = "payload_combine"
  val REALTIME_MERGE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.merge.type")
    .defaultValue(REALTIME_PAYLOAD_COMBINE_OPT_VAL)
    .withDocumentation("For Snapshot query on merge on read table, control whether we invoke the record " +
      s"payload implementation to merge (${REALTIME_PAYLOAD_COMBINE_OPT_VAL}) or skip merging altogether" +
      s"${REALTIME_SKIP_MERGE_OPT_VAL}")

  val READ_PATHS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.paths")
    .noDefaultValue()
    .withDocumentation("Comma separated list of file paths to read within a Hudi table.")

  val READ_PRE_COMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_NAME

  val ENABLE_HOODIE_FILE_INDEX: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.file.index.enable")
    .defaultValue(true)
    .withDocumentation("Enables use of the spark file index implementation for Hudi, "
      + "that speeds up listing of large tables.")

  val BEGIN_INSTANTTIME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.begin.instanttime")
    .noDefaultValue()
    .withDocumentation("Instant time to start incrementally pulling data from. The instanttime here need not necessarily " +
      "correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. " +
      "For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.")

  val END_INSTANTTIME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.end.instanttime")
    .noDefaultValue()
    .withDocumentation("Instant time to limit incrementally fetched data to. " +
      "New data written with an instant_time <= END_INSTANTTIME are fetched out.")

  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.schema.use.end.instanttime")
    .defaultValue("false")
    .withDocumentation("Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.")

  val PUSH_DOWN_INCR_FILTERS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.filters")
    .defaultValue("")
    .withDocumentation("For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies "
      + "opaque map functions, filters appearing late in the sequence of transformations cannot be automatically "
      + "pushed down. This option allows setting filters directly on Hoodie Source.")

  val INCR_PATH_GLOB: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.path.glob")
    .defaultValue("")
    .withDocumentation("For the use-cases like users only want to incremental pull from certain partitions "
      + "instead of the full table. This option allows using glob pattern to directly filter on path.")

  val TIME_TRAVEL_AS_OF_INSTANT: ConfigProperty[String] = ConfigProperty
    .key("as.of.instant")
    .noDefaultValue()
    .withDocumentation("The query instant for time travel. Without specified this option," +
      " we query the latest snapshot.")

  val ENABLE_DATA_SKIPPING: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.enable.data.skipping")
    .defaultValue(true)
    .sinceVersion("0.10.0")
    .withDocumentation("enable data skipping to boost query after doing z-order optimize for current table")

  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
  @Deprecated
  val QUERY_TYPE_OPT_KEY = QUERY_TYPE.key()
  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_QUERY_TYPE_OPT_VAL: String = QUERY_TYPE_SNAPSHOT_OPT_VAL
  /** @deprecated Use {@link REALTIME_MERGE} and its methods instead */
  @Deprecated
  val REALTIME_MERGE_OPT_KEY = REALTIME_MERGE.key()
  /** @deprecated Use {@link REALTIME_MERGE} and its methods instead */
  @Deprecated
  val DEFAULT_REALTIME_MERGE_OPT_VAL = REALTIME_PAYLOAD_COMBINE_OPT_VAL
  /** @deprecated Use {@link READ_PATHS} and its methods instead */
  @Deprecated
  val READ_PATHS_OPT_KEY = READ_PATHS.key()
  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
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
  /** @deprecated Use {@link BEGIN_INSTANTTIME} and its methods instead */
  @Deprecated
  val BEGIN_INSTANTTIME_OPT_KEY = BEGIN_INSTANTTIME.key()
  /** @deprecated Use {@link END_INSTANTTIME} and its methods instead */
  @Deprecated
  val END_INSTANTTIME_OPT_KEY = END_INSTANTTIME.key()
  /** @deprecated Use {@link INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME} and its methods instead */
  @Deprecated
  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY = INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key()
  /** @deprecated Use {@link INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME} and its methods instead */
  @Deprecated
  val DEFAULT_INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_VAL = INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue()
  /** @deprecated Use {@link PUSH_DOWN_INCR_FILTERS} and its methods instead */
  @Deprecated
  val PUSH_DOWN_INCR_FILTERS_OPT_KEY = PUSH_DOWN_INCR_FILTERS.key()
  /** @deprecated Use {@link PUSH_DOWN_INCR_FILTERS} and its methods instead */
  @Deprecated
  val DEFAULT_PUSH_DOWN_FILTERS_OPT_VAL = PUSH_DOWN_INCR_FILTERS.defaultValue()
  /** @deprecated Use {@link INCR_PATH_GLOB} and its methods instead */
  @Deprecated
  val INCR_PATH_GLOB_OPT_KEY = INCR_PATH_GLOB.key()
  /** @deprecated Use {@link INCR_PATH_GLOB} and its methods instead */
  @Deprecated
  val DEFAULT_INCR_PATH_GLOB_OPT_VAL = INCR_PATH_GLOB.defaultValue()
}

/**
  * Options supported for writing hoodie tables.
  */
object DataSourceWriteOptions {

  val BULK_INSERT_OPERATION_OPT_VAL = WriteOperationType.BULK_INSERT.value
  val INSERT_OPERATION_OPT_VAL = WriteOperationType.INSERT.value
  val UPSERT_OPERATION_OPT_VAL = WriteOperationType.UPSERT.value
  val DELETE_OPERATION_OPT_VAL = WriteOperationType.DELETE.value
  val DELETE_PARTITION_OPERATION_OPT_VAL = WriteOperationType.DELETE_PARTITION.value
  val BOOTSTRAP_OPERATION_OPT_VAL = WriteOperationType.BOOTSTRAP.value
  val INSERT_OVERWRITE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE.value
  val INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE_TABLE.value
  val OPERATION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.operation")
    .defaultValue(UPSERT_OPERATION_OPT_VAL)
    .withDocumentation("Whether to do upsert, insert or bulkinsert for the write operation. " +
      "Use bulkinsert to load new data into a table, and there on use upsert/insert. " +
      "bulk insert uses a disk based write path to scale to load large inputs without need to cache it.")


  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val TABLE_TYPE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.table.type")
    .defaultValue(COW_TABLE_TYPE_OPT_VAL)
    .withAlternatives("hoodie.datasource.write.storage.type")
    .withDocumentation("The table type for the underlying data, for this write. This can’t change between writes.")

  /**
    * Translate spark parameters to hudi parameters
    *
    * @param optParams Parameters to be translated
    * @return Parameters after translation
    */
  def translateSqlOptions(optParams: Map[String, String]): Map[String, String] = {
    var translatedOptParams = optParams
    // translate the api partitionBy of spark DataFrameWriter to PARTITIONPATH_FIELD
    if (optParams.contains(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)) {
      val partitionColumns = optParams.get(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(SparkDataSourceUtils.decodePartitioningColumns)
        .getOrElse(Nil)
      val keyGeneratorClass = optParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key(),
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.defaultValue)

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
      translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD.key -> partitionPathField)
    }
    translatedOptParams
  }

  val TABLE_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.table.name")
    .noDefaultValue()
    .withDocumentation("Table name for the datasource write. Also used to register the table into meta stores.")

  /**
   * Field used in preCombining before actual write. When two records have the same
   * key value, we will pick the one with the largest value for the precombine field,
   * determined by Object.compareTo(..)
   */
  val PRECOMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_NAME

  /**
   * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
   * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
   */
  val PAYLOAD_CLASS_NAME = HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME

  /**
   * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
   * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
   * the dot notation eg: `a.b.c`
   *
   */
  val RECORDKEY_FIELD = KeyGeneratorOptions.RECORDKEY_FIELD_NAME

  /**
   * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
   * value obtained by invoking .toString()
   */
  val PARTITIONPATH_FIELD = KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME

  /**
   * Flag to indicate whether to use Hive style partitioning.
   * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
   * By default false (the names of partition folders are only partition values)
   */
  val HIVE_STYLE_PARTITIONING = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE

  /**
    * Key generator class, that implements will extract the key out of incoming record
    *
    */
  val keyGeneraterInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: HoodieConfig) => {
    if (!p.contains(PARTITIONPATH_FIELD)) {
      Option.of(classOf[NonpartitionedKeyGenerator].getName)
    } else {
      val numOfPartFields = p.getString(PARTITIONPATH_FIELD).split(",").length
      if (numOfPartFields == 1) {
        Option.of(classOf[SimpleKeyGenerator].getName)
      } else {
        Option.of(classOf[ComplexKeyGenerator].getName)
      }
    }
  })
  val KEYGENERATOR_CLASS_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.keygenerator.class")
    .defaultValue(classOf[SimpleKeyGenerator].getName)
    .withInferFunction(keyGeneraterInferFunc)
    .withDocumentation("Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator`")

  val ENABLE_ROW_WRITER: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.row.writer.enable")
    .defaultValue("true")
    .withDocumentation("When set to true, will perform write operations directly using the spark native " +
      "`Row` representation, avoiding any additional conversion costs.")

  /**
   * Enable the bulk insert for sql insert statement.
   */
  val SQL_ENABLE_BULK_INSERT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.sql.bulk.insert.enable")
    .defaultValue("false")
    .withDocumentation("When set to true, the sql insert statement will use bulk insert.")

  val SQL_INSERT_MODE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.sql.insert.mode")
    .defaultValue("upsert")
    .withDocumentation("Insert mode when insert data to pk-table. The optional modes are: upsert, strict and non-strict." +
      "For upsert mode, insert statement do the upsert operation for the pk-table which will update the duplicate record." +
      "For strict mode, insert statement will keep the primary key uniqueness constraint which do not allow duplicate record." +
      "While for non-strict mode, hudi just do the insert operation for the pk-table.")

  val COMMIT_METADATA_KEYPREFIX: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.commitmeta.key.prefix")
    .defaultValue("_")
    .withDocumentation("Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. " +
      "This is useful to store checkpointing information, in a consistent way with the hudi timeline")

  val INSERT_DROP_DUPS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.insert.drop.duplicates")
    .defaultValue("false")
    .withDocumentation("If set to true, filters out all duplicate records from incoming dataframe, during insert operations.")

  val PARTITIONS_TO_DELETE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.partitions.to.delete")
    .noDefaultValue()
    .withDocumentation("Comma separated list of partitions to delete")

  val STREAMING_RETRY_CNT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.count")
    .defaultValue("3")
    .withDocumentation("Config to indicate how many times streaming job should retry for a failed micro batch.")

  val STREAMING_RETRY_INTERVAL_MS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.interval.ms")
    .defaultValue("2000")
    .withDocumentation(" Config to indicate how long (by millisecond) before a retry should issued for failed microbatch")

  /**
   * By default true (in favor of streaming progressing over data integrity)
   */
  val STREAMING_IGNORE_FAILED_BATCH: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.ignore.failed.batch")
    .defaultValue("true")
    .withDocumentation("Config to indicate whether to ignore any non exception error (e.g. writestatus error)"
      + " within a streaming microbatch")

  val META_SYNC_CLIENT_TOOL_CLASS_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.meta.sync.client.tool.class")
    .defaultValue(classOf[HiveSyncTool].getName)
    .withDocumentation("Sync tool class name used to sync to metastore. Defaults to Hive.")

  val RECONCILE_SCHEMA: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.datasource.write.reconcile.schema")
    .defaultValue(false)
    .withDocumentation("When a new batch of write has records with old schema, but latest table schema got "
      + "evolved, this config will upgrade the records to leverage latest table schema(default values will be "
      + "injected to missing fields). If not, the write batch would fail.")

  // HIVE SYNC SPECIFIC CONFIGS
  // NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset
  val HIVE_SYNC_ENABLED: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.enable")
    .defaultValue("false")
    .withDocumentation("When set to true, register/sync the table to Apache Hive metastore")

  val META_SYNC_ENABLED: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.meta.sync.enable")
    .defaultValue("false")
    .withDocumentation("")

  val HIVE_DATABASE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.database")
    .defaultValue("default")
    .withDocumentation("database to sync to")

  val hiveTableOptKeyInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: HoodieConfig) => {
    if (p.contains(TABLE_NAME)) {
      Option.of(p.getString(TABLE_NAME))
    } else if (p.contains(HoodieWriteConfig.TBL_NAME)) {
      Option.of(p.getString(HoodieWriteConfig.TBL_NAME))
    } else {
      Option.empty[String]()
    }
  })
  val HIVE_TABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.table")
    .defaultValue("unknown")
    .withInferFunction(hiveTableOptKeyInferFunc)
    .withDocumentation("table to sync to")

  val HIVE_BASE_FILE_FORMAT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.base_file_format")
    .defaultValue("PARQUET")
    .withDocumentation("Base file format for the sync.")

  val HIVE_USER: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.username")
    .defaultValue("hive")
    .withDocumentation("hive user name to use")

  val HIVE_PASS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.password")
    .defaultValue("hive")
    .withDocumentation("hive password to use")

  val HIVE_URL: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.jdbcurl")
    .defaultValue("jdbc:hive2://localhost:10000")
    .withDocumentation("Hive metastore url")

  val hivePartitionFieldsInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: HoodieConfig) => {
    if (p.contains(PARTITIONPATH_FIELD)) {
      Option.of(p.getString(PARTITIONPATH_FIELD))
    } else {
      Option.empty[String]()
    }
  })
  val HIVE_PARTITION_FIELDS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.partition_fields")
    .defaultValue("")
    .withDocumentation("Field in the table to use for determining hive partition columns.")
    .withInferFunction(hivePartitionFieldsInferFunc)

  val hivePartitionExtractorInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: HoodieConfig) => {
    if (!p.contains(PARTITIONPATH_FIELD)) {
      Option.of(classOf[NonPartitionedExtractor].getName)
    } else {
      val numOfPartFields = p.getString(PARTITIONPATH_FIELD).split(",").length
      if (numOfPartFields == 1 && p.contains(HIVE_STYLE_PARTITIONING) && p.getString(HIVE_STYLE_PARTITIONING) == "true") {
        Option.of(classOf[HiveStylePartitionValueExtractor].getName)
      } else {
        Option.of(classOf[MultiPartKeysValueExtractor].getName)
      }
    }
  })
  val HIVE_PARTITION_EXTRACTOR_CLASS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.partition_extractor_class")
    .defaultValue(classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName)
    .withDocumentation("Class which implements PartitionValueExtractor to extract the partition values, "
      + "default 'SlashEncodedDayPartitionValueExtractor'.")
    .withInferFunction(hivePartitionExtractorInferFunc)

  val HIVE_ASSUME_DATE_PARTITION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.assume_date_partitioning")
    .defaultValue("false")
    .withDocumentation("Assume partitioning is yyyy/mm/dd")

  val HIVE_USE_PRE_APACHE_INPUT_FORMAT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.use_pre_apache_input_format")
    .defaultValue("false")
    .withDocumentation("Flag to choose InputFormat under com.uber.hoodie package instead of org.apache.hudi package. "
      + "Use this when you are in the process of migrating from "
      + "com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to org.apache.hudi input format")

  /** @deprecated Use {@link HIVE_SYNC_MODE} instead of this config from 0.9.0 */
  @Deprecated
  val HIVE_USE_JDBC: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.use_jdbc")
    .defaultValue("true")
    .deprecatedAfter("0.9.0")
    .withDocumentation("Use JDBC when hive synchronization is enabled")

  val HIVE_AUTO_CREATE_DATABASE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.auto_create_database")
    .defaultValue("true")
    .withDocumentation("Auto create hive database if does not exists")

  val HIVE_IGNORE_EXCEPTIONS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.ignore_exceptions")
    .defaultValue("false")
    .withDocumentation("")

  val HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.skip_ro_suffix")
    .defaultValue("false")
    .withDocumentation("Skip the _ro suffix for Read optimized table, when registering")

  val HIVE_SUPPORT_TIMESTAMP_TYPE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.support_timestamp")
    .defaultValue("false")
    .withDocumentation("‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. " +
      "Disabled by default for backward compatibility.")

  val HIVE_TABLE_PROPERTIES: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.table_properties")
    .noDefaultValue()
    .withDocumentation("Additional properties to store with table.")

  val HIVE_TABLE_SERDE_PROPERTIES: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.serde_properties")
    .noDefaultValue()
    .withDocumentation("Serde properties to hive table.")

  val HIVE_SYNC_AS_DATA_SOURCE_TABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.sync_as_datasource")
    .defaultValue("true")
    .withDocumentation("")

  // Create table as managed table
  val HIVE_CREATE_MANAGED_TABLE: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.datasource.hive_sync.create_managed_table")
    .defaultValue(false)
    .withDocumentation("Whether to sync the table as managed table.")

  val HIVE_BATCH_SYNC_PARTITION_NUM: ConfigProperty[Int] = ConfigProperty
    .key("hoodie.datasource.hive_sync.batch_num")
    .defaultValue(1000)
    .withDocumentation("The number of partitions one batch when synchronous partitions to hive.")

  val HIVE_SYNC_MODE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.hive_sync.mode")
    .noDefaultValue()
    .withDocumentation("Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.")

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.compaction.async.enable")
    .defaultValue("true")
    .withDocumentation("Controls whether async compaction should be turned on for MOR table writing.")

  val INLINE_CLUSTERING_ENABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.clustering.inline.enable")
    .defaultValue("false")
    .sinceVersion("0.9.0")
    .withDocumentation("Enable inline clustering. Disabled by default.")

  val ASYNC_CLUSTERING_ENABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.clustering.async.enable")
    .defaultValue("false")
    .sinceVersion("0.9.0")
    .withDocumentation("Enable asynchronous clustering. Disabled by default.")

  val KAFKA_AVRO_VALUE_DESERIALIZER_CLASS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.deltastreamer.source.kafka.value.deserializer.class")
    .defaultValue("io.confluent.kafka.serializers.KafkaAvroDeserializer")
    .sinceVersion("0.9.0")
    .withDocumentation("This class is used by kafka client to deserialize the records")

  val DROP_PARTITION_COLUMNS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.drop.partition.columns")
    .defaultValue("false")
    .withDocumentation("When set to true, will not write the partition columns into hudi. " +
      "By default, false.")

  /** @deprecated Use {@link HIVE_ASSUME_DATE_PARTITION} and its methods instead */
  @Deprecated
  val HIVE_ASSUME_DATE_PARTITION_OPT_KEY = HIVE_ASSUME_DATE_PARTITION.key()
  /** @deprecated Use {@link HIVE_USE_PRE_APACHE_INPUT_FORMAT} and its methods instead */
  @Deprecated
  val HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY = HIVE_USE_PRE_APACHE_INPUT_FORMAT.key()
  /** @deprecated Use {@link HIVE_USE_JDBC} and its methods instead */
  @Deprecated
  val HIVE_USE_JDBC_OPT_KEY = HIVE_USE_JDBC.key()
  /** @deprecated Use {@link HIVE_AUTO_CREATE_DATABASE} and its methods instead */
  @Deprecated
  val HIVE_AUTO_CREATE_DATABASE_OPT_KEY = HIVE_AUTO_CREATE_DATABASE.key()
  /** @deprecated Use {@link HIVE_IGNORE_EXCEPTIONS} and its methods instead */
  @Deprecated
  val HIVE_IGNORE_EXCEPTIONS_OPT_KEY = HIVE_IGNORE_EXCEPTIONS.key()
  /** @deprecated Use {@link STREAMING_IGNORE_FAILED_BATCH} and its methods instead */
  @Deprecated
  val STREAMING_IGNORE_FAILED_BATCH_OPT_KEY = STREAMING_IGNORE_FAILED_BATCH.key()
  /** @deprecated Use {@link STREAMING_IGNORE_FAILED_BATCH} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_IGNORE_FAILED_BATCH_OPT_VAL = STREAMING_IGNORE_FAILED_BATCH.defaultValue()
  /** @deprecated Use {@link META_SYNC_CLIENT_TOOL_CLASS_NAME} and its methods instead */
  @Deprecated
  val META_SYNC_CLIENT_TOOL_CLASS = META_SYNC_CLIENT_TOOL_CLASS_NAME.key()
  /** @deprecated Use {@link META_SYNC_CLIENT_TOOL_CLASS_NAME} and its methods instead */
  @Deprecated
  val DEFAULT_META_SYNC_CLIENT_TOOL_CLASS = META_SYNC_CLIENT_TOOL_CLASS_NAME.defaultValue()
  /** @deprecated Use {@link HIVE_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val HIVE_SYNC_ENABLED_OPT_KEY = HIVE_SYNC_ENABLED.key()
  /** @deprecated Use {@link META_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val META_SYNC_ENABLED_OPT_KEY = META_SYNC_ENABLED.key()
  /** @deprecated Use {@link HIVE_DATABASE} and its methods instead */
  @Deprecated
  val HIVE_DATABASE_OPT_KEY = HIVE_DATABASE.key()
  /** @deprecated Use {@link HIVE_TABLE} and its methods instead */
  @Deprecated
  val HIVE_TABLE_OPT_KEY = HIVE_TABLE.key()
  /** @deprecated Use {@link HIVE_BASE_FILE_FORMAT} and its methods instead */
  @Deprecated
  val HIVE_BASE_FILE_FORMAT_OPT_KEY = HIVE_BASE_FILE_FORMAT.key()
  /** @deprecated Use {@link HIVE_USER} and its methods instead */
  @Deprecated
  val HIVE_USER_OPT_KEY = HIVE_USER.key()
  /** @deprecated Use {@link HIVE_PASS} and its methods instead */
  @Deprecated
  val HIVE_PASS_OPT_KEY = HIVE_PASS.key()
  /** @deprecated Use {@link HIVE_URL} and its methods instead */
  @Deprecated
  val HIVE_URL_OPT_KEY = HIVE_URL.key()
  /** @deprecated Use {@link HIVE_PARTITION_FIELDS} and its methods instead */
  @Deprecated
  val HIVE_PARTITION_FIELDS_OPT_KEY = HIVE_PARTITION_FIELDS.key()
  /** @deprecated Use {@link HIVE_PARTITION_EXTRACTOR_CLASS} and its methods instead */
  @Deprecated
  val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = HIVE_PARTITION_EXTRACTOR_CLASS.key()

  /** @deprecated Use {@link KEYGENERATOR_CLASS} and its methods instead */
  @Deprecated
  val DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = KEYGENERATOR_CLASS_NAME.defaultValue()
  /** @deprecated Use {@link KEYGENERATOR_CLASS} and its methods instead */
  @Deprecated
  val KEYGENERATOR_CLASS_OPT_KEY = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key()
  /** @deprecated Use {@link ENABLE_ROW_WRITER} and its methods instead */
  @Deprecated
  val ENABLE_ROW_WRITER_OPT_KEY = ENABLE_ROW_WRITER.key()
  /** @deprecated Use {@link ENABLE_ROW_WRITER} and its methods instead */
  @Deprecated
  val DEFAULT_ENABLE_ROW_WRITER_OPT_VAL = ENABLE_ROW_WRITER.defaultValue()
  /** @deprecated Use {@link HIVE_STYLE_PARTITIONING} and its methods instead */
  @Deprecated
  val HIVE_STYLE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()
  /** @deprecated Use {@link HIVE_STYLE_PARTITIONING} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = HIVE_STYLE_PARTITIONING.defaultValue()

  val URL_ENCODE_PARTITIONING = KeyGeneratorOptions.URL_ENCODE_PARTITIONING
  /** @deprecated Use {@link URL_ENCODE_PARTITIONING} and its methods instead */
  @Deprecated
  val URL_ENCODE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key()
  /** @deprecated Use {@link URL_ENCODE_PARTITIONING} and its methods instead */
  @Deprecated
  val DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL = URL_ENCODE_PARTITIONING.defaultValue()
  /** @deprecated Use {@link COMMIT_METADATA_KEYPREFIX} and its methods instead */
  @Deprecated
  val COMMIT_METADATA_KEYPREFIX_OPT_KEY = COMMIT_METADATA_KEYPREFIX.key()
  /** @deprecated Use {@link COMMIT_METADATA_KEYPREFIX} and its methods instead */
  @Deprecated
  val DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL = COMMIT_METADATA_KEYPREFIX.defaultValue()
  /** @deprecated Use {@link INSERT_DROP_DUPS} and its methods instead */
  @Deprecated
  val INSERT_DROP_DUPS_OPT_KEY = INSERT_DROP_DUPS.key()
  /** @deprecated Use {@link INSERT_DROP_DUPS} and its methods instead */
  @Deprecated
  val DEFAULT_INSERT_DROP_DUPS_OPT_VAL = INSERT_DROP_DUPS.defaultValue()
  /** @deprecated Use {@link STREAMING_RETRY_CNT} and its methods instead */
  @Deprecated
  val STREAMING_RETRY_CNT_OPT_KEY = STREAMING_RETRY_CNT.key()
  /** @deprecated Use {@link STREAMING_RETRY_CNT} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_RETRY_CNT_OPT_VAL = STREAMING_RETRY_CNT.defaultValue()
  /** @deprecated Use {@link STREAMING_RETRY_INTERVAL_MS} and its methods instead */
  @Deprecated
  val STREAMING_RETRY_INTERVAL_MS_OPT_KEY = STREAMING_RETRY_INTERVAL_MS.key()
  /** @deprecated Use {@link STREAMING_RETRY_INTERVAL_MS} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_RETRY_INTERVAL_MS_OPT_VAL = STREAMING_RETRY_INTERVAL_MS.defaultValue()

  /** @deprecated Use {@link RECORDKEY_FIELD} and its methods instead */
  @Deprecated
  val RECORDKEY_FIELD_OPT_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()
  /** @deprecated Use {@link RECORDKEY_FIELD} and its methods instead */
  @Deprecated
  val DEFAULT_RECORDKEY_FIELD_OPT_VAL = RECORDKEY_FIELD.defaultValue()
  /** @deprecated Use {@link PARTITIONPATH_FIELD} and its methods instead */
  @Deprecated
  val PARTITIONPATH_FIELD_OPT_KEY = KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()
  /** @deprecated Use {@link PARTITIONPATH_FIELD} and its methods instead */
  @Deprecated
  val DEFAULT_PARTITIONPATH_FIELD_OPT_VAL = null

  /** @deprecated Use {@link TABLE_NAME} and its methods instead */
  @Deprecated
  val TABLE_NAME_OPT_KEY = TABLE_NAME.key()
  /** @deprecated Use {@link PRECOMBINE_FIELD} and its methods instead */
  @Deprecated
  val PRECOMBINE_FIELD_OPT_KEY = HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key()
  /** @deprecated Use {@link PRECOMBINE_FIELD} and its methods instead */
  @Deprecated
  val DEFAULT_PRECOMBINE_FIELD_OPT_VAL = PRECOMBINE_FIELD.defaultValue()

  /** @deprecated Use {@link HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  val PAYLOAD_CLASS_OPT_KEY = HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key()
  /** @deprecated Use {@link HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  val DEFAULT_PAYLOAD_OPT_VAL = PAYLOAD_CLASS_NAME.defaultValue()

  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val TABLE_TYPE_OPT_KEY = TABLE_TYPE.key()
  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_TABLE_TYPE_OPT_VAL = TABLE_TYPE.defaultValue()

  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  @Deprecated
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  @Deprecated
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  /** @deprecated Use {@link OPERATION} and its methods instead */
  @Deprecated
  val OPERATION_OPT_KEY = OPERATION.key()
  /** @deprecated Use {@link OPERATION} and its methods instead */
  @Deprecated
  val DEFAULT_OPERATION_OPT_VAL = OPERATION.defaultValue()

  /** @deprecated Use {@link HIVE_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SYNC_ENABLED_OPT_VAL = HIVE_SYNC_ENABLED.defaultValue()
  /** @deprecated Use {@link META_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val DEFAULT_META_SYNC_ENABLED_OPT_VAL = META_SYNC_ENABLED.defaultValue()
  /** @deprecated Use {@link HIVE_DATABASE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_DATABASE_OPT_VAL = HIVE_DATABASE.defaultValue()
  /** @deprecated Use {@link HIVE_TABLE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_TABLE_OPT_VAL = HIVE_TABLE.defaultValue()
  /** @deprecated Use {@link HIVE_BASE_FILE_FORMAT} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_BASE_FILE_FORMAT_OPT_VAL = HIVE_BASE_FILE_FORMAT.defaultValue()
  /** @deprecated Use {@link HIVE_USER} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_USER_OPT_VAL = HIVE_USER.defaultValue()
  /** @deprecated Use {@link HIVE_PASS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PASS_OPT_VAL = HIVE_PASS.defaultValue()
  /** @deprecated Use {@link HIVE_URL} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_URL_OPT_VAL = HIVE_URL.defaultValue()
  /** @deprecated Use {@link HIVE_PARTITION_FIELDS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PARTITION_FIELDS_OPT_VAL = HIVE_PARTITION_FIELDS.defaultValue()
  /** @deprecated Use {@link HIVE_PARTITION_EXTRACTOR_CLASS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = HIVE_PARTITION_EXTRACTOR_CLASS.defaultValue()
  /** @deprecated Use {@link HIVE_ASSUME_DATE_PARTITION} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL = HIVE_ASSUME_DATE_PARTITION.defaultValue()
  @Deprecated
  val DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL = "false"
  /** @deprecated Use {@link HIVE_USE_JDBC} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_USE_JDBC_OPT_VAL = HIVE_USE_JDBC.defaultValue()
  /** @deprecated Use {@link HIVE_AUTO_CREATE_DATABASE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_AUTO_CREATE_DATABASE_OPT_KEY = HIVE_AUTO_CREATE_DATABASE.defaultValue()
  /** @deprecated Use {@link HIVE_IGNORE_EXCEPTIONS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_IGNORE_EXCEPTIONS_OPT_KEY = HIVE_IGNORE_EXCEPTIONS.defaultValue()
  /** @deprecated Use {@link HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE} and its methods instead */
  @Deprecated
  val HIVE_SKIP_RO_SUFFIX = HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key()
  /** @deprecated Use {@link HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SKIP_RO_SUFFIX_VAL = HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.defaultValue()
  /** @deprecated Use {@link HIVE_SUPPORT_TIMESTAMP_TYPE} and its methods instead */
  @Deprecated
  val HIVE_SUPPORT_TIMESTAMP = HIVE_SUPPORT_TIMESTAMP_TYPE.key()
  /** @deprecated Use {@link HIVE_SUPPORT_TIMESTAMP_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SUPPORT_TIMESTAMP = HIVE_SUPPORT_TIMESTAMP_TYPE.defaultValue()
  /** @deprecated Use {@link ASYNC_COMPACT_ENABLE} and its methods instead */
  @Deprecated
  val ASYNC_COMPACT_ENABLE_OPT_KEY = ASYNC_COMPACT_ENABLE.key()
  /** @deprecated Use {@link ASYNC_COMPACT_ENABLE} and its methods instead */
  @Deprecated
  val DEFAULT_ASYNC_COMPACT_ENABLE_OPT_VAL = ASYNC_COMPACT_ENABLE.defaultValue()
  /** @deprecated Use {@link KAFKA_AVRO_VALUE_DESERIALIZER_CLASS} and its methods instead */
  @Deprecated
  val KAFKA_AVRO_VALUE_DESERIALIZER = KAFKA_AVRO_VALUE_DESERIALIZER_CLASS.key()
  @Deprecated
  val SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class"
}

object DataSourceOptionsHelper {

  private val log = LogManager.getLogger(DataSourceOptionsHelper.getClass)

  // put all the configs with alternatives here
  val allConfigsWithAlternatives = List(
    DataSourceReadOptions.QUERY_TYPE,
    DataSourceWriteOptions.TABLE_TYPE,
    HoodieTableConfig.BASE_FILE_FORMAT,
    HoodieTableConfig.LOG_FILE_FORMAT
  )

  // put all the deprecated configs here
  val allDeprecatedConfigs: Set[String] = Set(
    ConsistencyGuardConfig.ENABLE.key
  )

  // maps the deprecated config name to its latest name
  val allAlternatives: Map[String, String] = {
    val alterMap = scala.collection.mutable.Map[String, String]()
    allConfigsWithAlternatives.foreach(cfg => cfg.getAlternatives.asScala.foreach(alternative => alterMap(alternative) = cfg.key))
    alterMap.toMap
  }

  val viewTypeValueMap: Map[String, String] = Map(
    DataSourceReadOptions.VIEW_TYPE_READ_OPTIMIZED_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_REALTIME_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)

  def translateConfigurations(optParams: Map[String, String]): Map[String, String] = {
    val translatedOpt = scala.collection.mutable.Map[String, String]() ++= optParams
    optParams.keySet.foreach(opt => {
      if (allAlternatives.contains(opt) && !optParams.contains(allAlternatives(opt))) {
        log.warn(opt + " is deprecated and will be removed in a later release; Please use " + allAlternatives(opt))
        if (opt == DataSourceReadOptions.VIEW_TYPE_OPT_KEY) {
          // special handle for VIEW_TYPE, also need to translate its values
          translatedOpt ++= Map(allAlternatives(opt) -> viewTypeValueMap(optParams(opt)))
        } else {
          translatedOpt ++= Map(allAlternatives(opt) -> optParams(opt))
        }
      }
      if (allDeprecatedConfigs.contains(opt)) {
        log.warn(opt + " is deprecated and should never be used anymore")
      }
    })
    translatedOpt.toMap
  }

  def parametersWithReadDefaults(parameters: Map[String, String]): Map[String, String] = {
    // First check if the ConfigUtils.IS_QUERY_AS_RO_TABLE has set by HiveSyncTool,
    // or else use query type from QUERY_TYPE.
    val queryType = parameters.get(ConfigUtils.IS_QUERY_AS_RO_TABLE)
      .map(is => if (is.toBoolean) QUERY_TYPE_READ_OPTIMIZED_OPT_VAL else QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .getOrElse(parameters.getOrElse(QUERY_TYPE.key, QUERY_TYPE.defaultValue()))

    Map(
      QUERY_TYPE.key -> queryType
    ) ++ translateConfigurations(parameters)
  }

  implicit def scalaFunctionToJavaFunction[From, To](function: (From) => To): JavaFunction[From, To] = {
    new JavaFunction[From, To] {
      override def apply (input: From): To = function (input)
    }
  }
}
