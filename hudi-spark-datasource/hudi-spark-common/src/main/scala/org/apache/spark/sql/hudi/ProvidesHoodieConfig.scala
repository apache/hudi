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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, TypedProperties}
import org.apache.hudi.common.model.{OverwriteWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.sql.InsertMode
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hudi.HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{isHoodieConfigKey, isUsingHiveCatalog}
import org.apache.spark.sql.hudi.ProvidesHoodieConfig.combineOptions
import org.apache.spark.sql.hudi.command.{SqlKeyGenerator, ValidateDuplicateKeyPayload}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.util.Locale
import scala.collection.JavaConverters._

trait ProvidesHoodieConfig extends Logging {

  def buildHoodieConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val sparkSession: SparkSession = hoodieCatalogTable.spark
    val tableConfig = hoodieCatalogTable.tableConfig

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = Option(tableConfig.getPreCombineField).getOrElse("")

    require(hoodieCatalogTable.primaryKeys.nonEmpty,
      s"There are no primary key in table ${hoodieCatalogTable.table.identifier}, cannot execute update operator")

    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)

    val defaultOpts = Map[String, String](
      OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
      SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_SYNC_MODE, HiveSyncMode.HMS.name()),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_DATABASE_NAME),
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_TABLE_NAME),
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> tableConfig.getPartitionFieldProp,
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS),
      HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString
    )

    val overridingOpts = Map[String, String](
      "path" -> hoodieCatalogTable.tableLocation,
      RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      PRECOMBINE_FIELD.key -> preCombineField,
      HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
      PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  /**
   * Build the default config for insert.
   *
   * @return
   */
  def buildHoodieInsertConfig(hoodieCatalogTable: HoodieCatalogTable,
                              sparkSession: SparkSession,
                              isOverwritePartition: Boolean,
                              isOverwriteTable: Boolean,
                              insertPartitions: Map[String, Option[String]] = Map.empty,
                              extraOptions: Map[String, String]): Map[String, String] = {

    if (insertPartitions.nonEmpty &&
      (insertPartitions.keys.toSet != hoodieCatalogTable.partitionFields.toSet)) {
      throw new IllegalArgumentException(s"Insert partition fields" +
        s"[${insertPartitions.keys.mkString(" ")}]" +
        s" not equal to the defined partition in table[${hoodieCatalogTable.partitionFields.mkString(",")}]")
    }
    val path = hoodieCatalogTable.tableLocation
    val tableType = hoodieCatalogTable.tableTypeName
    val tableConfig = hoodieCatalogTable.tableConfig

    val combinedOpts: Map[String, String] = combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = extraOptions)
    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig, extraOptions)

    val partitionFieldsStr = hoodieCatalogTable.partitionFields.mkString(",")

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = hoodieCatalogTable.preCombineKey.getOrElse("")

    val hiveStylePartitioningEnable = Option(tableConfig.getHiveStylePartitioningEnable).getOrElse("true")
    val urlEncodePartitioning = Option(tableConfig.getUrlEncodePartitioning).getOrElse("false")
    val keyGeneratorClassName = Option(tableConfig.getKeyGeneratorClassName)
      .getOrElse(classOf[ComplexKeyGenerator].getCanonicalName)

    val enableBulkInsert = combinedOpts.getOrElse(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key,
      DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.defaultValue()).toBoolean
    val dropDuplicate = sparkSession.conf
      .getOption(INSERT_DROP_DUPS.key).getOrElse(INSERT_DROP_DUPS.defaultValue).toBoolean

    val insertMode = InsertMode.of(combinedOpts.getOrElse(DataSourceWriteOptions.SQL_INSERT_MODE.key,
      DataSourceWriteOptions.SQL_INSERT_MODE.defaultValue()))
    val isNonStrictMode = insertMode == InsertMode.NON_STRICT
    val isPartitionedTable = hoodieCatalogTable.partitionFields.nonEmpty
    val hasPrecombineColumn = hoodieCatalogTable.preCombineKey.nonEmpty

    // NOTE: Target operation could be overridden by the user, therefore if it has been provided as an input
    //       we'd prefer that value over auto-deduced operation. Otherwise, we deduce target operation type
    val operationOverride = combinedOpts.get(DataSourceWriteOptions.OPERATION.key)
    val operation = operationOverride.getOrElse {
      (enableBulkInsert, isOverwritePartition, isOverwriteTable, dropDuplicate, isNonStrictMode, isPartitionedTable) match {
        case (true, _, _, _, false, _) =>
          throw new IllegalArgumentException(s"Table with primaryKey can not use bulk insert in ${insertMode.value()} mode.")
        case (true, true, _, _, _, true) =>
          throw new IllegalArgumentException(s"Insert Overwrite Partition can not use bulk insert.")
        case (true, _, _, true, _, _) =>
          throw new IllegalArgumentException(s"Bulk insert cannot support drop duplication." +
            s" Please disable $INSERT_DROP_DUPS and try again.")
        // if enableBulkInsert is true, use bulk insert for the insert overwrite non-partitioned table.
        case (true, false, true, _, _, false) => BULK_INSERT_OPERATION_OPT_VAL
        // insert overwrite table
        case (false, false, true, _, _, _) => INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
        // insert overwrite partition
        case (_, true, false, _, _, true) => INSERT_OVERWRITE_OPERATION_OPT_VAL
        // disable dropDuplicate, and provide preCombineKey, use the upsert operation for strict and upsert mode.
        case (false, false, false, false, false, _) if hasPrecombineColumn => UPSERT_OPERATION_OPT_VAL
        // if table is pk table and has enableBulkInsert use bulk insert for non-strict mode.
        case (true, _, _, _, true, _) => BULK_INSERT_OPERATION_OPT_VAL
        // for the rest case, use the insert operation
        case _ => INSERT_OPERATION_OPT_VAL
      }
    }

    val payloadClassName = if (operation == UPSERT_OPERATION_OPT_VAL &&
      tableType == COW_TABLE_TYPE_OPT_VAL && insertMode == InsertMode.STRICT) {
      // Validate duplicate key for COW, for MOR it will do the merge with the DefaultHoodieRecordPayload
      // on reading.
      // TODO use HoodieSparkValidateDuplicateKeyRecordMerger when SparkRecordMerger is default
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else if (operation == INSERT_OPERATION_OPT_VAL && tableType == COW_TABLE_TYPE_OPT_VAL &&
      insertMode == InsertMode.STRICT){
      // Validate duplicate key for inserts to COW table when using strict insert mode.
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else {
      classOf[OverwriteWithLatestAvroPayload].getCanonicalName
    }

    val defaultOpts = Map(
      PAYLOAD_CLASS_NAME.key -> payloadClassName,
      // NOTE: By default insert would try to do deduplication in case that pre-combine column is specified
      //       for the table
      HoodieWriteConfig.COMBINE_BEFORE_INSERT.key -> String.valueOf(hasPrecombineColumn),
      KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> keyGeneratorClassName,
      SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL,
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> partitionFieldsStr,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_SYNC_MODE, HiveSyncMode.HMS.name()),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_DATABASE_NAME),
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_TABLE_NAME),
      HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS)
    )

    val overridingOpts = extraOptions ++ Map(
      "path" -> path,
      TABLE_TYPE.key -> tableType,
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      OPERATION.key -> operation,
      HIVE_STYLE_PARTITIONING.key -> hiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> urlEncodePartitioning,
      RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
      PRECOMBINE_FIELD.key -> preCombineField,
      PARTITIONPATH_FIELD.key -> partitionFieldsStr
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def buildHoodieDropPartitionsConfig(sparkSession: SparkSession,
                                      hoodieCatalogTable: HoodieCatalogTable,
                                      partitionsToDrop: String): Map[String, String] = {
    val partitionFields = hoodieCatalogTable.partitionFields.mkString(",")
    val tableConfig = hoodieCatalogTable.tableConfig

    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)

    val overridingOpts = Map(
      "path" -> hoodieCatalogTable.tableLocation,
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      TABLE_TYPE.key -> hoodieCatalogTable.tableTypeName,
      OPERATION.key -> DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL,
      PARTITIONS_TO_DELETE.key -> partitionsToDrop,
      RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
      PRECOMBINE_FIELD.key -> hoodieCatalogTable.preCombineKey.getOrElse(""),
      PARTITIONPATH_FIELD.key -> partitionFields,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_SYNC_MODE, HiveSyncMode.HMS.name()),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_DATABASE_NAME),
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_TABLE_NAME),
      HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> partitionFields,
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS)
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = overridingOpts)
  }

  def buildHoodieDeleteTableConfig(hoodieCatalogTable: HoodieCatalogTable,
                                   sparkSession: SparkSession): Map[String, String] = {
    val path = hoodieCatalogTable.tableLocation
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableSchema = hoodieCatalogTable.tableSchema
    val partitionColumns = tableConfig.getPartitionFieldProp.split(",").map(_.toLowerCase(Locale.ROOT))
    val partitionSchema = StructType(tableSchema.filter(f => partitionColumns.contains(f.name)))

    assert(hoodieCatalogTable.primaryKeys.nonEmpty,
      s"There are no primary key defined in table ${hoodieCatalogTable.table.identifier}, cannot execute delete operation")

    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)

    val defaultOpts = Map(
      KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
      SqlKeyGenerator.PARTITION_SCHEMA -> partitionSchema.toDDL,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> hiveSyncConfig.getString(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_MODE.key -> hiveSyncConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_SYNC_MODE, HiveSyncMode.HMS.name()),
      HoodieSyncConfig.META_SYNC_DATABASE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_DATABASE_NAME),
      HoodieSyncConfig.META_SYNC_TABLE_NAME.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_TABLE_NAME),
      HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key -> hiveSyncConfig.getBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE).toString,
      HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key -> hoodieCatalogTable.partitionFields.mkString(","),
      HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key -> hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS)
    )

    val overridingOpts = Map(
      "path" -> path,
      RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
      TBL_NAME.key -> tableConfig.getTableName,
      HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
      OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp
    )

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def buildHiveSyncConfig(sparkSession: SparkSession,
                          hoodieCatalogTable: HoodieCatalogTable,
                          tableConfig: HoodieTableConfig,
                          extraOptions: Map[String, String] = Map.empty): HiveSyncConfig = {
    val combinedOpts = combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = extraOptions)
    val props = new TypedProperties(toProperties(combinedOpts))

    // Enable the hive sync by default if spark have enable the hive metastore.
    val enableHive = isUsingHiveCatalog(sparkSession)
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig(props)
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_ENABLED.key, enableHive.toString)
    hiveSyncConfig.setValue(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, enableHive.toString)
    hiveSyncConfig.setValue(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, props.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, HiveSyncMode.HMS.name()))
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_BASE_PATH, hoodieCatalogTable.tableLocation)
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT, hoodieCatalogTable.baseFileFormat)
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_DATABASE_NAME, hoodieCatalogTable.table.identifier.database.getOrElse("default"))
    hiveSyncConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_TABLE_NAME, hoodieCatalogTable.table.identifier.table)
    if (props.get(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key) != null) {
      hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS, props.getString(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key))
    }
    hiveSyncConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS, classOf[MultiPartKeysValueExtractor].getName)
    hiveSyncConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE, "true")
    if (hiveSyncConfig.useBucketSync())
      hiveSyncConfig.setValue(HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC,
        HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key),
          props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key)))
    if (props.containsKey(HiveExternalCatalog.CREATED_SPARK_VERSION))
      hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_SPARK_VERSION,
        props.getString(HiveExternalCatalog.CREATED_SPARK_VERSION))
    hiveSyncConfig
  }
}

object ProvidesHoodieConfig {

  // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
  //
  // Spark SQL operations configuration might be coming from a variety of diverse sources
  // that have to be ultimately combined under clear and consistent process:
  //
  //    - Default: specify default values preferred by the feature/component (could be
  //      overridden by any source)
  //
  //    - Overriding: specify mandatory values required for the feature/component (could NOT be
  //      overridden by any source)s
  //
  def combineOptions(catalogTable: HoodieCatalogTable,
                             tableConfig: HoodieTableConfig,
                             sqlConf: SQLConf,
                             defaultOpts: Map[String, String],
                             overridingOpts: Map[String, String] = Map.empty): Map[String, String] = {
    // NOTE: Properties are merged in the following order of priority (first has the highest priority, last has the
    //       lowest, which is inverse to the ordering in the code):
    //          1. (Extra) Option overrides
    //          2. Spark SQL configs
    //          3. Persisted Hudi's Table configs
    //          4. Table's properties in Spark Catalog
    //          5. Global DFS properties
    //          6. (Feature-specific) Default values
    filterNullValues(defaultOpts) ++
      DFSPropertiesConfiguration.getGlobalProps.asScala.toMap ++
      // NOTE: Catalog table provided t/h `TBLPROPERTIES` clause might contain Spark SQL specific
      //       properties that need to be mapped into Hudi's conventional ones
      mapSqlOptionsToDataSourceWriteConfigs(catalogTable.catalogProperties) ++
      tableConfig.getProps.asScala.toMap ++
      filterHoodieConfigs(sqlConf.getAllConfs) ++
      filterNullValues(overridingOpts)
  }

  private def filterNullValues(opts: Map[String, String]): Map[String, String] =
    opts.filter { case (_, v) => v != null }

  private def filterHoodieConfigs(opts: Map[String, String]): Map[String, String] =
    opts.filterKeys(isHoodieConfigKey)

}
