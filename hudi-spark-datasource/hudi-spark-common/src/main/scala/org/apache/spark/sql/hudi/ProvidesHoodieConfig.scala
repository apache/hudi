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

import org.apache.hudi.{DataSourceWriteOptions, HoodieFileIndex}
import org.apache.hudi.AutoRecordKeyGenerationUtils.shouldAutoGenerateRecordKeys
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, HoodieCommonConfig}
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME
import org.apache.hudi.common.util.{ReflectionUtils, StringUtils}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.HoodieIndex.BucketIndexEngineType
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator}
import org.apache.hudi.sql.InsertMode
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hudi.HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{filterHoodieConfigs, isUsingHiveCatalog}
import org.apache.spark.sql.hudi.ProvidesHoodieConfig.{buildOverridingOpts, buildOverridingOptsForDelete, combineOptions, getPartitionPathFieldWriteConfig}
import org.apache.spark.sql.hudi.command.SqlKeyGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PARTITION_OVERWRITE_MODE
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util.Locale

import scala.collection.JavaConverters._

trait ProvidesHoodieConfig extends Logging {

  def buildHoodieConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val sparkSession: SparkSession = hoodieCatalogTable.spark
    val tableConfig = hoodieCatalogTable.tableConfig

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineFields = tableConfig.getPreCombineFieldsStr.orElse("")
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

    val overridingOpts = buildOverridingOpts(hoodieCatalogTable, preCombineFields)
    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def buildBucketRescaleHoodieConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val sparkSession: SparkSession = hoodieCatalogTable.spark
    val tableConfig = hoodieCatalogTable.tableConfig
    val preCombineFields = tableConfig.getPreCombineFieldsStr.orElse("")
    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)

    val defaultOpts = Map[String, String](
      KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
      HoodieIndexConfig.INDEX_TYPE.key() -> HoodieIndex.IndexType.BUCKET.name(),
      HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key() -> BucketIndexEngineType.SIMPLE.name(),
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
      SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL,
      HoodieSyncConfig.META_SYNC_ENABLED.key -> hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_ENABLED.key),
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key -> "false"
    )

    val overridingOpts = buildOverridingOpts(hoodieCatalogTable, preCombineFields)
    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  /**
   * Deduce the sql write operation for INSERT_INTO
   */
  private def deduceSparkSqlInsertIntoWriteOperation(isOverwritePartition: Boolean, isOverwriteTable: Boolean,
                                                     shouldAutoKeyGen: Boolean, preCombineField: String,
                                                     sparkSqlInsertIntoOperationSet: Boolean, sparkSqlInsertIntoOperation: String): String = {
    if (isOverwriteTable) {
      INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
    } else if (isOverwritePartition) {
      INSERT_OVERWRITE_OPERATION_OPT_VAL
    } else if (!sparkSqlInsertIntoOperationSet && !shouldAutoKeyGen && preCombineField.nonEmpty) {
      UPSERT_OPERATION_OPT_VAL
    } else {
      sparkSqlInsertIntoOperation
    }
  }

  /**
   * Deduce the insert operation
   */
  private def deduceOperation(enableBulkInsert: Boolean, isOverwritePartition: Boolean, isOverwriteTable: Boolean,
                              dropDuplicate: Boolean, isNonStrictMode: Boolean, isPartitionedTable: Boolean,
                              combineBeforeInsert: Boolean, insertMode: InsertMode, autoGenerateRecordKeys: Boolean): String = {
    (enableBulkInsert, isOverwritePartition, isOverwriteTable, dropDuplicate, isNonStrictMode, isPartitionedTable, autoGenerateRecordKeys) match {
      case (true, _, _, _, false, _, _) =>
        throw new IllegalArgumentException(s"Table with primaryKey can not use bulk insert in ${insertMode.value()} mode.")
      case (true, _, _, true, _, _, _) =>
        throw new IllegalArgumentException(s"Bulk insert cannot support drop duplication." +
          s" Please disable $INSERT_DROP_DUPS and try again.")
      // Bulk insert with overwrite table
      case (true, false, true, _, _, _, _) =>
        BULK_INSERT_OPERATION_OPT_VAL
      // Bulk insert with overwrite table partition
      case (true, true, false, _, _, true, _) =>
        BULK_INSERT_OPERATION_OPT_VAL
      // insert overwrite table
      case (false, false, true, _, _, _, _) => INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
      // insert overwrite partition
      case (false, true, false, _, _, true, _) => INSERT_OVERWRITE_OPERATION_OPT_VAL
      // disable dropDuplicate, and provide preCombineKey, use the upsert operation for strict and upsert mode.
      case (false, false, false, false, false, _, _) if combineBeforeInsert => UPSERT_OPERATION_OPT_VAL
      // if table is pk table and has enableBulkInsert use bulk insert for non-strict mode.
      case (true, false, false, _, true, _, _) => BULK_INSERT_OPERATION_OPT_VAL
      // if auto record key generation is enabled, use bulk_insert
      case (_, _, _, _, _, _, true) => BULK_INSERT_OPERATION_OPT_VAL
      // for the rest case, use the insert operation
      case _ => INSERT_OPERATION_OPT_VAL
    }
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
                              extraOptions: Map[String, String],
                              staticOverwritePartitionPathOpt: Option[String] = Option.empty): Map[String, String] = {

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
    val preCombineField = combinedOpts.getOrElse(HoodieTableConfig.PRECOMBINE_FIELDS.key,
      combinedOpts.getOrElse(PRECOMBINE_FIELD.key, ""))

    val hiveStylePartitioningEnable = Option(tableConfig.getHiveStylePartitioningEnable).getOrElse("true")
    val urlEncodePartitioning = Option(tableConfig.getUrlEncodePartitioning).getOrElse("false")
    val keyGeneratorClassName = Option(tableConfig.getKeyGeneratorClassName)
      .getOrElse(classOf[ComplexKeyGenerator].getCanonicalName)

    val enableBulkInsert = combinedOpts.getOrElse(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key,
      DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.defaultValue()).toBoolean
    val dropDuplicate = sparkSession.conf
      .getOption(INSERT_DROP_DUPS.key).getOrElse(INSERT_DROP_DUPS.defaultValue).toBoolean
    val shouldAutoKeyGen: Boolean = shouldAutoGenerateRecordKeys(combinedOpts)

    val insertMode = InsertMode.of(combinedOpts.getOrElse(DataSourceWriteOptions.SQL_INSERT_MODE.key,
      DataSourceWriteOptions.SQL_INSERT_MODE.defaultValue()))
    val insertModeSet = combinedOpts.contains(SQL_INSERT_MODE.key)
    val sparkSqlInsertIntoOperationOpt = combinedOpts.get(SPARK_SQL_INSERT_INTO_OPERATION.key())
    val sparkSqlInsertIntoOperationSet = sparkSqlInsertIntoOperationOpt.nonEmpty
    val sparkSqlInsertIntoOperation = sparkSqlInsertIntoOperationOpt.getOrElse(SPARK_SQL_INSERT_INTO_OPERATION.defaultValue())
    val insertDupPolicyOpt = combinedOpts.get(INSERT_DUP_POLICY.key())
    val insertDupPolicySet = insertDupPolicyOpt.nonEmpty
    val insertDupPolicy = combinedOpts.getOrElse(INSERT_DUP_POLICY.key(), INSERT_DUP_POLICY.defaultValue())
    val isNonStrictMode = insertMode == InsertMode.NON_STRICT
    val isPartitionedTable = hoodieCatalogTable.partitionFields.nonEmpty
    val combineBeforeInsert = !hoodieCatalogTable.preCombineKeys.isEmpty && hoodieCatalogTable.primaryKeys.nonEmpty

    /*
     * The sql write operation has higher precedence than the legacy insert mode.
     * Only when the legacy insert mode is explicitly set, without setting sql write operation,
     * legacy configs will be honored. On all other cases (i.e when both are set, either is set,
     * or when only the sql write operation is set), we honor the sql write operation.
     */
    val useLegacyInsertModeFlow = insertModeSet && !sparkSqlInsertIntoOperationSet
    var operation = combinedOpts.getOrElse(OPERATION.key,
      if (useLegacyInsertModeFlow) {
        // NOTE: Target operation could be overridden by the user, therefore if it has been provided as an input
        //       we'd prefer that value over auto-deduced operation. Otherwise, we deduce target operation type
        deduceOperation(enableBulkInsert, isOverwritePartition, isOverwriteTable, dropDuplicate,
          isNonStrictMode, isPartitionedTable, combineBeforeInsert, insertMode, shouldAutoKeyGen)
      } else {
        deduceSparkSqlInsertIntoWriteOperation(isOverwritePartition, isOverwriteTable,
          shouldAutoKeyGen, preCombineField, sparkSqlInsertIntoOperationSet, sparkSqlInsertIntoOperation)
      }
    )

    val overwriteTableOpts = if (operation.equals(BULK_INSERT_OPERATION_OPT_VAL)) {
      if (isOverwriteTable) {
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE_TABLE.value())
      } else if (isOverwritePartition) {
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE.value())
      } else {
        Map()
      }
    } else if (operation.equals(INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)) {
      if (sparkSqlInsertIntoOperation.equals(BULK_INSERT_OPERATION_OPT_VAL) || enableBulkInsert) {
        operation = BULK_INSERT_OPERATION_OPT_VAL
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE_TABLE.value())
      } else {
        Map()
      }
    } else if (operation.equals(INSERT_OVERWRITE_OPERATION_OPT_VAL)) {
      if (sparkSqlInsertIntoOperation.equals(BULK_INSERT_OPERATION_OPT_VAL) || enableBulkInsert) {
        operation = BULK_INSERT_OPERATION_OPT_VAL
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE.value())
      } else {
        Map()
      }
    } else {
      Map()
    }

    val staticOverwritePartitionPathOptions = staticOverwritePartitionPathOpt match {
      case Some(staticOverwritePartitionPath) =>
        Map(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS.key() -> staticOverwritePartitionPath)
      case _ =>
        Map()
    }

    val defaultOpts = Map(
      // NOTE: By default insert would try to do deduplication in case that pre-combine column is specified
      //       for the table
      HoodieWriteConfig.COMBINE_BEFORE_INSERT.key -> String.valueOf(combineBeforeInsert),
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
    // for auto record key gen
    val recordKeyConfigValue = if (hoodieCatalogTable.primaryKeys.length > 1) {
      hoodieCatalogTable.primaryKeys.mkString(",")
    } else {
      null
    }

    val overridingOpts = extraOptions ++ Map(
      "path" -> path,
      TABLE_TYPE.key -> tableType,
      DATABASE_NAME.key -> hoodieCatalogTable.table.database,
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      OPERATION.key -> operation,
      HIVE_STYLE_PARTITIONING.key -> hiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> urlEncodePartitioning,
      RECORDKEY_FIELD.key -> recordKeyConfigValue,
      PRECOMBINE_FIELD.key -> preCombineField,
      PARTITIONPATH_FIELD.key -> getPartitionPathFieldWriteConfig(
        keyGeneratorClassName, partitionFieldsStr, hoodieCatalogTable)
    ) ++ overwriteTableOpts ++ getDropDupsConfig(useLegacyInsertModeFlow, combinedOpts) ++ staticOverwritePartitionPathOptions

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def getDropDupsConfig(useLegacyInsertModeFlow: Boolean, incomingParams: Map[String, String]): Map[String, String] = {
    if (!useLegacyInsertModeFlow) {
      Map(DataSourceWriteOptions.INSERT_DUP_POLICY.key() -> incomingParams.getOrElse(DataSourceWriteOptions.INSERT_DUP_POLICY.key(),
        DataSourceWriteOptions.INSERT_DUP_POLICY.defaultValue()),
        if (incomingParams.contains(DataSourceWriteOptions.INSERT_DUP_POLICY.key()) &&
          incomingParams(DataSourceWriteOptions.INSERT_DUP_POLICY.key()) == DROP_INSERT_DUP_POLICY) {
          DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "true"
        } else {
          DataSourceWriteOptions.INSERT_DROP_DUPS.key() -> "false"
        }
      )
    } else {
      Map()
    }
  }

  /**
   * Deduce the overwrite config based on writeOperation and overwriteMode config.
   * If hoodie.datasource.write.operation is insert_overwrite/insert_overwrite_table, use dynamic overwrite;
   * else if hoodie.datasource.overwrite.mode is configured, use it;
   * else use spark.sql.sources.partitionOverwriteMode.
   *
   * The returned staticOverwritePartitionPathOpt is defined only in static insert_overwrite case.
   *
   * @return (overwriteMode, isOverWriteTable, isOverWritePartition, staticOverwritePartitionPathOpt)
   */
  def deduceOverwriteConfig(sparkSession: SparkSession,
                            catalogTable: HoodieCatalogTable,
                            partitionSpec: Map[String, Option[String]],
                            extraOptions: Map[String, String]): (SaveMode, Boolean, Boolean, Option[String]) = {
    val overridingOpts = extraOptions ++ Map(
      "path" -> catalogTable.tableLocation
    )
    val combinedOpts: Map[String, String] = combineOptions(catalogTable, catalogTable.tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = overridingOpts)
    val operation = combinedOpts.getOrElse(OPERATION.key, null)
    val isOverwriteOperation = operation != null &&
      (operation.equals(INSERT_OVERWRITE_OPERATION_OPT_VAL) || operation.equals(INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL))
    // If hoodie.datasource.overwrite.mode configured, respect it, otherwise respect spark.sql.sources.partitionOverwriteMode
    val hoodieOverwriteMode = combinedOpts.getOrElse(OVERWRITE_MODE.key,
      sparkSession.sqlContext.getConf(PARTITION_OVERWRITE_MODE.key)).toUpperCase()
    val isStaticOverwrite = !isOverwriteOperation && (hoodieOverwriteMode match {
      case "STATIC" => true
      case "DYNAMIC" => false
      case _ => throw new IllegalArgumentException("Config hoodie.datasource.overwrite.mode is illegal")
    })
    val isOverWriteTable = operation match {
      case INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL => true
      case INSERT_OVERWRITE_OPERATION_OPT_VAL => false
      case _ =>
        // There are two cases where we need use insert_overwrite_table
        // 1. NonPartitioned table always insert overwrite whole table
        // 2. static mode and no partition values specified
        catalogTable.partitionFields.isEmpty || (isStaticOverwrite && partitionSpec.isEmpty)
    }
    val overwriteMode = if (isOverWriteTable) SaveMode.Overwrite else SaveMode.Append
    val staticPartitions = if (isStaticOverwrite && !isOverWriteTable) {
      val fileIndex = HoodieFileIndex(sparkSession, catalogTable.metaClient, None, combinedOpts, FileStatusCache.getOrCreate(sparkSession))
      val partitionNameToType = catalogTable.partitionSchema.fields.map(field => (field.name, field.dataType)).toMap
      val staticPartitionValues = partitionSpec.filter(p => p._2.isDefined).mapValues(_.get)
      val predicates = staticPartitionValues.map { case (k, v) =>
        val partition = AttributeReference(k, partitionNameToType(k))()
        val value = HoodieSqlCommonUtils.castIfNeeded(Literal.create(v), partitionNameToType(k))
        EqualTo(partition, value)
      }.toSeq
      Option(fileIndex.getPartitionPaths(predicates).map(_.getPath).mkString(","))
    } else {
      Option.empty
    }
    (overwriteMode, isOverWriteTable, !isOverWriteTable, staticPartitions)
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
      DATABASE_NAME.key -> hoodieCatalogTable.table.database,
      TABLE_TYPE.key -> hoodieCatalogTable.tableTypeName,
      OPERATION.key -> DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL,
      PARTITIONS_TO_DELETE.key -> partitionsToDrop,
      RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
      PRECOMBINE_FIELD.key -> String.join(",", hoodieCatalogTable.preCombineKeys),
      PARTITIONPATH_FIELD.key -> getPartitionPathFieldWriteConfig(
        tableConfig.getKeyGeneratorClassName, partitionFields, hoodieCatalogTable),
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
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableSchema = hoodieCatalogTable.tableSchema
    val partitionColumns = tableConfig.getPartitionFieldProp.split(",").map(_.toLowerCase(Locale.ROOT))
    val partitionSchema = StructType(tableSchema.filter(f => partitionColumns.contains(f.name)))

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

    val overridingOpts = buildOverridingOptsForDelete(hoodieCatalogTable)
    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def buildHiveSyncConfig(sparkSession: SparkSession,
                          hoodieCatalogTable: HoodieCatalogTable,
                          tableConfig: HoodieTableConfig,
                          extraOptions: Map[String, String] = Map.empty): HiveSyncConfig = {
    val combinedOpts = combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = extraOptions)
    val props = toProperties(combinedOpts)

    // Enable the hive sync by default if spark have enable the hive metastore.
    val enableHive = isUsingHiveCatalog(sparkSession)
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig(props)
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_ENABLED.key, enableHive.toString)
    hiveSyncConfig.setValue(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, enableHive.toString)
    hiveSyncConfig.setValue(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, props.getString(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, HiveSyncMode.HMS.name()))
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_BASE_PATH, hoodieCatalogTable.tableLocation)
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT, props.getString(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key, HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.defaultValue))
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_DATABASE_NAME, hoodieCatalogTable.table.identifier.database.getOrElse("default"))
    hiveSyncConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_TABLE_NAME, hoodieCatalogTable.table.identifier.table)
    if (props.get(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key) != null) {
      hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS, props.getString(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key))
    }
    hiveSyncConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS, classOf[MultiPartKeysValueExtractor].getName)
    // This is hardcoded to true to ensure consistency as Spark syncs TIMESTAMP types as TIMESTAMP by default
    // via Spark's externalCatalog API, which is used by AlterHoodieTableCommand.
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

  private val log = LoggerFactory.getLogger(getClass)

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

  /**
   * @param tableConfigKeyGeneratorClassName     key generator class name in the table config.
   * @param partitionFieldNamesWithoutKeyGenType partition field names without key generator types
   *                                             from the table config.
   * @param catalogTable                         HoodieCatalogTable instance to fetch table properties.
   * @return the write config value to set for "hoodie.datasource.write.partitionpath.field".
   */
  def getPartitionPathFieldWriteConfig(tableConfigKeyGeneratorClassName: String,
                                       partitionFieldNamesWithoutKeyGenType: String,
                                       catalogTable: HoodieCatalogTable): String = {
    if (StringUtils.isNullOrEmpty(tableConfigKeyGeneratorClassName)) {
      partitionFieldNamesWithoutKeyGenType
    } else {
      val writeConfigPartitionField = catalogTable.catalogProperties.get(PARTITIONPATH_FIELD.key())
      val keyGenClass = ReflectionUtils.getClass(tableConfigKeyGeneratorClassName)
      if (classOf[CustomKeyGenerator].equals(keyGenClass)
        || classOf[CustomAvroKeyGenerator].equals(keyGenClass)) {
        val partitionFieldWithKeyGenType = HoodieTableConfig.getPartitionFieldPropForKeyGenerator(catalogTable.tableConfig).orElse("")
        if (writeConfigPartitionField.isDefined) {
          writeConfigPartitionField.get
        } else if (StringUtils.nonEmpty(partitionFieldWithKeyGenType)) {
          partitionFieldWithKeyGenType
        } else {
          log.warn("Write config \"hoodie.datasource.write.partitionpath.field\" is not set for "
            + "custom key generator. This may fail the write operation.")
          partitionFieldNamesWithoutKeyGenType
        }
      } else {
        partitionFieldNamesWithoutKeyGenType
      }
    }
  }

  def isSchemaEvolutionEnabled(sparkSession: SparkSession): Boolean =
    sparkSession.sessionState.conf.getConfString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key,
      DFSPropertiesConfiguration.getGlobalProps.getString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(),
        HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue.toString)
    ).toBoolean

  private def filterNullValues(opts: Map[String, String]): Map[String, String] =
    opts.filter { case (_, v) => v != null }

  private def buildOverridingOpts(hoodieCatalogTable: HoodieCatalogTable,
                                  preCombineFields: String): Map[String, String] = {
    buildCommonOverridingOpts(hoodieCatalogTable) ++ Map(
      PRECOMBINE_FIELD.key -> preCombineFields
    )
  }

  private def buildOverridingOptsForDelete(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    buildCommonOverridingOpts(hoodieCatalogTable) ++ Map(
      OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
    )
  }

  private def buildCommonOverridingOpts(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val tableConfig = hoodieCatalogTable.tableConfig
    val baseOpts = Map(
      "path" -> hoodieCatalogTable.tableLocation,
      TBL_NAME.key -> tableConfig.getTableName,
      DATABASE_NAME.key -> hoodieCatalogTable.table.database,
      HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
      PARTITIONPATH_FIELD.key -> getPartitionPathFieldWriteConfig(
        tableConfig.getKeyGeneratorClassName,
        tableConfig.getPartitionFieldProp,
        hoodieCatalogTable)
    )
    if (hoodieCatalogTable.primaryKeys.isEmpty) {
      baseOpts
    } else {
      baseOpts + (RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","))
    }
  }
}
