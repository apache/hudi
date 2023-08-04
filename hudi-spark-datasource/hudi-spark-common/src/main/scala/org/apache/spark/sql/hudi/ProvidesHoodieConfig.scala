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
import org.apache.hudi.config.{HoodieIndexConfig, HoodieInternalConfig, HoodieWriteConfig}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
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
import org.apache.spark.sql.internal.SQLConf.PARTITION_OVERWRITE_MODE
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


  private def deducePayloadClassNameLegacy(operation: String, tableType: String, insertMode: InsertMode): String = {
    if (operation == UPSERT_OPERATION_OPT_VAL &&
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
  }

  /**
   * Deduce the sql write operation for INSERT_INTO
   */
  private def deduceSparkSqlInsertIntoWriteOperation(isOverwritePartition: Boolean, isOverwriteTable: Boolean,
                                                    sqlWriteOperation: String): String = {
    if (isOverwriteTable) {
      INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
    } else if (isOverwritePartition) {
      INSERT_OVERWRITE_OPERATION_OPT_VAL
    } else {
      sqlWriteOperation
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
      case (_, _, _, _, _,_,true) => BULK_INSERT_OPERATION_OPT_VAL
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
    val autoGenerateRecordKeys : Boolean = !combinedOpts.contains(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key());

    val insertMode = InsertMode.of(combinedOpts.getOrElse(DataSourceWriteOptions.SQL_INSERT_MODE.key,
      DataSourceWriteOptions.SQL_INSERT_MODE.defaultValue()))
    val insertModeSet = combinedOpts.contains(SQL_INSERT_MODE.key)
    val sqlWriteOperationOpt = combinedOpts.get(SPARK_SQL_INSERT_INTO_OPERATION.key())
    val sqlWriteOperationSet = sqlWriteOperationOpt.nonEmpty
    val sqlWriteOperation = sqlWriteOperationOpt.getOrElse(SPARK_SQL_INSERT_INTO_OPERATION.defaultValue())
    val insertDupPolicyOpt = combinedOpts.get(INSERT_DUP_POLICY.key())
    val insertDupPolicySet = insertDupPolicyOpt.nonEmpty
    val insertDupPolicy = combinedOpts.getOrElse(INSERT_DUP_POLICY.key(), INSERT_DUP_POLICY.defaultValue())
    val isNonStrictMode = insertMode == InsertMode.NON_STRICT
    val isPartitionedTable = hoodieCatalogTable.partitionFields.nonEmpty
    val combineBeforeInsert = hoodieCatalogTable.preCombineKey.nonEmpty && hoodieCatalogTable.primaryKeys.nonEmpty

    // try to use sql write operation instead of legacy insert mode. If only insert mode is explicitly specified, w/o specifying
    // any value for sql write operation, leagcy configs will be honored. But on all other cases (i.e when neither of the configs is set,
    // or when both configs are set, or when only sql write operation is set), we honor sql write operation and ignore
    // the insert mode.
    val useLegacyInsertModeFlow = insertModeSet && !sqlWriteOperationSet
    var operation = combinedOpts.getOrElse(OPERATION.key,
      if (useLegacyInsertModeFlow) {
        // NOTE: Target operation could be overridden by the user, therefore if it has been provided as an input
        //       we'd prefer that value over auto-deduced operation. Otherwise, we deduce target operation type
        deduceOperation(enableBulkInsert, isOverwritePartition, isOverwriteTable, dropDuplicate,
          isNonStrictMode, isPartitionedTable, combineBeforeInsert, insertMode, autoGenerateRecordKeys)
      } else {
        deduceSparkSqlInsertIntoWriteOperation(isOverwritePartition, isOverwriteTable, sqlWriteOperation)
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
      if (sqlWriteOperation.equals(BULK_INSERT_OPERATION_OPT_VAL) || enableBulkInsert) {
        operation = BULK_INSERT_OPERATION_OPT_VAL
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE_TABLE.value())
      } else {
        Map()
      }
    } else if (operation.equals(INSERT_OVERWRITE_OPERATION_OPT_VAL)) {
      if (sqlWriteOperation.equals(BULK_INSERT_OPERATION_OPT_VAL) || enableBulkInsert) {
        operation = BULK_INSERT_OPERATION_OPT_VAL
        Map(HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.INSERT_OVERWRITE.value())
      } else {
        Map()
      }
    } else {
      Map()
    }

    // try to use new insert dup policy instead of legacy insert mode to deduce payload class. If only insert mode is explicitly specified,
    // w/o specifying any value for insert dup policy, legacy configs will be honored. But on all other cases (i.e when neither of the configs is set,
    // or when both configs are set, or when only insert dup policy is set), we honor insert dup policy and ignore the insert mode.
    val useLegacyInsertDropDupFlow = insertModeSet && !insertDupPolicySet
    val payloadClassName =  if (useLegacyInsertDropDupFlow) {
      deducePayloadClassNameLegacy(operation, tableType, insertMode)
    } else {
      if (insertDupPolicy == FAIL_INSERT_DUP_POLICY) {
        classOf[ValidateDuplicateKeyPayload].getCanonicalName
      } else {
        classOf[OverwriteWithLatestAvroPayload].getCanonicalName
      }
    }

    val defaultOpts = Map(
      PAYLOAD_CLASS_NAME.key -> payloadClassName,
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
      TBL_NAME.key -> hoodieCatalogTable.tableName,
      OPERATION.key -> operation,
      HIVE_STYLE_PARTITIONING.key -> hiveStylePartitioningEnable,
      URL_ENCODE_PARTITIONING.key -> urlEncodePartitioning,
      RECORDKEY_FIELD.key -> recordKeyConfigValue,
      PRECOMBINE_FIELD.key -> preCombineField,
      PARTITIONPATH_FIELD.key -> partitionFieldsStr
    ) ++ overwriteTableOpts ++ getDropDupsConfig(useLegacyInsertModeFlow, combinedOpts)

    combineOptions(hoodieCatalogTable, tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = defaultOpts, overridingOpts = overridingOpts)
  }

  def getDropDupsConfig(useLegacyInsertModeFlow: Boolean, incomingParams : Map[String, String]): Map[String, String] = {
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

  def deduceIsOverwriteTable(sparkSession: SparkSession,
                             catalogTable: HoodieCatalogTable,
                             partitionSpec: Map[String, Option[String]],
                             extraOptions: Map[String, String]): Boolean = {
    val combinedOpts: Map[String, String] = combineOptions(catalogTable, catalogTable.tableConfig, sparkSession.sqlContext.conf,
      defaultOpts = Map.empty, overridingOpts = extraOptions)
    val operation = combinedOpts.getOrElse(OPERATION.key, null)
    operation match {
      case INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL =>
        true
      case INSERT_OVERWRITE_OPERATION_OPT_VAL =>
        false
      case _ =>
        // NonPartitioned table always insert overwrite whole table
        if (catalogTable.partitionFields.isEmpty) {
          true
        } else {
          // Insert overwrite partitioned table with PARTITION clause will always insert overwrite the specific partition
          if (partitionSpec.nonEmpty) {
            false
          } else {
            // If hoodie.datasource.overwrite.mode configured, respect it, otherwise respect spark.sql.sources.partitionOverwriteMode
            val hoodieOverwriteMode = combinedOpts.getOrElse(OVERWRITE_MODE.key,
              sparkSession.sqlContext.getConf(PARTITION_OVERWRITE_MODE.key)).toUpperCase()

            hoodieOverwriteMode match {
              case "STATIC" =>
                true
              case "DYNAMIC" =>
                false
              case _ =>
                throw new IllegalArgumentException("Config hoodie.datasource.overwrite.mode is illegal")
            }
          }
        }
    }
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
