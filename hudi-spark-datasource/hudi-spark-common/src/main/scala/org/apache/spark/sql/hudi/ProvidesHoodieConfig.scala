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
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.sql.InsertMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{isEnableHive, withSparkConf}
import org.apache.spark.sql.hudi.command.{SqlKeyGenerator, ValidateDuplicateKeyPayload}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

trait ProvidesHoodieConfig extends Logging {

  def buildHoodieConfig(hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val sparkSession: SparkSession = hoodieCatalogTable.spark
    val catalogProperties = hoodieCatalogTable.catalogProperties
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableId = hoodieCatalogTable.table.identifier

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = Option(tableConfig.getPreCombineField).getOrElse("")

    require(hoodieCatalogTable.primaryKeys.nonEmpty,
      s"There are no primary key in table ${hoodieCatalogTable.table.identifier}, cannot execute update operator")
    val enableHive = isEnableHive(sparkSession)

    withSparkConf(sparkSession, catalogProperties) {
      Map.apply(
        "path" -> hoodieCatalogTable.tableLocation,
        RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
        TBL_NAME.key -> hoodieCatalogTable.tableName,
        PRECOMBINE_FIELD.key -> preCombineField,
        HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
        URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitioning,
        KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
        SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
        OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp,
        META_SYNC_ENABLED.key -> enableHive.toString,
        HIVE_SYNC_MODE.key -> HiveSyncMode.HMS.name(),
        HIVE_USE_JDBC.key -> "false",
        HIVE_DATABASE.key -> tableId.database.getOrElse("default"),
        HIVE_TABLE.key -> tableId.table,
        HIVE_PARTITION_FIELDS.key -> tableConfig.getPartitionFieldProp,
        HIVE_PARTITION_EXTRACTOR_CLASS.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        HIVE_SUPPORT_TIMESTAMP_TYPE.key -> "true",
        HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL
      )
        .filter { case(_, v) => v != null }
    }
  }

  /**
   * Build the default config for insert.
   *
   * @return
   */
  def buildHoodieInsertConfig(hoodieCatalogTable: HoodieCatalogTable,
                              sparkSession: SparkSession,
                              isOverwrite: Boolean,
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
    val tableSchema = hoodieCatalogTable.tableSchema

    val options = hoodieCatalogTable.catalogProperties ++ tableConfig.getProps.asScala.toMap ++ extraOptions
    val parameters = withSparkConf(sparkSession, options)()

    val partitionFieldsStr = hoodieCatalogTable.partitionFields.mkString(",")

    // NOTE: Here we fallback to "" to make sure that null value is not overridden with
    // default value ("ts")
    // TODO(HUDI-3456) clean up
    val preCombineField = hoodieCatalogTable.preCombineKey.getOrElse("")

    val hiveStylePartitioningEnable = Option(tableConfig.getHiveStylePartitioningEnable).getOrElse("true")
    val urlEncodePartitioning = Option(tableConfig.getUrlEncodePartitioning).getOrElse("false")
    val keyGeneratorClassName = Option(tableConfig.getKeyGeneratorClassName)
      .getOrElse(classOf[ComplexKeyGenerator].getCanonicalName)

    val enableBulkInsert = parameters.getOrElse(DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key,
      DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.defaultValue()).toBoolean
    val dropDuplicate = sparkSession.conf
      .getOption(INSERT_DROP_DUPS.key).getOrElse(INSERT_DROP_DUPS.defaultValue).toBoolean

    val insertMode = InsertMode.of(parameters.getOrElse(DataSourceWriteOptions.SQL_INSERT_MODE.key,
      DataSourceWriteOptions.SQL_INSERT_MODE.defaultValue()))
    val isNonStrictMode = insertMode == InsertMode.NON_STRICT
    val isPartitionedTable = hoodieCatalogTable.partitionFields.nonEmpty
    val hasPrecombineColumn = hoodieCatalogTable.preCombineKey.nonEmpty
    val operation =
      (enableBulkInsert, isOverwrite, dropDuplicate, isNonStrictMode, isPartitionedTable) match {
        case (true, _, _, false, _) =>
          throw new IllegalArgumentException(s"Table with primaryKey can not use bulk insert in ${insertMode.value()} mode.")
        case (true, true, _, _, true) =>
          throw new IllegalArgumentException(s"Insert Overwrite Partition can not use bulk insert.")
        case (true, _, true, _, _) =>
          throw new IllegalArgumentException(s"Bulk insert cannot support drop duplication." +
            s" Please disable $INSERT_DROP_DUPS and try again.")
        // if enableBulkInsert is true, use bulk insert for the insert overwrite non-partitioned table.
        case (true, true, _, _, false) => BULK_INSERT_OPERATION_OPT_VAL
        // insert overwrite table
        case (false, true, _, _, false) => INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL
        // insert overwrite partition
        case (_, true, _, _, true) => INSERT_OVERWRITE_OPERATION_OPT_VAL
        // disable dropDuplicate, and provide preCombineKey, use the upsert operation for strict and upsert mode.
        case (false, false, false, false, _) if hasPrecombineColumn => UPSERT_OPERATION_OPT_VAL
        // if table is pk table and has enableBulkInsert use bulk insert for non-strict mode.
        case (true, _, _, true, _) => BULK_INSERT_OPERATION_OPT_VAL
        // for the rest case, use the insert operation
        case _ => INSERT_OPERATION_OPT_VAL
      }

    val payloadClassName = if (operation == UPSERT_OPERATION_OPT_VAL &&
      tableType == COW_TABLE_TYPE_OPT_VAL && insertMode == InsertMode.STRICT) {
      // Only validate duplicate key for COW, for MOR it will do the merge with the DefaultHoodieRecordPayload
      // on reading.
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else {
      classOf[OverwriteWithLatestAvroPayload].getCanonicalName
    }

    logInfo(s"Insert statement use write operation type: $operation, payloadClass: $payloadClassName")

    val enableHive = isEnableHive(sparkSession)

    withSparkConf(sparkSession, options) {
      Map(
        "path" -> path,
        TABLE_TYPE.key -> tableType,
        TBL_NAME.key -> hoodieCatalogTable.tableName,
        OPERATION.key -> operation,
        HIVE_STYLE_PARTITIONING.key -> hiveStylePartitioningEnable,
        URL_ENCODE_PARTITIONING.key -> urlEncodePartitioning,
        KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
        SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> keyGeneratorClassName,
        RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
        PRECOMBINE_FIELD.key -> preCombineField,
        PARTITIONPATH_FIELD.key -> partitionFieldsStr,
        PAYLOAD_CLASS_NAME.key -> payloadClassName,
        ENABLE_ROW_WRITER.key -> enableBulkInsert.toString,
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.key -> String.valueOf(hasPrecombineColumn),
        HIVE_PARTITION_FIELDS.key -> partitionFieldsStr,
        META_SYNC_ENABLED.key -> enableHive.toString,
        HIVE_SYNC_MODE.key -> HiveSyncMode.HMS.name(),
        HIVE_USE_JDBC.key -> "false",
        HIVE_DATABASE.key -> hoodieCatalogTable.table.identifier.database.getOrElse("default"),
        HIVE_TABLE.key -> hoodieCatalogTable.table.identifier.table,
        HIVE_SUPPORT_TIMESTAMP_TYPE.key -> "true",
        HIVE_PARTITION_EXTRACTOR_CLASS.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "200",
        HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL
      )
        .filter { case (_, v) => v != null }
    }
  }

}
