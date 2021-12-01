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

package org.apache.spark.sql.hudi.command

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.sql.InsertMode
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkSqlWriter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Command for insert into hoodie table.
 */
case class InsertIntoHoodieTableCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    partition: Map[String, Option[String]],
    overwrite: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(logicalRelation.catalogTable.isDefined, "Missing catalog table")

    val table = logicalRelation.catalogTable.get
    InsertIntoHoodieTableCommand.run(sparkSession, table, query, partition, overwrite)
    Seq.empty[Row]
  }
}

object InsertIntoHoodieTableCommand extends Logging {
  /**
   * Run the insert query. We support both dynamic partition insert and static partition insert.
   * @param sparkSession The spark session.
   * @param table The insert table.
   * @param query The insert query.
   * @param insertPartitions The specified insert partition map.
   *                         e.g. "insert into h(dt = '2021') select id, name from src"
   *                         "dt" is the key in the map and "2021" is the partition value. If the
   *                         partition value has not specified(in the case of dynamic partition)
   *                         , it is None in the map.
   * @param overwrite Whether to overwrite the table.
   * @param refreshTable Whether to refresh the table after insert finished.
   * @param extraOptions Extra options for insert.
   */
  def run(sparkSession: SparkSession,
      table: CatalogTable,
      query: LogicalPlan,
      insertPartitions: Map[String, Option[String]],
      overwrite: Boolean,
      refreshTable: Boolean = true,
      extraOptions: Map[String, String] = Map.empty): Boolean = {

    val hoodieCatalogTable = new HoodieCatalogTable(sparkSession, table)
    val config = buildHoodieInsertConfig(hoodieCatalogTable, sparkSession, overwrite, insertPartitions, extraOptions)

    val mode = if (overwrite && hoodieCatalogTable.partitionFields.isEmpty) {
      // insert overwrite non-partition table
      SaveMode.Overwrite
    } else {
      // for insert into or insert overwrite partition we use append mode.
      SaveMode.Append
    }
    val conf = sparkSession.sessionState.conf
    val alignedQuery = alignOutputFields(query, hoodieCatalogTable, insertPartitions, conf)
    // If we create dataframe using the Dataset.ofRows(sparkSession, alignedQuery),
    // The nullable attribute of fields will lost.
    // In order to pass the nullable attribute to the inputDF, we specify the schema
    // of the rdd.
    val inputDF = sparkSession.createDataFrame(
      Dataset.ofRows(sparkSession, alignedQuery).rdd, alignedQuery.schema)
    val success =
      HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, config, inputDF)._1
    if (success) {
      if (refreshTable) {
        sparkSession.catalog.refreshTable(table.identifier.unquotedString)
      }
      true
    } else {
      false
    }
  }

  /**
   * Aligned the type and name of query's output fields with the result table's fields.
   * @param query The insert query which to aligned.
   * @param hoodieCatalogTable The result hoodie catalog table.
   * @param insertPartitions The insert partition map.
   * @param conf The SQLConf.
   * @return
   */
  private def alignOutputFields(
    query: LogicalPlan,
    hoodieCatalogTable: HoodieCatalogTable,
    insertPartitions: Map[String, Option[String]],
    conf: SQLConf): LogicalPlan = {

    val targetPartitionSchema = hoodieCatalogTable.partitionSchema

    val staticPartitionValues = insertPartitions.filter(p => p._2.isDefined).mapValues(_.get)
    assert(staticPartitionValues.isEmpty ||
      staticPartitionValues.size == targetPartitionSchema.size,
      s"Required partition columns is: ${targetPartitionSchema.json}, Current static partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    val queryOutputWithoutMetaFields = removeMetaFields(query.output)
    assert(staticPartitionValues.size + queryOutputWithoutMetaFields.size
        == hoodieCatalogTable.tableSchemaWithoutMetaFields.size,
      s"Required select columns count: ${hoodieCatalogTable.tableSchemaWithoutMetaFields.size}, " +
        s"Current select columns(including static partition column) count: " +
        s"${staticPartitionValues.size + queryOutputWithoutMetaFields.size}，columns: " +
        s"(${(queryOutputWithoutMetaFields.map(_.name) ++ staticPartitionValues.keys).mkString(",")})")

    val queryDataFieldsWithoutMetaFields = if (staticPartitionValues.isEmpty) { // insert dynamic partition
      queryOutputWithoutMetaFields.dropRight(targetPartitionSchema.fields.length)
    } else { // insert static partition
      queryOutputWithoutMetaFields
    }
    // Align for the data fields of the query
    val dataProjectsWithputMetaFields = queryDataFieldsWithoutMetaFields.zip(
      hoodieCatalogTable.dataSchemaWithoutMetaFields.fields).map { case (dataAttr, targetField) =>
        val castAttr = castIfNeeded(dataAttr.withNullability(targetField.nullable),
          targetField.dataType, conf)
        Alias(castAttr, targetField.name)()
    }

    val partitionProjects = if (staticPartitionValues.isEmpty) { // insert dynamic partitions
      // The partition attributes is followed the data attributes in the query
      // So we init the partitionAttrPosition with the data schema size.
      var partitionAttrPosition = hoodieCatalogTable.dataSchemaWithoutMetaFields.size
      targetPartitionSchema.fields.map(f => {
        val partitionAttr = queryOutputWithoutMetaFields(partitionAttrPosition)
        partitionAttrPosition = partitionAttrPosition + 1
        val castAttr = castIfNeeded(partitionAttr.withNullability(f.nullable), f.dataType, conf)
        Alias(castAttr, f.name)()
      })
    } else { // insert static partitions
      targetPartitionSchema.fields.map(f => {
        val staticPartitionValue = staticPartitionValues.getOrElse(f.name,
        s"Missing static partition value for: ${f.name}")
        val castAttr = castIfNeeded(Literal.create(staticPartitionValue), f.dataType, conf)
        Alias(castAttr, f.name)()
      })
    }
    val alignedProjects = dataProjectsWithputMetaFields ++ partitionProjects
    Project(alignedProjects, query)
  }

  /**
   * Build the default config for insert.
   * @return
   */
  private def buildHoodieInsertConfig(
      hoodieCatalogTable: HoodieCatalogTable,
      sparkSession: SparkSession,
      isOverwrite: Boolean,
      insertPartitions: Map[String, Option[String]] = Map.empty,
      extraOptions: Map[String, String]): Map[String, String] = {

    if (insertPartitions.nonEmpty &&
      (insertPartitions.keys.toSet != hoodieCatalogTable.partitionFields.toSet)) {
      throw new IllegalArgumentException(s"Insert partition fields" +
        s"[${insertPartitions.keys.mkString(" " )}]" +
        s" not equal to the defined partition in table[${hoodieCatalogTable.partitionFields.mkString(",")}]")
    }
    val path = hoodieCatalogTable.tableLocation
    val tableType = hoodieCatalogTable.tableTypeName
    val tableConfig = hoodieCatalogTable.tableConfig
    val tableSchema = hoodieCatalogTable.tableSchema

    val options = hoodieCatalogTable.catalogProperties ++ tableConfig.getProps.asScala.toMap ++ extraOptions
    val parameters = withSparkConf(sparkSession, options)()

    val preCombineColumn = hoodieCatalogTable.preCombineKey.getOrElse("")
    val partitionFields = hoodieCatalogTable.partitionFields.mkString(",")

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
    val hasPrecombineColumn = preCombineColumn.nonEmpty
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

    val payloadClassName = if (operation ==  UPSERT_OPERATION_OPT_VAL &&
      tableType == COW_TABLE_TYPE_OPT_VAL && insertMode == InsertMode.STRICT) {
      // Only validate duplicate key for COW, for MOR it will do the merge with the DefaultHoodieRecordPayload
      // on reading.
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else {
      classOf[DefaultHoodieRecordPayload].getCanonicalName
    }
    logInfo(s"insert statement use write operation type: $operation, payloadClass: $payloadClassName")

    val enableHive = isEnableHive(sparkSession)
    withSparkConf(sparkSession, options) {
      Map(
        "path" -> path,
        TABLE_TYPE.key -> tableType,
        TBL_NAME.key -> hoodieCatalogTable.tableName,
        PRECOMBINE_FIELD.key -> preCombineColumn,
        OPERATION.key -> operation,
        HIVE_STYLE_PARTITIONING.key -> hiveStylePartitioningEnable,
        URL_ENCODE_PARTITIONING.key -> urlEncodePartitioning,
        KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
        SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> keyGeneratorClassName,
        RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
        PARTITIONPATH_FIELD.key -> partitionFields,
        PAYLOAD_CLASS_NAME.key -> payloadClassName,
        ENABLE_ROW_WRITER.key -> enableBulkInsert.toString,
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.key -> String.valueOf(hasPrecombineColumn),
        META_SYNC_ENABLED.key -> enableHive.toString,
        HIVE_SYNC_MODE.key -> HiveSyncMode.HMS.name(),
        HIVE_USE_JDBC.key -> "false",
        HIVE_DATABASE.key -> hoodieCatalogTable.table.identifier.database.getOrElse("default"),
        HIVE_TABLE.key -> hoodieCatalogTable.table.identifier.table,
        HIVE_SUPPORT_TIMESTAMP_TYPE.key -> "true",
        HIVE_PARTITION_FIELDS.key -> partitionFields,
        HIVE_PARTITION_EXTRACTOR_CLASS.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "200",
        HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> hoodieCatalogTable.partitionSchema.toDDL
      )
    }
  }
}

/**
 * Validate the duplicate key for insert statement without enable the INSERT_DROP_DUPS_OPT
 * config.
 */
class ValidateDuplicateKeyPayload(record: GenericRecord, orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) {

  def this(record: HOption[GenericRecord]) {
    this(if (record.isPresent) record.get else null, 0)
  }

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
      schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val key = currentValue.asInstanceOf[GenericRecord].get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString
    throw new HoodieDuplicateKeyException(key)
  }
}
