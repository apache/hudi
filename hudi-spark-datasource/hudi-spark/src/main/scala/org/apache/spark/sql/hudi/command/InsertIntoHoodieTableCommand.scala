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

import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.{HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.{HoodieOptionConfig, HoodieSqlUtils}
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.internal.SQLConf

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

object InsertIntoHoodieTableCommand {
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
   */
  def run(sparkSession: SparkSession, table: CatalogTable, query: LogicalPlan,
          insertPartitions: Map[String, Option[String]],
          overwrite: Boolean, refreshTable: Boolean = true): Boolean = {

    val config = buildHoodieInsertConfig(table, sparkSession, overwrite, insertPartitions)

    val mode = if (overwrite && table.partitionColumnNames.isEmpty) {
      // insert overwrite non-partition table
      SaveMode.Overwrite
    } else {
      // for insert into or insert overwrite partition we use append mode.
      SaveMode.Append
    }
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(config)
    val conf = sparkSession.sessionState.conf
    val alignedQuery = alignOutputFields(query, table, insertPartitions, conf)
    // If we create dataframe using the Dataset.ofRows(sparkSession, alignedQuery),
    // The nullable attribute of fields will lost.
    // In order to pass the nullable attribute to the inputDF, we specify the schema
    // of the rdd.
    val inputDF = sparkSession.createDataFrame(
      Dataset.ofRows(sparkSession, alignedQuery).rdd, alignedQuery.schema)
    val success =
      HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, parameters, inputDF)._1
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
   * @param table The result table.
   * @param insertPartitions The insert partition map.
   * @param conf The SQLConf.
   * @return
   */
  private def alignOutputFields(
    query: LogicalPlan,
    table: CatalogTable,
    insertPartitions: Map[String, Option[String]],
    conf: SQLConf): LogicalPlan = {

    val targetPartitionSchema = table.partitionSchema

    val staticPartitionValues = insertPartitions.filter(p => p._2.isDefined).mapValues(_.get)
    assert(staticPartitionValues.isEmpty ||
      staticPartitionValues.size == targetPartitionSchema.size,
      s"Required partition columns is: ${targetPartitionSchema.json}, Current static partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    assert(staticPartitionValues.size + query.output.size == table.schema.size,
      s"Required select columns count: ${removeMetaFields(table.schema).size}, " +
        s"Current select columns(including static partition column) count: " +
        s"${staticPartitionValues.size + removeMetaFields(query.output).size}，columns: " +
        s"(${(removeMetaFields(query.output).map(_.name) ++ staticPartitionValues.keys).mkString(",")})")
    val queryDataFields = if (staticPartitionValues.isEmpty) { // insert dynamic partition
      query.output.dropRight(targetPartitionSchema.fields.length)
    } else { // insert static partition
      query.output
    }
    val targetDataSchema = table.dataSchema
    // Align for the data fields of the query
    val dataProjects = queryDataFields.zip(targetDataSchema.fields).map {
      case (dataAttr, targetField) =>
        val castAttr = castIfNeeded(dataAttr.withNullability(targetField.nullable),
          targetField.dataType, conf)
        Alias(castAttr, targetField.name)()
    }

    val partitionProjects = if (staticPartitionValues.isEmpty) { // insert dynamic partitions
      // The partition attributes is followed the data attributes in the query
      // So we init the partitionAttrPosition with the data schema size.
      var partitionAttrPosition = targetDataSchema.size
      targetPartitionSchema.fields.map(f => {
        val partitionAttr = query.output(partitionAttrPosition)
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
    // Remove the hoodie meta fileds from the projects as we do not need these to write
    val withoutMetaFieldDataProjects = dataProjects.filter(c => !HoodieSqlUtils.isMetaField(c.name))
    val alignedProjects = withoutMetaFieldDataProjects ++ partitionProjects
    Project(alignedProjects, query)
  }

  /**
   * Build the default config for insert.
   * @return
   */
  private def buildHoodieInsertConfig(
      table: CatalogTable,
      sparkSession: SparkSession,
      isOverwrite: Boolean,
      insertPartitions: Map[String, Option[String]] = Map.empty): Map[String, String] = {

    if (insertPartitions.nonEmpty &&
      (insertPartitions.keys.toSet != table.partitionColumnNames.toSet)) {
      throw new IllegalArgumentException(s"Insert partition fields" +
        s"[${insertPartitions.keys.mkString(" " )}]" +
        s" not equal to the defined partition in table[${table.partitionColumnNames.mkString(",")}]")
    }
    val parameters = HoodieOptionConfig.mappingSqlOptionToHoodieParam(table.storage.properties)

    val tableType = parameters.getOrElse(TABLE_TYPE_OPT_KEY.key, TABLE_TYPE_OPT_KEY.defaultValue)

    val partitionFields = table.partitionColumnNames.mkString(",")
    val path = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing location for table ${table.identifier}")

    val tableSchema = table.schema
    val options = table.storage.properties
    val primaryColumns = HoodieOptionConfig.getPrimaryColumns(options)

    val keyGenClass = if (primaryColumns.nonEmpty) {
      classOf[SqlKeyGenerator].getCanonicalName
    } else {
      classOf[UuidKeyGenerator].getName
    }

    val dropDuplicate = sparkSession.conf
      .getOption(INSERT_DROP_DUPS_OPT_KEY.key)
      .getOrElse(INSERT_DROP_DUPS_OPT_KEY.defaultValue)
      .toBoolean

    val operation = if (isOverwrite) {
      if (table.partitionColumnNames.nonEmpty) {
        INSERT_OVERWRITE_OPERATION_OPT_VAL  // overwrite partition
      } else {
        INSERT_OPERATION_OPT_VAL
      }
    } else {
      if (primaryColumns.nonEmpty && !dropDuplicate) {
        UPSERT_OPERATION_OPT_VAL
      } else {
        INSERT_OPERATION_OPT_VAL
      }
    }

    val payloadClassName = if (primaryColumns.nonEmpty && !dropDuplicate &&
      tableType == COW_TABLE_TYPE_OPT_VAL) {
      // Only validate duplicate key for COW, for MOR it will do the merge with the DefaultHoodieRecordPayload
      // on reading.
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else {
      classOf[DefaultHoodieRecordPayload].getCanonicalName
    }
    val enableHive = isEnableHive(sparkSession)
    withSparkConf(sparkSession, options) {
      Map(
        "path" -> path,
        TABLE_TYPE_OPT_KEY.key -> tableType,
        TABLE_NAME.key -> table.identifier.table,
        PRECOMBINE_FIELD_OPT_KEY.key -> tableSchema.fields.last.name,
        OPERATION_OPT_KEY.key -> operation,
        KEYGENERATOR_CLASS_OPT_KEY.key -> keyGenClass,
        RECORDKEY_FIELD_OPT_KEY.key -> primaryColumns.mkString(","),
        PARTITIONPATH_FIELD_OPT_KEY.key -> partitionFields,
        PAYLOAD_CLASS_OPT_KEY.key -> payloadClassName,
        META_SYNC_ENABLED_OPT_KEY.key -> enableHive.toString,
        HIVE_USE_JDBC_OPT_KEY.key -> "false",
        HIVE_DATABASE_OPT_KEY.key -> table.identifier.database.getOrElse("default"),
        HIVE_TABLE_OPT_KEY.key -> table.identifier.table,
        HIVE_SUPPORT_TIMESTAMP.key -> "true",
        HIVE_STYLE_PARTITIONING_OPT_KEY.key -> "true",
        HIVE_PARTITION_FIELDS_OPT_KEY.key -> partitionFields,
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        URL_ENCODE_PARTITIONING_OPT_KEY.key -> "true",
        HoodieWriteConfig.INSERT_PARALLELISM.key -> "200",
        HoodieWriteConfig.UPSERT_PARALLELISM.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> table.partitionSchema.toDDL
      )
    }
  }
}

/**
 * Validate the duplicate key for insert statement without enable the INSERT_DROP_DUPS_OPT_KEY
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
