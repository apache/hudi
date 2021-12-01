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

import org.apache.hadoop.fs.Path

import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils

import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hudi.{HoodieOptionConfig, HoodieSqlUtils}
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SPARK_VERSION, SparkConf}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Command for create hoodie table.
 */
case class CreateHoodieTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand with SparkAdapterSupport {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableIsExists = sparkSession.sessionState.catalog.tableExists(table.identifier)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table ${table.identifier.unquotedString} already exists.")
      }
    }

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, table)
    // check if there are conflict between table configs defined in hoodie table and properties defined in catalog.
    CreateHoodieTableCommand.validateTblProperties(hoodieCatalogTable)
    // init hoodie table
    hoodieCatalogTable.initHoodieTable()

    try {
      // create catalog table for this hoodie table
      CreateHoodieTableCommand.createTableInCatalog(sparkSession, hoodieCatalogTable, ignoreIfExists)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to create catalog table in metastore: ${e.getMessage}")
    }
    Seq.empty[Row]
  }
}

object CreateHoodieTableCommand {

  def validateTblProperties(hoodieCatalogTable: HoodieCatalogTable): Unit = {
    if (hoodieCatalogTable.hoodieTableExists) {
      val originTableConfig = hoodieCatalogTable.tableConfig.getProps.asScala.toMap
      val tableOptions = hoodieCatalogTable.catalogProperties

      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PRECOMBINE_FIELD.key)
      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PARTITION_FIELDS.key)
      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.RECORDKEY_FIELDS.key)
      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)
      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.URL_ENCODE_PARTITIONING.key)
      checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)
    }
  }

  def createTableInCatalog(sparkSession: SparkSession,
      hoodieCatalogTable: HoodieCatalogTable, ignoreIfExists: Boolean): Unit = {
    val table = hoodieCatalogTable.table
    assert(table.tableType != CatalogTableType.VIEW)

    val catalog = sparkSession.sessionState.catalog
    val path = hoodieCatalogTable.tableLocation
    val tableConfig = hoodieCatalogTable.tableConfig
    val properties = tableConfig.getProps.asScala.toMap

    val tableType = tableConfig.getTableType.name()
    val inputFormat = tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetInputFormat].getCanonicalName
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetRealtimeInputFormat].getCanonicalName
      case _=> throw new IllegalArgumentException(s"UnKnow table type:$tableType")
    }
    val outputFormat = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET)
    val serdeFormat = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET)

    // only parameters irrelevant to hudi can be set to storage.properties
    val storageProperties = HoodieOptionConfig.deleteHoodieOptions(properties)
    val newStorage = new CatalogStorageFormat(
      Some(new Path(path).toUri),
      Some(inputFormat),
      Some(outputFormat),
      Some(serdeFormat),
      table.storage.compressed,
      storageProperties + ("path" -> path))

    val tablName = HoodieSqlUtils.formatName(sparkSession, table.identifier.table)
    val newDatabaseName = HoodieSqlUtils.formatName(sparkSession, table.identifier.database
      .getOrElse(catalog.getCurrentDatabase))

    val newTableIdentifier = table.identifier
      .copy(table = tablName, database = Some(newDatabaseName))

    // append pk, preCombineKey, type to the properties of table
    val newTblProperties = hoodieCatalogTable.catalogProperties ++ HoodieOptionConfig.extractSqlOptions(properties)
    val newTable = table.copy(
      identifier = newTableIdentifier,
      schema = hoodieCatalogTable.tableSchema,
      storage = newStorage,
      createVersion = SPARK_VERSION,
      properties = newTblProperties
    )

    // Create table in the catalog
    val enableHive = isEnableHive(sparkSession)
    if (enableHive) {
      createHiveDataSourceTable(sparkSession, newTable, ignoreIfExists)
    } else {
      catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
    }
  }

  /**
    * Create Hive table for hudi.
    * Firstly, do some check for the schema & table.
    * Secondly, append some table properties need for spark datasource table.
    * Thirdly, create hive table using the HiveClient.
    * @param table
    * @param sparkSession
    */
  private def createHiveDataSourceTable(sparkSession: SparkSession, table: CatalogTable,
      ignoreIfExists: Boolean): Unit = {
    val dbName = table.identifier.database.get
    // check database
    val dbExists = sparkSession.sessionState.catalog.databaseExists(dbName)
    if (!dbExists) {
      throw new NoSuchDatabaseException(dbName)
    }
    // check table exists
    if (sparkSession.sessionState.catalog.tableExists(table.identifier)) {
      throw new TableAlreadyExistsException(dbName, table.identifier.table)
    }
    // append some table properties need for spark data source table.
    val dataSourceProps = tableMetaToTableProps(sparkSession.sparkContext.conf,
      table, table.schema)

    val tableWithDataSourceProps = table.copy(properties = dataSourceProps ++ table.properties)
    val client = HiveClientUtils.newClientForMetadata(sparkSession.sparkContext.conf,
      sparkSession.sessionState.newHadoopConf())
    // create hive table.
    client.createTable(tableWithDataSourceProps, ignoreIfExists)
  }

  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#tableMetaToTableProps
  private def tableMetaToTableProps(sparkConf: SparkConf, table: CatalogTable,
      schema: StructType): Map[String, String] = {
    val partitionColumns = table.partitionColumnNames
    val bucketSpec = table.bucketSpec

    val properties = new mutable.HashMap[String, String]
    properties.put(DATASOURCE_PROVIDER, "hudi")
    properties.put(CREATED_SPARK_VERSION, table.createVersion)

    // Serialized JSON schema string may be too long to be stored into a single metastore table
    // property. In this case, we split the JSON string and store each part as a separate table
    // property.
    val threshold = sparkConf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
    val schemaJsonString = schema.json
    // Split the JSON string.
    val parts = schemaJsonString.grouped(threshold).toSeq
    properties.put(DATASOURCE_SCHEMA_NUMPARTS, parts.size.toString)
    parts.zipWithIndex.foreach { case (part, index) =>
      properties.put(s"$DATASOURCE_SCHEMA_PART_PREFIX$index", part)
    }

    if (partitionColumns.nonEmpty) {
      properties.put(DATASOURCE_SCHEMA_NUMPARTCOLS, partitionColumns.length.toString)
      partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_PARTCOL_PREFIX$index", partCol)
      }
    }

    if (bucketSpec.isDefined) {
      val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

      properties.put(DATASOURCE_SCHEMA_NUMBUCKETS, numBuckets.toString)
      properties.put(DATASOURCE_SCHEMA_NUMBUCKETCOLS, bucketColumnNames.length.toString)
      bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
        properties.put(s"$DATASOURCE_SCHEMA_BUCKETCOL_PREFIX$index", bucketCol)
      }

      if (sortColumnNames.nonEmpty) {
        properties.put(DATASOURCE_SCHEMA_NUMSORTCOLS, sortColumnNames.length.toString)
        sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
          properties.put(s"$DATASOURCE_SCHEMA_SORTCOL_PREFIX$index", sortCol)
        }
      }
    }

    properties.toMap
  }

  private def checkTableConfigEqual(
      originTableConfig: Map[String, String],
      newTableConfig: Map[String, String],
      configKey: String): Unit = {
    if (originTableConfig.contains(configKey) && newTableConfig.contains(configKey)) {
      assert(originTableConfig(configKey) == newTableConfig(configKey),
        s"Table config: $configKey in the create table is: ${newTableConfig(configKey)}, is not the same with the value in " +
        s"hoodie.properties, which is:  ${originTableConfig(configKey)}. Please keep the same.")
    }
  }
}

