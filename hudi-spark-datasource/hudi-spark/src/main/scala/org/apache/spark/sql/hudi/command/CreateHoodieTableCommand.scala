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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.command.CreateHoodieTableCommand.{initTableIfNeed, isEmptyPath}
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.{SPARK_VERSION, SparkConf}

import java.util.{Locale, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Command for create hoodie table.
 */
case class CreateHoodieTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand with SparkAdapterSupport {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = table.identifier.unquotedString

    val tableIsExists = sparkSession.sessionState.catalog.tableExists(table.identifier)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table $tableName already exists.")
      }
    }
    // Create table in the catalog
    val createTable = createTableInCatalog(sparkSession)
    // Init the hoodie.properties
    initTableIfNeed(sparkSession, createTable)
    Seq.empty[Row]
  }

  def createTableInCatalog(sparkSession: SparkSession,
    checkPathForManagedTable: Boolean = true): CatalogTable = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    val sessionState = sparkSession.sessionState
    val tableName = table.identifier.unquotedString
    val path = getTableLocation(table, sparkSession)
    val conf = sparkSession.sessionState.newHadoopConf()
    val isTableExists = tableExistsInPath(path, conf)
    // Get the schema & table options
    val (newSchema, tableOptions) = if (table.tableType == CatalogTableType.EXTERNAL &&
      isTableExists) {
      // If this is an external table & the table has already exists in the location,
      // load the schema from the table meta.
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(path)
        .setConf(conf)
        .build()
     val tableSchema = getTableSqlSchema(metaClient)

     // Get options from the external table and append with the options in ddl.
     val originTableConfig =  HoodieOptionConfig.mappingTableConfigToSqlOption(
       metaClient.getTableConfig.getProps.asScala.toMap)

     val allPartitionPaths = getAllPartitionPaths(sparkSession, table)
     var upgrateConfig = Map.empty[String, String]
     // If this is a non-hive-styled partition table, disable the hive style config.
     // (By default this config is enable for spark sql)
     upgrateConfig = if (isNotHiveStyledPartitionTable(allPartitionPaths, table)) {
        upgrateConfig + (DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "false")
     } else {
       upgrateConfig
     }
      upgrateConfig = if (isUrlEncodeDisable(allPartitionPaths, table)) {
        upgrateConfig + (DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key -> "false")
      } else {
        upgrateConfig
      }

      // Use the origin keygen to generate record key to keep the rowkey consistent with the old table for spark sql.
      // See SqlKeyGenerator#getRecordKey for detail.
      upgrateConfig = if (originTableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)) {
        upgrateConfig + (SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> originTableConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key))
      } else {
        upgrateConfig
      }
      val options = originTableConfig ++ upgrateConfig ++ table.storage.properties

      val userSpecifiedSchema = table.schema
      if (userSpecifiedSchema.isEmpty && tableSchema.isDefined) {
        (addMetaFields(tableSchema.get), options)
      } else if (userSpecifiedSchema.nonEmpty) {
        (addMetaFields(userSpecifiedSchema), options)
      } else {
        throw new IllegalArgumentException(s"Missing schema for Create Table: $tableName")
      }
    } else {
      assert(table.schema.nonEmpty, s"Missing schema for Create Table: $tableName")
      // SPARK-19724: the default location of a managed table should be non-existent or empty.
      if (checkPathForManagedTable && table.tableType == CatalogTableType.MANAGED
        && !isEmptyPath(path, conf)) {
        throw new AnalysisException(s"Can not create the managed table('$tableName')" +
          s". The associated location('$path') already exists.")
      }
      // Add the meta fields to the schema if this is a managed table or an empty external table.
      (addMetaFields(table.schema), table.storage.properties)
    }

    val tableType = HoodieOptionConfig.getTableType(table.storage.properties)
    val inputFormat = tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetInputFormat].getCanonicalName
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetRealtimeInputFormat].getCanonicalName
      case _=> throw new IllegalArgumentException(s"UnKnow table type:$tableType")
    }
    val outputFormat = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET)
    val serdeFormat = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET)

    val newStorage = new CatalogStorageFormat(Some(new Path(path).toUri),
      Some(inputFormat), Some(outputFormat), Some(serdeFormat),
      table.storage.compressed, tableOptions + ("path" -> path))

    val newDatabaseName = formatName(table.identifier.database
      .getOrElse(sessionState.catalog.getCurrentDatabase))
    val newTableName = formatName(table.identifier.table)

    val newTableIdentifier = table.identifier
      .copy(table = newTableName, database = Some(newDatabaseName))

    val newTable = table.copy(identifier = newTableIdentifier,
      schema = newSchema, storage = newStorage, createVersion = SPARK_VERSION)
    // validate the table
    validateTable(newTable)

    // Create table in the catalog
    val enableHive = isEnableHive(sparkSession)
    if (enableHive) {
      createHiveDataSourceTable(newTable, sparkSession)
    } else {
      sessionState.catalog.createTable(newTable, ignoreIfExists = false,
        validateLocation = checkPathForManagedTable)
    }
    newTable
  }

  /**
    * Create Hive table for hudi.
    * Firstly, do some check for the schema & table.
    * Secondly, append some table properties need for spark datasource table.
    * Thirdly, create hive table using the HiveClient.
    * @param table
    * @param sparkSession
    */
  private def createHiveDataSourceTable(table: CatalogTable, sparkSession: SparkSession): Unit = {
    // check schema
    verifyDataSchema(table.identifier, table.tableType, table.schema)
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

    val tableWithDataSourceProps = table.copy(properties = dataSourceProps)
    val client = HiveClientUtils.newClientForMetadata(sparkSession.sparkContext.conf,
      sparkSession.sessionState.newHadoopConf())
    // create hive table.
    client.createTable(tableWithDataSourceProps, ignoreIfExists)
  }

  private def formatName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#verifyDataSchema
  private def verifyDataSchema(tableName: TableIdentifier,
                               tableType: CatalogTableType,
                               dataSchema: StructType): Unit = {
    if (tableType != CatalogTableType.VIEW) {
      val invalidChars = Seq(",", ":", ";")
      def verifyNestedColumnNames(schema: StructType): Unit = schema.foreach { f =>
        f.dataType match {
          case st: StructType => verifyNestedColumnNames(st)
          case _ if invalidChars.exists(f.name.contains) =>
            val invalidCharsString = invalidChars.map(c => s"'$c'").mkString(", ")
            val errMsg = "Cannot create a table having a nested column whose name contains " +
              s"invalid characters ($invalidCharsString) in Hive metastore. Table: $tableName; " +
              s"Column: ${f.name}"
            throw new AnalysisException(errMsg)
          case _ =>
        }
      }

      dataSchema.foreach { f =>
        f.dataType match {
          // Checks top-level column names
          case _ if f.name.contains(",") =>
            throw new AnalysisException("Cannot create a table having a column whose name " +
              s"contains commas in Hive metastore. Table: $tableName; Column: ${f.name}")
          // Checks nested column names
          case st: StructType =>
            verifyNestedColumnNames(st)
          case _ =>
        }
      }
    }
  }
  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#tableMetaToTableProps
  private def tableMetaToTableProps( sparkConf: SparkConf,
                                     table: CatalogTable,
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

  private def validateTable(table: CatalogTable): Unit = {
    val options = table.storage.properties
    // validate the pk if it exist in the table.
    HoodieOptionConfig.getPrimaryColumns(options).foreach(pk => table.schema.fieldIndex(pk))
    // validate the version column if it exist in the table.
    HoodieOptionConfig.getPreCombineField(options).foreach(v => table.schema.fieldIndex(v))
    // validate the partition columns
    table.partitionColumnNames.foreach(p => table.schema.fieldIndex(p))
    // validate table type
    options.get(HoodieOptionConfig.SQL_KEY_TABLE_TYPE.sqlKeyName).foreach { tableType =>
      ValidationUtils.checkArgument(
        tableType.equalsIgnoreCase(HoodieOptionConfig.SQL_VALUE_TABLE_TYPE_COW) ||
          tableType.equalsIgnoreCase(HoodieOptionConfig.SQL_VALUE_TABLE_TYPE_MOR),
        s"'type' must be '${HoodieOptionConfig.SQL_VALUE_TABLE_TYPE_COW}' or " +
          s"'${HoodieOptionConfig.SQL_VALUE_TABLE_TYPE_MOR}'")
    }
  }

  /**
   * This method is used to compatible with the old non-hive-styled partition table.
   * By default we enable the "hoodie.datasource.write.hive_style_partitioning"
   * when writing data to hudi table by spark sql by default.
   * If the exist table is a non-hive-styled partitioned table, we should
   * disable the "hoodie.datasource.write.hive_style_partitioning" when
   * merge or update the table. Or else, we will get an incorrect merge result
   * as the partition path mismatch.
   */
  private def isNotHiveStyledPartitionTable(partitionPaths: Seq[String], table: CatalogTable): Boolean = {
    if (table.partitionColumnNames.nonEmpty) {
      val isHiveStylePartitionPath = (path: String) => {
        val fragments = path.split("/")
        if (fragments.size != table.partitionColumnNames.size) {
          false
        } else {
          fragments.zip(table.partitionColumnNames).forall {
            case (pathFragment, partitionColumn) => pathFragment.startsWith(s"$partitionColumn=")
          }
        }
      }
      !partitionPaths.forall(isHiveStylePartitionPath)
    } else {
      false
    }
  }

  /**
   * If this table has disable the url encode, spark sql should also disable it when writing to the table.
   */
  private def isUrlEncodeDisable(partitionPaths: Seq[String], table: CatalogTable): Boolean = {
    if (table.partitionColumnNames.nonEmpty) {
      !partitionPaths.forall(partitionPath => partitionPath.split("/").length == table.partitionColumnNames.size)
    } else {
      false
    }
  }

}

object CreateHoodieTableCommand extends Logging {

  /**
    * Init the hoodie.properties.
    */
  def initTableIfNeed(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val location = getTableLocation(table, sparkSession)

    val conf = sparkSession.sessionState.newHadoopConf()
    // Init the hoodie table
    val originTableConfig = if (tableExistsInPath(location, conf)) {
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(location)
        .setConf(conf)
        .build()
      metaClient.getTableConfig.getProps.asScala.toMap
    } else {
      Map.empty[String, String]
    }

    val tableName = table.identifier.table
    logInfo(s"Init hoodie.properties for $tableName")
    val tableOptions = HoodieOptionConfig.mappingSqlOptionToTableConfig(table.storage.properties)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PRECOMBINE_FIELD.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PARTITION_FIELDS.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.RECORDKEY_FIELDS.key)
    // Save all the table config to the hoodie.properties.
    val parameters = originTableConfig ++ tableOptions
    val properties = new Properties()
    properties.putAll(parameters.asJava)
    HoodieTableMetaClient.withPropertyBuilder()
      .fromProperties(properties)
      .setTableName(tableName)
      .setTableCreateSchema(SchemaConverters.toAvroType(table.schema).toString())
      .setPartitionFields(table.partitionColumnNames.mkString(","))
      .initTable(conf, location)
  }

  def checkTableConfigEqual(originTableConfig: Map[String, String],
    newTableConfig: Map[String, String], configKey: String): Unit = {
    if (originTableConfig.contains(configKey) && newTableConfig.contains(configKey)) {
      assert(originTableConfig(configKey) == newTableConfig(configKey),
        s"Table config: $configKey in the create table is: ${newTableConfig(configKey)}, is not the same with the value in " +
        s"hoodie.properties, which is:  ${originTableConfig(configKey)}. Please keep the same.")
    }
  }

  /**
   * Check if this is a empty table path.
   */
  def isEmptyPath(tablePath: String, conf: Configuration): Boolean = {
    val basePath = new Path(tablePath)
    val fs = basePath.getFileSystem(conf)
    if (fs.exists(basePath)) {
      fs.listStatus(basePath).isEmpty
    } else {
      true
    }
  }
}
