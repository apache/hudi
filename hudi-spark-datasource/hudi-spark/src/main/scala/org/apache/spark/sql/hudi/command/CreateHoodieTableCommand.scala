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

import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory

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
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.{SPARK_VERSION, SparkConf}

import java.util.{Locale, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Command for create hoodie table.
 */
case class CreateHoodieTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand with SparkAdapterSupport {

  val tableName = formatName(table.identifier.table)

  val tblProperties = table.storage.properties ++ table.properties

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

    // get schema with meta fields, table config if hudi table exists, options including
    // table configs and properties of the catalog tabe
    val path = getTableLocation(table, sparkSession)
    val (finalSchema, existingTableConfig, tableSqlOptions) = parseSchemaAndConfigs(sparkSession, path)

    // Init the hoodie.properties
    initTableIfNeed(sparkSession, tableName, path, finalSchema,
      table.partitionColumnNames, existingTableConfig, tableSqlOptions)

    try {
      // Create table in the catalog
      createTableInCatalog(sparkSession, finalSchema, tableSqlOptions)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to create catalog table in metastore: ${e.getMessage}")
        e.printStackTrace()
    }

    Seq.empty[Row]
  }

  def parseSchemaAndConfigs(sparkSession: SparkSession, path: String, ctas: Boolean = false)
      : (StructType, Map[String, String], Map[String, String]) = {
    val resolver = sparkSession.sessionState.conf.resolver
    val conf = sparkSession.sessionState.newHadoopConf
    // if CTAS, we treat the table we just created as nonexistent
    val isTableExists = if (ctas) false else tableExistsInPath(path, conf)
    var existingTableConfig = Map.empty[String, String]
    val sqlOptions = HoodieOptionConfig.withDefaultSqlOption(tblProperties)
    val catalogTableProps = HoodieOptionConfig.mappingSqlOptionToTableConfig(tblProperties)

    // get final schema and parameters
    val (finalSchema, tableSqlOptions) = (table.tableType, isTableExists) match {
      case (CatalogTableType.EXTERNAL, true) =>
        // If this is an external table & the table has already exists in the location,
        // load the schema from the table meta.
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(path)
          .setConf(conf)
          .build()
        val tableSchema = getTableSqlSchema(metaClient)
        existingTableConfig = metaClient.getTableConfig.getProps.asScala.toMap
        validateTableConfig(sparkSession, catalogTableProps, convertMapToHoodieConfig(existingTableConfig))

        val options = extraTableConfig(sparkSession, isTableExists, existingTableConfig) ++
          sqlOptions ++ HoodieOptionConfig.mappingTableConfigToSqlOption(existingTableConfig)

        val userSpecifiedSchema = table.schema
        val schema = if (userSpecifiedSchema.isEmpty && tableSchema.isDefined) {
          tableSchema.get
        } else if (userSpecifiedSchema.nonEmpty) {
          userSpecifiedSchema
        } else {
          throw new IllegalArgumentException(s"Missing schema for Create Table: $tableName")
        }

        (addMetaFields(schema), options)

      case (_, false) =>
        assert(table.schema.nonEmpty, s"Missing schema for Create Table: $tableName")
        val schema = table.schema
        val options = extraTableConfig(sparkSession, isTableExists = false) ++ sqlOptions
        (addMetaFields(schema), options)

      case (CatalogTableType.MANAGED, true) =>
        throw new AnalysisException(s"Can not create the managed table('$tableName')" +
          s". The associated location('$path') already exists.")
    }
    HoodieOptionConfig.validateTable(sparkSession, finalSchema, tableSqlOptions)

    val dataSchema = finalSchema.filterNot { f =>
      table.partitionColumnNames.exists(resolver(_, f.name))
    }
    verifyDataSchema(table.identifier, table.tableType, dataSchema)

    (finalSchema, existingTableConfig, tableSqlOptions)
  }

  def createTableInCatalog(sparkSession: SparkSession, finalSchema: StructType,
      options: Map[String, String]): Unit = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    val sessionState = sparkSession.sessionState
    val path = getTableLocation(table, sparkSession)
    val conf = sparkSession.sessionState.newHadoopConf()

    val tableType = HoodieOptionConfig.getTableType(options)
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
    val storageProperties = HoodieOptionConfig.deleteHooideOptions(options)
    val newStorage = new CatalogStorageFormat(
      Some(new Path(path).toUri),
      Some(inputFormat),
      Some(outputFormat),
      Some(serdeFormat),
      table.storage.compressed,
      storageProperties + ("path" -> path))

    val newDatabaseName = formatName(table.identifier.database
      .getOrElse(sessionState.catalog.getCurrentDatabase))

    val newTableIdentifier = table.identifier
      .copy(table = tableName, database = Some(newDatabaseName))

    // append pk, preCombineKey, type to the properties of table
    val newTblProperties = table.storage.properties ++ table.properties ++ HoodieOptionConfig.extractSqlOptions(options)
    val newTable = table.copy(
      identifier = newTableIdentifier,
      schema = finalSchema,
      storage = newStorage,
      createVersion = SPARK_VERSION,
      properties = newTblProperties
    )

    // Create table in the catalog
    val enableHive = isEnableHive(sparkSession)
    if (enableHive) {
      createHiveDataSourceTable(newTable, sparkSession)
    } else {
      sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
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
  private def createHiveDataSourceTable(table: CatalogTable, sparkSession: SparkSession): Unit = {
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

  private def formatName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#verifyDataSchema
  private def verifyDataSchema(tableName: TableIdentifier, tableType: CatalogTableType,
      dataSchema: Seq[StructField]): Unit = {
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

  def extraTableConfig(sparkSession: SparkSession, isTableExists: Boolean,
      originTableConfig: Map[String, String] = Map.empty): Map[String, String] = {
    val extraConfig = mutable.Map.empty[String, String]
    if (isTableExists) {
      val allPartitionPaths = getAllPartitionPaths(sparkSession, table)
      if (originTableConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)) {
        extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) =
          originTableConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)
      } else {
        extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) =
          String.valueOf(isHiveStyledPartitioning(allPartitionPaths, table))
      }
      if (originTableConfig.contains(HoodieTableConfig.URL_ENCODE_PARTITIONING.key)) {
        extraConfig(HoodieTableConfig.URL_ENCODE_PARTITIONING.key) =
        originTableConfig(HoodieTableConfig.URL_ENCODE_PARTITIONING.key)
      } else {
        extraConfig(HoodieTableConfig.URL_ENCODE_PARTITIONING.key) =
          String.valueOf(isUrlEncodeEnabled(allPartitionPaths, table))
      }
    } else {
      extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) = "true"
      extraConfig(HoodieTableConfig.URL_ENCODE_PARTITIONING.key) = HoodieTableConfig.URL_ENCODE_PARTITIONING.defaultValue()
    }

    if (originTableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)) {
      extraConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) =
        HoodieSparkKeyGeneratorFactory.convertToSparkKeyGenerator(
          originTableConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key))
    } else {
      extraConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) = classOf[ComplexKeyGenerator].getCanonicalName
    }
    extraConfig.toMap
  }
}

object CreateHoodieTableCommand extends Logging {

  /**
    * Init the hoodie.properties.
    */
  def initTableIfNeed(sparkSession: SparkSession,
      tableName: String,
      location: String,
      schema: StructType,
      partitionColumns: Seq[String],
      originTableConfig: Map[String, String],
      sqlOptions: Map[String, String]): Unit = {

    logInfo(s"Init hoodie.properties for $tableName")
    val conf = sparkSession.sessionState.newHadoopConf()

    val tableOptions = HoodieOptionConfig.mappingSqlOptionToTableConfig(sqlOptions)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PRECOMBINE_FIELD.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.PARTITION_FIELDS.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.RECORDKEY_FIELDS.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.URL_ENCODE_PARTITIONING.key)
    checkTableConfigEqual(originTableConfig, tableOptions, HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)
    // Save all the table config to the hoodie.properties.
    val parameters = originTableConfig ++ tableOptions
    val properties = new Properties()
    properties.putAll(parameters.asJava)
    HoodieTableMetaClient.withPropertyBuilder()
      .fromProperties(properties)
      .setTableName(tableName)
      .setTableCreateSchema(SchemaConverters.toAvroType(schema).toString())
      .setPartitionFields(partitionColumns.mkString(","))
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
