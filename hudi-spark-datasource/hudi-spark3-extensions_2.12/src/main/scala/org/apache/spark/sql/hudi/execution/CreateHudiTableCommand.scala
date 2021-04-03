/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.execution

import java.util.Properties

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_CLASS_PROP
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, TableAlreadyExistsException}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.SparkfunctionWrapper
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * Command for create hoodie table
 */
case class CreateHudiTableCommand(table: CatalogTable, ignoreIfExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val tableName = table.identifier.unquotedString
    val sessionState = sparkSession.sessionState
    val tableIsExists = sessionState.catalog.tableExists(table.identifier)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table ${table.identifier.quotedString} already exists")
      }
    }

    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    val path = HudiSQLUtils.getTablePath(sparkSession, table)
    val conf = sparkSession.sessionState.newHadoopConf()
    val isTableExists = HudiSQLUtils.tableExists(path, conf)
    val (newSchema, tableOptions) = if (table.tableType == CatalogTableType.EXTERNAL && isTableExists) {
      // if this is an external table & the table has already exists in the location,
      // infer schema from the table meta
      assert(table.schema.isEmpty, s"Should not specified table schema " +
        s"for an exists hoodie table: ${table.identifier.unquotedString}")
      // get Schema from the external table
      val metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(conf).build()
      val schemaResolver = new TableSchemaResolver(metaClient)
      val avroSchema = schemaResolver.getTableAvroSchema(true)
      val tableSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      (tableSchema, table.storage.properties ++ metaClient.getTableConfig.getProps.asScala)
    } else {
      // Add the meta fields to the scheme if this is a managed table or an empty external table
      val fullSchema: StructType = {
        val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
        val dataFields = table.schema.fields.filterNot(f => metaFields.contains(f.name))
        val fields = metaFields.map(StructField(_, StringType)) ++ dataFields
        StructType(fields)
      }
      (fullSchema, table.storage.properties)
    }

    // Append * to tablePath if create dataSourceV1 hudi table
    val newPath = if (!enableHive) {
      if (table.partitionColumnNames.nonEmpty) {
        var tempPath: String = path
        if (path.endsWith("/")) {
          tempPath = path.substring(0, path.length - 1)
        }
        for (_ <- 0 until table.partitionColumnNames.size + 1) {
          tempPath = s"$tempPath/*"
        }
        tempPath
      } else {
        path
      }
    } else {
      path
    }

    val tableType = table.storage.properties.getOrElse(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
    tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        val newTable = buildNewCatalogTable(table.copy(schema = newSchema), newPath, tableOptions, enableHive)
        if (enableHive) {
          createHiveDataSourceTable(newTable, sparkSession)
        } else {
          sessionState.catalog.createTable(newTable, ignoreIfExists = false)
        }
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL =>
        val newTable_rt = buildNewCatalogTable(table.copy(schema = newSchema), newPath, tableOptions, enableHive)
        val newTable_ro = buildNewCatalogTable(table.copy(schema = newSchema), newPath, tableOptions, enableHive, true)

        // create ro and rt table
        if (enableHive) {
          createHiveDataSourceTable(newTable_ro, sparkSession)
          createHiveDataSourceTable(newTable_rt, sparkSession)
        } else {
          val newTable = buildNewCatalogTable(table.copy(schema = newSchema), newPath, tableOptions, enableHive)
          sessionState.catalog.createTable(newTable, ignoreIfExists = false)
        }
      case _ => throw new IllegalArgumentException(s"Unknow table Type: $tableType")
    }

    CreateHudiTableCommand.initTableIfNeed(sparkSession, table)
    // init first commit
    HudiSQLUtils.initFirstCommit(sparkSession, newSchema,
      HudiSQLUtils.buildDefaultParameter(tableOptions, enableHive), conf, path, table.identifier.table)

    Seq.empty[Row]
  }

  /**

  build new CatalogTable, base on tableType
@param table
  @param tablePath
  @param tableOptions
  @param ro_table
    */
  def buildNewCatalogTable(table: CatalogTable, tablePath: String, tableOptions: Map[String, String], enableHive: Boolean, ro_table: Boolean = false): CatalogTable = {
    val tableType = table.storage.properties.getOrElse(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
    val outputFormat = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET)
    val serdeFormat = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET)
    val (inputFormat, newTableName) = tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        (classOf[HoodieParquetInputFormat].getCanonicalName, table.identifier.table)
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL if (!ro_table && enableHive) =>
        (classOf[HoodieParquetRealtimeInputFormat].getCanonicalName, table.identifier.table + "_rt")
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL if (ro_table && enableHive) =>
        (classOf[HoodieParquetInputFormat].getCanonicalName, table.identifier.table + "_ro")
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL if (!enableHive) =>
        (classOf[HoodieParquetInputFormat].getCanonicalName, table.identifier.table)
      case _=> throw new IllegalArgumentException(s"UnKnow table type:$tableType")
    }
    val newStorage = new CatalogStorageFormat(Some(new Path(tablePath).toUri),
      Some(inputFormat), Some(outputFormat), Some(serdeFormat),
      table.storage.compressed, tableOptions)
    val newTableIdentifier = table.identifier.copy(table = newTableName)
    table.copy(identifier = newTableIdentifier, storage = newStorage)
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

    val client = SparkfunctionWrapper.newClientForMetadata(sparkSession.sparkContext.conf,
      sparkSession.sessionState.newHadoopConf())
    // create hive table.
    client.createTable(table, ignoreIfExists)
  }
}

object CreateHudiTableCommand extends Logging {

  def saveNeededPropertiesIntoHoodie(fs: FileSystem, propertyPath: Path, properties: Properties): Unit = {
    if (fs.exists(propertyPath)) {
      var is: FSDataInputStream = null
      var os: FSDataOutputStream = null
      try {
        is = fs.open(propertyPath)
        val newProperties = new Properties()
        newProperties.load(is)

        // save precombine key
        if (newProperties.getProperty(PRECOMBINE_FIELD_OPT_KEY, "").isEmpty) {
          newProperties.put(PRECOMBINE_FIELD_OPT_KEY, properties.getProperty(PRECOMBINE_FIELD_OPT_KEY, ""))
        }

        // save record key
        if (newProperties.getProperty(RECORDKEY_FIELD_OPT_KEY, "").isEmpty) {
          newProperties.put(RECORDKEY_FIELD_OPT_KEY, properties.getProperty(RECORDKEY_FIELD_OPT_KEY, ""))
        }

        // save payloadClass
        if (newProperties.getProperty(PAYLOAD_CLASS_OPT_KEY, "").isEmpty) {
          newProperties.put(PAYLOAD_CLASS_OPT_KEY, properties.getProperty(PAYLOAD_CLASS_OPT_KEY,
            "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"))
        }

        // save partition path
        if (newProperties.getProperty(PARTITIONPATH_FIELD_OPT_KEY, "").isEmpty) {
          newProperties.put(PARTITIONPATH_FIELD_OPT_KEY,
            properties.getProperty(PARTITIONPATH_FIELD_OPT_KEY, ""))
        }

        // save keyGen, consider NonpartitionedKeyGenerator
        if (newProperties.getProperty(KEYGENERATOR_CLASS_PROP, "").isEmpty) {
          val keyGen = if (newProperties.getProperty(PARTITIONPATH_FIELD_OPT_KEY).isEmpty) {
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
          } else {
            "org.apache.hudi.keygen.ComplexKeyGenerator"
          }
          newProperties.put(KEYGENERATOR_CLASS_PROP, properties.getProperty(KEYGENERATOR_CLASS_PROP,
            keyGen))
        }

        if (newProperties.getProperty(HIVE_PARTITION_FIELDS_OPT_KEY, "").isEmpty) {
          newProperties.put(HIVE_PARTITION_FIELDS_OPT_KEY,
            properties.getProperty(HIVE_PARTITION_FIELDS_OPT_KEY, newProperties.getProperty(PARTITIONPATH_FIELD_OPT_KEY)))
        }

        if (newProperties.getProperty(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "").equals("")) {
          val hiveExtractorClass = if (newProperties.getProperty(PARTITIONPATH_FIELD_OPT_KEY).isEmpty) {
            "org.apache.hudi.hive.NonPartitionedExtractor"
          } else {
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
          }
          newProperties.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,
            properties.getProperty(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, hiveExtractorClass))
        }
        os = fs.create(propertyPath)
        newProperties.store(os, "")
      } finally {
        if (is != null) {
          is.close()
        }
        if (os != null) {
          os.close()
        }
      }
    }
  }

  /**
   * Init the hoodie table if it is not exists.
   * @param sparkSession
   * @param table
   * @return
   */
  def initTableIfNeed(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val location: String = {
      val url = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val fs = new Path(url.get).getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      val rawPath = fs.makeQualified(new Path(url.get)).toUri.toString
      // remove placeHolder
      if (rawPath.endsWith("-PLACEHOLDER")) {
        rawPath.substring(0, rawPath.length() - 16)
      } else {
        rawPath
      }
    }

    val conf = sparkSession.sessionState.newHadoopConf()
    val basePath = new Path(location)
    val fs = basePath.getFileSystem(conf)
    val metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    val tableExists = fs.exists(metaPath)

    // Init the hoodie table
    if (!tableExists) {
      val tableName = table.identifier.table
      val parameters = table.storage.properties
      val payloadClass = parameters.getOrElse(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, DataSourceWriteOptions.DEFAULT_PAYLOAD_OPT_VAL)
      val tableType = parameters.getOrElse(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY,
        DataSourceWriteOptions.DEFAULT_TABLE_TYPE_OPT_VAL)

      logInfo(s"Table $tableName is not exists, start to create the hudi table")
      val properties = new Properties()
      properties.putAll(parameters.asJava)
      // add table partition to properties
      properties.put(PARTITIONPATH_FIELD_OPT_KEY, table.partitionColumnNames.mkString(","))
      HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(properties)
        .setTableName(tableName)
        .setTableType(tableType)
        .setPayloadClassName(payloadClass)
        .initTable(conf, basePath.toString)

      // save necessary parameter in hoodie.properties
      val propertyPath = new Path(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE)
      saveNeededPropertiesIntoHoodie(fs, propertyPath, properties)
    }
  }
}
