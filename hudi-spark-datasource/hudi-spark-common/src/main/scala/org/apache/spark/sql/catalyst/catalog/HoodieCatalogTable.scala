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

package org.apache.spark.sql.catalyst.catalog

import org.apache.hudi.{AvroConversionUtils, DataSourceOptionsHelper}
import org.apache.hudi.DataSourceWriteOptions.OPERATION
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, TypedProperties}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.HoodieTableConfig.URL_ENCODE_PARTITIONING
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.constant.{KeyGeneratorOptions, KeyGeneratorType}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.storage.HoodieStorageUtils
import org.apache.hudi.util.SparkConfigUtils
import org.apache.hudi.util.SparkConfigUtils.getStringWithAltKeys

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieOptionConfig._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Table definition for SQL functionalities. Depending on the way of data generation,
 * meta of Hudi table can be from Spark catalog or meta directory on filesystem.
 * [[HoodieCatalogTable]] takes both meta sources into consideration when handling
 * EXTERNAL and MANAGED tables.
 *
 * NOTE: all the meta should be retrieved from meta directory on filesystem first.
 */
class HoodieCatalogTable(val spark: SparkSession, var table: CatalogTable) extends Logging {

  checkArgument(table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
    || table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "org.apache.hudi",
    s" ${table.qualifiedName} is not a Hudi table")

  private val storageConf = HadoopFSUtils.getStorageConfWithCopy(spark.sessionState.newHadoopConf)

  /**
   * database.table in catalog
   */
  val catalogTableName: String = table.qualifiedName

  /**
   * properties defined in catalog.
   */
  val catalogProperties: Map[String, String] = HoodieOptionConfig.makeOptionsCaseInsensitive(table.storage.properties ++ table.properties)

  /**
   * hoodie table's location.
   * if create managed hoodie table, use `catalog.defaultTablePath`.
   */
  val tableLocation: String = getTableLocation(table, spark)

  /**
   * A flag to whether the hoodie table exists.
   */
  val hoodieTableExists: Boolean =
    tableExistsInPath(tableLocation, HoodieStorageUtils.getStorage(tableLocation, storageConf))

  /**
   * Meta Client.
   */
  lazy val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
    .setBasePath(tableLocation)
    .setConf(storageConf)
    .build()

  /**
   * Hoodie Table Config
   */
  lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  /**
   * the name of table
   */
  lazy val tableName: String = tableConfig.getTableName

  /**
   * the name of database
   */
  lazy val databaseName: String = tableConfig.getDatabaseName

  /**
   * The name of type of table
   */
  lazy val tableType: HoodieTableType = tableConfig.getTableType

  /**
   * The type of table
   */
  lazy val tableTypeName: String = tableType.name()

  /**
   * Record Field List(Primary Key List)
   */
  lazy val primaryKeys: Array[String] = tableConfig.getRecordKeyFields.orElse(Array.empty)

  /**
   * Comparables Field
   */
  lazy val preCombineKeys: java.util.List[String] = tableConfig.getPreCombineFields

  /**
   * Partition Fields
   */
  lazy val partitionFields: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * For multiple base file formats
   */
  lazy val isMultipleBaseFileFormatsEnabled: Boolean = tableConfig.isMultipleBaseFileFormatsEnabled

  /**
   * Firstly try to load table schema from meta directory on filesystem.
   * If that fails then fallback to retrieving it from the Spark catalog.
   */
  lazy val tableSchema: StructType = {
    val schemaFromMetaOpt = loadTableSchemaByMetaClient()
    if (schemaFromMetaOpt.nonEmpty) {
      schemaFromMetaOpt.get
    } else if (table.schema.nonEmpty) {
      addMetaFields(table.schema)
    } else {
      throw new AnalysisException(
        s"$catalogTableName does not contains schema fields.")
    }
  }

  /**
   * The schema without hoodie meta fields
   */
  lazy val tableSchemaWithoutMetaFields: StructType = removeMetaFields(tableSchema)

  /**
   * The schema of data fields
   */
  lazy val dataSchema: StructType = {
    StructType(tableSchema.filterNot(f => partitionFields.contains(f.name)))
  }

  /**
   * The schema of partition fields
   */
  lazy val partitionSchema: StructType = StructType(tableSchema.filter(f => partitionFields.contains(f.name)))

  /**
   * All the partition paths, excludes lazily deleted partitions.
   */
  def getPartitionPaths: Seq[String] = {
    val droppedPartitions = TimelineUtils.getDroppedPartitions(metaClient, org.apache.hudi.common.util.Option.empty(), org.apache.hudi.common.util.Option.empty())

    getAllPartitionPaths(spark, table, metaClient)
      .filter(!droppedPartitions.contains(_))
  }

  /**
   * Check if table is a partitioned table
   */
  def isPartitionedTable: Boolean = table.partitionColumnNames.nonEmpty

  /**
   * Initializes table meta on filesystem when applying CREATE TABLE clause.
   */
  def initHoodieTable(): Unit = {
    logInfo(s"Init hoodie.properties for ${table.identifier.unquotedString}")
    val (finalSchema, tableConfigs) = parseSchemaAndConfigs()
    // The TableSchemaResolver#getTableAvroSchemaInternal has a premise that
    // the table create schema does not include the metadata fields.
    // When flag includeMetadataFields is false, no metadata fields should be included in the resolved schema.
    val dataSchema = removeMetaFields(finalSchema)

    table = table.copy(schema = finalSchema)

    // Save all the table config to the hoodie.properties.
    val properties = TypedProperties.fromMap(tableConfigs.asJava)

    val catalogDatabaseName = formatName(spark,
      table.identifier.database.getOrElse(spark.sessionState.catalog.getCurrentDatabase))
    if (hoodieTableExists) {
      checkArgument(StringUtils.isNullOrEmpty(databaseName) || databaseName == catalogDatabaseName,
        "The database names from this hoodie path and this catalog table is not same.")
      val recordName = AvroSchemaUtils.getAvroRecordQualifiedName(table.identifier.table)
      // just persist hoodie.table.create.schema
      HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .setDatabaseName(catalogDatabaseName)
        .setTableCreateSchema(SchemaConverters.toAvroType(dataSchema, recordName = recordName).toString())
        .initTable(storageConf, tableLocation)
    } else {
      val (recordName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(table.identifier.table)
      val schema = SchemaConverters.toAvroType(dataSchema, nullable = false, recordName, namespace)
      val partitionColumns = if (SparkConfigUtils.containsConfigProperty(tableConfigs, KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)) {
        SparkConfigUtils.getStringWithAltKeys(tableConfigs, KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)
      } else if (table.partitionColumnNames.isEmpty) {
        null
      } else {
        table.partitionColumnNames.mkString(",")
      }

      HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .setTableVersion(Integer.valueOf(getStringWithAltKeys(tableConfigs, HoodieWriteConfig.WRITE_TABLE_VERSION)))
        .setTableFormat(getStringWithAltKeys(tableConfigs, HoodieTableConfig.TABLE_FORMAT))
        .setDatabaseName(catalogDatabaseName)
        .setTableName(table.identifier.table)
        .setTableCreateSchema(schema.toString())
        .setPartitionFields(partitionColumns)
        .initTable(storageConf, tableLocation)
    }
  }

  /**
   * Derives the SQL schema and configurations for a Hudi table:
   * 1. Columns in the schema fall under two categories -- the data columns described in
   * CREATE TABLE clause and meta columns enumerated in [[HoodieRecord#HOODIE_META_COLUMNS]];
   * 2. Configurations derived come from config file, PROPERTIES and OPTIONS in CREATE TABLE clause.
   */
  private def parseSchemaAndConfigs(): (StructType, Map[String, String]) = {
    val globalProps = DFSPropertiesConfiguration.getGlobalProps.asScala.toMap
    val globalTableConfigs = mappingSparkDatasourceConfigsToTableConfigs(globalProps)
    val globalSqlOptions = mapHoodieConfigsToSqlOptions(globalTableConfigs)

    val sqlOptions = withDefaultSqlOptions(globalSqlOptions ++
      mapHoodieConfigsToSqlOptions(catalogProperties))

    // get final schema and parameters
    val (finalSchema, tableConfigs) = (table.tableType, hoodieTableExists) match {
      case (CatalogTableType.EXTERNAL, true) =>
        val existingTableConfig = tableConfig.getProps.asScala.toMap
        val currentTableConfig = globalTableConfigs ++ existingTableConfig
        val catalogTableProps = mapSqlOptionsToTableConfigs(catalogProperties)
        validateTableConfig(spark, catalogTableProps, convertMapToHoodieConfig(existingTableConfig))

        val options = extraTableConfig(hoodieTableExists, currentTableConfig) ++
          mapSqlOptionsToTableConfigs(sqlOptions) ++ currentTableConfig

        (tableSchema, options)

      case (_, false) =>
        checkArgument(table.schema.nonEmpty,
          s"Missing schema for Create Table: $catalogTableName")
        val schema = table.schema
        val options = extraTableConfig(tableExists = false, globalTableConfigs, sqlOptions) ++
          mapSqlOptionsToTableConfigs(sqlOptions)
        (addMetaFields(schema), options)

      case (CatalogTableType.MANAGED, true) =>
        throw new AnalysisException(s"Can not create the managed table('$catalogTableName')" +
          s". The associated location('$tableLocation') already exists.")
    }
    HoodieOptionConfig.validateTable(spark, finalSchema,
      mapHoodieConfigsToSqlOptions(tableConfigs))

    val resolver = spark.sessionState.conf.resolver
    val dataSchema = finalSchema.filterNot { f =>
      table.partitionColumnNames.exists(resolver(_, f.name))
    }
    verifyDataSchema(table.identifier, table.tableType, dataSchema)

    (finalSchema, tableConfigs)
  }

  private def extraTableConfig(tableExists: Boolean,
                               originTableConfig: Map[String, String] = Map.empty,
                               sqlOptions: Map[String, String] = Map.empty): Map[String, String] = {
    val extraConfig = mutable.Map.empty[String, String]
    if (tableExists) {
      val allPartitionPaths = getPartitionPaths
      if (originTableConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)) {
        extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) =
          originTableConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key)
      } else {
        extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) =
          String.valueOf(isHiveStyledPartitioning(allPartitionPaths, table))
      }
      if (originTableConfig.contains(URL_ENCODE_PARTITIONING.key)) {
        extraConfig(URL_ENCODE_PARTITIONING.key) =
          originTableConfig(URL_ENCODE_PARTITIONING.key)
      } else {
        extraConfig(URL_ENCODE_PARTITIONING.key) =
          String.valueOf(isUrlEncodeEnabled(allPartitionPaths, table))
      }
    } else {
      extraConfig(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key) = "true"
      extraConfig(URL_ENCODE_PARTITIONING.key) = URL_ENCODE_PARTITIONING.defaultValue()
    }

    if (originTableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)) {
      extraConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) =
        HoodieSparkKeyGeneratorFactory.convertToSparkKeyGenerator(
          originTableConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key))
    } else if (originTableConfig.contains(HoodieTableConfig.KEY_GENERATOR_TYPE.key)) {
      extraConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) =
        HoodieSparkKeyGeneratorFactory.convertToSparkKeyGenerator(
          KeyGeneratorType.valueOf(originTableConfig(HoodieTableConfig.KEY_GENERATOR_TYPE.key)).getClassName)
    } else {
      val primaryKeys = table.properties.getOrElse(SQL_KEY_TABLE_PRIMARY_KEY.sqlKeyName, table.storage.properties.get(SQL_KEY_TABLE_PRIMARY_KEY.sqlKeyName)).toString
      val partitionFieldsOpt = Option.apply(SparkConfigUtils.getStringWithAltKeys(originTableConfig, HoodieTableConfig.PARTITION_FIELDS))
        .orElse(sqlOptions.get(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()))
      val partitions = partitionFieldsOpt.getOrElse(table.partitionColumnNames.mkString(","))
      extraConfig(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) =
        DataSourceOptionsHelper.inferKeyGenClazz(primaryKeys, partitions)
    }
    extraConfig.toMap
  }

  private def loadTableSchemaByMetaClient(): Option[StructType] = {
    val resolver = spark.sessionState.conf.resolver
    try getTableSqlSchema(metaClient, includeMetadataFields = true).map(originSchema => {
      // Load table schema from meta on filesystem, and fill in 'comment'
      // information from Spark catalog.
      // Hoodie newly added columns are positioned after partition columns,
      // so it's necessary to reorder fields.
      val (partFields, dataFields) = originSchema.fields.map { f =>
        val nullableField: StructField = f.copy(nullable = true)
        val catalogField = findColumnByName(table.schema, nullableField.name, resolver)
        if (catalogField.isDefined) {
          catalogField.get.getComment().map(nullableField.withComment).getOrElse(nullableField)
        } else {
          nullableField
        }
      }.partition(f => partitionFields.contains(f.name))
      // insert_overwrite operation with partial partition values will mix up the order
      // of partition columns, so we also need reorder partition fields here.
      val nameToField = partFields.map(field => (field.name, field)).toMap
      val orderedPartFields = partitionFields.map(nameToField(_)).toSeq

      StructType(dataFields ++ orderedPartFields)
    })
    catch {
      case cause: Throwable =>
        logWarning("Failed to load table schema from meta client.", cause)
        None
    }
  }

  // This code is forked from org.apache.spark.sql.hive.HiveExternalCatalog#verifyDataSchema
  private def verifyDataSchema(tableIdentifier: TableIdentifier, tableType: CatalogTableType,
      dataSchema: Seq[StructField]): Unit = {
    if (tableType != CatalogTableType.VIEW) {
      val invalidChars = Seq(",", ":", ";")
      def verifyNestedColumnNames(schema: StructType): Unit = schema.foreach { f =>
        f.dataType match {
          case st: StructType => verifyNestedColumnNames(st)
          case _ if invalidChars.exists(f.name.contains) =>
            val invalidCharsString = invalidChars.map(c => s"'$c'").mkString(", ")
            val errMsg = "Cannot create a table having a nested column whose name contains " +
            s"invalid characters ($invalidCharsString) in Hive metastore. Table: $tableIdentifier; " +
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
            s"contains commas in Hive metastore. Table: $tableIdentifier; Column: ${f.name}")
          // Checks nested column names
          case st: StructType =>
            verifyNestedColumnNames(st)
          case _ =>
        }
      }
    }
  }
}

object HoodieCatalogTable {
  // The properties should not be used when create table
  val needFilterProps: List[String] = List(HoodieTableConfig.DATABASE_NAME.key, HoodieTableConfig.NAME.key, OPERATION.key)

  def apply(sparkSession: SparkSession, tableIdentifier: TableIdentifier): HoodieCatalogTable = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    HoodieCatalogTable(sparkSession, catalogTable)
  }

  def apply(sparkSession: SparkSession, catalogTable: CatalogTable): HoodieCatalogTable = {
    new HoodieCatalogTable(sparkSession, catalogTable)
  }
}
