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

import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.HoodieWriterUtils._
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.util.{Locale, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A wrapper of hoodie CatalogTable instance and hoodie Table.
 */
class HoodieCatalogTable(val spark: SparkSession, val table: CatalogTable) extends Logging {

  assert(table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi", "It's not a Hudi table")

  private val hadoopConf = spark.sessionState.newHadoopConf

  /**
   * database.table in catalog
   */
  val catalogTableName = table.qualifiedName

  /**
   * properties defined in catalog.
   */
  val catalogProperties: Map[String, String] = table.storage.properties ++ table.properties

  /**
   * hoodie table's location.
   * if create managed hoodie table, use `catalog.defaultTablePath`.
   */
  val tableLocation: String = getTableLocation(table, spark)

  /**
   * A flag to whether the hoodie table exists.
   */
  val hoodieTableExists: Boolean = tableExistsInPath(tableLocation, hadoopConf)

  /**
   * Meta Client.
   */
  lazy val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
    .setBasePath(tableLocation)
    .setConf(hadoopConf)
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
   * PreCombine Field
   */
  lazy val preCombineKey: Option[String] = Option(tableConfig.getPreCombineField)

  /**
   * Partition Fields
   */
  lazy val partitionFields: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * BaseFileFormat
   */
  lazy val baseFileFormat: String = metaClient.getTableConfig.getBaseFileFormat.name()

  /**
   * The schema of table.
   * Make StructField nullable and fill the comments in.
   */
  lazy val tableSchema: StructType = {
    val resolver = spark.sessionState.conf.resolver
    val originSchema = getTableSqlSchema(metaClient, includeMetadataFields = true).getOrElse(table.schema)
    val fields = originSchema.fields.map { f =>
      val nullableField: StructField = f.copy(nullable = true)
      val catalogField = findColumnByName(table.schema, nullableField.name, resolver)
      if (catalogField.isDefined) {
        catalogField.get.getComment().map(nullableField.withComment).getOrElse(nullableField)
      } else {
        nullableField
      }
    }
    StructType(fields)
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
   * The schema of data fields not including hoodie meta fields
   */
  lazy val dataSchemaWithoutMetaFields: StructType = removeMetaFields(dataSchema)

  /**
   * The schema of partition fields
   */
  lazy val partitionSchema: StructType = StructType(tableSchema.filter(f => partitionFields.contains(f.name)))

  /**
   * All the partition paths
   */
  def getPartitionPaths: Seq[String] = getAllPartitionPaths(spark, table)

  /**
   * Check if table is a partitioned table
   */
  def isPartitionedTable: Boolean = table.partitionColumnNames.nonEmpty

  /**
   * init hoodie table for create table (as select)
   */
  def initHoodieTable(): Unit = {
    logInfo(s"Init hoodie.properties for ${table.identifier.unquotedString}")
    val (finalSchema, tableConfigs) = parseSchemaAndConfigs()

    // Save all the table config to the hoodie.properties.
    val properties = new Properties()
    properties.putAll(tableConfigs.asJava)

    if (hoodieTableExists) {
      // just persist hoodie.table.create.schema
      HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(properties)
        .setTableCreateSchema(SchemaConverters.toAvroType(finalSchema).toString())
        .initTable(hadoopConf, tableLocation)
    } else {
      val (recordName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(table.identifier.table)
      val schema = SchemaConverters.toAvroType(finalSchema, false, recordName, namespace)
      val hoodieDatabaseName = formatName(spark, table.identifier.database.getOrElse(spark.sessionState.catalog.getCurrentDatabase))
      HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(properties)
        .setDatabaseName(hoodieDatabaseName)
        .setTableName(table.identifier.table)
        .setTableCreateSchema(schema.toString())
        .setPartitionFields(table.partitionColumnNames.mkString(","))
        .initTable(hadoopConf, tableLocation)
    }
  }

  /**
   * @return schema, table parameters in which all parameters aren't sql-styled.
   */
  private def parseSchemaAndConfigs(): (StructType, Map[String, String]) = {
    val globalProps = DFSPropertiesConfiguration.getGlobalProps.asScala.toMap
    val globalTableConfigs = mappingSparkDatasourceConfigsToTableConfigs(globalProps)
    val globalSqlOptions = HoodieOptionConfig.mappingTableConfigToSqlOption(globalTableConfigs)

    val sqlOptions = HoodieOptionConfig.withDefaultSqlOptions(globalSqlOptions ++ catalogProperties)

    // get final schema and parameters
    val (finalSchema, tableConfigs) = (table.tableType, hoodieTableExists) match {
      case (CatalogTableType.EXTERNAL, true) =>
        val existingTableConfig = tableConfig.getProps.asScala.toMap
        val currentTableConfig = globalTableConfigs ++ existingTableConfig
        val catalogTableProps = HoodieOptionConfig.mappingSqlOptionToTableConfig(catalogProperties)
        validateTableConfig(spark, catalogTableProps, convertMapToHoodieConfig(existingTableConfig))

        val options = extraTableConfig(spark, hoodieTableExists, currentTableConfig) ++
          HoodieOptionConfig.mappingSqlOptionToTableConfig(sqlOptions) ++ currentTableConfig

        ValidationUtils.checkArgument(tableSchema.nonEmpty || table.schema.nonEmpty,
          s"Missing schema for Create Table: $catalogTableName")
        val schema = if (tableSchema.nonEmpty) {
          tableSchema
        } else {
          addMetaFields(table.schema)
        }

        (schema, options)

      case (_, false) =>
        ValidationUtils.checkArgument(table.schema.nonEmpty,
          s"Missing schema for Create Table: $catalogTableName")
        val schema = table.schema
        val options = extraTableConfig(spark, isTableExists = false, globalTableConfigs) ++
          HoodieOptionConfig.mappingSqlOptionToTableConfig(sqlOptions)
        (addMetaFields(schema), options)

      case (CatalogTableType.MANAGED, true) =>
        throw new AnalysisException(s"Can not create the managed table('$catalogTableName')" +
          s". The associated location('$tableLocation') already exists.")
    }
    HoodieOptionConfig.validateTable(spark, finalSchema,
      HoodieOptionConfig.mappingTableConfigToSqlOption(tableConfigs))

    val resolver = spark.sessionState.conf.resolver
    val dataSchema = finalSchema.filterNot { f =>
      table.partitionColumnNames.exists(resolver(_, f.name))
    }
    verifyDataSchema(table.identifier, table.tableType, dataSchema)

    (finalSchema, tableConfigs)
  }

  private def extraTableConfig(sparkSession: SparkSession, isTableExists: Boolean,
      originTableConfig: Map[String, String] = Map.empty): Map[String, String] = {
    val extraConfig = mutable.Map.empty[String, String]
    if (isTableExists) {
      val allPartitionPaths = getPartitionPaths
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

  def apply(sparkSession: SparkSession, tableIdentifier: TableIdentifier): HoodieCatalogTable = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    HoodieCatalogTable(sparkSession, catalogTable)
  }

  def apply(sparkSession: SparkSession, catalogTable: CatalogTable): HoodieCatalogTable = {
    new HoodieCatalogTable(sparkSession, catalogTable)
  }
}
