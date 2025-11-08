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

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions.COMMIT_METADATA_KEYPREFIX
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieInstantTimeGenerator, HoodieTimeline, TimelineUtils}
import org.apache.hudi.common.table.timeline.TimelineUtils.parseDateFromInstantTime
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.storage.{HoodieStorage, StoragePath, StoragePathInfo}
import org.apache.hudi.util.SparkConfigUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

object HoodieSqlCommonUtils extends SparkAdapterSupport {
  // NOTE: {@code SimpleDataFormat} is NOT thread-safe
  // TODO replace w/ DateTimeFormatter
  private val defaultDateFormat =
  ThreadLocal.withInitial(new java.util.function.Supplier[SimpleDateFormat] {
    override def get() = new SimpleDateFormat("yyyy-MM-dd")
  })

  def getTableSqlSchema(metaClient: HoodieTableMetaClient,
                        includeMetadataFields: Boolean = false): Option[StructType] = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val avroSchema = try Some(schemaResolver.getTableAvroSchema(includeMetadataFields))
    catch {
      case _: Throwable => None
    }
    avroSchema.map(AvroConversionUtils.convertAvroSchemaToStructType)
  }

  def getAllPartitionPaths(spark: SparkSession, table: CatalogTable, metaClient: HoodieTableMetaClient): Seq[String] = {
    val sparkEngine = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataConfig = {
      val properties = TypedProperties.fromMap((spark.sessionState.conf.getAllConfs ++ table.storage.properties ++ table.properties).asJava)
      HoodieMetadataConfig.newBuilder.fromProperties(properties).build()
    }
    FSUtils.getAllPartitionPaths(sparkEngine, metaClient, metadataConfig).asScala.toSeq
  }

  def getFilesInPartitions(spark: SparkSession,
                           table: CatalogTable,
                           metaClient: HoodieTableMetaClient,
                           partitionPaths: Seq[String]): Map[String, Seq[StoragePathInfo]] = {
    val sparkEngine = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataConfig = {
      val properties = TypedProperties.fromMap((spark.sessionState.conf.getAllConfs ++ table.storage.properties ++ table.properties).asJava)
      HoodieMetadataConfig.newBuilder.fromProperties(properties).build()
    }
    FSUtils.getFilesInPartitions(sparkEngine, metaClient, metadataConfig, partitionPaths.toArray).asScala
      .map(e => (e._1, e._2.asScala.toSeq))
      .toMap
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
  def isHiveStyledPartitioning(partitionPaths: Seq[String], table: CatalogTable): Boolean = {
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
      partitionPaths.forall(isHiveStylePartitionPath)
    } else {
      true
    }
  }

  /**
   * Determine whether URL encoding is enabled
   */
  def isUrlEncodeEnabled(partitionPaths: Seq[String], table: CatalogTable): Boolean = {
    if (table.partitionColumnNames.nonEmpty) {
      partitionPaths.forall(partitionPath => partitionPath.split("/").length == table.partitionColumnNames.size)
    } else {
      false
    }
  }

  /**
   * Add the hoodie meta fields to the schema.
   * @param schema
   * @return
   */
  def addMetaFields(schema: StructType): StructType = {
    val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
    // filter the meta field to avoid duplicate field.
    val dataFields = schema.fields.filterNot(f => metaFields.contains(f.name))
    val fields = metaFields.map(StructField(_, StringType)) ++ dataFields
    StructType(fields.toSeq)
  }

  private lazy val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  /**
   * Remove the meta fields from the schema.
   * @param schema
   * @return
   */
  def removeMetaFields(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(f => isMetaField(f.name)))
  }

  def isMetaField(name: String): Boolean = {
    metaFields.contains(name)
  }

  def removeMetaFields[T <: Attribute](attrs: Seq[T]): Seq[T] = {
    attrs.filterNot(attr => isMetaField(attr.name))
  }

  /**
   * Get the table location.
   * @param tableId
   * @param spark
   * @return
   */
  def getTableLocation(tableId: TableIdentifier, spark: SparkSession): String = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    getTableLocation(table, spark)
  }

  def getTableLocation(properties: Map[String, String], identifier: TableIdentifier, sparkSession: SparkSession): String = {
    val location: Option[String] = Some(properties.getOrElse("location", ""))
    val isManaged = location.isEmpty || location.get.isEmpty
    val uri = if (isManaged) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(identifier))
    } else {
      Some(new StoragePath(location.get).toUri)
    }
    getTableLocation(uri, identifier, sparkSession)
  }

  def getTableLocation(table: CatalogTable, sparkSession: SparkSession): String = {
    val uri = table.storage.locationUri.orElse {
      Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
    }
    getTableLocation(uri, table.identifier, sparkSession)
  }

  def getTableLocation(uri: Option[URI], identifier: TableIdentifier, sparkSession: SparkSession): String = {
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
      .getOrElse(throw new IllegalArgumentException(s"Missing location for $identifier"))
  }

  private def removePlaceHolder(path: String): String = {
    if (path == null || path.length == 0) {
      path
    } else if (path.endsWith("-__PLACEHOLDER__")) {
      path.substring(0, path.length() - 16)
    } else {
      path
    }
  }

  def makePathQualified(path: URI, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri.toString
  }

  /**
   * Check if the hoodie.properties exists in the table path.
   */
  def tableExistsInPath(tablePath: String, storage: HoodieStorage): Boolean = {
    val basePath = new StoragePath(tablePath)
    val metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    storage.exists(metaPath)
  }

  /**
   * Check if Sql options are Hoodie Config keys.
   *
   * TODO: standardize the key prefix so that we don't need this helper (HUDI-4935)
   */
  private def isHoodieConfigKey(key: String, commitMetadataKeyPrefix: String): Boolean =
    key.startsWith("hoodie.") || key.startsWith(commitMetadataKeyPrefix) ||
      key == DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key

  def filterHoodieConfigs(opts: Map[String, String]): Map[String, String] =
    opts.filterKeys(isHoodieConfigKey(_,
      opts.getOrElse(COMMIT_METADATA_KEYPREFIX.key, COMMIT_METADATA_KEYPREFIX.defaultValue()))).toMap

  /**
   * Checks whether Spark is using Hive as Session's Catalog
   */
  def isUsingHiveCatalog(sparkSession: SparkSession): Boolean =
    sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive"

  /**
   * Convert different query instant time format to the commit time format.
   * Currently we support three kinds of instant time format for time travel query:
   * 1、yyyy-MM-dd HH:mm:ss
   * 2、yyyy-MM-dd
   *   This will convert to 'yyyyMMdd000000'.
   * 3、yyyyMMddHHmmss
   */
  def formatQueryInstant(queryInstant: String): String = {
    val instantLength = queryInstant.length
    if (instantLength == 19 || instantLength == 23) {
      // Handle "yyyy-MM-dd HH:mm:ss[.SSS]" format
      HoodieInstantTimeGenerator.getInstantForDateString(queryInstant)
    } else if (instantLength == HoodieInstantTimeGenerator.SECS_INSTANT_ID_LENGTH
      || instantLength  == HoodieInstantTimeGenerator.MILLIS_INSTANT_ID_LENGTH) {
      // Handle already serialized "yyyyMMddHHmmss[SSS]" format
      validateInstant(queryInstant)
      queryInstant
    } else if (instantLength == 10) { // for yyyy-MM-dd
      TimelineUtils.formatDate(defaultDateFormat.get().parse(queryInstant))
    } else {
      throw new IllegalArgumentException(s"Unsupported query instant time format: $queryInstant,"
        + s"Supported time format are: 'yyyy-MM-dd: HH:mm:ss.SSS' or 'yyyy-MM-dd' or 'yyyyMMddHHmmssSSS'")
    }
  }

  def formatName(sparkSession: SparkSession, name: String): String = {
    if (sparkSession.sessionState.conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
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

  // Find the origin column from schema by column name, throw an HoodieAnalysisException if the column
  // reference is invalid.
  def findColumnByName(schema: StructType, name: String, resolver: Resolver):Option[StructField] = {
    schema.fields.collectFirst {
      case field if resolver(field.name, name) => field
    }
  }

  // Compare a [[StructField]] to another, return true if they have the same column
  // name(by resolver) and dataType.
  def columnEqual(field: StructField, other: StructField, resolver: Resolver): Boolean = {
    resolver(field.name, other.name) && field.dataType == other.dataType
  }

  def castIfNeeded(child: Expression, dataType: DataType): Expression = {
    child match {
      case Literal(nul, NullType) => Literal(nul, dataType)
      case expr if child.dataType != dataType => Cast(expr, dataType, Option(SQLConf.get.sessionLocalTimeZone))
      case _ => child
    }
  }

  def normalizePartitionSpec[T](
                                 partitionSpec: Map[String, T],
                                 partColNames: Seq[String],
                                 tblName: String,
                                 resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new HoodieAnalysisException(s"$key is not a valid partition column in table $tblName.")
      }
      normalizedKey -> value
    }

    if (normalizedPartSpec.size < partColNames.size) {
      throw new HoodieAnalysisException(
        "All partition columns need to be specified for Hoodie's partition")
    }

    val lowerPartColNames = partColNames.map(_.toLowerCase)
    if (lowerPartColNames.distinct.length != lowerPartColNames.length) {
      val duplicateColumns = lowerPartColNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new HoodieAnalysisException(
        s"Found duplicate column(s) in the partition schema: ${duplicateColumns.mkString(", ")}")
    }

    normalizedPartSpec.toMap
  }

  def getPartitionPathToDrop(
                              hoodieCatalogTable: HoodieCatalogTable,
                              normalizedSpecs: Seq[Map[String, String]]): String = {
    normalizedSpecs.map(makePartitionPath(hoodieCatalogTable, _)).mkString(",")
  }

  private def makePartitionPath(partitionFields: Seq[String],
                                normalizedSpecs: Map[String, String],
                                enableEncodeUrl: Boolean,
                                enableHiveStylePartitioning: Boolean): String = {
    partitionFields.map { partitionColumn =>
      val encodedPartitionValue = if (enableEncodeUrl) {
        PartitionPathEncodeUtils.escapePathName(normalizedSpecs(partitionColumn))
      } else {
        normalizedSpecs(partitionColumn)
      }
      if (enableHiveStylePartitioning) s"$partitionColumn=$encodedPartitionValue" else encodedPartitionValue
    }.mkString("/")
  }

  def makePartitionPath(hoodieCatalogTable: HoodieCatalogTable,
                        normalizedSpecs: Map[String, String]): String = {
    val tableConfig = hoodieCatalogTable.tableConfig
    val enableHiveStylePartitioning =  java.lang.Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable)
    val enableEncodeUrl = java.lang.Boolean.parseBoolean(tableConfig.getUrlEncodePartitioning)

    makePartitionPath(hoodieCatalogTable.partitionFields, normalizedSpecs, enableEncodeUrl, enableHiveStylePartitioning)
  }

  private def validateInstant(queryInstant: String): Unit = {
    // Provided instant has to either
    //  - Match one of the bootstrapping instants
    //  - Be parse-able (as a date)
    val valid = queryInstant match {
      case HoodieTimeline.INIT_INSTANT_TS |
           HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS |
           HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS => true

      case _ => Try(parseDateFromInstantTime(queryInstant)).isSuccess
    }

    if (!valid) {
      throw new HoodieException(s"Got an invalid instant ($queryInstant)")
    }
  }

  /**
   * Check if Polaris catalog is enabled in the Spark session.
   * @param sparkSession The Spark session
   * @return true if Polaris catalog is configured, false otherwise
   */
  def isUsingPolarisCatalog(sparkSession: SparkSession): Boolean = {
    val sparkSessionConfigs = sparkSession.conf.getAll
    val polarisCatalogClassName = SparkConfigUtils.getStringWithAltKeys(
      sparkSessionConfigs, DataSourceReadOptions.POLARIS_CATALOG_CLASS_NAME)
    sparkSessionConfigs
      .filter(_._1.startsWith("spark.sql.catalog."))
      .exists(_._2 == polarisCatalogClassName)
  }
}
