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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, HoodieMetadataConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstantTimeGenerator}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.{AvroConversionUtils, SparkAdapterSupport}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Locale, Properties}
import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object HoodieSqlCommonUtils extends SparkAdapterSupport {
  // NOTE: {@code SimpleDataFormat} is NOT thread-safe
  // TODO replace w/ DateTimeFormatter
  private val defaultDateFormat =
  ThreadLocal.withInitial(new java.util.function.Supplier[SimpleDateFormat] {
    override def get() = new SimpleDateFormat("yyyy-MM-dd")
  })

  def getTableIdentifier(table: LogicalPlan): TableIdentifier = {
    table match {
      case SubqueryAlias(name, _) => sparkAdapter.toTableIdentifier(name)
      case _ => throw new IllegalArgumentException(s"Illegal table: $table")
    }
  }

  def getTableSqlSchema(metaClient: HoodieTableMetaClient,
                        includeMetadataFields: Boolean = false): Option[StructType] = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val avroSchema = try Some(schemaResolver.getTableAvroSchema(includeMetadataFields))
    catch {
      case _: Throwable => None
    }
    avroSchema.map(AvroConversionUtils.convertAvroSchemaToStructType)
  }

  def getAllPartitionPaths(spark: SparkSession, table: CatalogTable): Seq[String] = {
    val sparkEngine = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataConfig = {
      val properties = new Properties()
      properties.putAll((spark.sessionState.conf.getAllConfs ++ table.storage.properties ++ table.properties).asJava)
      HoodieMetadataConfig.newBuilder.fromProperties(properties).build()
    }
    FSUtils.getAllPartitionPaths(sparkEngine, metadataConfig, getTableLocation(table, spark)).asScala
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

  private def tripAlias(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case SubqueryAlias(_, relation: LogicalPlan) =>
        tripAlias(relation)
      case other =>
        other
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
    StructType(fields)
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

  def removeMetaFields(df: DataFrame): DataFrame = {
    val withoutMetaColumns = df.logicalPlan.output
      .filterNot(attr => isMetaField(attr.name))
      .map(new Column(_))
    if (withoutMetaColumns.length != df.logicalPlan.output.size) {
      df.select(withoutMetaColumns: _*)
    } else {
      df
    }
  }

  def removeMetaFields(attrs: Seq[Attribute]): Seq[Attribute] = {
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
      Some(new Path(location.get).toUri)
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
      .getOrElse(throw new IllegalArgumentException(s"Missing location for ${identifier}"))
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
  def tableExistsInPath(tablePath: String, conf: Configuration): Boolean = {
    val basePath = new Path(tablePath)
    val fs = basePath.getFileSystem(conf)
    val metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    fs.exists(metaPath)
  }

  /**
   * Split the expression to a sub expression seq by the AND operation.
   * @param expression
   * @return
   */
  def splitByAnd(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        splitByAnd(left) ++ splitByAnd(right)
      case exp => Seq(exp)
    }
  }

  /**
   * Append the spark config and table options to the baseConfig.
   */
  def withSparkConf(spark: SparkSession, options: Map[String, String])
                   (baseConfig: Map[String, String] = Map.empty): Map[String, String] = {
    baseConfig ++ DFSPropertiesConfiguration.getGlobalProps.asScala ++ // Table options has the highest priority
      (spark.sessionState.conf.getAllConfs ++ HoodieOptionConfig.mappingSqlOptionToHoodieParam(options))
        .filterKeys(_.startsWith("hoodie."))
  }

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
    if (instantLength == 19 || instantLength == 23) { // for yyyy-MM-dd HH:mm:ss[.SSS]
      HoodieInstantTimeGenerator.getInstantForDateString(queryInstant)
    } else if (instantLength == HoodieInstantTimeGenerator.SECS_INSTANT_ID_LENGTH
      || instantLength  == HoodieInstantTimeGenerator.MILLIS_INSTANT_ID_LENGTH) { // for yyyyMMddHHmmss[SSS]
      HoodieActiveTimeline.parseDateFromInstantTime(queryInstant) // validate the format
      queryInstant
    } else if (instantLength == 10) { // for yyyy-MM-dd
      HoodieActiveTimeline.formatDate(defaultDateFormat.get().parse(queryInstant))
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

  // Find the origin column from schema by column name, throw an AnalysisException if the column
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

  def castIfNeeded(child: Expression, dataType: DataType, conf: SQLConf): Expression = {
    child match {
      case Literal(nul, NullType) => Literal(nul, dataType)
      case _ => if (child.dataType != dataType)
        Cast(child, dataType, Option(conf.sessionLocalTimeZone)) else child
    }
  }

  def normalizePartitionSpec[T](
                                 partitionSpec: Map[String, T],
                                 partColNames: Seq[String],
                                 tblName: String,
                                 resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new AnalysisException(s"$key is not a valid partition column in table $tblName.")
      }
      normalizedKey -> value
    }

    if (normalizedPartSpec.size < partColNames.size) {
      throw new AnalysisException(
        "All partition columns need to be specified for Hoodie's partition")
    }

    val lowerPartColNames = partColNames.map(_.toLowerCase)
    if (lowerPartColNames.distinct.length != lowerPartColNames.length) {
      val duplicateColumns = lowerPartColNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) in the partition schema: ${duplicateColumns.mkString(", ")}")
    }

    normalizedPartSpec.toMap
  }

  def getPartitionPathToDrop(
                              hoodieCatalogTable: HoodieCatalogTable,
                              normalizedSpecs: Seq[Map[String, String]]): String = {
    val table = hoodieCatalogTable.table
    val allPartitionPaths = hoodieCatalogTable.getPartitionPaths
    val enableHiveStylePartitioning = isHiveStyledPartitioning(allPartitionPaths, table)
    val enableEncodeUrl = isUrlEncodeEnabled(allPartitionPaths, table)
    val partitionsToDrop = normalizedSpecs.map { spec =>
      hoodieCatalogTable.partitionFields.map { partitionColumn =>
        val encodedPartitionValue = if (enableEncodeUrl) {
          PartitionPathEncodeUtils.escapePathName(spec(partitionColumn))
        } else {
          spec(partitionColumn)
        }
        if (enableHiveStylePartitioning) {
          partitionColumn + "=" + encodedPartitionValue
        } else {
          encodedPartitionValue
        }
      }.mkString("/")
    }.mkString(",")
    partitionsToDrop
  }
}
