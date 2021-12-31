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

import scala.collection.JavaConverters._
import java.net.URI
import java.util.{Date, Locale, Properties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.hudi.{AvroConversionUtils, SparkAdapterSupport}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstantTimeGenerator}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.{DataType, NullType, StringType, StructField, StructType}

import java.text.SimpleDateFormat

import scala.collection.immutable.Map

object HoodieSqlUtils extends SparkAdapterSupport {
  // NOTE: {@code SimpleDataFormat} is NOT thread-safe
  // TODO replace w/ DateTimeFormatter
  private val defaultDateFormat =
    ThreadLocal.withInitial(new java.util.function.Supplier[SimpleDateFormat] {
      override def get() = new SimpleDateFormat("yyyy-MM-dd")
    })

  def isHoodieTable(table: CatalogTable): Boolean = {
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHoodieTable(table)
  }

  def isHoodieTable(table: LogicalPlan, spark: SparkSession): Boolean = {
    tripAlias(table) match {
      case LogicalRelation(_, _, Some(tbl), _) => isHoodieTable(tbl)
      case relation: UnresolvedRelation =>
        isHoodieTable(sparkAdapter.toTableIdentify(relation), spark)
      case _=> false
    }
  }

  def getTableIdentify(table: LogicalPlan): TableIdentifier = {
    table match {
      case SubqueryAlias(name, _) => sparkAdapter.toTableIdentify(name)
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

  def getTableLocation(table: CatalogTable, sparkSession: SparkSession): String = {
    val uri = if (table.tableType == CatalogTableType.MANAGED && isHoodieTable(table)) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
      .getOrElse(throw new IllegalArgumentException(s"Missing location for ${table.identifier}"))
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

  def castIfNeeded(child: Expression, dataType: DataType, conf: SQLConf): Expression = {
    child match {
      case Literal(nul, NullType) => Literal(nul, dataType)
      case _ => if (child.dataType != dataType)
        Cast(child, dataType, Option(conf.sessionLocalTimeZone)) else child
    }
  }

  /**
   * Get the TableIdentifier of the target table in MergeInto.
   */
  def getMergeIntoTargetTableId(mergeInto: MergeIntoTable): TableIdentifier = {
    val aliaId = mergeInto.targetTable match {
      case SubqueryAlias(_, SubqueryAlias(tableId, _)) => tableId
      case SubqueryAlias(tableId, _) => tableId
      case plan => throw new IllegalArgumentException(s"Illegal plan $plan in target")
    }
    sparkAdapter.toTableIdentify(aliaId)
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

  def isEnableHive(sparkSession: SparkSession): Boolean =
    "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)

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
}
