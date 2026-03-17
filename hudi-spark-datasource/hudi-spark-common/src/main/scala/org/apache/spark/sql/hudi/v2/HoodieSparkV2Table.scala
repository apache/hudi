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

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.catalog.HoodieV1WriteBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter, setAsJavaSetConverter}

/**
 * DSv2 table implementation for Hudi.
 *
 * Read path: uses [[HoodieScanBuilder]] to build COW snapshot scans via [[HoodieFileIndex]].
 * Write path: falls back to DSv1 via [[V2TableWithV1Fallback]] and [[HoodieV1WriteBuilder]].
 *
 * Schema is resolved via [[HoodieCatalogTable]] (catalog path) or [[HoodieTableMetaClient]] (DataFrame path).
 */
case class HoodieSparkV2Table(spark: SparkSession,
                              path: String,
                              catalogTable: Option[CatalogTable] = None,
                              options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table with SupportsRead with SupportsWrite with V2TableWithV1Fallback {

  lazy val hoodieCatalogTable: Option[HoodieCatalogTable] = catalogTable.map { ct =>
    HoodieCatalogTable(spark, ct)
  }

  private lazy val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
    .setBasePath(path)
    .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf))
    .build()

  private lazy val tableSchema: StructType = hoodieCatalogTable match {
    case Some(hct) => hct.tableSchemaWithoutMetaFields
    case None =>
      HoodieSqlCommonUtils.getTableSqlSchema(metaClient, includeMetadataFields = false)
        .getOrElse(new StructType())
  }

  override def name(): String = hoodieCatalogTable match {
    case Some(hct) => hct.table.identifier.unquotedString
    case None => metaClient.getTableConfig.getTableName
  }

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(
    BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, ACCEPT_ANY_SCHEMA
  ).asJava

  override def properties(): util.Map[String, String] = hoodieCatalogTable match {
    case Some(hct) => hct.catalogProperties.asJava
    case None => java.util.Collections.emptyMap()
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val tableProps = properties().asScala.toMap
    val scanOpts = options.asCaseSensitiveMap().asScala.toMap
    val mergedOpts = tableProps ++ scanOpts
    new HoodieScanBuilder(spark, metaClient, tableSchema, mergedOpts)
  }

  private def requireCatalogTable: HoodieCatalogTable =
    hoodieCatalogTable.getOrElse(
      throw new IllegalStateException(
        "Write operations require a catalog table. " +
          "DataFrame API writes are not supported via hudi_v2; use the SQL/catalog path."))

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new HoodieV1WriteBuilder(info.options, requireCatalogTable, spark)
  }

  override def v1Table: CatalogTable = requireCatalogTable.table

  def v1TableWrapper: V1Table = V1Table(v1Table)

  override def partitioning(): Array[Transform] = hoodieCatalogTable match {
    case Some(hct) =>
      hct.partitionFields.map { col =>
        new IdentityTransform(new FieldReference(Seq(col)))
      }.toArray
    case None => Array.empty
  }
}
