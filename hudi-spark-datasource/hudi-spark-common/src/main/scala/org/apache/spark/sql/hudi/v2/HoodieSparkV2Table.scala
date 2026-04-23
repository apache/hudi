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

import org.apache.hudi.{DataSourceReadOptions, DataSourceUtils, HoodieSchemaConversionUtils}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}

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
                              preResolvedCatalogTable: Option[HoodieCatalogTable] = None,
                              options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table with SupportsRead with SupportsWrite with V2TableWithV1Fallback {

  lazy val hoodieCatalogTable: Option[HoodieCatalogTable] =
    preResolvedCatalogTable.orElse(catalogTable.map(ct => HoodieCatalogTable(spark, ct)))

  private lazy val metaClient: HoodieTableMetaClient = hoodieCatalogTable match {
    case Some(hct) => hct.metaClient
    case None =>
      // Mirror DSv1 (DefaultSource.createRelation): when the user-supplied path points at
      // a partition or sub-path of a Hudi table, walk up to the table base path before
      // building the meta client; setBasePath on the partition path would fail to find
      // .hoodie. The original `path` is preserved as the query path through the
      // explicitOpts merge in newScanBuilder, so HoodieFileIndex still scopes the read
      // correctly.
      val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf)
      val storage = HoodieStorageUtils.getStorage(path, storageConf)
      val basePath = DataSourceUtils.getTablePath(
        storage, java.util.Collections.singletonList(new StoragePath(path)))
      HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storageConf)
        .build()
  }

  private lazy val timeTravelInstant: Option[String] =
    Option(options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key))
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  private lazy val tableSchema: StructType = {
    // Mirror DSv1 (HoodieBaseRelation): when populate.meta.fields=true (default), Hudi
    // writes _hoodie_commit_time / _hoodie_record_key / etc. into every base file. Hiding
    // them from the V2 schema breaks SELECT _hoodie_commit_time and joins on the meta
    // record-key. With ACCEPT_ANY_SCHEMA + the V1 INSERT fallback rule (Hudi rewrites
    // InsertIntoStatement → InsertIntoHoodieTableCommand, which realigns columns), DML
    // continues to work; HoodieInternalV2Table already exposes the full schema this way.
    val includeMetaFields = metaClient.getTableConfig.populateMetaFields()
    timeTravelInstant match {
      case Some(ts) =>
        // Mirror DSv1 (HoodieBaseRelation): for time-travel reads, resolve the schema as
        // of the requested instant so columns added later aren't exposed. Without this the
        // scan would let Spark project columns that don't exist in the snapshot, producing
        // schemas (and reads) that diverge from DSv1. Errors propagate so an unresolvable
        // instant surfaces clearly rather than silently falling back to the latest schema.
        val avroSchema = new TableSchemaResolver(metaClient).getTableSchema(ts)
        HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(avroSchema)
      case None =>
        hoodieCatalogTable match {
          case Some(hct) =>
            if (includeMetaFields) hct.tableSchema else hct.tableSchemaWithoutMetaFields
          case None =>
            HoodieSqlCommonUtils
              .getTableSqlSchema(metaClient, includeMetadataFields = includeMetaFields)
              .getOrElse(new StructType())
        }
    }
  }

  override def name(): String = hoodieCatalogTable match {
    case Some(hct) => hct.table.identifier.unquotedString
    case None => metaClient.getTableConfig.getTableName
  }

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    // OVERWRITE_BY_FILTER intentionally not advertised: HoodieV1WriteBuilder delegates to
    // the V1 insert_overwrite/partition-overwrite path, which cannot honor arbitrary
    // filter expressions. Advertising it would let df.writeTo(tbl).overwrite(expr)
    // silently rewrite whole partitions instead of filtered rows.
    val writeCaps =
      if (hoodieCatalogTable.isDefined) {
        Set(V1_BATCH_WRITE, TRUNCATE, ACCEPT_ANY_SCHEMA)
      } else {
        Set.empty[TableCapability]
      }
    (Set(BATCH_READ) ++ writeCaps).asJava
  }

  override def properties(): util.Map[String, String] = hoodieCatalogTable match {
    case Some(hct) => hct.catalogProperties.asJava
    case None => java.util.Collections.emptyMap()
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Catalog tables persist a "path" TBLPROPERTY containing only the URI path component
    // (no scheme/authority). Letting it through here would override the fully qualified
    // location passed via the V2Table constructor and silently retarget the scan at the
    // default filesystem (e.g. file:///table instead of s3://bucket/table). Drop it before
    // merging so the constructor-supplied path always wins.
    val tableProps = properties().asScala.toMap - "path"
    val scanOpts = options.asCaseSensitiveMap().asScala.toMap
    val constructorOpts = this.options.asCaseSensitiveMap().asScala.toMap
    // HoodieFileIndex.getQueryPaths requires "path". On the DataFrame-API path, Spark puts it in
    // the constructor options map, not the scan-time one — promote it here.
    // Resolving through HoodieV2ReadSupport.resolveReadOptions layers session/global read
    // defaults under the explicit options so DSv2 honors SQL confs and hudi-defaults.conf
    // the same way DSv1 does.
    val explicitOpts = Map("path" -> path) ++ constructorOpts ++ tableProps ++ scanOpts
    val mergedOpts = HoodieV2ReadSupport.resolveReadOptions(spark, explicitOpts)
    if (!HoodieV2ReadSupport.isSupportedByDSv2(metaClient, mergedOpts, spark)) {
      throw new HoodieException(
        "DSv2 read path does not support this query configuration " +
          "(MOR snapshot, non-Parquet base format, multiple base formats, " +
          "incremental/CDC, or bootstrap table). " +
          "Disable hoodie.datasource.read.use.v2 or use format(\"hudi\").")
    }
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
