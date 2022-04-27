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

package org.apache.spark.sql.hudi.catalog

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table.{alignSchema, generateUnsafeProjection}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, HoodieUnsafeCatalystUtils, SaveMode, SparkSession}

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

case class HoodieInternalV2Table(spark: SparkSession,
                                 path: String,
                                 catalogTable: Option[CatalogTable] = None,
                                 tableIdentifier: Option[String] = None,
                                 options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
  extends Table with SupportsWrite with V2TableWithV1Fallback {

  lazy val hoodieCatalogTable: HoodieCatalogTable = if (catalogTable.isDefined) {
    HoodieCatalogTable(spark, catalogTable.get)
  } else {
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(SparkSession.active.sessionState.newHadoopConf)
      .build()

    val tableConfig: HoodieTableConfig = metaClient.getTableConfig
    val tableName: String = tableConfig.getTableName

    HoodieCatalogTable(spark, TableIdentifier(tableName))
  }

  private lazy val tableSchema: StructType = hoodieCatalogTable.tableSchema

  override def name(): String = hoodieCatalogTable.table.identifier.unquotedString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(
    BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, ACCEPT_ANY_SCHEMA
  ).asJava

  override def properties(): util.Map[String, String] = {
    hoodieCatalogTable.catalogProperties.asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new HoodieV1WriteBuilder(info.options, hoodieCatalogTable, spark)
  }

  override def v1Table: CatalogTable = hoodieCatalogTable.table

  override def partitioning(): Array[Transform] = {
    hoodieCatalogTable.partitionFields.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

}

private class HoodieV1WriteBuilder(writeOptions: CaseInsensitiveStringMap,
                                     hoodieCatalogTable: HoodieCatalogTable,
                                     spark: SparkSession)
  extends SupportsTruncate with SupportsOverwrite with ProvidesHoodieConfig {

  private var forceOverwrite = false

  override def truncate(): HoodieV1WriteBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    forceOverwrite = true
    this
  }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation: InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val mode = if (forceOverwrite && hoodieCatalogTable.partitionFields.isEmpty) {
            // insert overwrite non-partition table
            SaveMode.Overwrite
          } else {
            // for insert into or insert overwrite partition we use append mode.
            SaveMode.Append
          }
          alignSchema(data, hoodieCatalogTable.tableSchema).write
            .format("org.apache.hudi")
            .mode(mode)
            .options(buildHoodieConfig(hoodieCatalogTable) ++
              buildHoodieInsertConfig(hoodieCatalogTable, spark, forceOverwrite, Map.empty, Map.empty))
            .save()
        }
      }
    }
  }
}

object HoodieInternalV2Table extends SparkAdapterSupport {

  def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection =
    sparkAdapter.createCatalystExpressionUtils().generateUnsafeProjection(from, to)

  private[catalog] def alignSchema(df: DataFrame, targetSchema: StructType): DataFrame = {
    // NOTE: This method makes sure that we write out the dataset in the schema that is persisted
    //       w/in the catalog.
    //       We're relying on Spark internal APIs to avoid de-/serialization
    //       penalty of converting [[InternalRow]] to [[Row]] and back
    // TODO validate schemas are compatible
    val unsafeProjection = generateUnsafeProjection(df.schema, targetSchema)
    val projectedRdd = HoodieUnsafeCatalystUtils.mapInternal(df.queryExecution.toRdd)(unsafeProjection)

    HoodieUnsafeCatalystUtils.createDataFrame(df.sparkSession, projectedRdd, targetSchema)
  }

}