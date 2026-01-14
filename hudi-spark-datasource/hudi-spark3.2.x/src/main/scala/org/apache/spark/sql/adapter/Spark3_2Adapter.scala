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

package org.apache.spark.sql.adapter

import org.apache.avro.Schema
import org.apache.hudi.Spark32HoodieFileScanRDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Command, DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.V2TableWithV1Fallback
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark32LegacyHoodieParquetFileFormat}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.analysis.TableValuedFunctions
import org.apache.spark.sql.parser.{HoodieExtendedParserInterface, HoodieSpark3_2ExtendedSqlParser}
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

/**
 * Implementation of [[SparkAdapter]] for Spark 3.2.x branch
 */
class Spark3_2Adapter extends BaseSpark3Adapter {
  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    super.resolveHoodieTable(plan).orElse {
      EliminateSubqueryAliases(plan) match {
        // First, we need to weed out unresolved plans
        case plan if !plan.resolved => None
        // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
        //       on top of it
        case PhysicalOperation(_, _, DataSourceV2Relation(v2: V2TableWithV1Fallback, _, _, _, _)) if isHoodieTable(v2.v1Table) =>
          Some(v2.v1Table)
        case _ => None
      }
    }
  }

  override def isColumnarBatchRow(r: InternalRow): Boolean = ColumnarUtils.isColumnarBatchRow(r)

  override def isTimestampNTZType(dataType: DataType): Boolean = {
    dataType.getClass.getSimpleName.startsWith("TimestampNTZType")
  }

  def createCatalystMetadataForMetaField: Metadata =
    new MetadataBuilder()
      .putBoolean(METADATA_COL_ATTR_KEY, value = true)
      .build()

  override def getCatalogUtils: HoodieSpark3CatalogUtils = HoodieSpark32CatalogUtils

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark32CatalystPlanUtils

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark32CatalystExpressionUtils

  override def getSchemaUtils: HoodieSchemaUtils = HoodieSpark32SchemaUtils

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils = HoodieSpark32PartitionedFileUtils

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark3_2AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark3_2AvroDeserializer(rootAvroType, rootCatalystType)

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    new HoodieSpark3_2ExtendedSqlParser(spark, delegate)

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean, tableAvroSchema: Schema): Option[ParquetFileFormat] = {
    Some(new Spark32LegacyHoodieParquetFileFormat(appendPartitionValues))
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark32HoodieFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def extractDeleteCondition(deleteFromTable: Command): Expression = {
    deleteFromTable.asInstanceOf[DeleteFromTable].condition.getOrElse(null)
  }

  override def injectTableFunctions(extensions: SparkSessionExtensions): Unit = {
    TableValuedFunctions.funcs.foreach(extensions.injectTableFunction)
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  override def convertStorageLevelToString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
    case DISK_ONLY_3 => "DISK_ONLY_3"
    case MEMORY_ONLY => "MEMORY_ONLY"
    case MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case OFF_HEAP => "OFF_HEAP"
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $level")
  }
}
