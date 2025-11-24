
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
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.{AvroConversionUtils, DefaultSource, Spark2HoodieFileScanRDD, Spark2RowSerDe}

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Command, DeleteFromTable, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark24LegacyHoodieParquetFileFormat}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.hudi.parser.HoodieSpark2ExtendedSqlParser
import org.apache.spark.sql.parser.HoodieExtendedParserInterface
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.convert.Wrappers.JConcurrentMapWrapper
import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of [[SparkAdapter]] for Spark 2.4.x
 */
class Spark2Adapter extends SparkAdapter {

  override def isColumnarBatchRow(r: InternalRow): Boolean = {
    // NOTE: In Spark 2.x there's no [[ColumnarBatchRow]], instead [[MutableColumnarRow]] is leveraged
    //       for vectorized reads
    r.isInstanceOf[MutableColumnarRow]
  }

  def createCatalystMetadataForMetaField: Metadata =
    // NOTE: Since [[METADATA_COL_ATTR_KEY]] flag is not available in Spark 2.x,
    //       we simply produce an empty [[Metadata]] instance
    new MetadataBuilder().build()

  private val cache = JConcurrentMapWrapper(
    new ConcurrentHashMap[ZoneId, DateFormatter](1))

  override def getCatalogUtils: HoodieCatalogUtils = {
    throw new UnsupportedOperationException("Catalog utilities are not supported in Spark 2.x");
  }

  override def isTimestampNTZType(dataType: DataType): Boolean = {
    dataType.getClass.getSimpleName.startsWith("TimestampNTZType")
  }

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark2CatalystPlanUtils

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark2CatalystExpressionUtils

  override def getSchemaUtils: HoodieSchemaUtils = HoodieSpark2SchemaUtils

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils = HoodieSpark2PartitionedFileUtils

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark2_4AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark2_4AvroDeserializer(rootAvroType, rootCatalystType)

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def createSparkRowSerDe(schema: StructType): SparkRowSerDe = {
    val encoder = getCatalystExpressionUtils.getEncoder(schema)
    new Spark2RowSerDe(encoder)
  }

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    new HoodieSpark2ExtendedSqlParser(spark, delegate)

  override def getSparkParsePartitionUtil: SparkParsePartitionUtil = Spark2ParsePartitionUtil

  override def getDateFormatter(tz: TimeZone): DateFormatter = {
    cache.getOrElseUpdate(tz.toZoneId, DateFormatter())
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   *
   * This is a copy of org.apache.spark.sql.execution.datasources.FilePartition#getFilePartitions from Spark 3.2.
   * And this will be called only in Spark 2.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark24LegacyHoodieParquetFileFormat(appendPartitionValues))
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    InterpretedPredicate.create(e)
  }

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: Schema,
                              globPaths: Array[StoragePath],
                              parameters: java.util.Map[String, String]): BaseRelation = {
    val dataSchema = Option(schema).map(AvroConversionUtils.convertAvroSchemaToStructType).orNull
    DefaultSource.createRelation(sqlContext, metaClient, dataSchema, globPaths, parameters.asScala.toMap)
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark2HoodieFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def extractDeleteCondition(deleteFromTable: Command): Expression = {
    deleteFromTable.asInstanceOf[DeleteFromTable].condition.getOrElse(null)
  }

  override def convertStorageLevelToString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
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

  /**
   * Spark2 doesn't support nestedPredicatePushdown,
   * so fail it if [[supportNestedPredicatePushdown]] is true here.
   */
  override def translateFilter(predicate: Expression,
                               supportNestedPredicatePushdown: Boolean = false): Option[Filter] = {
    if (supportNestedPredicatePushdown) {
      throw new UnsupportedOperationException("Nested predicate push down is not supported")
    }

    DataSourceStrategy.translateFilter(predicate)
  }

  override def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch = {
    val batch = new ColumnarBatch(vectors)
    batch.setNumRows(numRows)
    batch
  }
}
