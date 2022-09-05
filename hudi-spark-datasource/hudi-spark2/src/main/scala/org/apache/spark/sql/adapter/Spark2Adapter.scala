
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
import org.apache.hudi.{Spark2HoodieFileScanRDD, Spark2RowSerDe}
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Command, InsertIntoTable, Join, LogicalPlan, DeleteFromTable}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark24HoodieParquetFileFormat}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile, Spark2ParsePartitionUtil, SparkParsePartitionUtil}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.hudi.parser.HoodieSpark2ExtendedSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, HoodieCatalystPlansUtils, HoodieSpark2CatalystExpressionUtils, HoodieSpark2CatalystPlanUtils, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of [[SparkAdapter]] for Spark 2.4.x
 */
class Spark2Adapter extends SparkAdapter {

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark2CatalystExpressionUtils

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark2CatalystPlanUtils

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark2_4AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark2_4AvroDeserializer(rootAvroType, rootCatalystType)

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark2RowSerDe(encoder)
  }

  override def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = {
    Some(
      (spark: SparkSession, delegate: ParserInterface) => new HoodieSpark2ExtendedSqlParser(spark, delegate)
    )
  }

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = new Spark2ParsePartitionUtil

  override def parseMultipartIdentifier(parser: ParserInterface, sqlText: String): Seq[String] = {
    throw new IllegalStateException(s"Should not call ParserInterface#parseMultipartIdentifier for spark2")
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

  override def createHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark24HoodieParquetFileFormat(appendPartitionValues))
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    InterpretedPredicate.create(e)
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark2HoodieFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def resolveDeleteFromTable(deleteFromTable: Command,
                                      resolveExpression: Expression => Expression): DeleteFromTable = {
    val deleteFromTableCommand = deleteFromTable.asInstanceOf[DeleteFromTable]
    val resolvedCondition = deleteFromTableCommand.condition.map(resolveExpression)
    DeleteFromTable(deleteFromTableCommand.table, resolvedCondition)
  }

  override def extractDeleteCondition(deleteFromTable: Command): Expression = {
    deleteFromTable.asInstanceOf[DeleteFromTable].condition.getOrElse(null)
  }

  override def getQueryParserFromExtendedSqlParser(session: SparkSession, delegate: ParserInterface,
                                                   sqlText: String): LogicalPlan = {
    throw new UnsupportedOperationException(s"Unsupported parseQuery method in Spark earlier than Spark 3.3.0")
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
}
