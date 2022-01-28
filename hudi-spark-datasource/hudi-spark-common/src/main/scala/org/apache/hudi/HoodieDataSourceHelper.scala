/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SpecificInternalRow, SubqueryExpression, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object HoodieDataSourceHelper extends PredicateHelper {

  /**
   * Partition the given condition into two sequence of conjunctive predicates:
   * - predicates that can be evaluated using metadata only.
   * - other predicates.
   */
  def splitPartitionAndDataPredicates(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition(
      isPredicateMetadataOnly(_, partitionColumns, spark))
  }

  /**
   * Check if condition can be evaluated using only metadata. In Delta, this means the condition
   * only references partition columns and involves no subquery.
   */
  def isPredicateMetadataOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    isPredicatePartitionColumnsOnly(condition, partitionColumns, spark) &&
        !containsSubquery(condition)
  }

  /**
   * Does the predicate only contains partition columns?
   */
  def isPredicatePartitionColumnsOnly(
      condition: Expression,
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }

  /**
   * Check if condition involves a subquery expression.
   */
  def containsSubquery(condition: Expression): Boolean = {
    SubqueryExpression.hasSubquery(condition)
  }

  /**
   * Wrapper `readFunction` to deal with [[ColumnarBatch]] when enable parquet vectorized reader.
   */
  def readParquetFile(
      file: PartitionedFile,
      readFunction: PartitionedFile => Iterator[Any]): Iterator[InternalRow] = {
    val fileIterator = readFunction(file)
    val rows = fileIterator.flatMap(_ match {
      case r: InternalRow => Seq(r)
      case b: ColumnarBatch => b.rowIterator().asScala
    })
    rows
  }

  /**
   * Extract the required schema from [[InternalRow]]
   */
  def extractRequiredSchema(
      iter: Iterator[InternalRow],
      requiredSchema: StructType,
      requiredFieldPos: Seq[Int]): Iterator[InternalRow] = {
    val unsafeProjection = UnsafeProjection.create(requiredSchema)
    val rows = iter.map { row =>
      unsafeProjection(createInternalRowWithSchema(row, requiredSchema, requiredFieldPos))
    }
    rows
  }

  /**
   * Convert [[InternalRow]] to [[SpecificInternalRow]].
   */
  def createInternalRowWithSchema(
      row: InternalRow,
      schema: StructType,
      positions: Seq[Int]): InternalRow = {
    val rowToReturn = new SpecificInternalRow(schema)
    var curIndex = 0
    schema.zip(positions).foreach { case (field, pos) =>
      val curField = if (row.isNullAt(pos)) {
        null
      } else {
        row.get(pos, field.dataType)
      }
      rowToReturn.update(curIndex, curField)
      curIndex += 1
    }
    rowToReturn
  }


  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    val filePath = file.getPath
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    (0L until file.getLen by maxSplitBytes).map { offset =>
      val remaining = file.getLen - offset
      val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
      PartitionedFile(partitionValues, filePath.toUri.toString, offset, size)
    }
  }

  def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile]): Seq[FilePartition] = {
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
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
}
