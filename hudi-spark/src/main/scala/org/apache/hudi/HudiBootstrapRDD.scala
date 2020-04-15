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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class HudiBootstrapRDD(@transient spark: SparkSession,
                       dataReadFunction: PartitionedFile => Iterator[Any],
                       skeletonReadFunction: PartitionedFile => Iterator[Any],
                       regularReadFunction: PartitionedFile => Iterator[Any],
                       dataSchema: StructType,
                       skeletonSchema: StructType,
                       requiredColumns: Array[String],
                       tableState: HudiBootstrapTableState)
  extends RDD[InternalRow](spark.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[HudiBootstrapPartition]

    if (bootstrapPartition.split.skeletonFile.isDefined) {
      logInfo("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
        + bootstrapPartition.split.dataFile.filePath + ", Skeleton File: "
        + bootstrapPartition.split.skeletonFile.get.filePath)
    } else {
      logInfo("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
        + bootstrapPartition.split.dataFile.filePath)
    }

    var partitionedFileIterator: Iterator[Any] = null

    if (bootstrapPartition.split.skeletonFile.isDefined) {
      val dataFileIterator = dataReadFunction(bootstrapPartition.split.dataFile)
      val skeletonFileIterator = skeletonReadFunction(bootstrapPartition.split.skeletonFile.get)
      partitionedFileIterator = merge(skeletonFileIterator, dataFileIterator)
    } else {
      partitionedFileIterator = regularReadFunction(bootstrapPartition.split.dataFile)
    }

    import scala.collection.JavaConverters._
    val rows = partitionedFileIterator.flatMap(_ match {
      case r: InternalRow => Seq(r)
      case b: ColumnarBatch => b.rowIterator().asScala
    })
    rows
  }

  def merge(skeletonFileIterator: Iterator[Any], dataFileIterator: Iterator[Any]): Iterator[Any] = {
    new Iterator[Any] {
      override def hasNext: Boolean = skeletonFileIterator.hasNext && dataFileIterator.hasNext

      override def next(): Any = {
        val skeletonEntity = skeletonFileIterator.next()
        val dataEntity = dataFileIterator.next()

        (skeletonEntity, dataEntity) match {
          case (skeleton: ColumnarBatch, data: ColumnarBatch) => {
            mergeColumnarBatch(skeleton, data)
          }
          case (skeleton: InternalRow, data: InternalRow) => {
            mergeInternalRow(skeleton, data)
          }
        }
      }
    }
  }

  def mergeColumnarBatch(skeletonBatch: ColumnarBatch, dataBatch: ColumnarBatch): ColumnarBatch = {
    val mergedColumnVectors = requiredColumns.map(col => {
      if (skeletonSchema.fieldNames.contains(col)) {
        val idx = skeletonSchema.fieldIndex(col)
        skeletonBatch.column(idx)
      } else {
        val idx = dataSchema.fieldIndex(col)
        dataBatch.column(idx)
      }
    })

    val mergedBatch = new ColumnarBatch(mergedColumnVectors)
    mergedBatch.setNumRows(dataBatch.numRows())
    mergedBatch
  }

  def mergeInternalRow(skeletonRow: InternalRow, dataRow: InternalRow): InternalRow = {
    val skeletonArr  = skeletonRow.toSeq(skeletonSchema)
    val dataArr = dataRow.toSeq(dataSchema)
    // We need to return it in the order requested
    val mergedArr = requiredColumns.map(col => {
      if (skeletonSchema.fieldNames.contains(col)) {
        val idx = skeletonSchema.fieldIndex(col)
        skeletonArr(idx)
      } else {
        val idx = dataSchema.fieldIndex(col)
        dataArr(idx)
      }
    })

    logDebug("Merged data and skeleton values => " + mergedArr.mkString(","))
    val mergedRow = InternalRow.fromSeq(mergedArr)
    mergedRow
  }

  def read(partitionedFile: PartitionedFile, readFileFunction: PartitionedFile => Iterator[Any])
    : Iterator[InternalRow] = {
    val fileIterator = readFileFunction(partitionedFile)

    import scala.collection.JavaConverters._

    val rows = fileIterator.flatMap(_ match {
      case r: InternalRow => Seq(r)
      case b: ColumnarBatch => b.rowIterator().asScala
    })
    rows
  }

  override protected def getPartitions: Array[Partition] = {
    logInfo("Getting partitions..")

    tableState.files.zipWithIndex.map(file => {
      if (file._1.skeletonFile.isDefined) {
        logInfo("Forming partition with => " + file._2 + "," + file._1.dataFile.filePath
          + "," + file._1.skeletonFile.get.filePath)
        HudiBootstrapPartition(file._2, file._1)
      } else {
        logInfo("Forming partition with => " + file._2 + "," + file._1.dataFile.filePath)
        HudiBootstrapPartition(file._2, file._1)
      }
    }).toArray
  }
}

case class HudiBootstrapPartition(index: Int, split: HudiBootstrapSplit) extends Partition
