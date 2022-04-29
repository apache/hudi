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

import org.apache.hudi.HoodieDataSourceHelper._

class HoodieBootstrapRDD(@transient spark: SparkSession,
                        dataReadFunction: PartitionedFile => Iterator[InternalRow],
                        skeletonReadFunction: PartitionedFile => Iterator[InternalRow],
                        regularReadFunction: PartitionedFile => Iterator[InternalRow],
                        dataSchema: StructType,
                        skeletonSchema: StructType,
                        requiredColumns: Array[String],
                        tableState: HoodieBootstrapTableState)
  extends RDD[InternalRow](spark.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[HoodieBootstrapPartition]

    if (log.isDebugEnabled) {
      if (bootstrapPartition.split.skeletonFile.isDefined) {
        logDebug("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
          + bootstrapPartition.split.dataFile.filePath + ", Skeleton File: "
          + bootstrapPartition.split.skeletonFile.get.filePath)
      } else {
        logDebug("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
          + bootstrapPartition.split.dataFile.filePath)
      }
    }

    var partitionedFileIterator: Iterator[InternalRow] = null

    if (bootstrapPartition.split.skeletonFile.isDefined) {
      // It is a bootstrap split. Check both skeleton and data files.
      if (dataSchema.isEmpty) {
        // No data column to fetch, hence fetch only from skeleton file
        partitionedFileIterator = skeletonReadFunction(bootstrapPartition.split.skeletonFile.get)
      } else if (skeletonSchema.isEmpty) {
        // No metadata column to fetch, hence fetch only from data file
        partitionedFileIterator = dataReadFunction(bootstrapPartition.split.dataFile)
      } else {
        // Fetch from both data and skeleton file, and merge
        val dataFileIterator = dataReadFunction(bootstrapPartition.split.dataFile)
        val skeletonFileIterator = skeletonReadFunction(bootstrapPartition.split.skeletonFile.get)
        partitionedFileIterator = merge(skeletonFileIterator, dataFileIterator)
      }
    } else {
      partitionedFileIterator = regularReadFunction(bootstrapPartition.split.dataFile)
    }
    partitionedFileIterator
  }

  def merge(skeletonFileIterator: Iterator[InternalRow], dataFileIterator: Iterator[InternalRow])
  : Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      override def hasNext: Boolean = dataFileIterator.hasNext && skeletonFileIterator.hasNext
      override def next(): InternalRow = {
        mergeInternalRow(skeletonFileIterator.next(), dataFileIterator.next())
      }
    }
  }

  def mergeInternalRow(skeletonRow: InternalRow, dataRow: InternalRow): InternalRow = {
    val skeletonArr  = skeletonRow.copy().toSeq(skeletonSchema)
    val dataArr = dataRow.copy().toSeq(dataSchema)
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

  override protected def getPartitions: Array[Partition] = {
    tableState.files.zipWithIndex.map(file => {
      if (file._1.skeletonFile.isDefined) {
        logDebug("Forming partition with => Index: " + file._2 + ", Files: " + file._1.dataFile.filePath
          + "," + file._1.skeletonFile.get.filePath)
        HoodieBootstrapPartition(file._2, file._1)
      } else {
        logDebug("Forming partition with => Index: " + file._2 + ", File: " + file._1.dataFile.filePath)
        HoodieBootstrapPartition(file._2, file._1)
      }
    }).toArray
  }
}

case class HoodieBootstrapPartition(index: Int, split: HoodieBootstrapSplit) extends Partition
