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

import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

class HoodieBootstrapRDD(@transient spark: SparkSession,
                         bootstrapDataFileReader: BaseFileReader,
                         bootstrapSkeletonFileReader: BaseFileReader,
                         nativeFileReader: BaseFileReader,
                         requiredColumns: Array[String],
                         @transient splits: Seq[HoodieBootstrapSplit])
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

    bootstrapPartition.split.skeletonFile match {
      case Some(skeletonFile) =>
        // It is a bootstrap split. Check both skeleton and data files.
        if (bootstrapDataFileReader.schema.isEmpty) {
          // No data column to fetch, hence fetch only from skeleton file
          bootstrapSkeletonFileReader.read(skeletonFile)
        } else if (bootstrapSkeletonFileReader.schema.isEmpty) {
          // No metadata column to fetch, hence fetch only from data file
          bootstrapDataFileReader.read(bootstrapPartition.split.dataFile)
        } else {
          // Fetch from both data and skeleton file, and merge
          val dataFileIterator = bootstrapDataFileReader.read(bootstrapPartition.split.dataFile)
          val skeletonFileIterator = bootstrapSkeletonFileReader.read(skeletonFile)
          merge(skeletonFileIterator, dataFileIterator)
        }

      case _ =>
        nativeFileReader.read(bootstrapPartition.split.dataFile)
    }
  }

  def merge(skeletonFileIterator: Iterator[InternalRow], dataFileIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      override def hasNext: Boolean = dataFileIterator.hasNext && skeletonFileIterator.hasNext
      override def next(): InternalRow = {
        mergeInternalRow(skeletonFileIterator.next(), dataFileIterator.next())
      }
    }
  }

  // TODO revisit
  def mergeInternalRow(skeletonRow: InternalRow, dataRow: InternalRow): InternalRow = {
    val skeletonArr  = skeletonRow.copy().toSeq(bootstrapSkeletonFileReader.schema)
    val dataArr = dataRow.copy().toSeq(bootstrapDataFileReader.schema)
    // We need to return it in the order requested
    val mergedArr = requiredColumns.map(col => {
      if (bootstrapSkeletonFileReader.schema.fieldNames.contains(col)) {
        val idx = bootstrapSkeletonFileReader.schema.fieldIndex(col)
        skeletonArr(idx)
      } else {
        val idx = bootstrapDataFileReader.schema.fieldIndex(col)
        dataArr(idx)
      }
    })

    logDebug("Merged data and skeleton values => " + mergedArr.mkString(","))
    val mergedRow = InternalRow.fromSeq(mergedArr)
    mergedRow
  }

  override protected def getPartitions: Array[Partition] = {
    splits.zipWithIndex.map(file => {
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
