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
import org.apache.hudi.common.util.ValidationUtils.checkState

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType

class HoodieBootstrapRDD(@transient spark: SparkSession,
                         bootstrapDataFileReader: BaseFileReader,
                         bootstrapSkeletonFileReader: BaseFileReader,
                         regularFileReader: BaseFileReader,
                         requiredSchema: HoodieTableSchema,
                         @transient splits: Seq[BaseHoodieBootstrapSplit])
  extends RDD[InternalRow](spark.sparkContext, Nil) {


  protected def getSkeletonIteratorSchema(dataFile: PartitionedFile, skeletonFile: PartitionedFile): (Iterator[InternalRow], StructType) = {
    if (bootstrapDataFileReader.schema.isEmpty) {
      // No data column to fetch, hence fetch only from skeleton file
      (bootstrapSkeletonFileReader.read(skeletonFile), bootstrapSkeletonFileReader.schema)
    } else if (bootstrapSkeletonFileReader.schema.isEmpty) {
      // No metadata column to fetch, hence fetch only from data file
      (bootstrapDataFileReader.read(dataFile), bootstrapDataFileReader.schema)
    } else {
      // Fetch from both data and skeleton file, and merge
      val dataFileIterator = bootstrapDataFileReader.read(dataFile)
      val skeletonFileIterator = bootstrapSkeletonFileReader.read(skeletonFile)
      val mergedSchema = StructType(bootstrapSkeletonFileReader.schema.fields ++ bootstrapDataFileReader.schema.fields)

      (merge(skeletonFileIterator, dataFileIterator), mergedSchema)
    }
  }

  /**
   *  Here we have to project the [[InternalRow]]s fetched into the expected target schema.
   *  These could diverge for ex, when requested schema contains partition columns which might not be
   *  persisted w/in the data file, but instead would be parsed from the partition path. In that case
   *  output of the file-reader will have different ordering of the fields than the original required
   *  schema (for more details please check out [[ParquetFileFormat]] implementation).
   */
  protected def unsafeProjectIterator(iterator: Iterator[InternalRow], schema: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(schema, requiredSchema.structTypeSchema)
    iterator.map(unsafeProjection)
  }

  protected def maybeLog(bootstrapPartition: HoodieBootstrapPartition): Unit = {
    if (log.isDebugEnabled) {
      var msg = "Got Split => Index: " + bootstrapPartition.index + ", Data File: " +
        bootstrapPartition.split.dataFile.filePath
      if (bootstrapPartition.split.skeletonFile.isDefined) {
        msg += ", Skeleton File: " + bootstrapPartition.split.skeletonFile.get.filePath
      }
      logDebug(msg)
    }
  }

  protected def getIterator(bootstrapPartition: HoodieBootstrapPartition): Iterator[InternalRow] = {
    bootstrapPartition.split.skeletonFile match {
      case Some(skeletonFile) =>
        // It is a bootstrap split. Check both skeleton and data files.
        val (iterator, schema) = getSkeletonIteratorSchema(bootstrapPartition.split.dataFile, skeletonFile)
        unsafeProjectIterator(iterator, schema)
      case _ =>
        // NOTE: Regular file-reader is already projected into the required schema
        regularFileReader.read(bootstrapPartition.split.dataFile)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[HoodieBootstrapPartition]
    maybeLog(bootstrapPartition)
    getIterator(bootstrapPartition)
  }

  def merge(skeletonFileIterator: Iterator[InternalRow], dataFileIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private val combinedRow = new JoinedRow()

      override def hasNext: Boolean = {
        checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
        dataFileIterator.hasNext && skeletonFileIterator.hasNext
      }

      override def next(): InternalRow = {
        combinedRow(skeletonFileIterator.next(), dataFileIterator.next())
      }
    }
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

case class HoodieBootstrapPartition(index: Int, split: BaseHoodieBootstrapSplit) extends Partition
