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
                       dataReadFunction: PartitionedFile => Iterator[InternalRow],
                       skeletonReadFunction: PartitionedFile => Iterator[InternalRow],
                       dataSchema: StructType,
                       skeletonSchema: StructType,
                       tableState: HudiBootstrapTableState)
  extends RDD[InternalRow](spark.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[HudiBootstrapPartition]

    logInfo("Got Split => " + bootstrapPartition.index + ","
      + bootstrapPartition.split.dataFile.filePath + ","
      + bootstrapPartition.split.skeletonFile.filePath)

    val dataFileIterator = read(bootstrapPartition.split.dataFile, dataReadFunction)
    val skeletonFileIterator = read(bootstrapPartition.split.skeletonFile, skeletonReadFunction)
    merge(dataFileIterator, skeletonFileIterator)
  }

  def merge(left: Iterator[InternalRow], right: Iterator[InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      override def hasNext: Boolean = left.hasNext && right.hasNext

      override def next(): InternalRow = {
        val leftArr  = left.next().toSeq(dataSchema)
        val rightArr = right.next().toSeq(skeletonSchema)
        val merged  = leftArr ++ rightArr
        val mergedRow = InternalRow.fromSeq(merged)
        mergedRow
      }
    }
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
    var conf = spark.sessionState.newHadoopConf()
    conf = spark.sessionState.newHadoopConfWithOptions(Map.empty)
    conf = spark.sparkContext.hadoopConfiguration

    tableState.files.zipWithIndex.map(file => {
      logInfo("Forming partition with => " + file._2 + "," + file._1.dataFile.filePath
        + "," + file._1.skeletonFile.filePath)
      HudiBootstrapPartition(file._2, file._1)
    }).toArray
  }
}

case class HudiBootstrapPartition(index: Int, split: HudiBootstrapSplit) extends Partition
