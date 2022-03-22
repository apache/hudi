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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, SchemaColumnConvertNotSupportedException}
import org.apache.spark.{Partition, TaskContext}

case class HoodieBaseFileSplit(filePartition: FilePartition) extends HoodieFileSplit

/**
 * TODO eval if we actually need it
 */
class HoodieFileScanRDD(@transient private val sparkSession: SparkSession,
                        readFunction: PartitionedFile => Iterator[InternalRow],
                        @transient fileSplits: Seq[HoodieBaseFileSplit])
  extends HoodieUnsafeRDD(sparkSession.sparkContext) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[InternalRow] with AutoCloseable {
      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = _
      private[this] var currentIterator: Iterator[InternalRow] = _

      override def hasNext: Boolean = {
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }

      def next(): InternalRow = currentIterator.next()

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          logInfo(s"Reading File $currentFile")
          currentIterator = readFunction(currentFile)

          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              val message = "Parquet column cannot be converted in " +
                s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
                s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
              throw new QueryExecutionException(message, e)

            case e => throw e
          }
        } else {
          currentFile = null
          false
        }
      }

      override def close(): Unit = {}
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]]
  }

  override protected def getPartitions: Array[Partition] = fileSplits.map(_.filePartition).toArray
}
