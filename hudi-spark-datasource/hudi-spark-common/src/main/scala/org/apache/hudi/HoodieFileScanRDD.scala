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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, SchemaColumnConvertNotSupportedException}
import org.apache.spark.sql.types.StructType

/**
 * Similar to [[org.apache.spark.sql.execution.datasources.FileScanRDD]].
 *
 * This class will extract the fields needed according to [[requiredColumns]] and
 * return iterator of [[org.apache.spark.sql.Row]] directly.
 */
class HoodieFileScanRDD(
    @transient private val sparkSession: SparkSession,
    requiredColumns: Array[String],
    schema: StructType,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  private val requiredSchema = {
    val nameToStructField = schema.map(field => (field.name, field)).toMap
    StructType(requiredColumns.map(nameToStructField))
  }

  private val requiredFieldPos = HoodieSparkUtils.collectFieldIndexes(requiredSchema, schema)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val iterator = new Iterator[Object] with AutoCloseable {

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      override def hasNext: Boolean = {
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }

      def next(): Object = {
        currentIterator.next()
      }

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

    // extract required columns from row
    val iterAfterExtract = HoodieDataSourceHelper.extractRequiredSchema(
      iterator.asInstanceOf[Iterator[InternalRow]],
      requiredSchema,
      requiredFieldPos)

    // convert InternalRow to Row and return
    val converter = CatalystTypeConverters.createToScalaConverter(requiredSchema)
    iterAfterExtract.map(converter(_).asInstanceOf[Row])
  }

  override protected def getPartitions: Array[Partition] = filePartitions.toArray

}
