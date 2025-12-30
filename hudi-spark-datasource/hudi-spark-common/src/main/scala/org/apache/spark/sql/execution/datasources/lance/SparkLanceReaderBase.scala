/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.lance

import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.{LanceRecordIterator, HoodieSparkLanceReader}
import org.apache.hudi.storage.StorageConfiguration

import com.lancedb.lance.file.LanceFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.LanceArrowUtils

import java.io.IOException

import scala.collection.JavaConverters._

/**
 * Reader for Lance files in Spark datasource.
 * Implements vectorized reading using LanceArrowColumnVector.
 *
 * @param enableVectorizedReader whether to use vectorized reading (currently always true for Lance)
 */
class SparkLanceReaderBase(enableVectorizedReader: Boolean) extends SparkColumnarFileReader {

  // Batch size for reading Lance files (number of rows per batch)
  private val DEFAULT_BATCH_SIZE = 512

  /**
   * Read a Lance file with schema projection and partition column support.
   *
   * @param file              Lance file to read
   * @param requiredSchema    desired output schema of the data (columns to read)
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read (not currently used for Lance)
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile,
                    requiredSchema: StructType,
                    partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema],
                    filters: Seq[Filter],
                    storageConf: StorageConfiguration[Configuration],
                    tableSchemaOpt: util.Option[MessageType] = util.Option.empty()): Iterator[InternalRow] = {

    val filePath = file.filePath.toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {
      // Create child allocator for reading
      val allocator = HoodieArrowAllocator.newChildAllocator(getClass.getSimpleName + "-data-" + filePath,
        HoodieSparkLanceReader.LANCE_DATA_ALLOCATOR_SIZE);

      try {
        // Open Lance file reader
        val lanceReader = LanceFileReader.open(filePath, allocator)

        // Extract column names from required schema for projection
        val columnNames: java.util.List[String] = if (requiredSchema.nonEmpty) {
          requiredSchema.fields.map(_.name).toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Read data with column projection (filters not supported yet)
        val arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE)

        val schemaForIterator = if (requiredSchema.nonEmpty) {
          requiredSchema
        } else {
          // Only compute schema from Lance file when requiredSchema is empty
          val arrowSchema = lanceReader.schema()
          LanceArrowUtils.fromArrowSchema(arrowSchema)
        }

        // Create iterator using shared LanceRecordIterator
        val lanceIterator = new LanceRecordIterator(
          allocator,
          lanceReader,
          arrowReader,
          schemaForIterator,
          filePath
        )

        // Register cleanup listener with Spark task context
        Option(TaskContext.get()).foreach(
          _.addTaskCompletionListener[Unit](_ => lanceIterator.close())
        )

        // Need to convert to scala iterator for proper reading
        val iter = lanceIterator.asScala

        // Handle partition columns
        if (partitionSchema.length == 0) {
          // No partition columns - return rows directly
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          // Append partition values to each row using JoinedRow
          val joinedRow = new JoinedRow()
          iter.map(row => joinedRow(row, file.partitionValues))
        }

      } catch {
        case e: Exception =>
          allocator.close()
          throw new IOException(s"Failed to read Lance file: $filePath", e)
      }
    }
  }
}
