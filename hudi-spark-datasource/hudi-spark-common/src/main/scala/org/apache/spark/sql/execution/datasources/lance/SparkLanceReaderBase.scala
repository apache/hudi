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
import org.apache.hudi.io.storage.HoodieLanceRecordIterator
import org.apache.hudi.storage.StorageConfiguration
import com.lancedb.lance.file.LanceFileReader
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow}
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
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
                    storageConf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {

    val filePath = file.filePath.toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {
      // Create Arrow allocator for reading
      val allocator = new RootAllocator(Long.MaxValue)

      try {
        // Open Lance file reader
        val lanceReader = LanceFileReader.open(filePath, allocator)

        // Get schema from Lance file
        val arrowSchema = lanceReader.schema()
        val fileSchema = LanceArrowUtils.fromArrowSchema(arrowSchema)

        // TODO: Initial schema evolution (schema-on-read)
        // Schema evolution: Filter to columns that currently exist in the file, as requested col may not be present
        val fileFieldNames = fileSchema.fieldNames.toSet
        val (existingFields, missingFields) = if (requiredSchema.nonEmpty) {
          requiredSchema.fields.partition(f => fileFieldNames.contains(f.name))
        } else {
          (Array.empty[StructField], Array.empty[StructField])
        }

        // Only request columns that exist in the file
        val columnNames: java.util.List[String] = if (existingFields.nonEmpty) {
          existingFields.map(_.name).toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Read data with column projection (filters not supported yet in lanceReader)
        val arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE)

        // Create schema for the data we're actually reading
        val readSchema = StructType(existingFields)

        // Create iterator using shared HoodieLanceRecordIterator
        val lanceIterator = new HoodieLanceRecordIterator(
          allocator,
          lanceReader,
          arrowReader,
          if (readSchema.nonEmpty) readSchema else fileSchema,
          filePath
        )

        // Register cleanup listener with Spark task context
        Option(TaskContext.get()).foreach(
          _.addTaskCompletionListener[Unit](_ => lanceIterator.close())
        )

        // Need to convert to scala iterator for proper reading
        val baseIter = lanceIterator.asScala

        // Schema evolution: Add NULL padding for missing columns
        val iterWithNulls = if (missingFields.nonEmpty) {
          // Create a row with NULLs for missing columns
          val nullRow = new GenericInternalRow(missingFields.length)
          for (i <- missingFields.indices) {
            nullRow.setNullAt(i)
          }

          // Reorder columns to match the requiredSchema order
          val fieldIndexMap = requiredSchema.fields.zipWithIndex.map { case (field, idx) =>
            field.name -> idx
          }.toMap

          val existingFieldIndices = existingFields.map(f => fieldIndexMap(f.name))
          val missingFieldIndices = missingFields.map(f => fieldIndexMap(f.name))

          baseIter.map { row =>
            // Create result row with correct ordering
            val resultRow = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(requiredSchema.length)

            // Fill in existing columns
            existingFieldIndices.zipWithIndex.foreach { case (targetIdx, sourceIdx) =>
              if (row.isNullAt(sourceIdx)) {
                resultRow.setNullAt(targetIdx)
              } else {
                resultRow.update(targetIdx, row.get(sourceIdx, existingFields(sourceIdx).dataType))
              }
            }

            // Fill in missing columns with NULL
            missingFieldIndices.foreach { targetIdx =>
              resultRow.setNullAt(targetIdx)
            }

            resultRow.asInstanceOf[InternalRow]
          }
        } else {
          baseIter.asInstanceOf[Iterator[InternalRow]]
        }

        // Handle partition columns
        if (partitionSchema.length == 0) {
          // No partition columns - return rows directly
          iterWithNulls
        } else {
          // Append partition values to each row using JoinedRow
          val joinedRow = new JoinedRow()
          iterWithNulls.map(row => joinedRow(row, file.partitionValues))
        }

      } catch {
        case e: Exception =>
          allocator.close()
          throw new IOException(s"Failed to read Lance file: $filePath", e)
      }
    }
  }
}
