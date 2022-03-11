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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

object HoodieDataSourceHelper extends PredicateHelper {


  /**
   * Wrapper `buildReaderWithPartitionValues` of [[ParquetFileFormat]]
   * to deal with [[ColumnarBatch]] when enable parquet vectorized reader if necessary.
   */
  def buildHoodieParquetReader(sparkSession: SparkSession,
                               dataSchema: StructType,
                               partitionSchema: StructType,
                               requiredSchema: StructType,
                               filters: Seq[Filter],
                               options: Map[String, String],
                               hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val readParquetFile: PartitionedFile => Iterator[Any] = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sparkSession,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )

    file: PartitionedFile => {
      val iter = readParquetFile(file)
      iter.flatMap {
        case r: InternalRow => Seq(r)
        case b: ColumnarBatch => b.rowIterator().asScala
      }
    }
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

}
