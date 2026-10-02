/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, InterpretedUnsafeProjection, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.types.StructType

/**
 * Turns the rows a file group reader returns into UnsafeRows of the scan's output schema, with the
 * file's partition values appended.
 *
 * Spark copies every row of a Parquet-based row scan into an UnsafeRow of its own while
 * spark.sql.parquet.enableVectorizedReader is on (FileSourceScanExec.needsUnsafeRowConversion), and
 * generating an UnsafeProjection for a wide nested schema costs milliseconds per file. So when the
 * reader already returns UnsafeRows laid out as the output, these pass through, or get the partition
 * values appended by a row joiner, whose generated code only covers the top-level fields. The full
 * projection is generated only once a row needs it. Every row returned is an UnsafeRow either way,
 * so the output does not depend on Spark's copy.
 */
object FileGroupOutputProjection {

  /**
   * @param inputSchema     schema of the rows the reader returns
   * @param partitionSchema schema of the partition values to append, empty for none
   * @param partitionValues partition values of the file, laid out as `partitionSchema`
   * @param outputSchema    schema of the scan's output
   * @param projection      projects `inputSchema` followed by `partitionSchema` onto `outputSchema`,
   *                        by name; generated only for a row that needs it
   */
  def create(inputSchema: StructType,
             partitionSchema: StructType,
             partitionValues: InternalRow,
             outputSchema: StructType,
             projection: => UnsafeProjection): InternalRow => InternalRow = {
    lazy val fullProjection = projection
    val project: InternalRow => InternalRow = if (partitionSchema.isEmpty) {
      row => fullProjection(row)
    } else {
      val joinedRow = new JoinedRow()
      row => fullProjection(joinedRow(row, partitionValues))
    }
    val numInputFields = inputSchema.length
    if (!sameLayout(inputSchema, partitionSchema, outputSchema)) {
      project
    } else if (partitionSchema.isEmpty) {
      {
        case unsafe: UnsafeRow if unsafe.numFields == numInputFields => unsafe
        case row => project(row)
      }
    } else {
      val joiner = GenerateUnsafeRowJoiner.create(inputSchema, partitionSchema)
      val partitionFields = partitionSchema.fields.zipWithIndex.map { case (f, i) => BoundReference(i, f.dataType, f.nullable) }
      val unsafePartitionValues = InterpretedUnsafeProjection.createProjection(partitionFields.toSeq)(partitionValues).copy()

      {
        case unsafe: UnsafeRow if unsafe.numFields == numInputFields => joiner.join(unsafe, unsafePartitionValues)
        case row => project(row)
      }
    }
  }

  /**
   * Whether the output is the input followed by the partition values, field by field, so that an
   * input UnsafeRow only needs the partition values appended.
   */
  private def sameLayout(inputSchema: StructType, partitionSchema: StructType, outputSchema: StructType): Boolean = {
    val fields = inputSchema.fields ++ partitionSchema.fields
    fields.length == outputSchema.length && fields.zip(outputSchema.fields).forall {
      case (in, out) => in.name == out.name && in.dataType == out.dataType
    }
  }
}
