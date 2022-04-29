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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object HoodieUnsafeRowUtils {

  /**
   * TODO scala-doc
   */
  def getNestedRowValue(row: InternalRow, nestedFieldPath: Array[(Int, StructField)]): Any = {
    var curRow = row
    for (idx <- 0 until nestedFieldPath.length - 1) {
      val (ord, f) = nestedFieldPath(idx)
      if (curRow.isNullAt(ord)) {
        // scalastyle:off return
        if (f.nullable) return null
        else throw new IllegalArgumentException(s"Found null value for the field that is declared as non-nullable: $f")
        // scalastyle:on return
      } else {
        curRow = f.dataType match {
          case st: StructType =>
            curRow.getStruct(ord, st.fields.length)
          case dt@_ =>
            throw new IllegalArgumentException(s"Invalid nested-field path: expected StructType, but was $dt")
        }
      }
    }
    // Fetch terminal node of the path
    val (ord, f) = nestedFieldPath.last
    curRow.get(ord, f.dataType)
  }

  /**
   * TODO scala-doc
   */
  def composeNestedFieldPath(schema: StructType, nestedFieldRef: String): Array[(Int, StructField)] = {
    val fieldRefParts = nestedFieldRef.split('.')
    val ordSeq = ArrayBuffer[(Int, StructField)]()
    var curSchema = schema
    for (idx <- fieldRefParts.indices) {
      val fieldRefPart = fieldRefParts(idx)
      val ord = curSchema.fieldIndex(fieldRefPart)
      val field = curSchema(ord)
      // Append current field's (ordinal, data-type)
      ordSeq.append((ord, field))
      // Update current schema, unless terminal field-ref part
      if (idx < fieldRefParts.length - 1) {
        curSchema = field.dataType match {
          case st: StructType => st
          case dt @ _ =>
            throw new IllegalArgumentException(s"Invalid nested field reference ${fieldRefParts.drop(idx).mkString(".")} into $dt")
        }
      }
    }

    ordSeq.toArray
  }
}
