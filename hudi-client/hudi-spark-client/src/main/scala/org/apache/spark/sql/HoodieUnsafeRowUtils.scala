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
   * Fetches (nested) value w/in provided [[Row]] uniquely identified by the provided nested-field path
   * previously composed by [[composeNestedFieldPath]]
   */
  def getNestedRowValue(row: Row, nestedFieldPath: NestedFieldPath): Any = {
    var curRow = row
    for (idx <- nestedFieldPath.parts.indices) {
      val (ord, f) = nestedFieldPath.parts(idx)
      if (curRow.isNullAt(ord)) {
        // scalastyle:off return
        if (f.nullable) return null
        else throw new IllegalArgumentException(s"Found null value for the field that is declared as non-nullable: $f")
        // scalastyle:on return
      } else if (idx == nestedFieldPath.parts.length - 1) {
        // scalastyle:off return
        return curRow.get(ord)
        // scalastyle:on return
      } else {
        curRow = f.dataType match {
          case _: StructType =>
            curRow.getStruct(ord)
          case dt@_ =>
            throw new IllegalArgumentException(s"Invalid nested-field path: expected StructType, but was $dt")
        }
      }
    }
  }

  /**
   * Fetches (nested) value w/in provided [[InternalRow]] uniquely identified by the provided nested-field path
   * previously composed by [[composeNestedFieldPath]]
   */
  def getNestedInternalRowValue(row: InternalRow, nestedFieldPath: NestedFieldPath): Any = {
    if (nestedFieldPath.parts.length == 0) {
      throw new IllegalArgumentException("Nested field-path could not be empty")
    }

    var curRow = row
    var idx = 0
    while (idx < nestedFieldPath.parts.length) {
      val (ord, f) = nestedFieldPath.parts(idx)
      if (curRow.isNullAt(ord)) {
        // scalastyle:off return
        if (f.nullable) return null
        else throw new IllegalArgumentException(s"Found null value for the field that is declared as non-nullable: $f")
        // scalastyle:on return
      } else if (idx == nestedFieldPath.parts.length - 1) {
        // scalastyle:off return
        return curRow.get(ord, f.dataType)
        // scalastyle:on return
      } else {
        curRow = f.dataType match {
          case st: StructType =>
            curRow.getStruct(ord, st.fields.length)
          case dt@_ =>
            throw new IllegalArgumentException(s"Invalid nested-field path: expected StructType, but was $dt")
        }
      }
      idx += 1
    }
  }

  /**
   * For the provided [[nestedFieldRef]] (of the form "a.b.c") and [[schema]], produces nested-field path comprised
   * of (ordinal, data-type) tuples of the respective fields w/in the provided schema.
   *
   * This method produces nested-field path, that is subsequently used by [[getNestedInternalRowValue]], [[getNestedRowValue]]
   */
  def composeNestedFieldPath(schema: StructType, nestedFieldRef: String): NestedFieldPath = {
    val fieldRefParts = nestedFieldRef.split('.')
    val ordSeq = ArrayBuffer[(Int, StructField)]()
    var curSchema = schema
    var idx = 0
    while (idx < fieldRefParts.length) {
      val fieldRefPart = fieldRefParts(idx)
      val ord = curSchema.fieldIndex(fieldRefPart)
      val field = curSchema(ord)
      // Append current field's (ordinal, data-type)
      ordSeq.append((ord, field))
      // Update current schema, unless terminal field-ref part
      if (idx < fieldRefParts.length - 1) {
        curSchema = field.dataType match {
          case st: StructType => st
          case dt@_ =>
            throw new IllegalArgumentException(s"Invalid nested field reference ${fieldRefParts.drop(idx).mkString(".")} into $dt")
        }
      }
      idx += 1
    }

    NestedFieldPath(ordSeq.toArray)
  }

  case class NestedFieldPath(parts: Array[(Int, StructField)])
}
