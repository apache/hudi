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

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * Helper for Lance file format schema evolution.
 */
object LanceFileFormatHelper {

  /**
   * Build schema change info for Lance files.
   *
   * @param fileStruct The schema from the Lance file
   * @param requiredSchema The schema requested by the query
   * @return A tuple of (type change map, required schema)
   */
  def buildSchemaChangeInfo(fileStruct: StructType,
                            requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val implicitTypeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]] = new java.util.HashMap()

    // Create a map of field names to types from the file schema
    val fileStructMap = fileStruct.fields.map(f => (f.name, f.dataType)).toMap

    // Build the request schema, using FILE types for fields with type changes
    val sparkRequestStructFields = requiredSchema.map { f =>
      val requiredType = f.dataType
      if (fileStructMap.contains(f.name) && !isDataTypeEqual(requiredType, fileStructMap(f.name))) {
        val fileType = fileStructMap(f.name)
        // Record the type change: (required type, file type)
        implicitTypeChangeInfo.put(new Integer(requiredSchema.fieldIndex(f.name)),
                          org.apache.hudi.common.util.collection.Pair.of(requiredType, fileType))
        // Return field with FILE type so Lance reads data in its original type
        StructField(f.name, fileType, f.nullable)
      } else {
        f
      }
    }

    (implicitTypeChangeInfo, StructType(sparkRequestStructFields))
  }

  /**
   * Check if two data types are equal.
   * For basic type promotion, we simply check equality.
   *
   * @param requiredType The required data type
   * @param fileType The data type from the file
   * @return true if the types are equal, false otherwise
   */
  def isDataTypeEqual(requiredType: DataType, fileType: DataType): Boolean = {
    //TODO to revisit this for further cases (illegal cast, nested types, etc)
    requiredType == fileType
  }

  /**
   * Cast expression from source type to destination type.
   * Handles special cases that require intermediate String cast for precision.
   *
   * @param expr The expression to cast
   * @param srcType The source data type
   * @param dstType The destination data type
   * @param timeZoneId Optional timezone ID for time-aware casts
   * @return The cast expression
   */
  private def castPrimitiveExpression(expr: Expression,
                                      srcType: DataType,
                                      dstType: DataType,
                                      timeZoneId: Option[String]): Expression = {
    lazy val needTimeZone = Cast.needsTimeZone(srcType, dstType)
    (srcType, dstType) match {
      case (FloatType, DoubleType) =>
        // Float → String → Double for precision
        val toStr = Cast(expr, StringType, if (needTimeZone) timeZoneId else None)
        Cast(toStr, dstType, if (needTimeZone) timeZoneId else None)

      case (IntegerType | LongType | FloatType | DoubleType, dec: DecimalType) =>
        // Numeric → String → Decimal
        val toStr = Cast(expr, StringType, if (needTimeZone) timeZoneId else None)
        Cast(toStr, dec, if (needTimeZone) timeZoneId else None)

      case (StringType, dec: DecimalType) =>
        Cast(expr, dec, if (needTimeZone) timeZoneId else None)

      case (StringType, DateType) =>
        Cast(expr, DateType, if (needTimeZone) timeZoneId else None)

      case _ =>
        // Standard cast for all other primitive types (Int→Long, Long→Float, etc.)
        Cast(expr, dstType, if (needTimeZone) timeZoneId else None)
    }
  }

  /**
   * Generate unsafe projection for schema evolution.
   *
   * @param inputAttributes The attributes we read from the file (only existing fields)
   * @param requiredSchema The required schema (all fields including missing ones)
   * @param partitionSchema The partition schema
   * @param schemaUtils Schema utilities
   * @param typeChangeInfo Map of field index to (required type, file type) pairs
   * @return UnsafeProjection to transform rows
   */
  def generateUnsafeProjection(inputAttributes: Seq[Attribute],
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               schemaUtils: HoodieSchemaUtils,
                               typeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]]): UnsafeProjection = {

    val inputFieldMap = inputAttributes.map(a => a.name -> a).toMap

    val expressions = requiredSchema.fields.zipWithIndex.map { case (field, i) =>
      inputFieldMap.get(field.name) match {
        case Some(attr) =>
          if (typeChangeInfo.containsKey(i)) {
            // Type changed - apply appropriate cast
            val srcType = typeChangeInfo.get(i).getRight  // file type
            val dstType = typeChangeInfo.get(i).getLeft   // required type
            castPrimitiveExpression(attr, srcType, dstType, None)
          } else {
            // No type change
            attr
          }
        case None =>
          // Field missing from file, use NULL
          Literal(null, field.dataType)
      }
    }
    GenerateUnsafeProjection.generate(expressions, inputAttributes)
  }
}
