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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

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
    // TODO type evolution, for now just return empty map and required schema
    val typeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]] = new java.util.HashMap()
    (typeChangeInfo, requiredSchema)
  }

  /**
   * Generate unsafe projection for schema evolution.
   *
   * @param inputAttributes The attributes we read from the file (only existing fields)
   * @param requiredSchema The required schema (all fields including missing ones)
   * @param partitionSchema The partition schema
   * @param schemaUtils Schema utilities
   * @return UnsafeProjection to transform rows
   */
  def generateUnsafeProjection(inputAttributes: Seq[Attribute],
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               schemaUtils: HoodieSchemaUtils): UnsafeProjection = {
    // Build expression for each field in required schema
    val inputFieldMap = inputAttributes.map(a => a.name -> a).toMap

    val expressions = requiredSchema.fields.map { field =>
      inputFieldMap.get(field.name) match {
        case Some(attr) =>
          // Field exists in input, reference it
          attr
        case None =>
          // Field missing from file, use NULL
          Literal(null, field.dataType)
      }
    }
    GenerateUnsafeProjection.generate(expressions, inputAttributes)
  }
}
