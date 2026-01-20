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

import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType

/**
 * Schema evolution support for Lance file format
 *
 * @param fileSchema The schema from the Lance file
 * @param requiredSchema The schema requested by the query
 * @param sessionLocalTimeZone The session's local timezone
 */
class LanceBasicSchemaEvolution(fileSchema: StructType,
                                requiredSchema: StructType,
                                sessionLocalTimeZone: String) {

  val (implicitTypeChangeInfo, requestSchema) = LanceFileFormatHelper.buildSchemaChangeInfo(fileSchema, requiredSchema)

  /**
   * Get the schema to use when reading the Lance file.
   * Returns only fields that exist in the file.
   *
   * @return The schema to request from Lance reader (only existing fields)
   */
  def getRequestSchema: StructType = {
    // For Lance, we can only request fields that exist in the file
    val fileFieldNames = fileSchema.fieldNames.toSet
    StructType(requestSchema.fields.filter(f => fileFieldNames.contains(f.name)))
  }

  /**
   * Generate unsafe projection to transform rows from file schema to required schema.
   * Handles NULL padding for missing columns and column reordering.
   *
   * @return UnsafeProjection to apply to each row
   */
  def generateUnsafeProjection(): UnsafeProjection = {
    val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils
    val requestSchema = getRequestSchema

    LanceFileFormatHelper.generateUnsafeProjection(
      schemaUtils.toAttributes(requestSchema),  // what we read (existing fields)
      requiredSchema,                            // what we need (all fields)
      new StructType(),
      schemaUtils,
      implicitTypeChangeInfo
    )
  }
}
