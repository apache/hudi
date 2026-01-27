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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.model.HoodieFileFormat

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType


/**
 * Generic schema evolution handler for different file formats.
 * Supports Parquet (default), and Lance currently.
 *
 * @param fileSchema The schema from the file
 * @param requiredSchema The schema requested by the query
 * @param sessionLocalTimeZone The session's local timezone
 * @param fileFormat The file format being read. Defaults to PARQUET for backward compatibility.
 */
class SparkBasicSchemaEvolution(fileSchema: StructType,
                                requiredSchema: StructType,
                                sessionLocalTimeZone: String,
                                fileFormat: HoodieFileFormat = HoodieFileFormat.PARQUET) {

  val (implicitTypeChangeInfo, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

  def getRequestSchema: StructType = {
    fileFormat match {
      case HoodieFileFormat.PARQUET =>
        if (implicitTypeChangeInfo.isEmpty) {
          requiredSchema
        } else {
          sparkRequestSchema
        }
      case HoodieFileFormat.LANCE =>
        // need to filter to only fields that exist in file for lance
        val fileFieldNames = fileSchema.fieldNames.toSet
        StructType(sparkRequestSchema.fields.filter(f => fileFieldNames.contains(f.name)))
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
  }

  def generateUnsafeProjection(): UnsafeProjection = {
    val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils

    fileFormat match {
      case HoodieFileFormat.PARQUET =>
        HoodieParquetFileFormatHelper.generateUnsafeProjection(
          schemaUtils.toAttributes(requiredSchema),
          Some(sessionLocalTimeZone),
          implicitTypeChangeInfo,
          requiredSchema,
          new StructType(),
          schemaUtils
        )
      case HoodieFileFormat.LANCE =>
        // requires null padding for missing columns
        val requestSchema = getRequestSchema
        HoodieParquetFileFormatHelper.generateUnsafeProjection(
          schemaUtils.toAttributes(requestSchema),
          Some(sessionLocalTimeZone),
          implicitTypeChangeInfo,
          requiredSchema,
          new StructType(),
          schemaUtils,
          padMissingColumns = true
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
  }
}
