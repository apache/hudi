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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}


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
  
  val (implicitTypeChangeInfo, sparkRequestSchema) = SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

  /**
   * Recursively filters requested schema to only include fields that exist in file schema.
   */
  private def filterSchemaByFileSchema(requestedSchema: StructType, fileSchema: StructType): StructType = {
    val fileFieldMap = fileSchema.fields.map(f => f.name -> f).toMap

    val filteredFields = requestedSchema.fields.flatMap { requestedField =>
      fileFieldMap.get(requestedField.name).map { fileField =>
        val filteredDataType = filterDataType(requestedField.dataType, fileField.dataType)
        StructField(requestedField.name, filteredDataType, requestedField.nullable, requestedField.metadata)
      }
    }

    StructType(filteredFields)
  }

  /**
   * Recursively filters data types to only include nested fields that exist in file.
   */
  private def filterDataType(requestedType: DataType, fileType: DataType): DataType = (requestedType, fileType) match {
    case (requestedType, fileType) if requestedType == fileType => fileType

    case (ArrayType(requestedElement, containsNull), ArrayType(fileElement, _)) =>
      ArrayType(filterDataType(requestedElement, fileElement), containsNull)

    case (MapType(requestedKey, requestedValue, valueContainsNull), MapType(fileKey, fileValue, _)) =>
      MapType(
        filterDataType(requestedKey, fileKey),
        filterDataType(requestedValue, fileValue),
        valueContainsNull
      )

    case (StructType(requestedFields), StructType(fileFields)) =>
      val fileFieldMap = fileFields.map(f => f.name -> f).toMap
      val filteredFields = requestedFields.flatMap { requestedField =>
        fileFieldMap.get(requestedField.name).map { fileField =>
          StructField(
            requestedField.name,
            filterDataType(requestedField.dataType, fileField.dataType),
            requestedField.nullable,
            requestedField.metadata
          )
        }
      }
      StructType(filteredFields)

    case _ => requestedType
  }

  def getRequestSchema: StructType = {
    fileFormat match {
      case HoodieFileFormat.PARQUET =>
        if (implicitTypeChangeInfo.isEmpty) {
          requiredSchema
        } else {
          sparkRequestSchema
        }
      case HoodieFileFormat.LANCE =>
        val filtered = filterSchemaByFileSchema(sparkRequestSchema, fileSchema)
        filtered
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
  }

  def generateUnsafeProjection(): UnsafeProjection = {
    val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils

    fileFormat match {
      case HoodieFileFormat.PARQUET =>
        SparkSchemaTransformUtils.generateUnsafeProjection(
          schemaUtils.toAttributes(requiredSchema),
          Some(sessionLocalTimeZone),
          implicitTypeChangeInfo,
          requiredSchema,
          new StructType(),
          schemaUtils
        )
      case HoodieFileFormat.LANCE =>
        // Lance requires NULL padding for missing columns and can only read columns present in file
        val requestSchema = getRequestSchema
        // create a projection which will just handle null padding logic only
        val paddingProj = SparkSchemaTransformUtils.generateNullPaddingProjection(
          requestSchema,
          requiredSchema
        )

        // leverage existing generateUnsafeProjection which handles casting logic
        val castProj = SparkSchemaTransformUtils.generateUnsafeProjection(
          schemaUtils.toAttributes(requiredSchema),
          Some(sessionLocalTimeZone),
          implicitTypeChangeInfo,
          requiredSchema,
          new StructType(),
          schemaUtils
        )

        // Finally unify projections through chaining with padding first and then casting
        new UnsafeProjection {
          def apply(row: InternalRow): UnsafeRow = castProj(paddingProj(row))
        }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
  }
}
