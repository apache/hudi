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

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import java.time.ZoneId
import java.util

class HoodieParquetReadSupport(
                                convertTz: Option[ZoneId],
                                enableVectorizedReader: Boolean,
                                datetimeRebaseSpec: RebaseSpec,
                                int96RebaseSpec: RebaseSpec)
  extends ParquetReadSupport(convertTz, enableVectorizedReader, datetimeRebaseSpec, int96RebaseSpec) {

  override def init(context: InitContext): ReadContext = {
    super.init(context)
    val conf = context.getConfiguration
    val catalystRequestedSchema = {
      val schemaString = conf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
      assert(schemaString != null, "Parquet requested schema not set.")
      StructType.fromString(schemaString)
    }
    val fileSchema = new ParquetToSparkSchemaConverter(conf).convert(context.getFileSchema)
    val trimmedSchema = trimSchema(catalystRequestedSchema, fileSchema)
    val parquetRequestedSchema = ParquetReadSupport.getRequestedSchema(
      context.getFileSchema, trimmedSchema, conf, enableVectorizedReader)
    new ReadContext(parquetRequestedSchema, new util.HashMap[String, String]())
  }

  /**
   * The requiredSchema may have columns that are not present in the actual data schema.
   * If all the requested fields of a nested record are not present in the data schema, then this field must be
   * excluded from the requiredSchema to prevent runtime errors.
   * @param requiredSchema the schema required by the query
   * @param dataSchema the current file's data schema
   * @return trimmed schema that can be safely applied to the data
   */
  private def trimSchema(requiredSchema: StructType, dataSchema: StructType): StructType = {
    trimSchemaInternal(requiredSchema, dataSchema).getOrElse(requiredSchema)
  }

  private def trimSchemaInternal(requiredSchema: StructType, dataSchema: StructType): Option[StructType] = {
    if (requiredSchema.equals(dataSchema)) {
      Some(requiredSchema)
    } else {
      var hasAtLeastOneMatch = false
      val fields = requiredSchema.fields.map(field => {
        val dataSchemaFieldOpt = dataSchema.find(_.name.equalsIgnoreCase(field.name))
        if (dataSchemaFieldOpt.isEmpty) {
          Some(field)
        } else {
          hasAtLeastOneMatch = true
          val dataSchemaField = dataSchemaFieldOpt.get
          field.dataType match {
            case structType: StructType =>
              val trimmed = trimSchemaInternal(structType, dataSchemaField.dataType.asInstanceOf[StructType])
              trimmed.map(structType => field.copy(dataType = structType))
            case arrayType: ArrayType =>
              val elementType = arrayType.elementType
              val dataElementType = dataSchemaField.dataType.asInstanceOf[ArrayType].elementType
              elementType match {
                case structElementType: StructType if dataElementType.isInstanceOf[StructType] =>
                  val trimmed = trimSchemaInternal(structElementType, dataElementType.asInstanceOf[StructType])
                  trimmed.map(structType => field.copy(dataType = ArrayType(structType, arrayType.containsNull)))
                case _ => Some(field)
              }
            case mapType: MapType =>
              val valueType = mapType.valueType
              val dataValueType = dataSchemaField.dataType.asInstanceOf[MapType].valueType
              valueType match {
                case structValueType: StructType if dataValueType.isInstanceOf[StructType] =>
                  val trimmed = trimSchemaInternal(structValueType, dataValueType.asInstanceOf[StructType])
                  trimmed.map(structType => field.copy(dataType = MapType(mapType.keyType, structType, mapType.valueContainsNull)))
                case _ => Some(field)
              }
            case _ => Some(field)
          }
        }
      }).filter(_.isDefined).map(_.get)
      if (hasAtLeastOneMatch && fields.length > 0) {
        Some(StructType(fields))
      } else {
        None
      }
    }
  }
}
