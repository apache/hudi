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

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.internal.schema.HoodieSchemaException
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.JavaConverters._

/**
 * Utilities for converting between HoodieSchema and Spark SQL schemas.
 *
 * This object provides high-level conversion methods with utilities for
 * handling defaults and nullability alignment.
 */
object HoodieSchemaConversionUtils {

  /**
   * Converts HoodieSchema to Catalyst's StructType.
   *
   * @param hoodieSchema HoodieSchema to convert
   * @return Spark StructType corresponding to the HoodieSchema
   * @throws HoodieSchemaException if conversion fails
   */
  def convertHoodieSchemaToStructType(hoodieSchema: HoodieSchema): StructType = {
    try {
      HoodieSparkSchemaConverters.toSqlType(hoodieSchema) match {
        case (dataType, _) => dataType.asInstanceOf[StructType]
      }
    } catch {
      case e: Exception => throw new HoodieSchemaException(
        s"Failed to convert HoodieSchema to StructType: $hoodieSchema", e)
    }
  }

  /**
   * Converts HoodieSchema to Catalyst's DataType (general purpose, not just StructType).
   *
   * @param hoodieSchema HoodieSchema to convert
   * @return Spark DataType corresponding to the HoodieSchema
   * @throws HoodieSchemaException if conversion fails
   */
  def convertHoodieSchemaToDataType(hoodieSchema: HoodieSchema): DataType = {
    try {
      HoodieSparkSchemaConverters.toSqlType(hoodieSchema) match {
        case (dataType, _) => dataType
      }
    } catch {
      case e: Exception => throw new HoodieSchemaException(
        s"Failed to convert HoodieSchema to DataType: $hoodieSchema", e)
    }
  }

  /**
   * Converts StructType to HoodieSchema.
   *
   * @param structType Catalyst's StructType or DataType
   * @param qualifiedName HoodieSchema qualified name (namespace.name format)
   * @return HoodieSchema corresponding to the Spark DataType
   * @throws HoodieSchemaException if conversion fails
   */
  def convertStructTypeToHoodieSchema(structType: DataType, qualifiedName: String): HoodieSchema = {
    val (namespace, name) = {
      val parts = qualifiedName.split('.')
      if (parts.length > 1) {
        (parts.init.mkString("."), parts.last)
      } else {
        ("", parts.head)
      }
    }
    convertStructTypeToHoodieSchema(structType, name, namespace)
  }

  /**
   * Converts StructType to HoodieSchema.
   *
   * @param structType Catalyst's StructType or DataType
   * @param structName Schema record name
   * @param recordNamespace Schema record namespace
   * @return HoodieSchema corresponding to the Spark DataType
   * @throws HoodieSchemaException if conversion fails
   */
  def convertStructTypeToHoodieSchema(structType: DataType,
                                      structName: String,
                                      recordNamespace: String): HoodieSchema = {
    try {
      HoodieSparkSchemaConverters.toHoodieType(structType, nullable = false, structName, recordNamespace)
    } catch {
      case e: Exception => throw new HoodieSchemaException(
        s"Failed to convert struct type to HoodieSchema: $structType", e)
    }
  }

  /**
   * Recursively aligns the nullable property of Spark schema fields with HoodieSchema.
   *
   * @param sourceSchema Source Spark StructType to align
   * @param hoodieSchema HoodieSchema to use as source of truth
   * @return StructType with aligned nullability
   */
  def alignFieldsNullability(sourceSchema: StructType, hoodieSchema: HoodieSchema): StructType = {
    val hoodieFieldsMap = hoodieSchema.getFields.asScala.map(f => (f.name(), f)).toMap

    val alignedFields = sourceSchema.fields.map { field =>
      hoodieFieldsMap.get(field.name) match {
        case Some(hoodieField) =>
          val alignedField = field.copy(nullable = hoodieField.isNullable)

          field.dataType match {
            case structType: StructType =>
              val nestedSchema = hoodieField.schema().getNonNullType
              if (nestedSchema.getType == HoodieSchemaType.RECORD) {
                alignedField.copy(dataType = alignFieldsNullability(structType, nestedSchema))
              } else {
                alignedField
              }

            case ArrayType(elementType, _) =>
              val arraySchema = hoodieField.schema().getNonNullType
              if (arraySchema.getType == HoodieSchemaType.ARRAY) {
                val elemSchema = arraySchema.getElementType
                val newElementType = updateElementType(elementType, elemSchema)
                alignedField.copy(dataType = ArrayType(newElementType, elemSchema.isNullable))
              } else {
                alignedField
              }

            case MapType(keyType, valueType, _) =>
              val mapSchema = hoodieField.schema().getNonNullType
              if (mapSchema.getType == HoodieSchemaType.MAP) {
                val valueSchema = mapSchema.getValueType
                val newValueType = updateElementType(valueType, valueSchema)
                alignedField.copy(dataType = MapType(keyType, newValueType, valueSchema.isNullable))
              } else {
                alignedField
              }

            case _ => alignedField
          }

        case None => field.copy()
      }
    }

    StructType(alignedFields)
  }


  /**
   * Recursively updates element types for complex types (arrays, maps, structs).
   */
  private def updateElementType(dataType: DataType, hoodieSchema: HoodieSchema): DataType = {
    dataType match {
      case structType: StructType =>
        if (hoodieSchema.getType == HoodieSchemaType.RECORD) {
          alignFieldsNullability(structType, hoodieSchema)
        } else {
          structType
        }

      case ArrayType(elemType, _) =>
        if (hoodieSchema.getType == HoodieSchemaType.ARRAY) {
          val elemSchema = hoodieSchema.getElementType
          ArrayType(updateElementType(elemType, elemSchema), elemSchema.isNullable)
        } else {
          dataType
        }

      case MapType(keyType, valueType, _) =>
        if (hoodieSchema.getType == HoodieSchemaType.MAP) {
          val valueSchema = hoodieSchema.getValueType
          MapType(keyType, updateElementType(valueType, valueSchema), valueSchema.isNullable)
        } else {
          dataType
        }

      case _ => dataType
    }
  }

  //TODO need to fill in impl
  def createHoodieSchemaToInternalRowConverter(requiredAvroSchema: HoodieSchema, requiredRowSchema: StructType):  GenericRecord => Option[InternalRow] = ???
}