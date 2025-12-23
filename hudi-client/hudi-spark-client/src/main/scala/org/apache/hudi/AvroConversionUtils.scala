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

import org.apache.hudi.HoodieSparkUtils.{getCatalystRowSerDe, sparkAdapter}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.avro.HoodieAvroUtils.createNewSchemaField
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.avro.{AvroRuntimeException, JsonProperties, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.avro.HoodieSparkAvroSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

object AvroConversionUtils {
  private val ROW_TO_AVRO_CONVERTER_CACHE =
    new ConcurrentHashMap[Tuple3[StructType, Schema, Boolean], Function1[InternalRow, GenericRecord]]
  private val AVRO_SCHEMA_CACHE = new ConcurrentHashMap[Schema, StructType]

  /**
   * Creates converter to transform Avro payload into Spark's Catalyst one
   *
   * @param rootAvroType Avro [[Schema]] to be transformed from
   * @param rootCatalystType Catalyst [[StructType]] to be transformed into
   * @return converter accepting Avro payload and transforming it into a Catalyst one (in the form of [[InternalRow]])
   */
  def createAvroToInternalRowConverter(rootAvroType: Schema, rootCatalystType: StructType): GenericRecord => Option[InternalRow] = {
    val deserializer = sparkAdapter.createAvroDeserializer(HoodieSchema.fromAvroSchema(rootAvroType), rootCatalystType)
    record => deserializer
      .deserialize(record)
      .map(_.asInstanceOf[InternalRow])
  }

  /**
   * Creates converter to transform Catalyst payload into Avro one
   *
   * @param rootCatalystType Catalyst [[StructType]] to be transformed from
   * @param rootAvroType Avro [[Schema]] to be transformed into
   * @param nullable whether Avro record is nullable
   * @return converter accepting Catalyst payload (in the form of [[InternalRow]]) and transforming it into an Avro one
   */
  def createInternalRowToAvroConverter(rootCatalystType: StructType, rootAvroType: Schema, nullable: Boolean): InternalRow => GenericRecord = {
    val loader: java.util.function.Function[Tuple3[StructType, Schema, Boolean], Function1[InternalRow, GenericRecord]] = key => {
      val serializer = sparkAdapter.createAvroSerializer(key._1, HoodieSchema.fromAvroSchema(key._2), key._3)
      row => {
        try {
          serializer
            .serialize(row)
            .asInstanceOf[GenericRecord]
        } catch {
          case e: HoodieSchemaException => throw e
          case e => throw new SchemaCompatibilityException("Failed to convert spark record into avro record", e)
        }
      }
    }
    ROW_TO_AVRO_CONVERTER_CACHE.computeIfAbsent(Tuple3.apply(rootCatalystType, rootAvroType, nullable), loader)
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createAvroToInternalRowConverter]]
   */
  @Deprecated
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: StructType): GenericRecord => Row = {
    val serde = getCatalystRowSerDe(targetSqlType)
    val converter = AvroConversionUtils.createAvroToInternalRowConverter(sourceAvroSchema, targetSqlType)

    avro => converter.apply(avro).map(serde.deserializeRow).get
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createInternalRowToAvroConverter]]
   */
  @Deprecated
  def createConverterToAvro(sourceSqlType: StructType,
                            structName: String,
                            recordNamespace: String): Row => GenericRecord = {
    val serde = getCatalystRowSerDe(sourceSqlType)
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceSqlType, structName, recordNamespace)
    val nullable = AvroSchemaUtils.getNonNullTypeFromUnion(avroSchema) != avroSchema

    val converter = AvroConversionUtils.createInternalRowToAvroConverter(sourceSqlType, avroSchema, nullable)

    row => converter.apply(serde.serializeRow(row))
  }

  /**
   * Creates [[org.apache.spark.sql.DataFrame]] from the provided [[RDD]] of [[GenericRecord]]s
   *
   * TODO convert directly from GenericRecord into InternalRow instead
   */
  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss: SparkSession): Dataset[Row] = {
    ss.createDataFrame(rdd.mapPartitions { records =>
      if (records.isEmpty) Iterator.empty
      else {
        val schema = new Schema.Parser().parse(schemaStr)
        val dataType = convertAvroSchemaToStructType(schema)
        val converter = createConverterToRow(schema, dataType)
        records.map { r => converter(r) }
      }
    }, convertAvroSchemaToStructType(new Schema.Parser().parse(schemaStr)))
  }

  /**
   * Converts [[StructType]] into Avro's [[Schema]]
   *
   * @param structType    Catalyst's [[StructType]]
   * @param qualifiedName Avro's schema qualified name
   * @return Avro schema corresponding to given struct type.
   */
  def convertStructTypeToAvroSchema(structType: DataType,
                                    qualifiedName: String): Schema = {
    val (namespace, name) = {
      val parts = qualifiedName.split('.')
      (parts.init.mkString("."), parts.last)
    }
    convertStructTypeToAvroSchema(structType, name, namespace)
  }


  /**
   * Converts [[StructType]] into Avro's [[Schema]]
   *
   * @param structType      Catalyst's [[StructType]]
   * @param structName      Avro record name
   * @param recordNamespace Avro record namespace
   * @return Avro schema corresponding to given struct type.
   */
  def convertStructTypeToAvroSchema(structType: DataType,
                                    structName: String,
                                    recordNamespace: String): Schema = {
    try {
      HoodieSparkAvroSchemaConverters.toAvroType(structType, nullable = false, structName, recordNamespace)
    } catch {
      case a: AvroRuntimeException => throw new HoodieSchemaException(a.getMessage, a)
      case e: Exception => throw new HoodieSchemaException("Failed to convert struct type to avro schema: " + structType, e)
    }
  }

  /**
   * Converts Avro's [[Schema]] to Catalyst's [[StructType]]
   */
  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    val loader: java.util.function.Function[Schema, StructType] = key => {
      try {
        HoodieSparkAvroSchemaConverters.toSqlType(key) match {
          case (dataType, _) => dataType.asInstanceOf[StructType]
        }
      } catch {
        case e: Exception => throw new HoodieSchemaException("Failed to convert avro schema to struct type: " + avroSchema, e)
      }
    }
    AVRO_SCHEMA_CACHE.computeIfAbsent(avroSchema, loader)
  }

  /**
   * Converts Avro's [[Schema]] to Catalyst's [[DataType]]
   */
  def convertAvroSchemaToDataType(avroSchema: Schema): DataType = {
    try {
      HoodieSparkAvroSchemaConverters.toSqlType(avroSchema) match {
        case (dataType, _) => dataType
      }
    } catch {
      case e: Exception => throw new HoodieSchemaException("Failed to convert avro schema to DataType: " + avroSchema, e)
    }
  }

  /**
   * Please use [[AvroSchemaUtils.getAvroRecordQualifiedName(String)]]
   */
  @Deprecated
  def getAvroRecordNameAndNamespace(tableName: String): (String, String) = {
    val qualifiedName = AvroSchemaUtils.getAvroRecordQualifiedName(tableName)
    val nameParts = qualifiedName.split('.')
    (nameParts.last, nameParts.init.mkString("."))
  }

  /**
   * Recursively aligns the nullable property of hoodie table schema, supporting nested structures
   */
  def alignFieldsNullability(sourceSchema: StructType, avroSchema: Schema): StructType = {
    // Converts Avro fields to a Map for efficient lookup
    val avroFieldsMap = avroSchema.getFields.asScala.map(f => (f.name, f)).toMap

    // Recursively process fields
    val alignedFields = sourceSchema.fields.map { field =>
      avroFieldsMap.get(field.name) match {
        case Some(avroField) =>
          // Process the nullable property of the current field
          val alignedField = field.copy(nullable = avroField.schema.isNullable)

          // Recursively handle nested structures
          field.dataType match {
            case structType: StructType =>
              // For struct type, recursively process its internal fields
              val nestedAvroSchema = unwrapNullableSchema(avroField.schema)
              if (nestedAvroSchema.getType == Schema.Type.RECORD) {
                alignedField.copy(dataType = alignFieldsNullability(structType, nestedAvroSchema))
              } else {
                alignedField
              }

            case ArrayType(elementType, containsNull) =>
              // For array type, process element type
              val arraySchema = unwrapNullableSchema(avroField.schema)
              if (arraySchema.getType == Schema.Type.ARRAY) {
                val elemSchema = arraySchema.getElementType
                val newElementType = updateElementType(elementType, elemSchema)
                alignedField.copy(dataType = ArrayType(newElementType, elemSchema.isNullable))
              } else {
                alignedField
              }

            case MapType(keyType, valueType, valueContainsNull) =>
              // For Map type, process value type
              val mapSchema = unwrapNullableSchema(avroField.schema)
              if (mapSchema.getType == Schema.Type.MAP) {
                val valueSchema = mapSchema.getValueType
                val newValueType = updateElementType(valueType, valueSchema)
                alignedField.copy(dataType = MapType(keyType, newValueType, valueSchema.isNullable))
              } else {
                alignedField
              }

            case _ => alignedField // Basic types are returned directly
          }

        case None => field.copy() // Field not found in Avro schema remains unchanged
      }
    }

    StructType(alignedFields)
  }

  /**
   * Returns the non-null schema if the schema is a UNION type containing NULL
   */
  private def unwrapNullableSchema(schema: Schema): Schema = {
    if (schema.getType == Schema.Type.UNION) {
      val types = schema.getTypes.asScala
      val nonNullTypes = types.filter(_.getType != Schema.Type.NULL)
      if (nonNullTypes.size == 1) nonNullTypes.head else schema
    } else {
      schema
    }
  }

  /**
   * Updates the element type, handling nested structures
   */
  private def updateElementType(dataType: DataType, avroSchema: Schema): DataType = {
    dataType match {
      case structType: StructType =>
        if (avroSchema.getType == Schema.Type.RECORD) {
          alignFieldsNullability(structType, avroSchema)
        } else {
          structType
        }

      case ArrayType(elemType, containsNull) =>
        if (avroSchema.getType == Schema.Type.ARRAY) {
          val elemSchema = avroSchema.getElementType
          ArrayType(updateElementType(elemType, elemSchema), elemSchema.isNullable)
        } else {
          dataType
        }

      case MapType(keyType, valueType, valueContainsNull) =>
        if (avroSchema.getType == Schema.Type.MAP) {
          val valueSchema = avroSchema.getValueType
          MapType(keyType, updateElementType(valueType, valueSchema), valueSchema.isNullable)
        } else {
          dataType
        }

      case _ => dataType // Basic types are returned directly
    }
  }
}
