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
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType, HoodieSchemaUtils}
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Utilities for converting between HoodieSchema and Spark SQL schemas.
 *
 * This object provides high-level conversion methods with utilities for
 * handling defaults and nullability alignment.
 */
object HoodieSchemaConversionUtils {
  private val SCHEMA_CACHE = new ConcurrentHashMap[HoodieSchema, StructType]


  /**
   * Converts HoodieSchema to Catalyst's StructType.
   *
   * @param hoodieSchema HoodieSchema to convert
   * @return Spark StructType corresponding to the HoodieSchema
   * @throws HoodieSchemaException if conversion fails
   */
  def convertHoodieSchemaToStructType(hoodieSchema: HoodieSchema): StructType = {
    val loader: java.util.function.Function[HoodieSchema, StructType] =
      new java.util.function.Function[HoodieSchema, StructType]() {
        override def apply(schema: HoodieSchema): StructType = {
          try {
            HoodieSparkSchemaConverters.toSqlType(schema) match {
              case (dataType, _) => dataType.asInstanceOf[StructType]
            }
          } catch {
            case e: Exception => throw new HoodieSchemaException(
              s"Failed to convert HoodieSchema to StructType: $schema", e)
          }
        }
      }
    SCHEMA_CACHE.computeIfAbsent(hoodieSchema, loader)
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
   * Converts StructType to HoodieSchema with nullable = false.
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
    convertStructTypeToHoodieSchema(structType, structName, recordNamespace, nullable = false)
  }

  /**
   * Converts StructType to HoodieSchema.
   *
   * @param structType Catalyst's StructType or DataType
   * @param structName Schema record name
   * @param recordNamespace Schema record namespace
   * @param nullable Whether the top-level schema should be nullable
   * @return HoodieSchema corresponding to the Spark DataType
   * @throws HoodieSchemaException if conversion fails
   */
  def convertStructTypeToHoodieSchema(structType: DataType,
                                      structName: String,
                                      recordNamespace: String,
                                      nullable: Boolean): HoodieSchema = {
    try {
      HoodieSparkSchemaConverters.toHoodieType(structType, nullable, structName, recordNamespace)
    } catch {
      case h: HoodieSchemaException => throw h
      case a: AvroRuntimeException => throw new HoodieSchemaException(a.getMessage, a)
      case e: Exception => throw new HoodieSchemaException(
        s"Failed to convert struct type to HoodieSchema: $structType", e)
    }
  }

  /**
   * Re-attach custom Hudi logical-type metadata (e.g. VECTOR, BLOB) from `targetSchema` onto
   * matching fields of `sourceSchema`. Spark's TableOutputResolver wraps write-path outputs in
   * Cast(...) (and UPDATE assignments go through castIfNeeded), both of which drop the
   * StructField metadata that marks these custom types. Without re-attaching, downstream
   * conversion to HoodieSchema yields the backing physical type (e.g. plain ARRAY for VECTOR,
   * plain STRUCT for BLOB) and the schema-compat check against the persisted
   * tableCreateSchema fails.
   *
   * Recurses into nested StructType, ArrayType whose element is a StructType, and MapType
   * whose value is a StructType - the shapes where BLOB can legally be nested. Fields
   * without a matching target (source-only columns such as MERGE join keys) are returned
   * unchanged.
   *
   * @param sourceSchema  the schema whose fields may have lost custom-type metadata
   * @param targetSchema  the catalog schema that owns the authoritative metadata
   * @param caseSensitive whether field name matching should be case-sensitive (mirrors
   *                      `spark.sql.caseSensitive`)
   */
  def reattachCustomTypeMetadata(sourceSchema: StructType,
                                 targetSchema: StructType,
                                 caseSensitive: Boolean): StructType = {
    val targetByName: Map[String, StructField] =
      if (caseSensitive) {
        targetSchema.fields.map(f => f.name -> f).toMap
      } else {
        targetSchema.fields.map(f => f.name.toLowerCase -> f).toMap
      }
    val lookupKey: String => String =
      if (caseSensitive) identity else (_: String).toLowerCase

    StructType(sourceSchema.fields.map { field =>
      targetByName.get(lookupKey(field.name)) match {
        case Some(target) => reattachField(field, target, caseSensitive)
        case None => field
      }
    })
  }

  private def reattachField(source: StructField,
                            target: StructField,
                            caseSensitive: Boolean): StructField = {
    val withNestedDataType = (source.dataType, target.dataType) match {
      case (s: StructType, t: StructType) =>
        source.copy(dataType = reattachCustomTypeMetadata(s, t, caseSensitive))
      case (ArrayType(sElem: StructType, nullable), ArrayType(tElem: StructType, _)) =>
        source.copy(dataType = ArrayType(reattachCustomTypeMetadata(sElem, tElem, caseSensitive), nullable))
      case (MapType(sKey, sVal: StructType, valueContainsNull), MapType(_, tVal: StructType, _)) =>
        source.copy(
          dataType = MapType(sKey, reattachCustomTypeMetadata(sVal, tVal, caseSensitive), valueContainsNull))
      case _ => source
    }

    if (target.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
      val enrichedMetadata = new MetadataBuilder()
        .withMetadata(withNestedDataType.metadata)
        .putString(
          HoodieSchema.TYPE_METADATA_FIELD,
          target.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
        .build()
      withNestedDataType.copy(metadata = enrichedMetadata)
    } else {
      withNestedDataType
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

  /**
   * Creates a converter from GenericRecord to InternalRow using HoodieSchema.
   *
   * @param requiredSchema the HoodieSchema to use for deserialization
   * @param requiredRowSchema the Spark StructType for the output InternalRow
   * @return a function that converts GenericRecord to Option[InternalRow]
   */
  def createGenericRecordToInternalRowConverter(requiredSchema: HoodieSchema, requiredRowSchema: StructType): GenericRecord => Option[InternalRow] = {
    val deserializer = sparkAdapter.createAvroDeserializer(requiredSchema, requiredRowSchema)
    record => deserializer
      .deserialize(record)
      .map(_.asInstanceOf[InternalRow])
  }

  /**
   * Gets the fully-qualified Avro record name and namespace for a Hudi table
   * This delegates to [[HoodieSchemaUtils.getRecordQualifiedName]] which in turn
   * delegates to [[AvroSchemaUtils.getAvroRecordQualifiedName]].
   *
   * The qualified name follows the pattern: hoodie.{tableName}.{tableName}_record
   * where tableName is sanitized for Avro compatibility.
   *
   * @param tableName the Hudi table name
   */
  def getRecordNameAndNamespace(tableName: String): (String, String) = {
    val qualifiedName = HoodieSchemaUtils.getRecordQualifiedName(tableName)
    val nameParts = qualifiedName.split('.')
    (nameParts.last, nameParts.init.mkString("."))
  }

  /**
   * Creates a [[org.apache.spark.sql.DataFrame]] from the provided [[RDD]] of [[GenericRecord]]s
   * using a HoodieSchema.
   *
   * @param rdd RDD of GenericRecords to convert
   * @param hoodieSchema the HoodieSchema for the records
   * @param sparkSession the SparkSession to use
   * @return DataFrame containing the converted records
   */
  def createDataFrame(rdd: RDD[GenericRecord], hoodieSchema: HoodieSchema, sparkSession: SparkSession): Dataset[Row] = {
    val structType = convertHoodieSchemaToStructType(hoodieSchema)

    sparkSession.createDataFrame(rdd.mapPartitions { records =>
      if (records.isEmpty) Iterator.empty
      else {
        val serde = HoodieSparkUtils.getCatalystRowSerDe(structType)
        val converter = createGenericRecordToInternalRowConverter(hoodieSchema, structType)
        records.map { record =>
          converter(record).map(serde.deserializeRow).get
        }
      }
    }, structType)
  }
}