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

import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.avro.{AvroRuntimeException, JsonProperties, Schema}
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AvroConversionUtils {

  /**
   * Check the nullability of the input Avro type and resolve it when it is nullable. The first
   * return value is a [[Boolean]] indicating if the input Avro type is nullable. The second
   * return value is either provided Avro type if it's not nullable, or its resolved non-nullable part
   * in case it is
   */
  def resolveAvroTypeNullability(avroType: Schema): (Boolean, Schema) = {
    if (avroType.getType == Type.UNION) {
      val fields = avroType.getTypes.asScala
      val actualType = fields.filter(_.getType != Type.NULL)
      if (fields.length != 2 || actualType.length != 1) {
        throw new AvroRuntimeException(
          s"Unsupported Avro UNION type $avroType: Only UNION of a null type and a non-null " +
            "type is supported")
      }
      (true, actualType.head)
    } else {
      (false, avroType)
    }
  }

  /**
   * Creates converter to transform Avro payload into Spark's Catalyst one
   *
   * @param rootAvroType Avro [[Schema]] to be transformed from
   * @param rootCatalystType Catalyst [[StructType]] to be transformed into
   * @return converter accepting Avro payload and transforming it into a Catalyst one (in the form of [[InternalRow]])
   */
  def createAvroToInternalRowConverter(rootAvroType: Schema, rootCatalystType: StructType): GenericRecord => Option[InternalRow] = {
    val deserializer = sparkAdapter.createAvroDeserializer(rootAvroType, rootCatalystType)
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
    val serializer = sparkAdapter.createAvroSerializer(rootCatalystType, rootAvroType, nullable)
    row => serializer
      .serialize(row)
      .asInstanceOf[GenericRecord]
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createAvroToInternalRowConverter]]
   */
  @Deprecated
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: StructType): GenericRecord => Row = {
    val encoder = RowEncoder.apply(targetSqlType).resolveAndBind()
    val serde = sparkAdapter.createSparkRowSerDe(encoder)
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
    val encoder = RowEncoder.apply(sourceSqlType).resolveAndBind()
    val serde = sparkAdapter.createSparkRowSerDe(encoder)
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceSqlType, structName, recordNamespace)
    val (nullable, _) = resolveAvroTypeNullability(avroSchema)

    val converter = AvroConversionUtils.createInternalRowToAvroConverter(sourceSqlType, avroSchema, nullable)

    row => converter.apply(serde.serializeRow(row))
  }

  /**
   * Creates [[org.apache.spark.sql.DataFrame]] from the provided [[RDD]] of [[GenericRecord]]s
   *
   * TODO convert directly from GenericRecord into InternalRow instead
   */
  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss: SparkSession): Dataset[Row] = {
    if (rdd.isEmpty()) {
      ss.emptyDataFrame
    } else {
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
  }

  /**
   *
   * Returns avro schema from spark StructType.
   *
   * @param structType      Dataframe Struct Type.
   * @param structName      Avro record name.
   * @param recordNamespace Avro record namespace.
   * @return Avro schema corresponding to given struct type.
   */
  def convertStructTypeToAvroSchema(structType: DataType,
                                    structName: String,
                                    recordNamespace: String): Schema = {
    val schemaConverters = sparkAdapter.getAvroSchemaConverters
    val avroSchema = schemaConverters.toAvroType(structType, nullable = false, structName, recordNamespace)
    getAvroSchemaWithDefaults(avroSchema, structType)
  }

  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    val schemaConverters = sparkAdapter.getAvroSchemaConverters
    schemaConverters.toSqlType(avroSchema) match {
      case (dataType, _) => dataType.asInstanceOf[StructType]
    }
  }

  /**
   *
   * Method to add default value of null to nullable fields in given avro schema
   *
   * @param schema input avro schema
   * @return Avro schema with null default set to nullable fields
   */
  def getAvroSchemaWithDefaults(schema: Schema, dataType: DataType): Schema = {

    schema.getType match {
      case Schema.Type.RECORD => {
        val structType = dataType.asInstanceOf[StructType]
        val structFields = structType.fields
        val modifiedFields = schema.getFields.map(field => {
          val i: Int = structType.fieldIndex(field.name())
          val comment: String = if (structFields(i).metadata.contains("comment")) {
            structFields(i).metadata.getString("comment")
          } else {
            field.doc()
          }
          val newSchema = getAvroSchemaWithDefaults(field.schema(), structFields(i).dataType)
          field.schema().getType match {
            case Schema.Type.UNION => {
              val innerFields = newSchema.getTypes
              val containsNullSchema = innerFields.foldLeft(false)((nullFieldEncountered, schema) => nullFieldEncountered | schema.getType == Schema.Type.NULL)
              if(containsNullSchema) {
                // Need to re shuffle the fields in list because to set null as default, null schema must be head in union schema
                val restructuredNewSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL)) ++ innerFields.filter(innerSchema => !(innerSchema.getType == Schema.Type.NULL)))
                new Schema.Field(field.name(), restructuredNewSchema, comment, JsonProperties.NULL_VALUE)
              } else {
                new Schema.Field(field.name(), newSchema, comment, field.defaultVal())
              }
            }
            case _ => new Schema.Field(field.name(), newSchema, comment, field.defaultVal())
          }
        }).toList
        Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, modifiedFields)
      }

      case Schema.Type.UNION => {
        Schema.createUnion(schema.getTypes.map(innerSchema => getAvroSchemaWithDefaults(innerSchema, dataType)))
      }

      case Schema.Type.MAP => {
        Schema.createMap(getAvroSchemaWithDefaults(schema.getValueType, dataType.asInstanceOf[MapType].valueType))
      }

      case Schema.Type.ARRAY => {
        Schema.createArray(getAvroSchemaWithDefaults(schema.getElementType, dataType.asInstanceOf[ArrayType].elementType))
      }

      case _ => schema
    }
  }

  def getAvroRecordNameAndNamespace(tableName: String): (String, String) = {
    val name = HoodieAvroUtils.sanitizeName(tableName)
    (s"${name}_record", s"hoodie.${name}")
  }
}
