/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{Fixed, Record}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object AvroConversionUtils {

  def createRdd(df: DataFrame, structName: String, recordNamespace: String): RDD[GenericRecord] = {
    val avroSchema = convertStructTypeToAvroSchema(df.schema, structName, recordNamespace)
    createRdd(df, avroSchema.toString, structName, recordNamespace)
  }

  def createRdd(df: DataFrame, avroSchemaAsJsonString: String, structName: String, recordNamespace: String): RDD[GenericRecord] = {
    val dataType = df.schema
    val encoder = RowEncoder.apply(dataType).resolveAndBind()
    df.queryExecution.toRdd.map(encoder.fromRow)
      .mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val avroSchema = new Schema.Parser().parse(avroSchemaAsJsonString)
          val convertor = createConverterToAvro(avroSchema, dataType, structName, recordNamespace)
          records.map { x => convertor(x).asInstanceOf[GenericRecord] }
        }
      }
  }

  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss : SparkSession): Dataset[Row] = {
    if (rdd.isEmpty()) {
      ss.emptyDataFrame
    } else {
      ss.createDataFrame(rdd.mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val schema = Schema.parse(schemaStr)
          val dataType = convertAvroSchemaToStructType(schema)
          val convertor = createConverterToRow(schema, dataType)
          records.map { x => convertor(x).asInstanceOf[Row] }
        }
      }, convertAvroSchemaToStructType(Schema.parse(schemaStr))).asInstanceOf[Dataset[Row]]
    }
  }

  def getNewRecordNamespace(elementDataType: DataType,
                            currentRecordNamespace: String,
                            elementName: String): String = {

    elementDataType match {
      case StructType(_) => s"$currentRecordNamespace.$elementName"
      case _ => currentRecordNamespace
    }
  }

  /**
    * NOTE : This part of code is copied from com.databricks.spark.avro.SchemaConverters.scala (133:310) (spark-avro)
    *
    * Returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
    *                       by user.
    * @param targetSqlType Target catalyst sql type after the conversion.
    * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
    */
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: DataType): AnyRef => AnyRef = {
    SchemaConverters.createConverterToSQL(sourceAvroSchema, targetSqlType)
  }

  def createConverterToAvro(avroSchema: Schema,
                            dataType: DataType,
                            structName: String,
                            recordNamespace: String): Any => Any = {
    dataType match {
      case BinaryType => (item: Any) =>
        item match {
          case null => null
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
        }
      case IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case ByteType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Byte].intValue
      case ShortType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Short].intValue
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      //case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case dec: DecimalType => (item: Any) =>
        Option(item).map { i =>
          val bigDecimalValue = item.asInstanceOf[java.math.BigDecimal]
          val decimalConversions = new DecimalConversion()
          decimalConversions.toFixed(bigDecimalValue, avroSchema.getField(structName).schema().getTypes.get(0),
            LogicalTypes.decimal(dec.precision, dec.scale))
        }.orNull
      case TimestampType => (item: Any) =>
        //if (item == null) null else item.asInstanceOf[Timestamp].getTime
        Option(item).map(_.asInstanceOf[Timestamp].getTime).orNull
      case DateType => (item: Any) =>
        //if (item == null) null else item.asInstanceOf[Date].getTime
        Option(item).map(_.asInstanceOf[Date].toLocalDate.toEpochDay.toInt).orNull
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(
          avroSchema,
          elementType,
          structName,
          getNewRecordNamespace(elementType, recordNamespace, structName))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetList = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetList.add(elementConverter(sourceArray(idx)))
              idx += 1
            }
            targetList
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(
          avroSchema,
          valueType,
          structName,
          getNewRecordNamespace(valueType, recordNamespace, structName))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new util.HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(
            avroSchema,
            field.dataType,
            field.name,
            getNewRecordNamespace(field.dataType, recordNamespace, field.name)))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }

  def convertStructTypeToAvroSchema(structType: StructType,
                                    structName: String,
                                    recordNamespace: String): Schema = {
    val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
    SchemaConverters.convertStructToAvro(structType, builder, recordNamespace)
  }

  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType];
  }
}
