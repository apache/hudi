/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package com.uber.hoodie

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}


object AvroConversionUtils {

  /** Create an RDD of avro serialized records from a dataframe.
    *
    * @param df         the dataframe
    * @param recordName the root record name in the resulting avro record schema
    * @param nameSpace  the namespace of the records in the resulting avro record schema
    * @return an RDD of avro serialized records
    */
  def createRdd(df: DataFrame, recordName: String, nameSpace: String): RDD[GenericRecord] = {
    val dataSchema = df.schema
    val encoder = RowEncoder.apply(dataSchema).resolveAndBind()
    val converter = CatalystTypeConverters.createToCatalystConverter(dataSchema)
    df.queryExecution.toRdd.map(encoder.fromRow).mapPartitions { rows: Iterator[Row] =>
      val targetSchema = convertStructTypeToAvroSchema(dataSchema, recordName, nameSpace)
      val serializer = new AvroSerializer(dataSchema, targetSchema, false)
      rows.map { row: Row =>
        val internalRow = converter(row).asInstanceOf[InternalRow]
        serializer.serialize(internalRow).asInstanceOf[GenericRecord]
      }
    }
  }

  /** Create a dataframe from an RDD of serialized avro records.
    *
    * @param rdd              the rdd containing avro records
    * @param avroSchemaString the schema of records in the rdd (stored in string to ensure spark can serialize it)
    * @param ss               an active spark session
    * @return a dataframe
    */
  def createDataFrame(rdd: RDD[GenericRecord], avroSchemaString: String, ss: SparkSession): DataFrame = {
    val dataSchema: StructType = convertAvroSchemaToStructType(new Schema.Parser().parse(avroSchemaString))
    val encoder = RowEncoder.apply(dataSchema).resolveAndBind()
    val rowRDD: RDD[Row] = rdd.mapPartitions { records: Iterator[GenericRecord] =>
      val avroSchema = new Schema.Parser().parse(avroSchemaString)
      val deserializer = new AvroDeserializer(avroSchema, dataSchema)
      records.map { record: GenericRecord =>
        encoder.fromRow(deserializer.deserialize(record).asInstanceOf[InternalRow])
      }
    }
    ss.createDataFrame(rowRDD, dataSchema)
  }

  /** Convert a struct type into an avro schema.
    *
    * @param structType the struct type
    * @param recordName the root record name in the resulting schema
    * @param nameSpace  the namespace of the records in the resulting schema
    * @return an avro schema
    */
  def convertStructTypeToAvroSchema(structType: StructType, recordName: String, nameSpace: String): Schema = {
    SchemaConverters.toAvroType(catalystType = structType, nullable = false, recordName = recordName,
      nameSpace = nameSpace)
  }

  /** Convert an avro schema into a struct type.
    *
    * @param avroSchema the avro schema
    * @return a struct type
    */
  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    SchemaConverters.toSqlType(avroSchema).dataType match {
      case t: StructType => t
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:|
           |${avroSchema.toString(true)}
         """.stripMargin)
    }
  }
}
