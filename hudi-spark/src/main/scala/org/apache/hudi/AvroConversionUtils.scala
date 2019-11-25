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

package org.apache.hudi

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hudi.common.model.HoodieKey
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object AvroConversionUtils {

  def createRdd(df: DataFrame, structName: String, recordNamespace: String): RDD[GenericRecord] = {
    val dataType = df.schema
    val encoder = RowEncoder.apply(dataType).resolveAndBind()
    df.queryExecution.toRdd.map(encoder.fromRow)
      .mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val convertor = AvroConversionHelper.createConverterToAvro(dataType, structName, recordNamespace)
          records.map { x => convertor(x).asInstanceOf[GenericRecord] }
        }
      }
  }

  def createRddForDeletes(df: DataFrame, rowField: String, partitionField: String): RDD[HoodieKey] = {
    df.rdd.map(row => (new HoodieKey(row.getAs[String](rowField), row.getAs[String](partitionField))))
  }

  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss: SparkSession): Dataset[Row] = {
    if (rdd.isEmpty()) {
      ss.emptyDataFrame
    } else {
      ss.createDataFrame(rdd.mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val schema = new Schema.Parser().parse(schemaStr)
          val dataType = convertAvroSchemaToStructType(schema)
          val convertor = AvroConversionHelper.createConverterToRow(schema, dataType)
          records.map { x => convertor(x).asInstanceOf[Row] }
        }
      }, convertAvroSchemaToStructType(new Schema.Parser().parse(schemaStr))).asInstanceOf[Dataset[Row]]
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
