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

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

object AvroConversionUtils {

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
      }, convertAvroSchemaToStructType(new Schema.Parser().parse(schemaStr)))
    }
  }

  def convertStructTypeToAvroSchema(structType: StructType,
                                    structName: String,
                                    recordNamespace: String): Schema = {
    SchemaConverters.toAvroType(structType, nullable = false, structName, recordNamespace)
  }

  def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
  }

  def buildAvroRecordBySchema(record: IndexedRecord,
                              requiredSchema: Schema,
                              requiredPos: List[Int],
                              recordBuilder: GenericRecordBuilder): GenericRecord = {
    val requiredFields = requiredSchema.getFields.asScala
    assert(requiredFields.length == requiredPos.length)
    val positionIterator = requiredPos.iterator
    requiredFields.foreach(f => recordBuilder.set(f, record.get(positionIterator.next())))
    recordBuilder.build()
  }

  def getAvroRecordNameAndNamespace(tableName: String): (String, String) = {
    val name = HoodieAvroUtils.sanitizeName(tableName)
    (s"${name}_record", s"hoodie.${name}")
  }
}
