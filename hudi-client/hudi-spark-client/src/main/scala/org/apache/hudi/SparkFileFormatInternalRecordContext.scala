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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.avro.AvroSchemaUtils.isNullable
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hudi.SparkAdapter

import scala.collection.mutable

/**
 * Spark record context for reading and transforming Spark InternalRow records.
 */
class SparkFileFormatInternalRecordContext(tableConfig: HoodieTableConfig,
                                           shouldUseMetaFields: Boolean)
  extends BaseSparkInternalRecordContext(tableConfig, shouldUseMetaFields) {

  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()
  private val serializerMap: mutable.Map[Schema, HoodieAvroSerializer] = mutable.Map()

  override def supportsParquetRowIndex: Boolean = {
    HoodieSparkUtils.gteqSpark3_5
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an [[InternalRow]].
   *
   * @param avroRecord The Avro record.
   * @return An [[InternalRow]].
   */
  override def convertAvroRecord(avroRecord: IndexedRecord): InternalRow = {
    val schema = avroRecord.getSchema
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val deserializer = deserializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroDeserializer(schema, structType)
    })
    deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
  }

  override def convertToAvroRecord(record: InternalRow, schema: Schema): GenericRecord = {
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val serializer = serializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroSerializer(structType, schema, isNullable(schema))
    })
    serializer.serialize(record).asInstanceOf[GenericRecord]
  }
}
