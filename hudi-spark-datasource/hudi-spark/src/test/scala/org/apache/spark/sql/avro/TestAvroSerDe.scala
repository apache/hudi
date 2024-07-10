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

package org.apache.spark.sql.avro

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, IntWrapper}

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.avro.SchemaConverters.SchemaType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestAvroSerDe extends SparkAdapterSupport {

  @Test
  def testAvroUnionSerDe(): Unit = {
    val originalAvroRecord = {
      val minValue = new GenericData.Record(IntWrapper.SCHEMA$)
      minValue.put("value", 9)
      val maxValue = new GenericData.Record(IntWrapper.SCHEMA$)
      maxValue.put("value", 10)

      val record = new GenericData.Record(HoodieMetadataColumnStats.SCHEMA$)
      record.put("fileName", "9388c460-4ace-4274-9a0b-d44606af60af-0_2-25-35_20220520154514641.parquet")
      record.put("columnName", "c8")
      record.put("minValue", minValue)
      record.put("maxValue", maxValue)
      record.put("valueCount", 10L)
      record.put("nullCount", 0L)
      record.put("totalSize", 94L)
      record.put("totalUncompressedSize", 54L)
      record.put("isDeleted", false)
      record
    }

    val avroSchema = HoodieMetadataColumnStats.SCHEMA$
    val SchemaType(catalystSchema, _) = SchemaConverters.toSqlType(avroSchema)

    val deserializer = sparkAdapter.createAvroDeserializer(avroSchema, catalystSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, avroSchema, nullable = false)

    val row = deserializer.deserialize(originalAvroRecord).get
    val deserializedAvroRecord = serializer.serialize(row)

    assertEquals(originalAvroRecord, deserializedAvroRecord)
  }
}
