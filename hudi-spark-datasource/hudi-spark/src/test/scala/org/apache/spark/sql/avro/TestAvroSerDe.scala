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
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test

import java.nio.{ByteBuffer, ByteOrder}
import java.util

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

    val schema = HoodieSchema.fromAvroSchema(HoodieMetadataColumnStats.SCHEMA$)
    val (catalystSchema, _) = HoodieSparkSchemaConverters.toSqlType(schema)

    val deserializer = sparkAdapter.createAvroDeserializer(schema, catalystSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, schema, nullable = false)

    val row = deserializer.deserialize(originalAvroRecord).get
    val deserializedAvroRecord = serializer.serialize(row)

    assertEquals(originalAvroRecord, deserializedAvroRecord)
  }

  @Test
  def testVectorFloatSerDe(): Unit = {
    val dimension = 4
    val vectorSchema = HoodieSchema.createVector(dimension, HoodieSchema.Vector.VectorElementType.FLOAT)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("embedding", vectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("FloatVectorRecord", "test", null, fields)
    val avroSchema = hoodieSchema.toAvroSchema

    // Build a GenericData.Record with float vector data
    val buffer = ByteBuffer.allocate(dimension * 4).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putFloat(1.0f).putFloat(2.5f).putFloat(-3.0f).putFloat(0.0f)
    val fixedField = new GenericData.Fixed(avroSchema.getField("embedding").schema(), buffer.array())

    val originalRecord = new GenericData.Record(avroSchema)
    originalRecord.put("embedding", fixedField)

    val (catalystSchema, _) = HoodieSparkSchemaConverters.toSqlType(hoodieSchema)
    val deserializer = sparkAdapter.createAvroDeserializer(hoodieSchema, catalystSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, hoodieSchema, nullable = false)

    val row = deserializer.deserialize(originalRecord).get
    val deserializedRecord = serializer.serialize(row)

    assertEquals(originalRecord, deserializedRecord)
  }

  @Test
  def testVectorDoubleSerDe(): Unit = {
    val dimension = 3
    val vectorSchema = HoodieSchema.createVector(dimension, HoodieSchema.Vector.VectorElementType.DOUBLE)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("embedding", vectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("DoubleVectorRecord", "test", null, fields)
    val avroSchema = hoodieSchema.toAvroSchema

    val buffer = ByteBuffer.allocate(dimension * 8).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putDouble(1.0).putDouble(-2.5).putDouble(3.14159)
    val fixedField = new GenericData.Fixed(avroSchema.getField("embedding").schema(), buffer.array())

    val originalRecord = new GenericData.Record(avroSchema)
    originalRecord.put("embedding", fixedField)

    val (catalystSchema, _) = HoodieSparkSchemaConverters.toSqlType(hoodieSchema)
    val deserializer = sparkAdapter.createAvroDeserializer(hoodieSchema, catalystSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, hoodieSchema, nullable = false)

    val row = deserializer.deserialize(originalRecord).get
    val deserializedRecord = serializer.serialize(row)

    assertEquals(originalRecord, deserializedRecord)
  }

  @Test
  def testVectorFloatByteOrder(): Unit = {
    val dimension = 2
    val vectorSchema = HoodieSchema.createVector(dimension, HoodieSchema.Vector.VectorElementType.FLOAT)
    val fields = util.Arrays.asList(
      HoodieSchemaField.of("embedding", vectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("FloatByteOrderRecord", "test", null, fields)
    val avroSchema = hoodieSchema.toAvroSchema

    // Build input: Spark array of floats → serialize → Avro Fixed
    val inputFloats = Array(1.0f, -2.0f)
    val sparkRow = {
      val row = new GenericInternalRow(1)
      row.update(0, ArrayData.toArrayData(inputFloats))
      row
    }

    val (catalystSchema, _) = HoodieSparkSchemaConverters.toSqlType(hoodieSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, hoodieSchema, nullable = false)
    val serialized = serializer.serialize(sparkRow).asInstanceOf[GenericData.Record]
    val fixedBytes = serialized.get("embedding").asInstanceOf[GenericData.Fixed].bytes()

    // Assert little-endian IEEE 754 layout byte-by-byte:
    //   1.0f  LE = 0x00 0x00 0x80 0x3F
    //  -2.0f  LE = 0x00 0x00 0x00 0xC0
    val expectedBytes = Array[Byte](
      0x00, 0x00, 0x80.toByte, 0x3F,
      0x00, 0x00, 0x00, 0xC0.toByte
    )
    assertArrayEquals(expectedBytes, fixedBytes,
      "Serialized bytes must match little-endian IEEE 754 float layout")

    // Deserialize the raw bytes back and verify float values
    val fixedField = new GenericData.Fixed(avroSchema.getField("embedding").schema(), fixedBytes)
    val record = new GenericData.Record(avroSchema)
    record.put("embedding", fixedField)

    val deserializer = sparkAdapter.createAvroDeserializer(hoodieSchema, catalystSchema)
    val row = deserializer.deserialize(record).get.asInstanceOf[InternalRow]
    val resultArray = row.getArray(0)
    assertEquals(1.0f, resultArray.getFloat(0), 0.0f)
    assertEquals(-2.0f, resultArray.getFloat(1), 0.0f)
  }

  @Test
  def testVectorInt8SerDe(): Unit = {
    val dimension = 5
    val vectorSchema = HoodieSchema.createVector(dimension, HoodieSchema.Vector.VectorElementType.INT8)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("embedding", vectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("Int8VectorRecord", "test", null, fields)
    val avroSchema = hoodieSchema.toAvroSchema

    val bytes = Array[Byte](1, -2, 127, -128, 0)
    val fixedField = new GenericData.Fixed(avroSchema.getField("embedding").schema(), bytes)

    val originalRecord = new GenericData.Record(avroSchema)
    originalRecord.put("embedding", fixedField)

    val (catalystSchema, _) = HoodieSparkSchemaConverters.toSqlType(hoodieSchema)
    val deserializer = sparkAdapter.createAvroDeserializer(hoodieSchema, catalystSchema)
    val serializer = sparkAdapter.createAvroSerializer(catalystSchema, hoodieSchema, nullable = false)

    val row = deserializer.deserialize(originalRecord).get
    val deserializedRecord = serializer.serialize(row)

    assertEquals(originalRecord, deserializedRecord)
  }
}
