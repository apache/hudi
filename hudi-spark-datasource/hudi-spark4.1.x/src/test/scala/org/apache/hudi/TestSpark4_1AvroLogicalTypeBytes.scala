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
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.avro.HoodieSpark4_1AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.io.ByteArrayOutputStream

/**
 * Validates the storage-byte invariant under the Spark 4.1 profile, which pulls in Avro 1.12.
 *
 * The Avro on-wire encoding for `date`, `timestamp-millis`, and `timestamp-micros` logical types
 * is fixed by spec to int / long, and is identical across Avro 1.11.x and Avro 1.12. As long as
 * Hudi's Spark→Avro path (`HoodieSpark4_1AvroSerializer` → `AvroSerializer`) emits raw
 * `java.lang.Long` / `java.lang.Integer` into the `GenericRecord` — and never `java.time.Instant` /
 * `java.time.LocalDate` — the bytes Hudi writes on Spark 4.1 are bit-identical to what it writes
 * on Spark 3.5 / 4.0 (which use Avro 1.11.4), preserving forward/backward compatibility for
 * readers across profiles.
 *
 * This test pins that invariant down:
 *   1. The serializer outputs raw primitives in the GenericRecord (not java.time types).
 *   2. `GenericDatumWriter` produces the expected canonical bytes for known values.
 */
class TestSpark4_1AvroLogicalTypeBytes {

  // 2024-05-20T00:00:00.123456Z
  private val epochMicros: Long = 1716163200_000000L + 123456L
  private val epochMillis: Long = 1716163200_000L + 123L
  private val epochDay: Int = java.time.LocalDate.of(2024, 5, 20).toEpochDay.toInt

  private val avroSchema: Schema = new Schema.Parser().parse(
    """{"type":"record","name":"r","fields":[
      |{"name":"d","type":{"type":"int","logicalType":"date"}},
      |{"name":"ts_millis","type":{"type":"long","logicalType":"timestamp-millis"}},
      |{"name":"ts_micros","type":{"type":"long","logicalType":"timestamp-micros"}}
      |]}""".stripMargin)

  private val sparkSchema: StructType = StructType(Seq(
    StructField("d", DataTypes.DateType, nullable = false),
    StructField("ts_millis", DataTypes.TimestampType, nullable = false),
    StructField("ts_micros", DataTypes.TimestampType, nullable = false)
  ))

  // Catalyst representation: epoch-day (Int) for DateType, epoch-micros (Long) for TimestampType.
  // ts_millis is also passed as micros in catalyst — AvroSerializer converts to millis on write.
  private def serializeFixtureRow(): GenericRecord = {
    val serializer = new HoodieSpark4_1AvroSerializer(sparkSchema, avroSchema, nullable = false)
    serializer.serialize(InternalRow(epochDay, epochMillis * 1000L, epochMicros)).asInstanceOf[GenericRecord]
  }

  @Test
  def testSerializerEmitsPrimitivesNotJavaTime(): Unit = {
    val record = serializeFixtureRow()

    // The on-disk byte stability for cross-Spark-version compatibility hinges on the GenericRecord
    // holding the Avro 1.11.x primitive form, not the Avro 1.12 java.time form. If the serializer
    // ever started emitting Instants/LocalDates, Avro 1.12's GenericDatumWriter would route the
    // value through a Conversion before encoding. Even though the resulting bytes would still be
    // spec-compliant, that path is harder to reason about, so we pin the primitive contract here.
    assertTrue(record.get("d").isInstanceOf[java.lang.Integer],
      s"expected raw Integer for date logical type, got ${record.get("d").getClass}")
    assertTrue(record.get("ts_millis").isInstanceOf[java.lang.Long],
      s"expected raw Long for timestamp-millis logical type, got ${record.get("ts_millis").getClass}")
    assertTrue(record.get("ts_micros").isInstanceOf[java.lang.Long],
      s"expected raw Long for timestamp-micros logical type, got ${record.get("ts_micros").getClass}")

    assertEquals(epochDay, record.get("d"))
    assertEquals(epochMillis, record.get("ts_millis"))
    assertEquals(epochMicros, record.get("ts_micros"))
  }

  @Test
  def testEncodedBytesMatchAvroSpec(): Unit = {
    val baos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    val writer = new GenericDatumWriter[GenericRecord](avroSchema)
    writer.write(serializeFixtureRow(), encoder)
    encoder.flush()
    val bytes = baos.toByteArray

    // Compute the expected canonical bytes manually using Avro's zig-zag varlong encoding (spec).
    // This independent computation is independent of the running Avro version and proves that
    // whatever bytes Avro 1.12 writes here equal what Avro 1.11.x would write.
    val expected = new ByteArrayOutputStream()
    writeZigZagLong(expected, epochDay.toLong)
    writeZigZagLong(expected, epochMillis)
    writeZigZagLong(expected, epochMicros)
    assertEquals(expected.toByteArray.toSeq, bytes.toSeq,
      "encoded bytes must match the spec-defined Avro encoding regardless of the Avro version on the classpath")
  }

  // Avro's variable-length zig-zag long encoding, per the Avro spec — used here as an independent
  // reference so the test does not depend on any Avro classes for the expected-bytes computation.
  private def writeZigZagLong(out: ByteArrayOutputStream, value: Long): Unit = {
    var n = (value << 1) ^ (value >> 63)
    while ((n & ~0x7FL) != 0) {
      out.write(((n & 0x7F) | 0x80).toInt)
      n >>>= 7
    }
    out.write(n.toInt)
  }
}
