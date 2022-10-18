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

package org.apache.hudi.common.model

import org.apache.hudi.HoodieInternalRowUtils
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.commmon.model.HoodieSparkRecord
import org.apache.hudi.common.model.TestHoodieSparkRecord.{cloneUsingKryo, toUnsafeRow}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.addMetaFields
import org.apache.spark.sql.types.{Decimal, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

class TestHoodieSparkRecord {

  private val rowSchema = StructType.fromDDL("a INT, b STRING, c DATE, d TIMESTAMP, e STRUCT<a: DECIMAL(3, 2)>")

  @Test
  def testSerialization(): Unit = {
    def routine(row: InternalRow, schema: StructType, serializedSize: Int): Unit = {
      val record = row match {
        case ur: UnsafeRow => new HoodieSparkRecord(ur)
        case _ => new HoodieSparkRecord(row, schema)
      }

      // Step 1: Serialize/de- original [[HoodieSparkRecord]]
      val (cloned, originalBytes) = cloneUsingKryo(record)

      assertEquals(serializedSize, originalBytes.length)
      // NOTE: That in case when original row isn't an instance of [[UnsafeRow]]
      //       it would be
      //         - Projected into [[UnsafeRow]] (prior to serialization by Kryo)
      //         - Re-constructed as [[UnsafeRow]]
      row match {
        case _: UnsafeRow => assertEquals(record, cloned)
        case _ =>
          val convertedRecord = new HoodieSparkRecord(toUnsafeRow(row, schema))
          assertEquals(convertedRecord, cloned)
      }

      // Step 2: Serialize the already cloned record, and assert that ser/de loop is lossless
      val (_, clonedBytes) = cloneUsingKryo(cloned)
      assertEquals(ByteBuffer.wrap(originalBytes), ByteBuffer.wrap(clonedBytes))
    }

    val row = Row(1, "test", Date.valueOf(LocalDate.of(2022, 10, 1)),
      Timestamp.from(Instant.parse("2022-10-01T23:59:59.00Z")), Row(Decimal.apply(123, 3, 2)))

    val unsafeRow: UnsafeRow = toUnsafeRow(row, rowSchema)
    val hoodieInternalRow = new HoodieInternalRow(new Array[UTF8String](5), unsafeRow, false)

    Seq(
      (unsafeRow, rowSchema, 135),
      (hoodieInternalRow, addMetaFields(rowSchema), 175)
    ) foreach { case (row, schema, expectedSize) => routine(row, schema, expectedSize) }
  }
}

object TestHoodieSparkRecord {

  private def cloneUsingKryo(r: HoodieSparkRecord): (HoodieSparkRecord, Array[Byte]) = {
    val serializer = SerializerSupport.newSerializer(true)

    val buf = serializer.serialize(r)
    val cloned: HoodieSparkRecord = serializer.deserialize(buf)

    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)

    (cloned, bytes)
  }

  private def toUnsafeRow(row: InternalRow, schema: StructType): UnsafeRow = {
    val project = HoodieInternalRowUtils.getCachedUnsafeProjection(schema, schema)
    project(row)
  }

  private def toUnsafeRow(row: Row, schema: StructType): UnsafeRow = {
    val encoder = RowEncoder(schema).resolveAndBind()
    val internalRow = encoder.toRow(row)
    internalRow.asInstanceOf[UnsafeRow]
  }

}
