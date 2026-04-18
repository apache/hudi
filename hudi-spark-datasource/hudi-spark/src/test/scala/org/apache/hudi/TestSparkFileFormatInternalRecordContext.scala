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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.Test

import java.util.{Arrays => JArrays, Properties}

/**
 * Verifies the contract of [[SparkFileFormatInternalRecordContext.extractDataFromRecord]]
 * that replaced the legacy `IdentityHashMap[InternalRow, GenericRecord]` cache: the original
 * Avro record produced by Avro-payload-based paths (e.g. ExpressionPayload in SQL MERGE INTO)
 * must be propagated through [[org.apache.hudi.common.engine.ExtractedData]] when its schema
 * differs from the requested read schema, so downstream code can decode payload bytes with
 * the correct source schema instead of re-serializing through a possibly-mismatched schema.
 */
class TestSparkFileFormatInternalRecordContext {

  private val dataSchema: HoodieSchema = HoodieSchema.createRecord("TestRecord", null, null,
    JArrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
    ))

  private val writeSchemaWithMeta: HoodieSchema = HoodieSchema.createRecord("TestRecord", null, null,
    JArrays.asList(
      HoodieSchemaField.of("_hoodie_commit_time",
        HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
      HoodieSchemaField.of("_hoodie_record_key",
        HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
    ))

  private def newContext(): SparkFileFormatInternalRecordContext = SparkFileFormatInternalRecordContext()

  private def newAvroRecord(id: Long, name: String): GenericRecord =
    new GenericRecordBuilder(dataSchema.toAvroSchema)
      .set("id", id)
      .set("name", name)
      .build()

  @Test
  def testExtractDataPropagatesOriginalAvroForMismatchedSchema(): Unit = {
    val avroRecord = newAvroRecord(42L, "alice")
    val record = new HoodieAvroIndexedRecord(avroRecord)
    val ctx = newContext()

    val extracted = ctx.extractDataFromRecord(record, writeSchemaWithMeta, new Properties())

    assertNotNull(extracted.getData,
      "InternalRow projection of the avro payload should be present")
    val original = extracted.getOriginalAvroRecord
    assertNotNull(original,
      "originalAvroRecord must be propagated when payload schema differs from requested schema")
    assertEquals(dataSchema.toAvroSchema, original.getSchema,
      "Propagated avro record must retain its source (data) schema, not the requested write schema")
    assertEquals(42L, original.get("id"))
    assertEquals("alice", original.get("name").toString)
  }

  @Test
  def testExtractDataLeavesOriginalAvroNullWhenSchemaMatches(): Unit = {
    val avroRecord = newAvroRecord(7L, "bob")
    val record = new HoodieAvroIndexedRecord(avroRecord)
    val ctx = newContext()

    val extracted = ctx.extractDataFromRecord(record, dataSchema, new Properties())

    assertNotNull(extracted.getData)
    assertNull(extracted.getOriginalAvroRecord,
      "originalAvroRecord must be null when payload schema already matches the requested schema "
        + "— downstream code can re-serialize the InternalRow through the cached serializer")
  }

  @Test
  def testRoundTripExtractAndConvertPreservesFieldValues(): Unit = {
    val avroRecord = newAvroRecord(123L, "charlie")
    val record = new HoodieAvroIndexedRecord(avroRecord)
    val ctx = newContext()

    val extracted = ctx.extractDataFromRecord(record, dataSchema, new Properties())
    val roundTripped = ctx.convertToAvroRecord(extracted.getData, dataSchema)

    assertNotNull(roundTripped)
    assertEquals(dataSchema.toAvroSchema, roundTripped.getSchema)
    assertEquals(123L, roundTripped.get("id"))
    assertEquals("charlie", roundTripped.get("name").toString)
  }

  @Test
  def testExtractDataReturnsNullDataForNullPayload(): Unit = {
    val record = new HoodieAvroIndexedRecord(null.asInstanceOf[IndexedRecord])
    val ctx = newContext()

    val extracted = ctx.extractDataFromRecord(record, dataSchema, new Properties())

    assertTrue(extracted.getData == null,
      "Null payload must produce ExtractedData with null data and no originalAvroRecord")
    assertNull(extracted.getOriginalAvroRecord)
  }
}
