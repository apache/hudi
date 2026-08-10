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

package org.apache.hudi.blob

import org.apache.hudi.SparkDatasetMixin
import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.{HoodieAvroIndexedRecord, HoodieFileFormat, HoodieKey, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.nio.ByteBuffer
import java.util.{Arrays, Properties}

import scala.collection.JavaConverters._

/**
 * End-to-end test for blob support including schema creation, multi-commit writes,
 * DataFrame reads, and SQL blob retrieval.
 *
 * This test validates:
 * <ul>
 *   <li>Creating tables with blob columns</li>
 *   <li>Writing blob records with out-of-line references</li>
 *   <li>Multi-commit operations (insert + upsert)</li>
 *   <li>Reading blob references via DataFrame</li>
 *   <li>SQL read_blob() function integration</li>
 * </ul>
 */
class TestBlobSupport extends HoodieClientTestBase with SparkDatasetMixin {
  val SCHEMA: HoodieSchema = HoodieSchema.createRecord("test_blobs", null, null, Arrays.asList(
    HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
    HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT), null, null),
    HoodieSchemaField.of("data", HoodieSchema.createBlob(), null, null)
  ))

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testEndToEnd(tableType: HoodieTableType): Unit = {
    val filePath1 = createTestFile(tempDir, "file1.bin", 1000)
    val filePath2 = createTestFile(tempDir, "file2.bin", 1000)

    val properties = new Properties()
    properties.put("hoodie.datasource.write.recordkey.field", "id")
    properties.put("hoodie.datasource.write.partitionpath.field", "")
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id")
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "")
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.PARQUET.toString)

    // Initialize table
    HoodieTableMetaClient.newTableBuilder()
      .setTableName("test_blob_table")
      .setTableType(tableType)
      .fromProperties(properties)
      .initTable(storageConf, basePath)

    var client: SparkRDDWriteClient[IndexedRecord] = null
    val config = getConfigBuilder(SCHEMA.toString())
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(HoodieIndex.IndexType.SIMPLE).build)
      .build()
    try {
      client = getHoodieWriteClient(config).asInstanceOf[SparkRDDWriteClient[IndexedRecord]]

      // First commit - insert
      val commit1 = client.startCommit()
      val firstBatch = createTestRecords(filePath1)
      val statuses1 = client.insert(jsc.parallelize(firstBatch.asJava, 1), commit1).collect()
      client.commit(commit1, jsc.parallelize(statuses1, 1))

      // Second commit - upsert
      val commit2 = client.startCommit()
      val secondBatch = createTestRecords(filePath2)
      val statuses2 = client.upsert(jsc.parallelize(secondBatch.asJava, 1), commit2).collect()
      client.commit(commit2, jsc.parallelize(statuses2, 1))
    } finally {
      if (client != null) client.close()
    }

    // Read and verify DataFrame results
    val table = sparkSession.read.format("hudi").load(basePath)
    val rows = table.collectAsList()
    assertEquals(10, rows.size())

    rows.asScala.foreach { row =>
      val i = row.getInt(row.fieldIndex("value"))
      val data = row.getStruct(row.fieldIndex("data"))
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE,
        data.getString(data.fieldIndex(HoodieSchema.Blob.TYPE)))
      assertTrue(data.isNullAt(data.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)))
      val reference = data.getStruct(data.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE))
      val filePath = reference.getString(reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH))
      assertTrue(filePath.endsWith("file2.bin"))
      assertEquals(i * 100L,
        reference.getLong(reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET)))
      assertEquals(100L,
        reference.getLong(reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH)))
      assertEquals(false,
        reference.getBoolean(reference.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED)))
    }

    // Verify SQL read_blob() returns bytes matching the referenced file region.
    table.createOrReplaceTempView("hudi_table_view")
    val sqlRows = sparkSession.sql(
      "SELECT id, value, read_blob(data) as full_bytes from hudi_table_view ORDER BY value")
      .collectAsList()
    assertEquals(10, sqlRows.size())
    sqlRows.asScala.foreach { row =>
      val i = row.getInt(row.fieldIndex("value"))
      val bytes = row.getAs[Array[Byte]]("full_bytes")
      assertEquals(100, bytes.length)
      assertBytesContent(bytes, expectedOffset = i * 100)
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testEndToEndInline(tableType: HoodieTableType): Unit = {
    val properties = new Properties()
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id")
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "")
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.PARQUET.toString)

    HoodieTableMetaClient.newTableBuilder()
      .setTableName("test_inline_blob_table")
      .setTableType(tableType)
      .fromProperties(properties)
      .initTable(storageConf, basePath)

    var client: SparkRDDWriteClient[IndexedRecord] = null
    val config = getConfigBuilder(SCHEMA.toString())
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(HoodieIndex.IndexType.SIMPLE).build)
      .build()
    try {
      client = getHoodieWriteClient(config).asInstanceOf[SparkRDDWriteClient[IndexedRecord]]

      // First commit - insert ids 0..9 with payload prefix 0xA.
      val commit1 = client.startCommit()
      val firstBatch = createInlineTestRecords(0 until 10, payloadPrefix = 0xA.toByte)
      val statuses1 = client.insert(jsc.parallelize(firstBatch.asJava, 1), commit1).collect()
      client.commit(commit1, jsc.parallelize(statuses1, 1))

      // Second commit - upsert only ids 5..9 with payload prefix 0xB. This leaves ids 0..4
      // untouched (still 0xA) and forces the read side to correctly merge updated and
      // non-updated records — a pure full-overwrite upsert would not exercise that path.
      val commit2 = client.startCommit()
      val secondBatch = createInlineTestRecords(5 until 10, payloadPrefix = 0xB.toByte)
      val statuses2 = client.upsert(jsc.parallelize(secondBatch.asJava, 1), commit2).collect()
      client.commit(commit2, jsc.parallelize(statuses2, 1))
    } finally {
      if (client != null) client.close()
    }

    val table = sparkSession.read.format("hudi").load(basePath)
    val rows = table.collectAsList()
    assertEquals(10, rows.size())

    // Direct struct-field access — verifies INLINE bytes round-tripped
    // through Parquet and the data field is populated (reference null).
    // ids 0..4 retain the 0xA payload from commit1; ids 5..9 carry the
    // 0xB payload produced by the partial upsert in commit2.
    rows.asScala.foreach { row =>
      val data = row.getStruct(row.fieldIndex("data"))
      assertEquals(HoodieSchema.Blob.INLINE,
        data.getString(data.fieldIndex(HoodieSchema.Blob.TYPE)))
      assertTrue(data.isNullAt(data.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))
      val bytes = data.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD)
      val value = row.getInt(row.fieldIndex("value"))
      val expectedPrefix: Byte = if (value < 5) 0xA.toByte else 0xB.toByte
      assertArrayEquals(expectedInlinePayload(expectedPrefix, value), bytes)
    }

    // SQL read_blob() — INLINE passthrough through the planner/exec path.
    table.createOrReplaceTempView("hudi_inline_table_view")
    val sqlRows = sparkSession.sql(
      "SELECT id, value, read_blob(data) as full_bytes from hudi_inline_table_view ORDER BY value")
      .collectAsList()
    assertEquals(10, sqlRows.size())
    sqlRows.asScala.foreach { row =>
      val value = row.getInt(row.fieldIndex("value"))
      val bytes = row.getAs[Array[Byte]]("full_bytes")
      val expectedPrefix: Byte = if (value < 5) 0xA.toByte else 0xB.toByte
      assertArrayEquals(expectedInlinePayload(expectedPrefix, value), bytes)
    }
  }

  private def expectedInlinePayload(prefix: Byte, value: Int): Array[Byte] = {
    // Distinct per-record payload so upserts can be verified by content.
    Array[Byte](prefix, value.toByte, (value + 1).toByte, (value + 2).toByte)
  }

  private def createInlineTestRecords(ids: Range, payloadPrefix: Byte): Seq[HoodieRecord[IndexedRecord]] = {
    ids.map { i =>
      val id = s"id_$i"
      val key = new HoodieKey(id, "")

      val dataSchema = SCHEMA.getField("data").get.schema
      val blobRecord = new GenericData.Record(dataSchema.toAvroSchema)
      blobRecord.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(
        dataSchema.getField(HoodieSchema.Blob.TYPE).get.schema.toAvroSchema,
        HoodieSchema.Blob.INLINE))
      blobRecord.put(HoodieSchema.Blob.INLINE_DATA_FIELD,
        ByteBuffer.wrap(expectedInlinePayload(payloadPrefix, i)))
      // EXTERNAL_REFERENCE left null — the union default in HoodieSchema.Blob.

      val record = new GenericData.Record(SCHEMA.toAvroSchema)
      record.put("id", id)
      record.put("value", i)
      record.put("data", blobRecord)

      new HoodieAvroIndexedRecord(key, record)
    }
  }

  private def createTestRecords(filePath: String): Seq[HoodieRecord[IndexedRecord]] = {
    (0 until 10).map { i =>
      val id = s"id_$i"
      val key = new HoodieKey(id, "")

      val dataSchema = SCHEMA.getField("data").get.schema
      val fileReference = new GenericData.Record(dataSchema.getField(HoodieSchema.Blob.EXTERNAL_REFERENCE)
        .get.getNonNullSchema.toAvroSchema)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, filePath)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, i * 100L)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, 100L)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, false)

      val blobRecord = new GenericData.Record(dataSchema.toAvroSchema)
      blobRecord.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(dataSchema.getField(HoodieSchema.Blob.TYPE)
        .get.schema.toAvroSchema, HoodieSchema.Blob.OUT_OF_LINE))
      blobRecord.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, fileReference)

      val record = new GenericData.Record(SCHEMA.toAvroSchema)
      record.put("id", id)
      record.put("value", i)
      record.put("data", blobRecord)

      new HoodieAvroIndexedRecord(key, record)
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMixedInlineAndOutOfLine(tableType: HoodieTableType): Unit = {
    val filePath = createTestFile(tempDir, "mixed_file.bin", 1000)

    val properties = new Properties()
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id")
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "")
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.PARQUET.toString)

    HoodieTableMetaClient.newTableBuilder()
      .setTableName("test_mixed_blob_table")
      .setTableType(tableType)
      .fromProperties(properties)
      .initTable(storageConf, basePath)

    var client: SparkRDDWriteClient[IndexedRecord] = null
    val config = getConfigBuilder(SCHEMA.toString())
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(HoodieIndex.IndexType.SIMPLE).build)
      .build()
    try {
      client = getHoodieWriteClient(config).asInstanceOf[SparkRDDWriteClient[IndexedRecord]]
      val commit = client.startCommit()
      val records = (0 until 10).map { i =>
        val storageType = if (i % 2 == 0) HoodieSchema.Blob.INLINE else HoodieSchema.Blob.OUT_OF_LINE
        createMixedRecord(i, storageType, filePath, inlinePrefix = 0xC.toByte)
      }
      val statuses = client.insert(jsc.parallelize(records.asJava, 1), commit).collect()
      client.commit(commit, jsc.parallelize(statuses, 1))
    } finally {
      if (client != null) client.close()
    }

    val table = sparkSession.read.format("hudi").load(basePath)
    table.createOrReplaceTempView("hudi_mixed_table_view")
    val sqlRows = sparkSession.sql(
      "SELECT id, value, read_blob(data) as full_bytes from hudi_mixed_table_view ORDER BY value")
      .collectAsList()
    assertEquals(10, sqlRows.size())
    sqlRows.asScala.foreach { row =>
      val value = row.getInt(row.fieldIndex("value"))
      val bytes = row.getAs[Array[Byte]]("full_bytes")
      if (value % 2 == 0) {
        assertArrayEquals(expectedInlinePayload(0xC.toByte, value), bytes)
      } else {
        assertEquals(100, bytes.length)
        assertBytesContent(bytes, expectedOffset = value * 100)
      }
    }
  }

  private def createMixedRecord(
      value: Int,
      storageType: String,
      filePath: String,
      inlinePrefix: Byte): HoodieRecord[IndexedRecord] = {
    val id = s"id_$value"
    val key = new HoodieKey(id, "")
    val dataSchema = SCHEMA.getField("data").get.schema
    val blobRecord = new GenericData.Record(dataSchema.toAvroSchema)

    if (storageType == HoodieSchema.Blob.INLINE) {
      blobRecord.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(
        dataSchema.getField(HoodieSchema.Blob.TYPE).get.schema.toAvroSchema,
        HoodieSchema.Blob.INLINE))
      blobRecord.put(HoodieSchema.Blob.INLINE_DATA_FIELD,
        ByteBuffer.wrap(expectedInlinePayload(inlinePrefix, value)))
    } else {
      val fileReference = new GenericData.Record(dataSchema.getField(HoodieSchema.Blob.EXTERNAL_REFERENCE)
        .get.getNonNullSchema.toAvroSchema)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, filePath)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, value * 100L)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, 100L)
      fileReference.put(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, false)
      blobRecord.put(HoodieSchema.Blob.TYPE, new GenericData.EnumSymbol(
        dataSchema.getField(HoodieSchema.Blob.TYPE).get.schema.toAvroSchema,
        HoodieSchema.Blob.OUT_OF_LINE))
      blobRecord.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, fileReference)
    }

    val record = new GenericData.Record(SCHEMA.toAvroSchema)
    record.put("id", id)
    record.put("value", value)
    record.put("data", blobRecord)
    new HoodieAvroIndexedRecord(key, record)
  }
}
