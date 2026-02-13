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

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.{HoodieAvroIndexedRecord, HoodieFileFormat, HoodieKey, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

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
class TestBlobSupport extends HoodieClientTestBase {
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
      val data = row.getStruct(row.fieldIndex("data"))
      val reference = data.getStruct(data.fieldIndex("reference"))
      val filePath = reference.getString(reference.fieldIndex("file"))
      assertTrue(filePath.endsWith("file2.bin"))
    }

    // Verify SQL read_blob() function
    table.createOrReplaceTempView("hudi_table_view")
    val sqlRows = sparkSession.sql("SELECT id, value, read_blob(data) as full_bytes from hudi_table_view").collectAsList()
    assertEquals(10, sqlRows.size())
  }

  private def createTestRecords(filePath: String): Seq[HoodieRecord[IndexedRecord]] = {
    (0 until 10).map { i =>
      val id = s"id_$i"
      val key = new HoodieKey(id, "")

      val fileReference = new GenericData.Record(
        SCHEMA.getField("data").get.schema.getField("reference").get.getNonNullSchema.toAvroSchema)
      fileReference.put("file", filePath)
      fileReference.put("position", i * 100)
      fileReference.put("length", 100)
      fileReference.put("managed", false)

      val blobRecord = new GenericData.Record(SCHEMA.getField("data").get.schema.toAvroSchema)
      blobRecord.put("storage_type", "out_of_line")
      blobRecord.put("reference", fileReference)

      val record = new GenericData.Record(SCHEMA.toAvroSchema)
      record.put("id", id)
      record.put("value", i)
      record.put("data", blobRecord)

      new HoodieAvroIndexedRecord(key, record)
    }
  }
}
