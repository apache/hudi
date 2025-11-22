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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getHoodieSchemaFromResource;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract class for unit tests of {@link HoodieFileReader} and {@link HoodieFileWriter}
 * for different file format
 */
public abstract class TestHoodieReaderWriterBase {
  protected static final int NUM_RECORDS = 50;
  @TempDir
  protected File tempDir;

  protected abstract StoragePath getFilePath();

  protected abstract HoodieAvroFileWriter createWriter(
      Schema avroSchema, boolean populateMetaFields) throws Exception;

  protected abstract HoodieAvroFileReader createReader(
      HoodieStorage storage) throws Exception;

  protected abstract void verifyMetadata(HoodieStorage storage) throws IOException;

  protected abstract void verifySchema(HoodieStorage storage, String schemaPath) throws IOException;

  @BeforeEach
  @AfterEach
  public void clearTempFile() {
    File file = new File(getFilePath().toString());
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testWriteReadMetadata() throws Exception {
    HoodieSchema hoodieSchema = getHoodieSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    writeFileWithSimpleSchema();

    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    verifyMetadata(storage);

    try (HoodieAvroFileReader hoodieReader = createReader(storage)) {
      BloomFilter filter = hoodieReader.readBloomFilter();
      for (int i = 0; i < NUM_RECORDS; i++) {
        String key = "key" + String.format("%02d", i);
        assertTrue(filter.mightContain(key));
      }
      assertFalse(filter.mightContain("non-existent-key"));
      assertEquals(hoodieSchema, hoodieReader.getSchema());
      assertEquals(NUM_RECORDS, hoodieReader.getTotalRecords());
      String[] minMaxRecordKeys = hoodieReader.readMinMaxRecordKeys();
      assertEquals(2, minMaxRecordKeys.length);
      assertEquals("key00", minMaxRecordKeys[0]);
      assertEquals("key" + (NUM_RECORDS - 1), minMaxRecordKeys[1]);
    }
  }

  @Test
  public void testWriteReadPrimitiveRecord() throws Exception {
    String schemaPath = "/exampleSchema.avsc";
    writeFileWithSimpleSchema();

    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    verifyMetadata(storage);
    verifySchema(storage, schemaPath);
    verifySimpleRecords(createReader(storage).getRecordIterator());
  }

  @Test
  public void testWriteReadComplexRecord() throws Exception {
    String schemaPath = "/exampleSchemaWithUDT.avsc";
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, schemaPath);
    Schema udtSchema = avroSchema.getField("driver").schema().getTypes().get(1);
    HoodieAvroFileWriter writer = createWriter(avroSchema, true);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = "key" + String.format("%02d", i);
      record.put("_row_key", key);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      GenericRecord innerRecord = new GenericData.Record(udtSchema);
      innerRecord.put("driver_name", "driver" + i);
      innerRecord.put("list", Collections.singletonList(i));
      innerRecord.put("map", Collections.singletonMap(key, "value" + i));
      record.put("driver", innerRecord);
      writer.writeAvro(key, record);
    }
    writer.close();

    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    verifyMetadata(storage);
    verifySchema(storage, schemaPath);
    verifyComplexRecords(createReader(storage).getRecordIterator());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/exampleEvolvedSchema.avsc",
      "/exampleEvolvedSchemaChangeOrder.avsc",
      "/exampleEvolvedSchemaColumnType.avsc",
      "/exampleEvolvedSchemaDeleteColumn.avsc"
  })
  public void testWriteReadWithEvolvedSchema(String evolvedSchemaPath) throws Exception {
    writeFileWithSimpleSchema();
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    try (HoodieAvroFileReader hoodieReader = createReader(storage)) {
      verifyReaderWithSchema(evolvedSchemaPath, hoodieReader);
    }
  }

  @Test
  public void testReaderFilterRowKeys() throws Exception {
    writeFileWithSchemaWithMeta();
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    verifyMetadata(storage);
    verifyFilterRowKeys(createReader(storage));
  }

  protected void writeFileWithSimpleSchema() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    HoodieAvroFileWriter writer = createWriter(avroSchema, true);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = "key" + String.format("%02d", i);
      record.put("_row_key", key);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      HoodieRecord avroRecord = new HoodieAvroIndexedRecord(record);
      writer.write(key, avroRecord, HoodieSchema.fromAvroSchema(avroSchema));
    }
    writer.close();
  }

  private void writeFileWithSchemaWithMeta() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieAvroFileWriter writer = createWriter(avroSchema, true);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = "key" + String.format("%02d", i);
      record.put("_row_key", key);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      writer.writeAvroWithMetadata(new HoodieKey((String) record.get("_row_key"),
          Integer.toString((Integer) record.get("number"))), record);
    }
    writer.close();
  }

  protected void verifySimpleRecords(Iterator<HoodieRecord<IndexedRecord>> iterator) {
    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = (GenericRecord) iterator.next().getData();
      String key = "key" + String.format("%02d", index);
      assertEquals(key, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      index++;
    }
  }

  private void verifyComplexRecords(Iterator<HoodieRecord<IndexedRecord>> iterator) {
    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = (GenericRecord) iterator.next().getData();
      String key = "key" + String.format("%02d", index);
      assertEquals(key, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      GenericRecord innerRecord = (GenericRecord) record.get("driver");
      assertEquals("driver" + index, innerRecord.get("driver_name").toString());
      assertEquals(1, ((List<?>) innerRecord.get("list")).size());
      assertEquals(index, ((List<?>) innerRecord.get("list")).get(0));
      Map<?, ?> mapping = (Map<?, ?>) innerRecord.get("map");
      boolean match = false;
      for (Object innerKey : mapping.keySet()) {
        // The innerKey may not be in the type of String, so we have to
        // use the following logic for validation
        if (innerKey.toString().equals(key)) {
          assertEquals("value" + index, mapping.get(innerKey).toString());
          match = true;
        }
      }
      assertTrue(match);
      index++;
    }
  }

  private void verifyFilterRowKeys(HoodieAvroFileReader hoodieReader) {
    Set<String> candidateRowKeys = IntStream.range(40, NUM_RECORDS * 2)
        .mapToObj(i -> "key" + String.format("%02d", i)).collect(Collectors.toCollection(TreeSet::new));
    Set<Pair<String, Long>> expectedKeys = IntStream.range(40, NUM_RECORDS)
        .mapToObj(i -> {
          // record position is not written for HFile format
          long position = hoodieReader instanceof HoodieAvroHFileReaderImplBase
              ? HoodieRecordLocation.INVALID_POSITION : (long) i;
          String key = "key" + String.format("%02d", i);
          return Pair.of(key, position);
        }).collect(Collectors.toSet());
    assertEquals(expectedKeys, hoodieReader.filterRowKeys(candidateRowKeys));
  }

  private void verifyReaderWithSchema(String schemaPath, HoodieAvroFileReader hoodieReader) throws IOException {
    Schema evolvedSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, schemaPath);
    Iterator<HoodieRecord<IndexedRecord>> iter = hoodieReader.getRecordIterator(HoodieSchema.fromAvroSchema(evolvedSchema));
    int index = 0;
    while (iter.hasNext()) {
      verifyRecord(schemaPath, (GenericRecord) iter.next().getData(), index);
      index++;
    }
  }

  private void verifyRecord(String schemaPath, GenericRecord record, int index) {
    String numStr = String.format("%02d", index);
    assertEquals("key" + numStr, record.get("_row_key").toString());
    assertEquals(Integer.toString(index), record.get("time").toString());
    if (schemaPath.equals("/exampleEvolvedSchemaColumnType.avsc")) {
      assertEquals(Integer.toString(index), record.get("number").toString());
      assertNull(record.getSchema().getField("added_field"));
    } else if (schemaPath.equals("/exampleEvolvedSchemaDeleteColumn.avsc")) {
      assertNull(record.getSchema().getField("number"));
      assertNull(record.getSchema().getField("added_field"));
    } else {
      assertEquals(index, record.get("number"));
      assertNull(record.get("added_field"));
    }
  }
}
