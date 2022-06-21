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

package org.apache.hudi.io.storage;

import org.apache.avro.AvroRuntimeException;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  protected abstract Path getFilePath();

  protected abstract HoodieFileWriter<GenericRecord> createWriter(
      Schema avroSchema, boolean populateMetaFields) throws Exception;

  protected abstract HoodieFileReader<GenericRecord> createReader(
      Configuration conf) throws Exception;

  protected abstract void verifyMetadata(Configuration conf) throws IOException;

  protected abstract void verifySchema(Configuration conf, String schemaPath) throws IOException;

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
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    writeFileWithSimpleSchema();

    Configuration conf = new Configuration();
    verifyMetadata(conf);

    HoodieFileReader<GenericRecord> hoodieReader = createReader(conf);
    BloomFilter filter = hoodieReader.readBloomFilter();
    for (int i = 0; i < NUM_RECORDS; i++) {
      String key = "key" + String.format("%02d", i);
      assertTrue(filter.mightContain(key));
    }
    assertFalse(filter.mightContain("non-existent-key"));
    assertEquals(avroSchema, hoodieReader.getSchema());
    assertEquals(NUM_RECORDS, hoodieReader.getTotalRecords());
    String[] minMaxRecordKeys = hoodieReader.readMinMaxRecordKeys();
    assertEquals(2, minMaxRecordKeys.length);
    assertEquals("key00", minMaxRecordKeys[0]);
    assertEquals("key" + (NUM_RECORDS - 1), minMaxRecordKeys[1]);
  }

  @Test
  public void testWriteReadPrimitiveRecord() throws Exception {
    String schemaPath = "/exampleSchema.avsc";
    writeFileWithSimpleSchema();

    Configuration conf = new Configuration();
    verifyMetadata(conf);
    verifySchema(conf, schemaPath);
    verifySimpleRecords(createReader(conf).getRecordIterator());
  }

  @Test
  public void testWriteReadComplexRecord() throws Exception {
    String schemaPath = "/exampleSchemaWithUDT.avsc";
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, schemaPath);
    Schema udtSchema = avroSchema.getField("driver").schema().getTypes().get(1);
    HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, true);
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

    Configuration conf = new Configuration();
    verifyMetadata(conf);
    verifySchema(conf, schemaPath);
    verifyComplexRecords(createReader(conf).getRecordIterator());
  }

  @Test
  public void testWriteReadWithEvolvedSchema() throws Exception {
    writeFileWithSimpleSchema();

    Configuration conf = new Configuration();
    HoodieFileReader<GenericRecord> hoodieReader = createReader(conf);
    String[] schemaList = new String[] {
        "/exampleEvolvedSchema.avsc", "/exampleEvolvedSchemaChangeOrder.avsc",
        "/exampleEvolvedSchemaColumnRequire.avsc", "/exampleEvolvedSchemaColumnType.avsc",
        "/exampleEvolvedSchemaDeleteColumn.avsc"};

    for (String evolvedSchemaPath : schemaList) {
      verifyReaderWithSchema(evolvedSchemaPath, hoodieReader);
    }
  }

  @Test
  public void testReaderFilterRowKeys() throws Exception {
    writeFileWithSchemaWithMeta();
    Configuration conf = new Configuration();
    verifyMetadata(conf);
    verifyFilterRowKeys(createReader(conf));
  }

  protected void writeFileWithSimpleSchema() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
    HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, true);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = "key" + String.format("%02d", i);
      record.put("_row_key", key);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      writer.writeAvro(key, record);
    }
    writer.close();
  }

  protected void writeFileWithSchemaWithMeta() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieFileWriter<GenericRecord> writer = createWriter(avroSchema, true);
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

  protected void verifySimpleRecords(Iterator<GenericRecord> iterator) {
    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
      String key = "key" + String.format("%02d", index);
      assertEquals(key, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      index++;
    }
  }

  protected void verifyComplexRecords(Iterator<GenericRecord> iterator) {
    int index = 0;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
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

  private void verifyFilterRowKeys(HoodieFileReader<GenericRecord> hoodieReader) {
    Set<String> candidateRowKeys = IntStream.range(40, NUM_RECORDS * 2)
        .mapToObj(i -> "key" + String.format("%02d", i)).collect(Collectors.toCollection(TreeSet::new));
    List<String> expectedKeys = IntStream.range(40, NUM_RECORDS)
        .mapToObj(i -> "key" + String.format("%02d", i)).sorted().collect(Collectors.toList());
    assertEquals(expectedKeys, hoodieReader.filterRowKeys(candidateRowKeys)
        .stream().sorted().collect(Collectors.toList()));
  }

  private void verifyReaderWithSchema(String schemaPath, HoodieFileReader<GenericRecord> hoodieReader) throws IOException {
    Schema evolvedSchema = getSchemaFromResource(TestHoodieReaderWriterBase.class, schemaPath);
    Iterator<GenericRecord> iter = hoodieReader.getRecordIterator(evolvedSchema);
    int index = 0;
    while (iter.hasNext()) {
      verifyRecord(schemaPath, iter.next(), index);
      index++;
    }
  }

  private void verifyRecord(String schemaPath, GenericRecord record, int index) {
    String numStr = String.format("%02d", index);
    assertEquals("key" + numStr, record.get("_row_key").toString());
    assertEquals(Integer.toString(index), record.get("time").toString());
    if ("/exampleEvolvedSchemaColumnType.avsc".equals(schemaPath)) {
      assertEquals(Integer.toString(index), record.get("number").toString());
    } else if ("/exampleEvolvedSchemaDeleteColumn.avsc".equals(schemaPath)) {
      assertIfFieldExistsInRecord(record, "number");
    } else {
      assertEquals(index, record.get("number"));
    }
    assertIfFieldExistsInRecord(record, "added_field");
  }

  private void assertIfFieldExistsInRecord(GenericRecord record, String field) {
    try {
      assertNull(record.get(field));
    } catch (AvroRuntimeException e) {
      assertEquals("Not a valid schema field: " + field, e.getMessage());
    }
  }
}
