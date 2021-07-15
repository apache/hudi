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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.io.storage.HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieOrcReaderWriter {
  private final Path filePath = new Path(System.getProperty("java.io.tmpdir") + "/f1_1-0-1_000.orc");

  @BeforeEach
  @AfterEach
  public void clearTempFile() {
    File file = new File(filePath.toString());
    if (file.exists()) {
      file.delete();
    }
  }

  private HoodieOrcWriter createOrcWriter(Schema avroSchema) throws Exception {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    Configuration conf = new Configuration();
    int orcStripSize = Integer.parseInt(HoodieStorageConfig.DEFAULT_ORC_STRIPE_SIZE);
    int orcBlockSize = Integer.parseInt(HoodieStorageConfig.DEFAULT_ORC_BLOCK_SIZE);
    int maxFileSize = Integer.parseInt(HoodieStorageConfig.DEFAULT_ORC_FILE_MAX_BYTES);
    HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB, orcStripSize, orcBlockSize, maxFileSize, filter);
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    String instantTime = "000";
    return new HoodieOrcWriter(instantTime, filePath, config, avroSchema, mockTaskContextSupplier);
  }

  @Test
  public void testWriteReadMetadata() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    for (int i = 0; i < 3; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", "key" + i);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      writer.writeAvro("key" + i, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    Reader orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
    assertEquals(4, orcReader.getMetadataKeys().size());
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_MIN_RECORD_KEY_FOOTER));
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_MAX_RECORD_KEY_FOOTER));
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
    assertTrue(orcReader.getMetadataKeys().contains(AVRO_SCHEMA_METADATA_KEY));
    assertEquals(CompressionKind.ZLIB.name(), orcReader.getCompressionKind().toString());

    HoodieFileReader<GenericRecord> hoodieReader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    BloomFilter filter = hoodieReader.readBloomFilter();
    for (int i = 0; i < 3; i++) {
      assertTrue(filter.mightContain("key" + i));
    }
    assertFalse(filter.mightContain("non-existent-key"));
    assertEquals(3, hoodieReader.getTotalRecords());
    String[] minMaxRecordKeys = hoodieReader.readMinMaxRecordKeys();
    assertEquals(2, minMaxRecordKeys.length);
    assertEquals("key0", minMaxRecordKeys[0]);
    assertEquals("key2", minMaxRecordKeys[1]);
  }

  @Test
  public void testWriteReadPrimitiveRecord() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    for (int i = 0; i < 3; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", "key" + i);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      writer.writeAvro("key" + i, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    Reader orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
    assertEquals("struct<_row_key:string,time:string,number:int>", orcReader.getSchema().toString());
    assertEquals(3, orcReader.getNumberOfRows());

    HoodieFileReader<GenericRecord> hoodieReader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    Iterator<GenericRecord> iter = hoodieReader.getRecordIterator();
    int index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      index++;
    }
  }

  @Test
  public void testWriteReadComplexRecord() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchemaWithUDT.avsc");
    Schema udtSchema = avroSchema.getField("driver").schema().getTypes().get(1);
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    for (int i = 0; i < 3; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", "key" + i);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      GenericRecord innerRecord = new GenericData.Record(udtSchema);
      innerRecord.put("driver_name", "driver" + i);
      innerRecord.put("list", Collections.singletonList(i));
      innerRecord.put("map", Collections.singletonMap("key" + i, "value" + i));
      record.put("driver", innerRecord);
      writer.writeAvro("key" + i, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
    assertEquals("struct<_row_key:string,time:string,number:int,driver:struct<driver_name:string,list:array<int>,map:map<string,string>>>",
        reader.getSchema().toString());
    assertEquals(3, reader.getNumberOfRows());

    HoodieFileReader<GenericRecord> hoodieReader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    Iterator<GenericRecord> iter = hoodieReader.getRecordIterator();
    int index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      GenericRecord innerRecord = (GenericRecord) record.get("driver");
      assertEquals("driver" + index, innerRecord.get("driver_name").toString());
      assertEquals(1, ((List<?>)innerRecord.get("list")).size());
      assertEquals(index, ((List<?>)innerRecord.get("list")).get(0));
      assertEquals("value" + index, ((Map<?,?>)innerRecord.get("map")).get("key" + index).toString());
      index++;
    }
  }

  @Test
  public void testWriteReadWithEvolvedSchema() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    for (int i = 0; i < 3; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", "key" + i);
      record.put("time", Integer.toString(i));
      record.put("number", i);
      writer.writeAvro("key" + i, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    HoodieFileReader<GenericRecord> hoodieReader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    Schema evolvedSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleEvolvedSchema.avsc");
    Iterator<GenericRecord> iter = hoodieReader.getRecordIterator(evolvedSchema);
    int index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      assertNull(record.get("added_field"));
      index++;
    }

    evolvedSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleEvolvedSchemaChangeOrder.avsc");
    iter = hoodieReader.getRecordIterator(evolvedSchema);
    index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      assertNull(record.get("added_field"));
      index++;
    }

    evolvedSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleEvolvedSchemaColumnRequire.avsc");
    iter = hoodieReader.getRecordIterator(evolvedSchema);
    index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(index, record.get("number"));
      assertNull(record.get("added_field"));
      index++;
    }

    evolvedSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleEvolvedSchemaColumnType.avsc");
    iter = hoodieReader.getRecordIterator(evolvedSchema);
    index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertEquals(Integer.toString(index), record.get("number").toString());
      assertNull(record.get("added_field"));
      index++;
    }

    evolvedSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleEvolvedSchemaDeleteColumn.avsc");
    iter = hoodieReader.getRecordIterator(evolvedSchema);
    index = 0;
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      assertEquals("key" + index, record.get("_row_key").toString());
      assertEquals(Integer.toString(index), record.get("time").toString());
      assertNull(record.get("number"));
      assertNull(record.get("added_field"));
      index++;
    }
  }
}
