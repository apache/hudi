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
import org.apache.orc.RecordReader;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.io.storage.HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieOrcWriter {
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
  public void testWriteMetadata() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcWriter.class, "/exampleSchema.avsc");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("_row_key", "key1");
    record.put("time", "0");
    record.put("number", 1);
    writer.writeAvro("key1", record);
    record.put("_row_key", "key2");
    record.put("time", "2");
    record.put("number", null);
    writer.writeAvro("key2", record);
    writer.close();

    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
    assertEquals(2, reader.getNumberOfRows());
    assertEquals(4, reader.getMetadataKeys().size());
    assertTrue(reader.getMetadataKeys().contains(HOODIE_MIN_RECORD_KEY_FOOTER));
    ByteBuffer minRecordKey = reader.getMetadataValue(HOODIE_MIN_RECORD_KEY_FOOTER);
    assertEquals("key1", StandardCharsets.US_ASCII.decode(minRecordKey).toString());
    assertTrue(reader.getMetadataKeys().contains(HOODIE_MAX_RECORD_KEY_FOOTER));
    ByteBuffer maxRecordKey = reader.getMetadataValue(HOODIE_MAX_RECORD_KEY_FOOTER);
    assertEquals("key2", StandardCharsets.US_ASCII.decode(maxRecordKey).toString());
    assertTrue(reader.getMetadataKeys().contains(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
    ByteBuffer filterBuffer = reader.getMetadataValue(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    String filterStr = StandardCharsets.US_ASCII.decode(filterBuffer).toString();
    BloomFilter filter = BloomFilterFactory.fromString(filterStr, BloomFilterTypeCode.SIMPLE.name());
    assertTrue(filter.mightContain("key1"));
    assertTrue(filter.mightContain("key2"));
    assertFalse(filter.mightContain("non-existent-key"));
    assertTrue(reader.getMetadataKeys().contains(AVRO_SCHEMA_METADATA_KEY));
    ByteBuffer schemaBuffer = reader.getMetadataValue(AVRO_SCHEMA_METADATA_KEY);
    assertEquals(avroSchema.toString(), StandardCharsets.US_ASCII.decode(schemaBuffer).toString());
    assertEquals(CompressionKind.ZLIB.name(), reader.getCompressionKind().toString());
  }

  @Test
  public void testWritePrimitiveRecord() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcWriter.class, "/exampleSchema.avsc");
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("_row_key", "key1");
    record.put("time", "0");
    record.put("number", 1);
    writer.writeAvro("key1", record);
    record.put("_row_key", "key2");
    record.put("time", "1");
    record.put("number", null);
    writer.writeAvro("key2", record);
    writer.close();

    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
    assertEquals("struct<_row_key:string,time:string,number:int>", reader.getSchema().toString());
    assertEquals(2, reader.getNumberOfRows());
  }

  @Test
  public void testWriteComplexRecord() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcWriter.class, "/exampleSchemaWithUDT.avsc");
    Schema udtSchema = avroSchema.getField("driver").schema().getTypes().get(1);
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    GenericRecord record = new GenericData.Record(avroSchema);
    GenericRecord innerRecord = new GenericData.Record(udtSchema);
    record.put("_row_key", "key1");
    record.put("time", "0");
    record.put("number", 1);
    innerRecord.put("driver_name", "driver1");
    innerRecord.put("list", Collections.singletonList(1));
    innerRecord.put("map", Collections.singletonMap("key1", "value1"));
    record.put("driver", innerRecord);
    writer.writeAvro("key1", record);
    record.put("_row_key", "key2");
    record.put("time", "1");
    record.put("number", null);
    innerRecord.put("driver_name", "driver2");
    innerRecord.put("list", Collections.singletonList(2));
    innerRecord.put("map", Collections.singletonMap("key2", "value2"));
    record.put("driver", innerRecord);
    writer.writeAvro("key2", record);
    writer.close();

    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
    assertEquals("struct<_row_key:string,time:string,number:int,driver:struct<driver_name:string,list:array<int>,map:map<string,string>>>",
        reader.getSchema().toString());
    assertEquals(2, reader.getNumberOfRows());
  }

  @Test
  public void testWriteReadOrcFile() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcWriter.class, "/exampleSchemaWithUDT.avsc");
    Schema udtSchema = avroSchema.getField("driver").schema().getTypes().get(1);
    HoodieOrcWriter writer = createOrcWriter(avroSchema);
    GenericRecord record = new GenericData.Record(avroSchema);
    GenericRecord innerRecord = new GenericData.Record(udtSchema);
    record.put("_row_key", "key1");
    record.put("time", "0");
    record.put("number", 1);
    innerRecord.put("driver_name", "driver1");
    innerRecord.put("list", Collections.singletonList(1));
    innerRecord.put("map", Collections.singletonMap("key1", "value1"));
    record.put("driver", innerRecord);
    writer.writeAvro("key1", record);
    record.put("_row_key", "key2");
    record.put("time", "1");
    record.put("number", null);
    innerRecord.put("driver_name", "driver2");
    innerRecord.put("list", Collections.singletonList(2));
    innerRecord.put("map", Collections.singletonMap("key2", "value2"));
    record.put("driver", innerRecord);
    writer.writeAvro("key2", record);
    writer.close();

    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
    RecordReader records = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    records.nextBatch(batch);
    records.close();
    assertEquals(4, batch.numCols);
    assertEquals("key1", ((BytesColumnVector) batch.cols[0]).toString(0));
    assertEquals("key2", ((BytesColumnVector) batch.cols[0]).toString(1));
    assertEquals("0", ((BytesColumnVector) batch.cols[1]).toString(0));
    assertEquals("1", ((BytesColumnVector) batch.cols[1]).toString(1));
    assertEquals(1, ((LongColumnVector) batch.cols[2]).vector[0]);
    assertTrue(batch.cols[2].isNull[1]);
    StructColumnVector structColumnVector = (StructColumnVector) batch.cols[3];
    assertEquals(3, structColumnVector.fields.length);
    assertEquals("driver1", ((BytesColumnVector)structColumnVector.fields[0]).toString(0));
    assertEquals("driver2", ((BytesColumnVector)structColumnVector.fields[0]).toString(1));
  }
}
