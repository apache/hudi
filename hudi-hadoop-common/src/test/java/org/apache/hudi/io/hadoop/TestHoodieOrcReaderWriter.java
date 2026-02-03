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

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.io.storage.hadoop.HoodieAvroOrcWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.apache.hudi.io.storage.HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestHoodieOrcReaderWriter extends TestHoodieReaderWriterBase {

  @Override
  protected StoragePath getFilePath() {
    return new StoragePath(tempDir.toString() + "/f1_1-0-1_000.orc");
  }

  @Override
  protected HoodieAvroOrcWriter createWriter(
      HoodieSchema schema, boolean populateMetaFields) throws Exception {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();
    int orcStripSize = Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue());
    int orcBlockSize = Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue());
    int maxFileSize = Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue());
    HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB, orcStripSize, orcBlockSize, maxFileSize, filter);
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = Mockito.mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);
    String instantTime = "000";
    return new HoodieAvroOrcWriter(instantTime, getFilePath(), config, schema, mockTaskContextSupplier);
  }

  @Override
  protected HoodieAvroFileReader createReader(
      HoodieStorage storage) throws Exception {
    return (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, getFilePath());
  }

  @Override
  protected void verifyMetadata(HoodieStorage storage) throws IOException {
    Reader orcReader = OrcFile.createReader(
        new Path(getFilePath().toUri()), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)));
    assertEquals(4, orcReader.getMetadataKeys().size());
    assertTrue(orcReader.getMetadataKeys().contains(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER));
    assertTrue(orcReader.getMetadataKeys().contains(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER));
    assertTrue(orcReader.getMetadataKeys().contains(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
    assertTrue(orcReader.getMetadataKeys().contains(AVRO_SCHEMA_METADATA_KEY));
    assertEquals(CompressionKind.ZLIB.name(), orcReader.getCompressionKind().toString());
    assertEquals(NUM_RECORDS, orcReader.getNumberOfRows());
  }

  @Override
  protected void verifySchema(HoodieStorage storage, String schemaPath) throws IOException {
    Reader orcReader = OrcFile.createReader(
        new Path(getFilePath().toUri()), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)));
    if ("/exampleSchema.avsc".equals(schemaPath)) {
      assertEquals("struct<_row_key:string,time:string,number:int>",
          orcReader.getSchema().toString());
    } else if ("/exampleSchemaWithUDT.avsc".equals(schemaPath)) {
      assertEquals("struct<_row_key:string,time:string,number:int,driver:struct<driver_name:string,list:array<int>,map:map<string,string>>>",
          orcReader.getSchema().toString());
    }
  }
}
