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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.FooterKeyDecryptionFactory;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HadoopDefaultResourceLoadCounter.countDefaultResourceLoads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link HoodieAvroParquetReader}.
 */
class TestHoodieAvroParquetReader {

  @TempDir
  java.nio.file.Path tmpDir;

  @Test
  void testReadDoesNotLoadHadoopDefaultResources() throws Throwable {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("file.parquet").toAbsolutePath().toString());
    List<GenericRecord> records = new HoodieTestDataGenerator(0xDEED).generateGenericRecords(20);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");
    HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("001", parquetPath, config, schema, new LocalTaskContextSupplier());
    for (GenericRecord record : records) {
      writer.writeAvro((String) record.get("_row_key"), record);
    }
    writer.close();

    List<String> readKeys = new ArrayList<>();
    int loads = countDefaultResourceLoads(() -> {
      try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath);
           ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(schema)) {
        iterator.forEachRemaining(record -> readKeys.add(((GenericRecord) record).get("_row_key").toString()));
      }
    });
    assertEquals(records.stream().map(record -> record.get("_row_key").toString()).collect(Collectors.toList()), readKeys);
    assertEquals(0, loads, "Reading a parquet file must reuse the storage configuration");
  }

  @Test
  void testReadEncryptedFileResolvesDecryptionWithFilePath() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    storage.getConf().set(FooterKeyDecryptionFactory.CRYPTO_FACTORY_CLASS, FooterKeyDecryptionFactory.class.getName());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("encrypted.parquet").toAbsolutePath().toString());
    FooterKeyDecryptionFactory.writeEncryptedFile(storage.getConf().unwrapAs(Configuration.class), new Path(parquetPath.toUri()), 10);
    FooterKeyDecryptionFactory.drainRequestedPaths();

    List<String> readKeys = new ArrayList<>();
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath);
         ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieSchema.fromAvroSchema(FooterKeyDecryptionFactory.SCHEMA))) {
      iterator.forEachRemaining(record -> readKeys.add(((GenericRecord) record).get("_row_key").toString()));
    }
    assertEquals(IntStream.range(0, 10).mapToObj(i -> "key" + i).collect(Collectors.toList()), readKeys);
    List<Path> requestedPaths = FooterKeyDecryptionFactory.drainRequestedPaths();
    assertFalse(requestedPaths.isEmpty());
    requestedPaths.forEach(requested -> assertEquals("encrypted.parquet", requested == null ? null : requested.getName()));
  }
}
