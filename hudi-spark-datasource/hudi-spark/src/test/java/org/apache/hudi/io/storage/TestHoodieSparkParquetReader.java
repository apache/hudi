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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.FooterKeyDecryptionFactory;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.hudi.common.testutils.HadoopDefaultResourceLoadCounter.countDefaultResourceLoads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link HoodieSparkParquetReader}.
 */
class TestHoodieSparkParquetReader {

  private static final int NUM_RECORDS = 20;

  @TempDir
  java.nio.file.Path tmpDir;

  private HoodieStorage storage;
  private StoragePath parquetPath;
  private HoodieSchema schema;
  private List<GenericRecord> records;

  @BeforeEach
  void setUp() throws Exception {
    storage = HoodieTestUtils.getStorage(tmpDir.toString());
    parquetPath = new StoragePath(tmpDir.resolve("file.parquet").toAbsolutePath().toString());
    records = new HoodieTestDataGenerator(0xDEED).generateGenericRecords(NUM_RECORDS);
    schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");
    HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) HoodieFileWriterFactory.getFileWriter(
        "001", parquetPath, storage, config, schema, new LocalTaskContextSupplier(), HoodieRecord.HoodieRecordType.AVRO);
    for (GenericRecord record : records) {
      writer.writeAvro((String) record.get("_row_key"), record);
    }
    writer.close();
  }

  @Test
  void testReadDoesNotLoadHadoopDefaultResources() throws Throwable {
    int[] rowCount = new int[1];
    int loads = countDefaultResourceLoads(() -> rowCount[0] = readRows(Collections.emptyList()));
    assertEquals(NUM_RECORDS, rowCount[0]);
    assertEquals(0, loads, "Reading a parquet file must reuse the storage configuration");
  }

  @Test
  void testReadAppliesPushedDownFilters() throws Exception {
    String firstKey = (String) records.get(0).get("_row_key");
    // Row group statistics keep the matching row group, and drop it for a key above every UUID key.
    assertEquals(NUM_RECORDS, readRows(Collections.singletonList(new EqualTo("_row_key", firstKey))));
    assertEquals(0, readRows(Collections.singletonList(new EqualTo("_row_key", "~"))));
  }

  @Test
  void testReadEncryptedFileResolvesDecryptionWithFilePath() throws Exception {
    storage.getConf().set(FooterKeyDecryptionFactory.CRYPTO_FACTORY_CLASS, FooterKeyDecryptionFactory.class.getName());
    StoragePath encryptedPath = new StoragePath(tmpDir.resolve("encrypted.parquet").toAbsolutePath().toString());
    FooterKeyDecryptionFactory.writeEncryptedFile(storage.getConf().unwrapAs(Configuration.class), new Path(encryptedPath.toUri()), 10);
    FooterKeyDecryptionFactory.drainRequestedPaths();

    List<Long> readTs = new ArrayList<>();
    try (HoodieSparkParquetReader reader = (HoodieSparkParquetReader) new HoodieSparkFileReaderFactory(storage)
        .newParquetFileReader(encryptedPath);
         ClosableIterator<UnsafeRow> iterator = reader.getUnsafeRowIterator(HoodieSchema.fromAvroSchema(FooterKeyDecryptionFactory.SCHEMA))) {
      iterator.forEachRemaining(row -> readTs.add(row.getLong(1)));
    }
    assertEquals(LongStream.range(0, 10).boxed().collect(Collectors.toList()), readTs);
    List<Path> requestedPaths = FooterKeyDecryptionFactory.drainRequestedPaths();
    assertFalse(requestedPaths.isEmpty());
    requestedPaths.forEach(requested -> assertEquals("encrypted.parquet", requested == null ? null : requested.getName()));
  }

  private int readRows(List<Filter> filters) throws Exception {
    int count = 0;
    try (HoodieSparkParquetReader reader = (HoodieSparkParquetReader) new HoodieSparkFileReaderFactory(storage)
        .newParquetFileReader(parquetPath);
         ClosableIterator<UnsafeRow> iterator = reader.getUnsafeRowIterator(schema, filters)) {
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }
    }
    return count;
  }
}
