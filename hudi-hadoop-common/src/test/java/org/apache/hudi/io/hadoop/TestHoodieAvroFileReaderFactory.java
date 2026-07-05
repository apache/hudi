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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.core.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.io.storage.hadoop.HoodieAvroOrcReader;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieFileReaderFactory}.
 */
public class TestHoodieAvroFileReaderFactory {
  @TempDir
  public java.nio.file.Path tempDir;

  private static final StoragePath HFILE_PATH = new StoragePath("/partition/path/f1_1-0-1_000.hfile");

  @Test
  public void testGetFileReader() throws IOException {
    // parquet file format.
    final HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    final StoragePath parquetPath = new StoragePath("/partition/path/f1_1-0-1_000.parquet");
    HoodieFileReader parquetReader = HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, parquetPath);
    assertTrue(parquetReader instanceof HoodieAvroParquetReader);

    // log file format.
    final StoragePath logPath = new StoragePath(
        "/partition/path/f.b51192a8-574b-4a85-b246-bcfec03ac8bf_100.log.2_1-0-1");
    final Throwable thrown = assertThrows(UnsupportedOperationException.class, () -> {
      HoodieFileReader logWriter = HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, logPath);
    }, "should fail since log storage reader is not supported yet.");
    assertTrue(thrown.getMessage().contains("format not supported yet."));

    // Orc file format.
    final StoragePath orcPath = new StoragePath("/partition/path/f1_1-0-1_000.orc");
    HoodieFileReader orcReader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, orcPath);
    assertTrue(orcReader instanceof HoodieAvroOrcReader);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHFileReaderPassesBloomFilterConfig(boolean bloomFilterEnabled)
      throws IOException, ReflectiveOperationException {
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    HoodieFileReaderFactory readerFactory = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .withProperties(DEFAULT_HUDI_CONFIG_FOR_READER.getProps())
        .withProperties(bloomFilterProps(bloomFilterEnabled))
        .build();

    assertBloomFilterConfig(readerFactory.getFileReader(metadataConfig, HFILE_PATH),
        bloomFilterEnabled);
    assertBloomFilterConfig(readerFactory.getFileReader(metadataConfig,
        new StoragePathInfo(HFILE_PATH, 100, false, (short) 0, 0, 0), HoodieFileFormat.HFILE,
        Option.<HoodieSchema>empty()), bloomFilterEnabled);
    assertBloomFilterConfig(readerFactory.getContentReader(metadataConfig, HFILE_PATH, HoodieFileFormat.HFILE,
        storage, new byte[0], Option.<HoodieSchema>empty()),
        bloomFilterEnabled);
  }

  @Test
  public void testHFileReaderDoesNotUseBloomFilterByDefault()
      throws IOException, ReflectiveOperationException {
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    HoodieFileReaderFactory readerFactory = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO);

    assertBloomFilterConfig(readerFactory.getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, HFILE_PATH), false);
    assertBloomFilterConfig(readerFactory.getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER,
        new StoragePathInfo(HFILE_PATH, 100, false, (short) 0, 0, 0), HoodieFileFormat.HFILE,
        Option.<HoodieSchema>empty()), false);
    assertBloomFilterConfig(readerFactory.getContentReader(DEFAULT_HUDI_CONFIG_FOR_READER, HFILE_PATH,
        HoodieFileFormat.HFILE, storage, new byte[0], Option.<HoodieSchema>empty()),
        false);
  }

  private static TypedProperties bloomFilterProps(boolean enableBloomFilter) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieMetadataConfig.BLOOM_FILTER_ENABLE.key(), Boolean.toString(enableBloomFilter));
    return properties;
  }

  private static void assertBloomFilterConfig(HoodieFileReader reader, boolean expectedBloomFilterEnabled)
      throws ReflectiveOperationException {
    HoodieNativeAvroHFileReader hfileReader = assertInstanceOf(HoodieNativeAvroHFileReader.class, reader);
    Field useBloomFilterField = HoodieNativeAvroHFileReader.class.getDeclaredField("useBloomFilter");
    useBloomFilterField.setAccessible(true);
    assertEquals(expectedBloomFilterEnabled, useBloomFilterField.getBoolean(hfileReader));
  }
}
