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

import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieFileReaderFactory}.
 */
public class TestHoodieAvroFileReaderFactory {
  @TempDir
  public java.nio.file.Path tempDir;

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
}
