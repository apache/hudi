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

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieTestDataGenerator;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieStorageWriterFactory}.
 */
public class TestHoodieStorageWriterFactory extends HoodieClientTestBase {

  @Test
  public void testGetStorageWriter() throws IOException {
    // parquet file format.
    final String instantTime = "100";
    final Path parquetPath = new Path(basePath + "/partition/path/f1_1-0-1_000.parquet");
    final HoodieWriteConfig cfg = getConfig();
    HoodieTable table = HoodieTable.create(metaClient, cfg, hadoopConf);
    SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();
    HoodieStorageWriter<IndexedRecord> parquetWriter = HoodieStorageWriterFactory.getStorageWriter(instantTime,
        parquetPath, table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    assertTrue(parquetWriter instanceof HoodieParquetWriter);

    // other file format exception.
    final Path logPath = new Path(basePath + "/partition/path/f.b51192a8-574b-4a85-b246-bcfec03ac8bf_100.log.2_1-0-1");
    final Throwable thrown = assertThrows(UnsupportedOperationException.class, () -> {
      HoodieStorageWriter<IndexedRecord> logWriter = HoodieStorageWriterFactory.getStorageWriter(instantTime, logPath,
          table, cfg, HoodieTestDataGenerator.AVRO_SCHEMA, supplier);
    }, "should fail since log storage writer is not supported yet.");
    assertTrue(thrown.getMessage().contains("format not supported yet."));
  }
}
