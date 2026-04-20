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
import org.apache.hudi.common.testutils.DisableDictionaryInjector;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieAvroParquetConfigInjector} functionality in {@link HoodieAvroFileWriterFactory}.
 */
public class TestHoodieAvroParquetConfigInjector {

  @TempDir
  java.nio.file.Path tmpDir;

  @Test
  public void testDisableDictionaryEncodingViaInjector() throws Exception {
    final String instantTime = "100";
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    final StoragePath parquetPath = new StoragePath(
        tmpDir.resolve("test_dictionary_" + instantTime + ".parquet").toAbsolutePath().toString());

    // Generate test data
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    List<GenericRecord> records = dataGen.generateGenericRecords(100);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());

    // Create config with the custom injector
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "true"); // Start with dictionary enabled
    config.setValue(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, DisableDictionaryInjector.class.getName());

    // Create writer and write some data
    HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter(instantTime, parquetPath, config, schema, new LocalTaskContextSupplier());

    assertTrue(writer instanceof HoodieAvroParquetWriter);

    // Write test records
    HoodieAvroParquetWriter avroWriter = (HoodieAvroParquetWriter) writer;
    for (GenericRecord record : records) {
      avroWriter.writeAvro((String) record.get("_row_key"), record);
    }
    writer.close();

    // Verify the parquet file was created
    assertTrue(storage.exists(parquetPath));

    // Read parquet metadata and verify dictionary encoding is disabled
    Configuration hadoopConf = new Configuration();
    Path hadoopPath = new Path(parquetPath.toUri());
    ParquetFileReader reader = ParquetFileReader.open(hadoopConf, hadoopPath);
    ParquetMetadata metadata = reader.getFooter();
    reader.close();

    assertNotNull(metadata);

    // Verify that dictionary encoding is NOT used for any column
    // When dictionary encoding is disabled, columns should use PLAIN or other encodings but not RLE_DICTIONARY
    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        // Check all encodings used for this column - should not include RLE_DICTIONARY or PLAIN_DICTIONARY
        for (Encoding encoding : column.getEncodings()) {
          assertFalse(encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY,
              "Column " + column.getPath() + " should not use dictionary encoding, but found: " + encoding);
        }
      }
    }
  }

  @Test
  public void testInvalidInjectorClassThrowsException() throws IOException {
    final String instantTime = "102";
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    final StoragePath parquetPath = new StoragePath(
        tmpDir.resolve("test_invalid_" + instantTime + ".parquet").toAbsolutePath().toString());

    // Generate test data
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    List<GenericRecord> records = dataGen.generateGenericRecords(10);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());

    // Create config with an invalid/non-existent injector class
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "org.apache.hudi.NonExistentInjector");

    // Should throw an exception when trying to create the writer
    assertThrows(Exception.class, () -> {
      new HoodieAvroFileWriterFactory(storage)
          .newParquetFileWriter(instantTime, parquetPath, config, schema, new LocalTaskContextSupplier());
    });
  }

  @Test
  public void testNoInjectorUsesDefaultConfig() throws Exception {
    final String instantTime = "103";
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    final StoragePath parquetPath = new StoragePath(
        tmpDir.resolve("test_no_injector_" + instantTime + ".parquet").toAbsolutePath().toString());

    // Generate test data
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);
    List<GenericRecord> records = dataGen.generateGenericRecords(10);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());

    // Create config WITHOUT injector - should use default settings
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "true");

    // Create writer and write some data
    HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter(instantTime, parquetPath, config, schema, new LocalTaskContextSupplier());

    assertTrue(writer instanceof HoodieAvroParquetWriter);

    // Write test records
    HoodieAvroParquetWriter avroWriter = (HoodieAvroParquetWriter) writer;
    for (GenericRecord record : records) {
      avroWriter.writeAvro((String) record.get("_row_key"), record);
    }
    writer.close();

    // Verify the parquet file was created
    assertTrue(storage.exists(parquetPath));
  }
}
