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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.testutils.DisableDictionaryInjector;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.HoodieParquetConfigInjector;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieParquetConfigInjector} functionality in {@link HoodieRowDataFileWriterFactory}.
 */
public class TestHoodieRowDataParquetConfigInjector extends HoodieFlinkClientTestHarness {

  @TempDir
  java.nio.file.Path tmpDir;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  /**
   * Test implementation that modifies Hadoop configuration for Parquet bloom filters.
   */
  public static class ParquetBloomFilterInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path,
                                                               StorageConfiguration storageConf,
                                                               HoodieConfig hoodieConfig) {
      // Enable native Parquet bloom filter on a specific column
      Configuration hadoopConf = (Configuration) storageConf.unwrapAs(Configuration.class);
      hadoopConf.set("parquet.bloom.filter.enabled#uuid", "true");
      hadoopConf.set("parquet.bloom.filter.expected.ndv#uuid", "1000");

      return Pair.of(new HadoopStorageConfiguration(hadoopConf), hoodieConfig);
    }
  }

  private RowType getTestRowType() {
    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("name", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("age", DataTypes.INT()),
        DataTypes.FIELD("partition", DataTypes.VARCHAR(20))
    ).notNull();
    return (RowType) dataType.getLogicalType();
  }

  @Test
  public void testDisableDictionaryEncodingViaInjector() throws Exception {
    final String instantTime = "100";
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    final StoragePath parquetPath = new StoragePath(
        basePath + "/partition/path/test_dictionary_" + instantTime + ".parquet");

    RowType rowType = getTestRowType();

    // Create config with the custom injector
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "true"); // Start with dictionary enabled
    config.setValue(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, DisableDictionaryInjector.class.getName());

    // Create writer and write some data
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(storage);
    HoodieFileWriter writer = factory.newParquetFileWriter(
        instantTime, parquetPath, config, rowType, new LocalTaskContextSupplier());

    assertTrue(writer instanceof HoodieRowDataParquetWriter);

    // Write test records
    HoodieRowDataParquetWriter rowDataWriter = (HoodieRowDataParquetWriter) writer;
    for (int i = 0; i < 100; i++) {
      GenericRowData row = new GenericRowData(4);
      row.setField(0, StringData.fromString("id" + i));
      row.setField(1, StringData.fromString("name" + i));
      row.setField(2, 20 + i);
      row.setField(3, StringData.fromString("partition/path"));
      rowDataWriter.write(row);
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
        basePath + "/partition/path/test_invalid_" + instantTime + ".parquet");

    RowType rowType = getTestRowType();

    // Create config with an invalid/non-existent injector class
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "org.apache.hudi.NonExistentInjector");

    // Should throw an exception when trying to create the writer
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(storage);
    assertThrows(Exception.class, () -> {
      factory.newParquetFileWriter(instantTime, parquetPath, config, rowType, new LocalTaskContextSupplier());
    });
  }

  @Test
  public void testNoInjectorUsesDefaultConfig() throws Exception {
    final String instantTime = "103";
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    final StoragePath parquetPath = new StoragePath(
        basePath + "/partition/path/test_no_injector_" + instantTime + ".parquet");

    RowType rowType = getTestRowType();

    // Create config WITHOUT injector - should use default settings
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "true");

    // Create writer and write some data
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(storage);
    HoodieFileWriter writer = factory.newParquetFileWriter(
        instantTime, parquetPath, config, rowType, new LocalTaskContextSupplier());

    assertTrue(writer instanceof HoodieRowDataParquetWriter);

    // Write test records
    HoodieRowDataParquetWriter rowDataWriter = (HoodieRowDataParquetWriter) writer;
    for (int i = 0; i < 10; i++) {
      GenericRowData row = new GenericRowData(4);
      row.setField(0, StringData.fromString("id" + i));
      row.setField(1, StringData.fromString("name" + i));
      row.setField(2, 20 + i);
      row.setField(3, StringData.fromString("partition/path"));
      rowDataWriter.write(row);
    }
    writer.close();

    // Verify the parquet file was created
    assertTrue(storage.exists(parquetPath));
  }
}
