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

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Public row-writer construction must honor the configured Parquet injector. */
public class TestHoodieInternalRowParquetConfigInjector {
  private static final StructType SCHEMA = new StructType().add("value", DataTypes.StringType, false);
  private static final int ROW_COUNT = 128;
  private static final String STORAGE_MARKER = "row.writer.injected.storage";

  @TempDir
  java.nio.file.Path tempDir;

  private HoodieWriteConfig config(String injector) {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "uncompressed");
    if (injector != null) {
      properties.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), injector);
    }
    return HoodieWriteConfig.newBuilder().withPath(tempDir.toString())
        .withSchema("{\"type\":\"record\",\"name\":\"row_record\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}")
        .withProperties(properties).build();
  }

  private HoodieInternalRowFileWriter writer(StoragePath path, HoodieWriteConfig config, Configuration storageConfig) throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    when(table.getStorageConf()).thenReturn(new HadoopStorageConfiguration(storageConfig));
    return HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(path, table, config, SCHEMA);
  }

  private void writeAndRead(StoragePath path, HoodieWriteConfig config, Configuration storageConfig) throws Exception {
    HoodieInternalRowFileWriter writer = writer(path, config, storageConfig);
    try {
      for (int i = 0; i < ROW_COUNT; i++) {
        writer.writeRow(new GenericInternalRow(new Object[] {UTF8String.fromString("repeated-value")}));
      }
    } finally {
      writer.close();
    }
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path.toUri()))
        .withConf(storageConfig).build()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        assertEquals("repeated-value", reader.read().get("value").toString());
      }
      assertNull(reader.read());
    }
  }

  @Test
  void invalidInjectorFailsPublicWriterConstruction() {
    StoragePath path = new StoragePath(tempDir.resolve("invalid.parquet").toUri());
    assertThrows(HoodieException.class, () -> {
      HoodieInternalRowFileWriter writer = writer(path, config("org.apache.hudi.io.storage.row.MissingConfigInjector"), new Configuration());
      writer.close();
    });
  }

  @Test
  void absentInjectorWritesAndReadsNormally() throws Exception {
    StoragePath path = new StoragePath(tempDir.resolve("control.parquet").toUri());
    Configuration storageConfig = new Configuration();
    writeAndRead(path, config(null), storageConfig);
    try (ParquetFileReader reader = ParquetFileReader.open(storageConfig, new Path(path.toUri()))) {
      assertEquals(ROW_COUNT, reader.getFooter().getBlocks().get(0).getRowCount());
      ColumnChunkMetaData column = reader.getFooter().getBlocks().get(0).getColumns().get(0);
      assertEquals(CompressionCodecName.UNCOMPRESSED, column.getCodec());
      assertTrue(column.getEncodings().contains(Encoding.RLE_DICTIONARY)
          || column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
    }
  }

  @Test
  void copiedConfigsControlEncodingAndCodecWithoutMutatingInputs() throws Exception {
    StoragePath path = new StoragePath(tempDir.resolve("injected.parquet").toUri());
    HoodieWriteConfig config = config(CopyingInjector.class.getName());
    // Global Hadoop default resources may be registered lazily while the writer is initialized.
    Configuration storageConfig = new Configuration(false);
    storageConfig.set("row.writer.original.storage", "original");
    String originalWriteSupport = config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS);

    writeAndRead(path, config, storageConfig);

    assertEquals("uncompressed", config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME));
    assertTrue(config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    assertEquals(originalWriteSupport, config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS));
    assertEquals(CopyingInjector.class.getName(), config.getString(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS));
    assertEquals("original", storageConfig.get("row.writer.original.storage"));
    assertNull(storageConfig.get(STORAGE_MARKER));
    try (ParquetFileReader reader = ParquetFileReader.open(storageConfig, new Path(path.toUri()))) {
      assertEquals("copied-storage", reader.getFooter().getFileMetaData().getKeyValueMetaData().get(STORAGE_MARKER));
      reader.getFooter().getBlocks().forEach(block -> block.getColumns().forEach(column -> {
        assertEquals(CompressionCodecName.GZIP, column.getCodec());
        assertFalse(column.getEncodings().contains(Encoding.RLE_DICTIONARY));
        assertFalse(column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
      }));
    }
  }

  @Test
  void bloomTypeOnWriteConfigReachesRowWriter() throws Exception {
    Map<String, String> footer = writeOneKeyedRowAndReadFooter("bloom-control.parquet", keyedConfig(null, BloomFilterTypeCode.SIMPLE.name()));
    assertTrue(footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), footer.toString());
    assertFalse(footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE), footer.toString());
  }

  @Test
  void injectedBloomTypeReachesRowWriter() throws Exception {
    HoodieWriteConfig config = keyedConfig(SimpleBloomInjector.class.getName(), null);
    assertEquals(BloomFilterTypeCode.DYNAMIC_V0.name(), config.getBloomFilterType());
    Map<String, String> footer = writeOneKeyedRowAndReadFooter("bloom-injected.parquet", config);
    assertTrue(footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), footer.toString());
    assertFalse(footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE),
        "the injected SIMPLE bloom filter type was ignored; footer type code = "
            + footer.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE));
  }

  /** A config with meta fields populated, so the row writer writes a bloom filter. */
  private HoodieWriteConfig keyedConfig(String injector, String bloomType) {
    Properties properties = new Properties();
    if (injector != null) {
      properties.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), injector);
    }
    if (bloomType != null) {
      properties.setProperty(HoodieStorageConfig.BLOOM_FILTER_TYPE.key(), bloomType);
    }
    return HoodieWriteConfig.newBuilder().withPath(tempDir.toString())
        .withSchema("{\"type\":\"record\",\"name\":\"row_record\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}")
        .withProperties(properties).build();
  }

  private Map<String, String> writeOneKeyedRowAndReadFooter(String fileName, HoodieWriteConfig config) throws Exception {
    StoragePath path = new StoragePath(tempDir.resolve(fileName).toUri());
    Configuration storageConfig = new Configuration();
    HoodieInternalRowFileWriter writer = writer(path, config, storageConfig);
    try {
      writer.writeRow(UTF8String.fromString("key1"), new GenericInternalRow(new Object[] {UTF8String.fromString("v")}));
    } finally {
      writer.close();
    }
    try (ParquetFileReader reader = ParquetFileReader.open(storageConfig, new Path(path.toUri()))) {
      return reader.getFooter().getFileMetaData().getKeyValueMetaData();
    }
  }

  /** Switches the Hudi bloom filter to SIMPLE on a copy, as the injector contract requires. */
  public static class SimpleBloomInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf,
                                                                 HoodieConfig hoodieConfig) {
      HoodieConfig copied = HoodieConfig.copy(hoodieConfig.getProps());
      copied.setValue(HoodieStorageConfig.BLOOM_FILTER_TYPE, BloomFilterTypeCode.SIMPLE.name());
      return Pair.of(storageConf, copied);
    }
  }

  /** Returns independent copies, as required by the public injector contract. */
  public static class CopyingInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf,
                                                                 HoodieConfig hoodieConfig) {
      Configuration copiedStorage = new Configuration((Configuration) storageConf.unwrap());
      copiedStorage.set(STORAGE_MARKER, "copied-storage");
      HoodieConfig copiedConfig = HoodieConfig.copy(hoodieConfig.getProps());
      copiedConfig.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "false");
      copiedConfig.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "gzip");
      copiedConfig.setValue(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS, StorageMarkerWriteSupport.class.getName());
      return Pair.of(new HadoopStorageConfiguration(copiedStorage), copiedConfig);
    }
  }

  /** Persists the constructor's storage marker, so the footer verifies storage reached write support. */
  public static class StorageMarkerWriteSupport extends HoodieRowParquetWriteSupport {
    private final String storageMarker;

    public StorageMarkerWriteSupport(Configuration conf, StructType schema, Option<BloomFilter> bloomFilter, HoodieConfig config) {
      super(conf, schema, bloomFilter, config);
      storageMarker = conf.get(STORAGE_MARKER);
    }

    @Override
    public WriteContext init(Configuration configuration) {
      WriteContext context = super.init(configuration);
      Map<String, String> metadata = new HashMap<>(context.getExtraMetaData());
      metadata.put(STORAGE_MARKER, storageMarker);
      return new WriteContext(context.getSchema(), metadata);
    }
  }

}
