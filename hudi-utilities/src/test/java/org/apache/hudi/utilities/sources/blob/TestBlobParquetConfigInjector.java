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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source activation and caller injector composition through the registered writer hook. */
@Tag("functional")
public class TestBlobParquetConfigInjector extends UtilitiesTestBase {
  /** A job's own row-group size, well under the file target, in the factory test. */

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  /**
   * The blob job keeps Hudi's small-file packing on every Parquet (the inline cap, not a packing switch,
   * bounds a row on a Parquet without per-column statistics).
   */
  @Test
  void sourceKeepsSmallFilePackingInPlannerConfig() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    assertEquals(104857600, plannerConfig(props).getParquetSmallFileLimit());
    assertFalse(props.containsKey(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key()));
  }

  @Test
  void ordinaryPlannerKeepsSmallFilePackingDefaultsAndCallerSettings() {
    TypedProperties props = new TypedProperties();
    assertEquals(104857600, plannerConfig(props).getParquetSmallFileLimit());
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "65536");
    assertEquals(65536, plannerConfig(props).getParquetSmallFileLimit());
  }

  private HoodieWriteConfig plannerConfig(TypedProperties props) {
    // Same compaction-config then shared-properties order used by StreamSync's actual write config.
    return HoodieWriteConfig.newBuilder().withPath(tempDir.resolve("table").toString()).forTable("blob")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().build()).withProps(props).build();
  }

  @Test
  void sourceActivatesBoundsInWriterProps() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    HoodieConfig writerConfig = new HoodieConfig(TypedProperties.copy(props));
    Configuration conf = new Configuration(false);
    HadoopStorageConfiguration storage = new HadoopStorageConfiguration(conf);
    Pair<StorageConfiguration, HoodieConfig> result = HoodieParquetConfigInjector.applyConfigInjector(
        new StoragePath(tempDir.resolve("output.parquet").toString()), storage, writerConfig);
    // Parquet 1.15 here: no file cap of the injector's own, Hudi's default file size target
    assertBounds(result, 1048576, Integer.parseInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE.defaultValue()),
        Long.parseLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.defaultValue()));
    assertNotSame(storage, result.getLeft());
    assertNotSame(writerConfig, result.getRight());
    assertFalse(storage.getString("hoodie.parquet.internal.page.size.row.check.min").isPresent());
    assertFalse(writerConfig.contains(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
  }

  @Test
  void composesCallerFirstPreservesSmallerCapsAndRepeatedSources() throws Exception {
    TypedProperties props = sourceProps();
    // the job's own injector, which the blob injector runs first
    props.setProperty(BlobParquetConfigInjector.DELEGATE_CLASS, CallerInjector.class.getName());
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "1024");
    props.setProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE.key(), "65536");
    props.setProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), "262144");
    props.setProperty(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), "65536");
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "67108864");
    newSource(props);
    newSource(props);
    assertEquals(67108864, plannerConfig(props).getParquetSmallFileLimit(), "the caller's packing limit is kept");
    HoodieConfig writerConfig = new HoodieConfig(TypedProperties.copy(props));
    HadoopStorageConfiguration storage = new HadoopStorageConfiguration(new Configuration(false));
    Pair<StorageConfiguration, HoodieConfig> result = HoodieParquetConfigInjector.applyConfigInjector(
        new StoragePath(tempDir.resolve("composed.parquet").toString()), storage, writerConfig);
    assertBounds(result, 65536, 131072, 65536);
    assertEquals("1", result.getRight().getString("caller.invocations"));
    assertEquals("kept", result.getRight().getString("caller.setting"));
    assertEquals("kept", result.getLeft().getString("caller.storage.setting").get());
    assertFalse(writerConfig.contains("caller.setting"));
    assertFalse(storage.getString("caller.storage.setting").isPresent());
    assertEquals("65536", writerConfig.getString(HoodieStorageConfig.PARQUET_PAGE_SIZE));
  }

  /**
   * A page larger than Hudi's 1 MiB default costs a table without blob columns the same per page, so the
   * blob writer keeps the job's page size; the per-record check still closes a page at about one inline value.
   */
  @Test
  void callerPageSizeIsKept() throws Exception {
    TypedProperties props = sourceProps();
    props.setProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE.key(), "2097152");
    newSource(props);
    Pair<StorageConfiguration, HoodieConfig> result = new BlobParquetConfigInjector().injectConfig(
        new StoragePath(tempDir.resolve("page.parquet").toString()), new HadoopStorageConfiguration(new Configuration(false)),
        new HoodieConfig(TypedProperties.copy(props)));
    assertEquals(2097152, result.getRight().getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE));
    assertEquals("1", result.getLeft().getString("hoodie.parquet.internal.page.size.row.check.min", "missing"));
  }

  /**
   * Once the per-record check bounds the overshoot, an open or read row group of the job's size costs
   * a table without blob columns the same, so the blob writer keeps the job's row-group size (Hudi's default 120 MiB when unset).
   */
  @Test
  void callerAndDefaultRowGroupSizesAreKept() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    StoragePath path = new StoragePath(tempDir.resolve("group.parquet").toString());
    Pair<StorageConfiguration, HoodieConfig> defaults = new BlobParquetConfigInjector().injectConfig(path,
        new HadoopStorageConfiguration(new Configuration(false)), new HoodieConfig(TypedProperties.copy(props)));
    assertEquals(Integer.parseInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE.defaultValue()),
        defaults.getRight().getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE));
    props.setProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), "268435456");
    Pair<StorageConfiguration, HoodieConfig> caller = new BlobParquetConfigInjector().injectConfig(path,
        new HadoopStorageConfiguration(new Configuration(false)), new HoodieConfig(TypedProperties.copy(props)));
    assertEquals(268435456, caller.getRight().getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE));
    assertEquals("1", caller.getLeft().getString("hoodie.parquet.internal.page.size.row.check.max", "missing"));
  }

  /**
   * On a Parquet without per-column statistics the injector sets no file cap of its own: Hudi's file
   * size target applies, and the per-row page and row-group checks stay.
   */
  @Test
  void parquetWithoutTheStatisticsSwitchHasNoFileCapOfItsOwn() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    HadoopStorageConfiguration storage = new HadoopStorageConfiguration(new Configuration(false));
    Pair<StorageConfiguration, HoodieConfig> result = new BlobParquetConfigInjector().injectConfig(
        new StoragePath(tempDir.resolve("legacy.parquet").toString()), storage, new HoodieConfig(TypedProperties.copy(props)), false);
    assertEquals("1", result.getLeft().getString("hoodie.parquet.internal.page.size.row.check.min", "missing"));
    assertEquals("1", result.getLeft().getString("hoodie.parquet.internal.page.size.row.check.max", "missing"));
    assertEquals(Long.parseLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.defaultValue()),
        result.getRight().getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE));
    assertFalse(result.getLeft().getString("hoodie.parquet.internal.column.statistics.enabled#blob.data").isPresent());
    assertEquals("false", result.getLeft().getString("hoodie.parquet.internal.column.dictionary.enabled#blob.data", "missing"));
  }

  /**
   * A blob data file is uncompressed, its blob bytes PLAIN (no dictionary; the other columns keep theirs), and its
   * footer has no record-key bloom filter.
   */
  @Test
  void blobFilesHaveTheDefaultParquetSetup() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    writeSmallBlobFile(new HoodieConfig(TypedProperties.copy(props)), "defaults");
    Configuration conf = new Configuration();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(tempDir.resolve("defaults.parquet").toUri()), conf))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          assertEquals(CompressionCodecName.UNCOMPRESSED, column.getCodec(), column.getPath().toDotString());
          boolean dictionary = column.getEncodings().contains(Encoding.RLE_DICTIONARY) || column.getEncodings().contains(Encoding.PLAIN_DICTIONARY);
          if ("blob.data".equals(column.getPath().toDotString())) {
            assertFalse(dictionary, "blob.data " + column.getEncodings());
            assertTrue(column.getEncodings().contains(Encoding.PLAIN), "blob.data " + column.getEncodings());
          } else if ("blob.type".equals(column.getPath().toDotString())) {
            // INLINE on every row: the file's dictionary still applies to the other columns, even of the blob struct
            assertTrue(dictionary, "blob.type keeps the dictionary: " + column.getEncodings());
          }
        }
      }
    }
    assertEquals("uncompressed", props.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "unset"));
    // a job's own codec is kept
    TypedProperties gzip = sourceProps();
    gzip.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "gzip");
    newSource(gzip);
    assertEquals("gzip", gzip.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key()));
  }

  @Test
  void blobFilesCarryNoRecordKeyBloomFilterUnlessTheIndexNeedsIt() throws Exception {
    TypedProperties props = sourceProps();
    newSource(props);
    Map<String, String> footer = writeSmallBlobFile(new HoodieConfig(TypedProperties.copy(props)), "no-bloom");
    assertFalse(footer.containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY), footer.keySet().toString());
    assertEquals("false", props.getString(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "unset"));

    // a job that sets the switch, or picks a BLOOM index, keeps the filter the index reads
    TypedProperties enabled = sourceProps();
    enabled.setProperty(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "true");
    newSource(enabled);
    assertEquals("true", enabled.getString(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key()));
    assertTrue(writeSmallBlobFile(new HoodieConfig(TypedProperties.copy(enabled)), "bloom-enabled")
        .containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
    TypedProperties bloomIndex = sourceProps();
    bloomIndex.setProperty("hoodie.index.type", "BLOOM");
    newSource(bloomIndex);
    assertTrue(writeSmallBlobFile(new HoodieConfig(TypedProperties.copy(bloomIndex)), "bloom-index")
        .containsKey(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
  }

  /** Writes eight 4 KB inline rows with all meta fields through the real factory; returns the footer's key-value metadata. */
  private Map<String, String> writeSmallBlobFile(HoodieConfig config, String name) throws Exception {
    Schema blobSchema = HoodieSchema.createBlob().toAvroSchema();
    Schema avro = SchemaBuilder.record("BloomBlob").fields().requiredString("source_path_sha")
        .name("blob").type(blobSchema).noDefault().endRecord();
    HoodieSchema withMeta = HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(avro));
    Configuration conf = new Configuration();
    StoragePath output = new StoragePath(tempDir.resolve(name + ".parquet").toString());
    try (HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) HoodieFileWriterFactory.getFileWriter("001", output,
        new HoodieHadoopStorage(output, new HadoopStorageConfiguration(conf)), config,
        withMeta, new LocalTaskContextSupplier(), HoodieRecordType.AVRO)) {
      for (int i = 0; i < 8; i++) {
        // two distinct values, so a dictionary would be worth it and Parquet would keep it if the setup allowed one
        byte[] data = new byte[4096];
        new Random(i % 2).nextBytes(data);
        GenericRecord blob = new GenericData.Record(blobSchema);
        blob.put("type", "INLINE");
        blob.put("data", ByteBuffer.wrap(data));
        blob.put("reference", null);
        GenericRecord row = new GenericData.Record(withMeta.toAvroSchema());
        row.put("source_path_sha", Integer.toString(i));
        row.put("blob", blob);
        writer.writeAvro(Integer.toString(i), row);
      }
    }
    Path file = tempDir.resolve(name + ".parquet");
    byte[] bytes = Files.readAllBytes(file);
    int footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    // read by the evidence script: the footer size of a blob data file with and without the filter
    System.out.println("BLOB-FOOTER " + name + " file=" + bytes.length + " footer=" + footerLength);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      return reader.getFooter().getFileMetaData().getKeyValueMetaData();
    }
  }

  private TypedProperties sourceProps() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), tempDir.resolve("events").toUri().toString());
    props.setProperty("hoodie.datasource.write.recordkey.field", BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE);
    // the blob job's writer settings, as blob-table.properties sets them
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), BlobParquetConfigInjector.class.getName());
    props.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "uncompressed");
    props.setProperty(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "false");
    return props;
  }

  /** The blob table source built over {@code props}, as the streamer builds it: it validates them and changes none. */
  private static void newSource(TypedProperties props) {
    TypedProperties before = TypedProperties.copy(props);
    new UnstructuredDataSource(props, jsc, sparkSession, (SchemaProvider) null, null);
    assertEquals(before, props, "the source changes none of the job's properties");
  }

  private static void assertBounds(Pair<StorageConfiguration, HoodieConfig> result, int page, int block, long file) {
    assertEquals("1", result.getLeft().getString("hoodie.parquet.internal.page.size.row.check.min", "missing"));
    assertEquals("1", result.getLeft().getString("hoodie.parquet.internal.page.size.row.check.max", "missing"));
    assertEquals(page, result.getRight().getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE));
    assertEquals(block, result.getRight().getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE));
    assertEquals("false", result.getLeft().getString("hoodie.parquet.internal.column.dictionary.enabled#blob.data", "missing"));
    assertEquals(file, result.getRight().getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE));
    boolean statisticsSwitch;
    try {
      org.apache.parquet.hadoop.ParquetWriter.Builder.class.getMethod("withStatisticsEnabled", String.class, boolean.class);
      statisticsSwitch = true;
    } catch (NoSuchMethodException e) {
      statisticsSwitch = false;
    }
    assertEquals(statisticsSwitch ? "false" : "missing", result.getLeft().getString("hoodie.parquet.internal.column.statistics.enabled#blob.data", "missing"));
    assertEquals(statisticsSwitch ? "true" : "missing", result.getLeft().getString("hoodie.parquet.internal.size.check.every.record", "missing"));
  }

  /** Deliberately mutates its arguments to prove the wrapper isolates caller configurations before delegation. */
  public static class CallerInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storage, HoodieConfig config) {
      config.setValue("caller.invocations", Integer.toString(Integer.parseInt(config.getStringOrDefault("caller.invocations", "0")) + 1));
      config.setValue("caller.setting", "kept");
      config.setValue(HoodieStorageConfig.PARQUET_PAGE_SIZE, "2097152");
      config.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE, "131072");
      config.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "true");
      config.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, "16777216");
      storage.set("caller.storage.setting", "kept");
      storage.set("hoodie.parquet.internal.page.size.row.check.min", "100");
      storage.set("hoodie.parquet.internal.page.size.row.check.max", "10000");
      return Pair.of(storage, config);
    }
  }
}
