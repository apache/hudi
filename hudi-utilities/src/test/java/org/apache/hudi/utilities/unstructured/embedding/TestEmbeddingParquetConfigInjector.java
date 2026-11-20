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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding job's Parquet setup: the job names {@link EmbeddingParquetConfigInjector}, which its source requires, the
 * writer's injector hook turns it into the per-column settings of {@code prepared_image.data}, and a file written
 * through the real writer factory stores those bytes without a dictionary (and without min/max statistics where Parquet has the
 * switch), while the table-wide settings stay the job's.
 */
public class TestEmbeddingParquetConfigInjector {

  private static final String DATA = EmbeddingTableSchema.PREPARED_IMAGE + "." + HoodieSchema.Blob.INLINE_DATA_FIELD;
  private static final String DICTIONARY_KEY = HoodieBaseParquetWriter.COLUMN_DICTIONARY_ENABLED + "#" + DATA;
  private static final String STATISTICS_KEY = HoodieBaseParquetWriter.COLUMN_STATISTICS_ENABLED + "#" + DATA;

  @TempDir
  Path tempDir;

  /** The writer's hook applies the per-column settings, after a job's own injector, whose settings are kept. */
  @Test
  public void theWriterHookAppliesThePerColumnSettingsAfterTheJobsInjector() {
    TypedProperties props = embeddingJobProps();
    props.setProperty(EmbeddingParquetConfigInjector.DELEGATE_CLASS, JobInjector.class.getName());

    HoodieConfig writerConfig = new HoodieConfig(TypedProperties.copy(props));
    StorageConfiguration<?> storage = new HadoopStorageConfiguration(new Configuration(false));
    Pair<StorageConfiguration, HoodieConfig> result = HoodieParquetConfigInjector.applyConfigInjector(
        new StoragePath(tempDir.resolve("out.parquet").toString()), storage, writerConfig);
    assertEquals("false", result.getLeft().getString(DICTIONARY_KEY, "missing"));
    assertEquals(HoodieBaseParquetWriter.supportsColumnStatisticsSwitch() ? "false" : "missing",
        result.getLeft().getString(STATISTICS_KEY, "missing"));
    assertEquals("kept", result.getLeft().getString(JobInjector.STORAGE_KEY, "missing"), "the job's injector ran first");
    assertEquals("missing", result.getLeft().getString(HoodieBaseParquetWriter.COLUMN_DICTIONARY_ENABLED + "#source_path_sha",
        "missing"), "no other column is changed");
    assertFalse(storage.getString(DICTIONARY_KEY).isPresent(), "the caller's configuration is not changed");

    // a Parquet without the per-column statistics switch gets the dictionary setting alone
    Pair<StorageConfiguration, HoodieConfig> older = new EmbeddingParquetConfigInjector().injectConfig(
        new StoragePath(tempDir.resolve("older.parquet").toString()), storage, writerConfig, false);
    assertEquals("false", older.getLeft().getString(DICTIONARY_KEY, "missing"));
    assertEquals("missing", older.getLeft().getString(STATISTICS_KEY, "missing"));
  }

  /** Row checks override caller and delegate intervals while keeping the job's other writer settings and caller isolation. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void pageChecksRunAfterEveryRowAfterTheDelegate(boolean statisticsSwitch) {
    TypedProperties props = embeddingJobProps();
    props.setProperty(EmbeddingParquetConfigInjector.DELEGATE_CLASS, JobInjector.class.getName());
    props.setProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE.key(), "65536");
    props.setProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), "268435456");
    props.setProperty(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), "536870912");
    props.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy");
    props.setProperty(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "false");
    HoodieConfig config = new HoodieConfig(props);
    HadoopStorageConfiguration storage = new HadoopStorageConfiguration(new Configuration(false));
    storage.set(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "50");
    storage.set(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "500");
    Pair<StorageConfiguration, HoodieConfig> result = new EmbeddingParquetConfigInjector().injectConfig(
        new StoragePath(tempDir.resolve("checks.parquet").toString()), storage, config, statisticsSwitch);
    assertEquals("1", result.getLeft().getString(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "missing"));
    assertEquals("1", result.getLeft().getString(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "missing"));
    assertEquals("false", result.getLeft().getString(DICTIONARY_KEY, "missing"));
    assertEquals(statisticsSwitch ? "false" : "missing", result.getLeft().getString(STATISTICS_KEY, "missing"));
    assertEquals("2097152", result.getRight().getString(HoodieStorageConfig.PARQUET_PAGE_SIZE), "the delegate's page size is kept");
    assertEquals("268435456", result.getRight().getString(HoodieStorageConfig.PARQUET_BLOCK_SIZE));
    assertEquals("536870912", result.getRight().getString(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE));
    assertEquals("snappy", result.getRight().getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME));
    assertEquals("false", result.getRight().getString(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED));
    assertEquals("50", storage.getString(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "missing"));
    assertEquals("500", storage.getString(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "missing"));
    assertEquals("65536", config.getString(HoodieStorageConfig.PARQUET_PAGE_SIZE), "the caller's size is unchanged");
    assertFalse(storage.getString(JobInjector.STORAGE_KEY).isPresent(), "the delegate receives a copy");
  }

  /** Through the real writer factory, prepared_image.data is written PLAIN; without the injector the same rows get a dictionary. */
  @Test
  public void preparedImageBytesAreWrittenWithoutADictionary() throws Exception {
    TypedProperties props = embeddingJobProps();
    Map<String, ColumnChunkMetaData> withInjector = write(new HoodieConfig(TypedProperties.copy(props)), "embedding");
    ColumnChunkMetaData data = withInjector.get(DATA);
    assertFalse(hasDictionary(data), data.getEncodings().toString());
    assertTrue(data.getEncodings().contains(Encoding.PLAIN), data.getEncodings().toString());
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      assertFalse(data.getStatistics().hasNonNullValue(), "no min/max of the image bytes: " + data.getStatistics());
    }
    assertTrue(hasDictionary(withInjector.get("source_path_sha")), "the other columns keep the dictionary");

    // control: the writer without the embedding injector (a job the embedding source refuses) writes the same bytes with the
    // writer's dictionary
    TypedProperties plain = new TypedProperties();
    ColumnChunkMetaData control = write(new HoodieConfig(TypedProperties.copy(plain)), "control").get(DATA);
    assertTrue(hasDictionary(control), control.getEncodings().toString());
  }

  /** A 4 KB inline value exceeds the 2 KB page target; supported builders check each row, older builders keep their cadence. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void inlineRowsAboveThePageTargetAreFlushedAfterEachRow(boolean explicitInternalCadence) throws Exception {
    TypedProperties props = embeddingJobProps();
    props.setProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE.key(), "2048");
    if (explicitInternalCadence) {
      props.setProperty(EmbeddingParquetConfigInjector.DELEGATE_CLASS, PageCheckControlInjector.class.getName());
    }
    String name = "page-checks-" + explicitInternalCadence;
    write(new HoodieConfig(props), name);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(tempDir.resolve(name + ".parquet").toUri()), new Configuration()))) {
      PageReadStore rowGroup = reader.readNextRowGroup();
      try {
        PageReader pages = rowGroup.getPageReader(reader.getFileMetaData().getSchema().getColumnDescription(DATA.split("\\.")));
        int count = 0;
        while (pages.readPage() != null) {
          count++;
        }
        assertEquals(supportsPageSizeCheckCadence() ? 8 : 1, count, "per-row checks when supported, otherwise the legacy cadence");
      } finally {
        if (rowGroup instanceof AutoCloseable) {
          ((AutoCloseable) rowGroup).close();
        }
      }
    }
  }

  private static boolean supportsPageSizeCheckCadence() {
    try {
      ParquetWriter.Builder.class.getMethod("withMinRowCountForPageSizeCheck", int.class);
      ParquetWriter.Builder.class.getMethod("withMaxRowCountForPageSizeCheck", int.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /** Passing control: an explicit internal cadence reaches the real writer even on the earlier injector. */
  public static class PageCheckControlInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storage, HoodieConfig config) {
      storage.set(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
      storage.set(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
      return Pair.of(storage, config);
    }
  }

  /** The embedding job's Parquet setting, as the job sets it, validated as its source validates it. */
  private static TypedProperties embeddingJobProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), EmbeddingParquetConfigInjector.class.getName());
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    BlobSourceSetup.validate(props, UnstructuredMode.EMBEDDING, UnstructuredDataSource.name(UnstructuredMode.EMBEDDING));
    return props;
  }

  private static boolean hasDictionary(ColumnChunkMetaData column) {
    return column.getEncodings().contains(Encoding.RLE_DICTIONARY) || column.getEncodings().contains(Encoding.PLAIN_DICTIONARY);
  }

  /** Writes eight rows with a 4 KB inline prepared_image through the writer factory; returns the first row group's columns. */
  private Map<String, ColumnChunkMetaData> write(HoodieConfig config, String name) throws Exception {
    // The direct Apache writer requires the codec normally resolved by the engine write config.
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "snappy");
    Schema blobSchema = HoodieSchema.createBlob().toAvroSchema();
    Schema avro = SchemaBuilder.record("EmbeddingRow").fields().requiredString("source_path_sha")
        .name(EmbeddingTableSchema.PREPARED_IMAGE).type(blobSchema).noDefault().endRecord();
    HoodieSchema withMeta = HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(avro));
    Configuration conf = new Configuration();
    StoragePath output = new StoragePath(tempDir.resolve(name + ".parquet").toString());
    try (HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) HoodieFileWriterFactory.getFileWriter("001", output,
        new HoodieHadoopStorage(output, new HadoopStorageConfiguration(conf)), config, withMeta, new LocalTaskContextSupplier(),
        HoodieRecordType.AVRO)) {
      for (int i = 0; i < 8; i++) {
        // two distinct images, so a dictionary would pay off and Parquet would keep it if the setup allowed one
        byte[] image = new byte[4096];
        new Random(i % 2).nextBytes(image);
        GenericRecord blob = new GenericData.Record(blobSchema);
        blob.put(HoodieSchema.Blob.TYPE, HoodieSchema.Blob.INLINE);
        blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(image));
        blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);
        GenericRecord row = new GenericData.Record(withMeta.toAvroSchema());
        row.put("source_path_sha", "sha-" + (i % 2));
        row.put(EmbeddingTableSchema.PREPARED_IMAGE, blob);
        writer.writeAvro(Integer.toString(i), row);
      }
    }
    Map<String, ColumnChunkMetaData> columns = new HashMap<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      for (ColumnChunkMetaData column : block.getColumns()) {
        columns.put(column.getPath().toDotString(), column);
      }
    }
    return columns;
  }

  /** A job's own injector: it marks its storage copy and supplies page settings. */
  public static class JobInjector implements HoodieParquetConfigInjector {
    static final String STORAGE_KEY = "job.injector.setting";

    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storage, HoodieConfig config) {
      storage.set(STORAGE_KEY, "kept");
      storage.set(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "100");
      storage.set(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "10000");
      config.setValue(HoodieStorageConfig.PARQUET_PAGE_SIZE, "2097152");
      return Pair.of(storage, config);
    }
  }
}
