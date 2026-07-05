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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hudi.common.util.ParquetUtils.getCompressionCodecName;

/**
 * Factory to assist in instantiating a new {@link HoodieRowDataFileWriter}.
 */
public class HoodieRowDataFileWriterFactory extends HoodieFileWriterFactory {

  public HoodieRowDataFileWriterFactory(HoodieStorage storage) {
    super(storage);
  }

  /**
   * Create a parquet writer on a given OutputStream.
   *
   * @param outputStream outputStream where parquet bytes will be written into
   * @param config       hoodie config
   * @param schema       write schema
   *
   * @return an HoodieFileWriter for writing hoodie records.
   */
  @Override
  protected HoodieFileWriter newParquetFileWriter(
      OutputStream outputStream,
      HoodieConfig config,
      HoodieSchema schema) throws IOException {
    HoodieRowDataParquetWriteSupport writeSupport =
        new HoodieRowDataParquetWriteSupport(
            storage.getConf().unwrapAs(Configuration.class), schema, null);
    return new HoodieRowDataParquetOutputStreamWriter(
        new FSDataOutputStream(outputStream, null), writeSupport, getParquetConfig(config, writeSupport));
  }

  /**
   * Create a parquet RowData writer on a given storage path.
   *
   * @param instantTime         instant time to write
   * @param storagePath         file storage path
   * @param config              hoodie configuration
   * @param schema              write schema
   * @param taskContextSupplier task context supplier
   *
   * @return a RowData parquet writer
   */
  @Override
  public HoodieFileWriter newParquetFileWriter(
      String instantTime,
      StoragePath storagePath,
      HoodieConfig config,
      HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    boolean withOperation = config.getBooleanOrDefault(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD);

    Pair<StorageConfiguration, HoodieConfig> injectedConfigs = HoodieParquetConfigInjector.applyConfigInjector(storagePath, storage.getConf(), config);
    StorageConfiguration storageConfiguration = injectedConfigs.getLeft();
    HoodieConfig hoodieConfig = injectedConfigs.getRight();

    Configuration conf = (Configuration) storageConfiguration.unwrapAs(Configuration.class);
    BloomFilter filter = createBloomFilter(hoodieConfig);
    HoodieRowDataParquetWriteSupport writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
        hoodieConfig.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_FLINK_ROW_DATA_WRITE_SUPPORT_CLASS),
        new Class<?>[] {Configuration.class, HoodieSchema.class, BloomFilter.class},
        conf, schema, filter);

    return new HoodieRowDataParquetWriter(storagePath, getParquetConfig(hoodieConfig, writeSupport),
        instantTime, taskContextSupplier, populateMetaFields, withOperation);
  }

  @Override
  public HoodieFileWriter newLanceFileWriter(
      String instantTime,
      StoragePath path,
      HoodieConfig config,
      HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    boolean withOperation = config.getBooleanOrDefault(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD);
    Option<org.apache.hudi.common.bloom.BloomFilter> bloomFilter = enableBloomFilter(populateMetaFields, config)
        ? Option.of(createBloomFilter(config)) : Option.empty();
    RowType rowType = HoodieSchemaConverter.convertToRowType(schema);
    return new HoodieRowDataLanceWriter(
        path,
        rowType,
        instantTime,
        taskContextSupplier,
        bloomFilter,
        config.getLongOrDefault(HoodieStorageConfig.LANCE_MAX_FILE_SIZE),
        config.getLongOrDefault(HoodieStorageConfig.LANCE_WRITE_ALLOCATOR_SIZE_BYTES),
        config.getLongOrDefault(HoodieStorageConfig.LANCE_WRITE_FLUSH_BYTE_WATERMARK),
        config.getBooleanOrDefault(HoodieStorageConfig.WRITE_UTC_TIMEZONE),
        populateMetaFields,
        withOperation);
  }

  private static HoodieParquetConfig<HoodieRowDataParquetWriteSupport> getParquetConfig(
      HoodieConfig config, HoodieRowDataParquetWriteSupport writeSupport) {
    return new HoodieParquetConfig<>(
        writeSupport,
        getCompressionCodecName(config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        new HadoopStorageConfiguration(writeSupport.getHadoopConf()),
        config.getDoubleOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
  }
}
