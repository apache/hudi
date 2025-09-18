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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieHFileConfig;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroAdapter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class HoodieAvroFileWriterFactory extends HoodieFileWriterFactory {

  public HoodieAvroFileWriterFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    HoodieAvroWriteSupport writeSupport = getHoodieAvroWriteSupport(schema, config, enableBloomFilter(populateMetaFields, config));

    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    // Support PARQUET_COMPRESSION_CODEC_NAME is ""
    if (compressionCodecName.isEmpty()) {
      compressionCodecName = null;
    }
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.fromConf(compressionCodecName),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        storage.getConf(), config.getDoubleOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieAvroParquetWriter(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
  }

  protected HoodieFileWriter newParquetFileWriter(
      OutputStream outputStream, HoodieConfig config, Schema schema) throws IOException {
    HoodieAvroWriteSupport writeSupport = getHoodieAvroWriteSupport(schema, config, false);
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.fromConf(config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE), // todo: 1024*1024*1024
        storage.getConf(), config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieParquetStreamWriter(new FSDataOutputStream(outputStream, null), parquetConfig);
  }

  protected HoodieFileWriter newHFileFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieHFileConfig hfileConfig = new HoodieHFileConfig(
        storage.getConf(),
        CompressionCodec.findCodecByName(
            config.getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME)),
        config.getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.HFILE_MAX_FILE_SIZE),
        HoodieAvroHFileReaderImplBase.KEY_FIELD_NAME, filter);
    return new HoodieAvroHFileWriter(instantTime, path, hfileConfig, schema, taskContextSupplier, config.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  protected HoodieFileWriter newOrcFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieOrcConfig orcConfig = new HoodieOrcConfig(storage.getConf(),
        CompressionKind.valueOf(config.getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.ORC_STRIPE_SIZE),
        config.getInt(HoodieStorageConfig.ORC_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.ORC_FILE_MAX_SIZE), filter);
    return new HoodieAvroOrcWriter(instantTime, path, orcConfig, schema, taskContextSupplier);
  }

  private static final AvroAdapter AVRO_ADAPTER = AvroAdapter.getAdapter();

  private HoodieAvroWriteSupport getHoodieAvroWriteSupport(Schema schema,
                                                           HoodieConfig config, boolean enableBloomFilter) {
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    return (HoodieAvroWriteSupport) ReflectionUtils.loadClass(
        config.getStringOrDefault(HoodieStorageConfig.HOODIE_AVRO_WRITE_SUPPORT_CLASS),
        new Class<?>[] {MessageType.class, Schema.class, Option.class, Properties.class},
        AVRO_ADAPTER.getAvroSchemaConverter(storage.getConf()).convert(schema), schema, filter, config.getProps());
  }
}