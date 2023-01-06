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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import static org.apache.hudi.io.storage.HoodieHFileConfig.CACHE_DATA_IN_L1;
import static org.apache.hudi.io.storage.HoodieHFileConfig.DROP_BEHIND_CACHE_COMPACTION;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileConfig.PREFETCH_ON_OPEN;

public class HoodieAvroFileWriterFactory extends HoodieFileWriterFactory {

  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    boolean enableBloomFilter = populateMetaFields;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);
    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    // Support PARQUET_COMPRESSION_CODEC_NAME is ""
    if (compressionCodecName.isEmpty()) {
      compressionCodecName = null;
    }
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig(writeSupport,
        CompressionCodecName.fromConf(compressionCodecName),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        conf, config.getDoubleOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieAvroParquetWriter(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
  }

  protected HoodieFileWriter newParquetFileWriter(
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    boolean enableBloomFilter = false;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
        CompressionCodecName.fromConf(config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE), // todo: 1024*1024*1024
        conf, config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieParquetStreamWriter(outputStream, parquetConfig);
  }

  protected HoodieFileWriter newHFileFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieHFileConfig hfileConfig = new HoodieHFileConfig(conf,
        Compression.Algorithm.valueOf(config.getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME)),
        config.getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.HFILE_MAX_FILE_SIZE),
        HoodieAvroHFileReader.KEY_FIELD_NAME, PREFETCH_ON_OPEN, CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION,
        filter, HFILE_COMPARATOR);

    return new HoodieAvroHFileWriter(instantTime, path, hfileConfig, schema, taskContextSupplier, config.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  protected HoodieFileWriter newOrcFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieOrcConfig orcConfig = new HoodieOrcConfig(conf,
        CompressionKind.valueOf(config.getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.ORC_STRIPE_SIZE),
        config.getInt(HoodieStorageConfig.ORC_BLOCK_SIZE),
        config.getLong(HoodieStorageConfig.ORC_FILE_MAX_SIZE), filter);
    return new HoodieAvroOrcWriter(instantTime, path, orcConfig, schema, taskContextSupplier);
  }
}
