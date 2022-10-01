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
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.io.storage.HoodieHFileConfig.CACHE_DATA_IN_L1;
import static org.apache.hudi.io.storage.HoodieHFileConfig.DROP_BEHIND_CACHE_COMPACTION;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileConfig.PREFETCH_ON_OPEN;

public class HoodieFileWriterFactory {

  public static <T extends HoodieRecordPayload, R extends IndexedRecord, I, K, O> HoodieFileWriter<R> getFileWriter(
      String instantTime, Path path, HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileWriter(instantTime, path, config, schema, hoodieTable, taskContextSupplier, config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS));
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileWriter(
          instantTime, path, config, schema, hoodieTable.getHadoopConf(), taskContextSupplier);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileWriter(
          instantTime, path, config, schema, hoodieTable.getHadoopConf(), taskContextSupplier);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newParquetFileWriter(
      String instantTime, Path path, HoodieWriteConfig config, Schema schema, HoodieTable hoodieTable,
      TaskContextSupplier taskContextSupplier, boolean populateMetaFields) throws IOException {
    return newParquetFileWriter(instantTime, path, config, schema, hoodieTable.getHadoopConf(),
        taskContextSupplier, populateMetaFields, populateMetaFields);
  }

  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newParquetFileWriter(
      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
      TaskContextSupplier taskContextSupplier, boolean populateMetaFields, boolean enableBloomFilter) throws IOException {
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);

    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport, config.getParquetCompressionCodec(),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE), config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE), config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        conf, config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION), config.getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));

    return new HoodieAvroParquetWriter<>(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
  }

  static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newHFileFileWriter(
      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
      TaskContextSupplier taskContextSupplier) throws IOException {

    BloomFilter filter = createBloomFilter(config);
    HoodieHFileConfig hfileConfig = new HoodieHFileConfig(conf,
        Compression.Algorithm.valueOf(config.getString(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME)),
        config.getInt(HoodieStorageConfig.HFILE_BLOCK_SIZE), config.getLong(HoodieStorageConfig.HFILE_MAX_FILE_SIZE),
        HoodieHFileReader.KEY_FIELD_NAME, PREFETCH_ON_OPEN, CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION,
        filter, HFILE_COMPARATOR);

    return new HoodieHFileWriter<>(instantTime, path, hfileConfig, schema, taskContextSupplier, config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  private static <T extends HoodieRecordPayload, R extends IndexedRecord> HoodieFileWriter<R> newOrcFileWriter(
      String instantTime, Path path, HoodieWriteConfig config, Schema schema, Configuration conf,
      TaskContextSupplier taskContextSupplier) throws IOException {
    BloomFilter filter = createBloomFilter(config);
    HoodieOrcConfig orcConfig = new HoodieOrcConfig(conf, CompressionKind.valueOf(config.getString(HoodieStorageConfig.ORC_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.ORC_STRIPE_SIZE), config.getInt(HoodieStorageConfig.ORC_BLOCK_SIZE), config.getLong(HoodieStorageConfig.ORC_FILE_MAX_SIZE), filter);
    return new HoodieOrcWriter<>(instantTime, path, orcConfig, schema, taskContextSupplier);
  }

  private static BloomFilter createBloomFilter(HoodieWriteConfig config) {
    return BloomFilterFactory.createBloomFilter(config.getInt(HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE), config.getDouble(HoodieIndexConfig.BLOOM_FILTER_FPP_VALUE),
        config.getInt(HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES),
        config.getString(HoodieIndexConfig.BLOOM_FILTER_TYPE));
  }
}
