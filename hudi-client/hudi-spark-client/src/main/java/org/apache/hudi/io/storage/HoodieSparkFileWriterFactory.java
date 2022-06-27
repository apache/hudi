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

import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.row.HoodieRowParquetConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class HoodieSparkFileWriterFactory extends HoodieFileWriterFactory {

  private static class SingletonHolder {

    private static final HoodieSparkFileWriterFactory INSTANCE = new HoodieSparkFileWriterFactory();
  }

  public static HoodieFileWriterFactory getFileWriterFactory() {
    return HoodieSparkFileWriterFactory.SingletonHolder.INSTANCE;
  }

  @Override
  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    boolean enableBloomFilter = populateMetaFields;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    // Support PARQUET_COMPRESSION_CODEC_NAME is ""
    if (compressionCodecName.isEmpty()) {
      compressionCodecName = null;
    }
    HoodieRowParquetWriteSupport writeSupport = new HoodieRowParquetWriteSupport(conf,
        HoodieInternalRowUtils.getCachedSchema(schema), filter,
        HoodieStorageConfig.newBuilder().fromProperties(config.getProps()).build());
    HoodieRowParquetConfig parquetConfig = new HoodieRowParquetConfig(writeSupport,
        CompressionCodecName.fromConf(compressionCodecName),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        conf,
        config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    parquetConfig.getHadoopConf().addResource(writeSupport.getHadoopConf());

    return new HoodieSparkParquetWriter(path, parquetConfig, instantTime, taskContextSupplier, config.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  protected HoodieFileWriter newParquetFileWriter(
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    boolean enableBloomFilter = false;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieRowParquetWriteSupport writeSupport = new HoodieRowParquetWriteSupport(conf,
        HoodieInternalRowUtils.getCachedSchema(schema), filter,
        HoodieStorageConfig.newBuilder().fromProperties(config.getProps()).build());
    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    // Support PARQUET_COMPRESSION_CODEC_NAME is ""
    if (compressionCodecName.isEmpty()) {
      compressionCodecName = null;
    }
    HoodieRowParquetConfig parquetConfig = new HoodieRowParquetConfig(writeSupport,
        CompressionCodecName.fromConf(compressionCodecName),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        writeSupport.getHadoopConf(), config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    parquetConfig.getHadoopConf().addResource(writeSupport.getHadoopConf());
    return new HoodieSparkParquetStreamWriter(outputStream, parquetConfig);
  }

  @Override
  protected HoodieFileWriter newHFileFileWriter(String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new HoodieIOException("Not support write to HFile");
  }

  @Override
  protected HoodieFileWriter newOrcFileWriter(String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new HoodieIOException("Not support write to Orc file");
  }
}
