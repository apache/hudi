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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.io.storage.HoodieHFileConfig.CACHE_DATA_IN_L1;
import static org.apache.hudi.io.storage.HoodieHFileConfig.DROP_BEHIND_CACHE_COMPACTION;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileConfig.PREFETCH_ON_OPEN;

public class HoodieFileWriterFactory {

  private static HoodieFileWriterFactory writerFactory;

  public static HoodieFileWriterFactory getFileWriterFactory() {
    if (writerFactory == null) {
      writerFactory = new HoodieFileWriterFactory();
    }
    return writerFactory;
  }

  private static HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return getFileWriterFactory();
      case SPARK:
        Exception exception = null;
        try {
          Class<?> clazz = ReflectionUtils.getClass("org.apache.hudi.io.storage.HoodieSparkFileWriterFactory");
          Method method = clazz.getMethod("getFileWriterFactory", null);
          return (HoodieFileWriterFactory) method.invoke(null,null);
        } catch (NoSuchMethodException e) {
          exception = e;
        } catch (IllegalAccessException e) {
          exception = e;
        } catch (IllegalArgumentException e) {
          exception = e;
        } catch (InvocationTargetException e) {
          exception = e;
        }
        if (exception != null) {
          throw new HoodieException("Unable to create hoodie spark file writer factory", exception);
        }
        break;
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported yet.");
    }
    return null;
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    HoodieFileWriterFactory factory = getWriterFactory(// TODO: a better way to get info
        HoodieRecord.HoodieRecordType.valueOf(config.getProps().getString("hoodie.datasource.write.record.type", HoodieRecord.HoodieRecordType.AVRO.toString())));
    return factory.getFileWriterByFormat(extension, instantTime, path, conf, config, schema, taskContextSupplier);
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(HoodieFileFormat format,
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    HoodieFileWriterFactory factory = getWriterFactory(// TODO: a better way to get info
        HoodieRecord.HoodieRecordType.valueOf(config.getProps().getString("hoodie.datasource.write.record.type", HoodieRecord.HoodieRecordType.AVRO.toString())));
    return factory.getFileWriterByFormat(format, outputStream, conf, config, schema);
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(
      String extension, String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(HoodieFileFormat format,
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileWriter(outputStream, conf, config, schema);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    boolean populateMetaFields = config.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS);
    boolean enableBloomFilter = populateMetaFields;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);
    HoodieAvroParquetConfig parquetConfig = new HoodieAvroParquetConfig(writeSupport,
        CompressionCodecName.fromConf(config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
        conf, config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
        config.getBoolean(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED));
    return new HoodieAvroParquetWriter(path, parquetConfig, instantTime, taskContextSupplier, populateMetaFields);
  }

  protected HoodieFileWriter newParquetFileWriter(
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    boolean populateMetaFields = true;
    boolean enableBloomFilter = false;
    Option<BloomFilter> filter = enableBloomFilter ? Option.of(createBloomFilter(config)) : Option.empty();
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, filter);
    HoodieAvroParquetConfig parquetConfig = new HoodieAvroParquetConfig(writeSupport,
        CompressionCodecName.fromConf(config.getString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        config.getInt(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
        config.getInt(HoodieStorageConfig.PARQUET_PAGE_SIZE),
        config.getLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE), // todo: 1024*1024*1024
        conf, config.getDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION));
    return new HoodieAvroParquetWriter(outputStream, parquetConfig, populateMetaFields);
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

  protected BloomFilter createBloomFilter(HoodieConfig config) {
    return BloomFilterFactory.createBloomFilter(60000, 0.000000001, 100000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
  }
}
