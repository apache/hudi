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
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.util.ParquetUtils.getCompressionCodecName;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

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
   * @param config hoodie config
   * @param schema write schema
   * @return an HoodieFileWriter for writing hoodie records.
   */
  @Override
  protected HoodieFileWriter newParquetFileWriter(
      OutputStream outputStream,
      HoodieConfig config,
      Schema schema) throws IOException {
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(schema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    HoodieRowDataParquetWriteSupport writeSupport =
        new HoodieRowDataParquetWriteSupport(
            storage.getConf().unwrapAs(Configuration.class), rowType, null);
    return new HoodieRowDataParquetOutputStreamWriter(
        new FSDataOutputStream(outputStream, null),
        writeSupport,
        new HoodieParquetConfig<>(
            writeSupport,
            getCompressionCodecName(config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
            config.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
            config.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE),
            config.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE),
            new HadoopStorageConfiguration(writeSupport.getHadoopConf()),
            config.getDoubleOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION),
            config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED)));
  }

  /**
   * Create a parquet RowData writer on a given storage path.
   *
   * @param instantTime instant time to write
   * @param storagePath file storage path
   * @param config Hoodie configuration
   * @param schema write schema
   * @param taskContextSupplier task context supplier
   * @return A RowData parquet writer
   */
  @Override
  protected HoodieFileWriter newParquetFileWriter(
      String instantTime,
      StoragePath storagePath,
      HoodieConfig config,
      Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(schema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    return newParquetInternalRowFileWriter(new Path(storagePath.toUri()), (HoodieWriteConfig) config, rowType, conf);
  }

  /**
   * Factory method to assist in instantiating an instance of {@link HoodieRowDataFileWriter}.
   *
   * @param path        path of the RowFileWriter.
   * @param hoodieTable instance of {@link HoodieTable} in use.
   * @param config      instance of {@link HoodieWriteConfig} to use.
   * @param schema      schema of the dataset in use.
   * @return the instantiated {@link HoodieRowDataFileWriter}.
   * @throws IOException if format is not supported or if any exception during instantiating the RowFileWriter.
   */
  public static HoodieRowDataFileWriter getRowDataFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, RowType schema)
      throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      Configuration conf = (Configuration) hoodieTable.getStorageConf().unwrap();
      return newParquetInternalRowFileWriter(path, config, schema, conf);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static HoodieRowDataFileWriter newParquetInternalRowFileWriter(
      Path path, HoodieWriteConfig writeConfig, RowType rowType, Configuration conf)
      throws IOException {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());

    HoodieRowDataParquetWriteSupport writeSupport = (HoodieRowDataParquetWriteSupport) ReflectionUtils.loadClass(
        writeConfig.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_FLINK_ROW_DATA_WRITE_SUPPORT_CLASS),
        new Class<?>[] {Configuration.class, RowType.class, BloomFilter.class},
        conf, rowType, filter);

    return new HoodieRowDataParquetWriter(
        convertToStoragePath(path),
        new HoodieParquetConfig<>(
            writeSupport,
            getCompressionCodecName(writeConfig.getParquetCompressionCodec()),
            writeConfig.getParquetBlockSize(),
            writeConfig.getParquetPageSize(),
            writeConfig.getParquetMaxFileSize(),
            new HadoopStorageConfiguration(writeSupport.getHadoopConf()),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled()));
  }
}
