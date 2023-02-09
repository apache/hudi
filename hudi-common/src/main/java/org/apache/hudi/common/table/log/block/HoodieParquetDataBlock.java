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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_BLOCK_SIZE;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_MAX_FILE_SIZE;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_PAGE_SIZE;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  private final Option<CompressionCodecName> compressionCodecName;
  private final Option<Double> expectedCompressionRatio;
  private final Option<Boolean> useDictionaryEncoding;

  public HoodieParquetDataBlock(FSDataInputStream inputStream,
                                Option<byte[]> content,
                                boolean readBlockLazily,
                                HoodieLogBlockContentLocation logBlockContentLocation,
                                Option<Schema> readerSchema,
                                Map<HeaderMetadataType, String> header,
                                Map<HeaderMetadataType, String> footer,
                                String keyField) {
    super(content, inputStream, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, keyField, false);

    this.compressionCodecName = Option.empty();
    this.expectedCompressionRatio = Option.empty();
    this.useDictionaryEncoding = Option.empty();
  }

  public HoodieParquetDataBlock(List<HoodieRecord> records,
                                Map<HeaderMetadataType, String> header,
                                String keyField,
                                CompressionCodecName compressionCodecName,
                                double expectedCompressionRatio,
                                boolean useDictionaryEncoding
  ) {
    super(records, header, new HashMap<>(), keyField);

    this.compressionCodecName = Option.of(compressionCodecName);
    this.expectedCompressionRatio = Option.of(expectedCompressionRatio);
    this.useDictionaryEncoding = Option.of(useDictionaryEncoding);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords(List<HoodieRecord> records) throws IOException {
    if (records.size() == 0) {
      return new byte[0];
    }

    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (FSDataOutputStream outputStream = new FSDataOutputStream(baos, null)) {
      HoodieFileWriter parquetWriter = null;
      HoodieConfig config = new HoodieConfig();
      config.setValue(PARQUET_COMPRESSION_CODEC_NAME.key(), compressionCodecName.get().name());
      config.setValue(PARQUET_BLOCK_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_BLOCK_SIZE));
      config.setValue(PARQUET_PAGE_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_PAGE_SIZE));
      config.setValue(PARQUET_MAX_FILE_SIZE.key(), String.valueOf(1024 * 1024 * 1024));
      config.setValue(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(expectedCompressionRatio.get()));
      config.setValue(PARQUET_DICTIONARY_ENABLED, String.valueOf(useDictionaryEncoding.get()));
      HoodieRecordType recordType = records.iterator().next().getRecordType();
      try {
        parquetWriter = HoodieFileWriterFactory.getFileWriter(
            HoodieFileFormat.PARQUET,
            outputStream,
            new Configuration(),
            config,
            writerSchema,
            recordType);
        for (HoodieRecord<?> record : records) {
          String recordKey = getRecordKey(record).orElse(null);
          parquetWriter.write(recordKey, record, writerSchema);
        }
        outputStream.flush();
      } finally {
        if (parquetWriter != null) {
          parquetWriter.close();
        }
      }
    }

    return baos.toByteArray();
  }

  /**
   * NOTE: We're overriding the whole reading sequence to make sure we properly respect
   *       the requested Reader's schema and only fetch the columns that have been explicitly
   *       requested by the caller (providing projected Reader's schema)
   */
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecordType type) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    Configuration inlineConf = new Configuration(blockContentLoc.getHadoopConf());
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
    inlineConf.setClassLoader(InLineFileSystem.class.getClassLoader());

    Path inlineLogFilePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().getFileSystem(inlineConf).getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());

    Schema writerSchema = new Schema.Parser().parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    ClosableIterator<HoodieRecord<T>> iterator = HoodieFileReaderFactory.getReaderFactory(type).getFileReader(inlineConf, inlineLogFilePath, PARQUET)
        .getRecordIterator(writerSchema, readerSchema);
    return iterator;
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }
}
