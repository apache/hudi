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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.io.storage.HoodieAvroFileReader.HoodieRecordTransformIterator;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetStreamWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  private final Option<CompressionCodecName> compressionCodecName;

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
  }

  public HoodieParquetDataBlock(
      @Nonnull List<HoodieRecord> records,
      @Nonnull Map<HeaderMetadataType, String> header,
      @Nonnull String keyField,
      @Nonnull CompressionCodecName compressionCodecName
  ) {
    super(records, header, new HashMap<>(), keyField);

    this.compressionCodecName = Option.of(compressionCodecName);
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

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(writerSchema), writerSchema, Option.empty());

    HoodieAvroParquetConfig avroParquetConfig =
        new HoodieAvroParquetConfig(
            writeSupport,
            compressionCodecName.get(),
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            1024 * 1024 * 1024,
            new Configuration(),
            Double.parseDouble(String.valueOf(0.1)));//HoodieStorageConfig.PARQUET_COMPRESSION_RATIO.defaultValue()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (FSDataOutputStream outputStream = new FSDataOutputStream(baos)) {
      try (HoodieParquetStreamWriter parquetWriter = new HoodieParquetStreamWriter(outputStream, avroParquetConfig)) {
        for (HoodieRecord record : records) {
          String recordKey = getRecordKey(record).orElse(null);
          parquetWriter.write(recordKey, record, writerSchema);
        }
        outputStream.flush();
      }
    }

    return baos.toByteArray();
  }

  public static ClosableIterator<IndexedRecord> getProjectedParquetRecordsIterator(Configuration conf,
                                                                                   Schema readerSchema,
                                                                                   InputFile inputFile) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, readerSchema);
    AvroReadSupport.setRequestedProjection(conf, readerSchema);

    ParquetReader<IndexedRecord> reader =
        AvroParquetReader.<IndexedRecord>builder(inputFile).withConf(conf).build();
    return new ParquetReaderIterator<>(reader);
  }

  /**
   * NOTE: We're overriding the whole reading sequence to make sure we properly respect
   *       the requested Reader's schema and only fetch the columns that have been explicitly
   *       requested by the caller (providing projected Reader's schema)
   */
  @Override
  protected ClosableIterator<HoodieRecord> readRecordsFromBlockPayload(HoodieRecord.Mapper mapper) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    Configuration inlineConf = new Configuration(blockContentLoc.getHadoopConf());
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());

    Path inlineLogFilePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().getFileSystem(inlineConf).getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());

    ClosableIterator<IndexedRecord> iterator = getProjectedParquetRecordsIterator(
        inlineConf,
        readerSchema,
        HadoopInputFile.fromPath(inlineLogFilePath, inlineConf));
    return new HoodieRecordTransformIterator(iterator, mapper);
  }

  @Override
  protected ClosableIterator<HoodieRecord> deserializeRecords(byte[] content, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }
}