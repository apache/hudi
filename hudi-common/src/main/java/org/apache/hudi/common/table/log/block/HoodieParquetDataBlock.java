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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetStreamWriter;
import org.apache.hudi.parquet.io.ByteBufferBackedInputFile;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  public HoodieParquetDataBlock(
      HoodieLogFile logFile,
      FSDataInputStream inputStream,
      Option<byte[]> content,
      boolean readBlockLazily, long position, long blockSize, long blockEndpos,
      Option<Schema> readerSchema,
      Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer,
      String keyField,
      boolean enablePointLookups
  ) {
    super(
        content,
        inputStream,
        readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)),
        readerSchema,
        header,
        footer,
        keyField,
        enablePointLookups);
  }

  public HoodieParquetDataBlock(
      @Nonnull List<IndexedRecord> records,
      @Nonnull Map<HeaderMetadataType, String> header,
      @Nonnull String keyField
  ) {
    super(records, header, new HashMap<>(), keyField);
  }

  public HoodieParquetDataBlock(
      @Nonnull List<IndexedRecord> records,
      @Nonnull Map<HeaderMetadataType, String> header
  ) {
    super(records, header, new HashMap<>(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  public static Iterator<IndexedRecord> getParquetRecordsIterator(
      Configuration conf,
      Schema schema,
      InputFile inputFile
  ) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, schema);
    ParquetReader<IndexedRecord> reader =
        AvroParquetReader.<IndexedRecord>builder(inputFile).withConf(conf).build();
    return new ParquetReaderIterator<>(reader);
  }

  @Override
  protected byte[] serializeRecords(List<IndexedRecord> records) throws IOException {
    if (records.size() == 0) {
      return new byte[0];
    }

    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(writerSchema), writerSchema, Option.empty());

    HoodieAvroParquetConfig avroParquetConfig =
        new HoodieAvroParquetConfig(
            writeSupport,
            // TODO fetch compression codec from the config
            CompressionCodecName.GZIP,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            1024 * 1024 * 1024,
            new Configuration(),
            Double.parseDouble(String.valueOf(0.1)));//HoodieStorageConfig.PARQUET_COMPRESSION_RATIO.defaultValue()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (FSDataOutputStream outputStream = new FSDataOutputStream(baos)) {
      try (HoodieParquetStreamWriter<IndexedRecord> parquetWriter = new HoodieParquetStreamWriter<>(outputStream, avroParquetConfig)) {
        for (IndexedRecord record : records) {
          String recordKey = getRecordKey(record);
          parquetWriter.writeAvro(recordKey, record);
        }
        outputStream.flush();
      }
    }

    return baos.toByteArray();
  }

  @Override
  protected List<IndexedRecord> deserializeRecords(byte[] content) throws IOException {
    if (content.length == 0) {
      return Collections.emptyList();
    }

    ArrayList<IndexedRecord> records = new ArrayList<>();
    getParquetRecordsIterator(new Configuration(), readerSchema, new ByteBufferBackedInputFile(content))
        .forEachRemaining(records::add);

    return records;
  }
}