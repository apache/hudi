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
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetStreamWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  public HoodieParquetDataBlock(
      @Nonnull Map<HeaderMetadataType, String> logBlockHeader,
      @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
      @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation,
      @Nonnull Option<byte[]> content,
      FSDataInputStream inputStream,
      boolean readBlockLazily
  ) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
  }

  public HoodieParquetDataBlock(
      HoodieLogFile logFile,
      FSDataInputStream inputStream,
      Option<byte[]> content,
      boolean readBlockLazily, long position, long blockSize, long blockEndpos,
      Schema readerSchema,
      Map<HeaderMetadataType, String> header,
      Map<HeaderMetadataType, String> footer,
      String keyField
  ) {
    super(
        content,
        inputStream,
        readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)),
        readerSchema,
        header,
        footer,
        keyField);
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

    // TODO: Need to decide from where to fetch all config values required below. We can't re-use index config as the purpose is different.
    // And these are very specific to data blocks. Once we have consensus, we might need to route them to log block constructors (
    // as of now, log block constructors does not take in any configs in general).
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        Integer.parseInt("60000"),//HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES.defaultValue()),
        Double.parseDouble("0.000000001"),//HoodieIndexConfig.BLOOM_FILTER_FPP.defaultValue()),
        Integer.parseInt("100000"),//HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue()),
        BloomFilterTypeCode.SIMPLE.name());//HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE.defaultValue());

    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(writerSchema), writerSchema, Option.of(filter));

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

    try (DataOutputStream outputStream = new DataOutputStream(baos)) {
      try (HoodieParquetStreamWriter<IndexedRecord> parquetWriter = new HoodieParquetStreamWriter<>(outputStream, avroParquetConfig)) {
        Iterator<IndexedRecord> itr = records.iterator();
        if (getRecordKey(records.get(0)) == null) {
          throw new HoodieIOException("Record key field missing from schema for records to be written to Parquet data block");
        }

        while (itr.hasNext()) {
          IndexedRecord record = itr.next();
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
    Configuration conf = new Configuration();

    ArrayList<IndexedRecord> records = new ArrayList<>();

    getParquetRecordsIterator(conf, readerSchema, new ByteBufferBackedInputFile(content))
        .forEachRemaining(records::add);

    return records;
  }

  static class ByteBufferBackedInputFile implements InputFile {
    private final byte[] buffer;
    private final int offset;
    private final int length;

    ByteBufferBackedInputFile(byte[] buffer, int offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }

    ByteBufferBackedInputFile(byte[] buffer) {
      this(buffer, 0, buffer.length);
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(new ByteBufferBackedInputStream(buffer, offset, length)) {
        @Override
        public long getPos() {
          return ((ByteBufferBackedInputStream) getStream()).getPosition();
        }

        @Override
        public void seek(long newPos) {
          ((ByteBufferBackedInputStream) getStream()).seek(newPos);
        }
      };
    }
  }
}