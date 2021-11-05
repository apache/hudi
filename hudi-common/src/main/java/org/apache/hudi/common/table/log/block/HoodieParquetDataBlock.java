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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetReader;
import org.apache.hudi.io.storage.HoodieParquetStreamWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

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

  public HoodieParquetDataBlock(@Nonnull Map<HeaderMetadataType, String> logBlockHeader,
                                @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
                                @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation, @Nonnull Option<byte[]> content,
                                FSDataInputStream inputStream, boolean readBlockLazily) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
  }

  public HoodieParquetDataBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
                                boolean readBlockLazily, long position, long blockSize, long blockEndpos, Schema readerSchema,
                                Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)), readerSchema, header,
        footer);
  }

  public HoodieParquetDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header) {
    super(records, header, new HashMap<>());
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords() throws IOException {
    // TODO: Need to decide from where to fetch all config values required below. We can't re-use index config as the purpose is different.
    // And these are very specific to data blocks. Once we have consensus, we might need to route them to log block constructors (
    // as of now, log block constructors does not take in any configs in general).
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        Integer.parseInt("60000"),//HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES.defaultValue()),
        Double.parseDouble("0.000000001"),//HoodieIndexConfig.BLOOM_FILTER_FPP.defaultValue()),
        Integer.parseInt("100000"),//HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue()),
        BloomFilterTypeCode.SIMPLE.name());//HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE.defaultValue());

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, Option.of(filter));

    HoodieAvroParquetConfig avroParquetConfig = new HoodieAvroParquetConfig(writeSupport, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 1024 * 1024 * 1024,
        new Configuration(), Double.parseDouble(String.valueOf(0.1)));//HoodieStorageConfig.PARQUET_COMPRESSION_RATIO.defaultValue()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = null;
    HoodieParquetStreamWriter<IndexedRecord> parquetWriter = null;
    try {
      outputStream = new DataOutputStream(baos);
      parquetWriter = new HoodieParquetStreamWriter<>(outputStream, avroParquetConfig);
      Iterator<IndexedRecord> itr = records.iterator();
      if (records.size() > 0) {
        Schema.Field keyField = records.get(0).getSchema().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
        if (keyField == null) {
          throw new HoodieIOException("Record key field missing from schema for records to be written to Parquet data block");
        }
        while (itr.hasNext()) {
          IndexedRecord record = itr.next();
          String recordKey = record.get(keyField.pos()).toString();
          parquetWriter.writeAvro(recordKey, record);
        }
        outputStream.flush();
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
      if (parquetWriter != null) {
        parquetWriter.close();
      }
    }

    return baos.toByteArray();
  }

  @Override
  public List<IndexedRecord> getRecords() {
    try {
      records = new ArrayList<>();
      // Get schema from the header
      Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
      // If readerSchema was not present, use writerSchema
      if (schema == null) {
        schema = writerSchema;
      }
      Configuration conf = new Configuration();
      Configuration inlineConf = new Configuration();
      inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());

      Path inlinePath = InLineFSUtils.getInlineFilePath(
          getBlockContentLocation().get().getLogFile().getPath(),
          getBlockContentLocation().get().getLogFile().getPath().getFileSystem(conf).getScheme(),
          getBlockContentLocation().get().getContentPositionInLogFile(),
          getBlockContentLocation().get().getBlockSize());

      HoodieParquetReader<IndexedRecord> parquetReader = new HoodieParquetReader<>(inlineConf, inlinePath);
      Iterator<IndexedRecord> recordIterator = parquetReader.getRecordIterator(schema);
      while (recordIterator.hasNext()) {
        records.add(recordIterator.next());
      }
      return records;
    } catch (IOException e) {
      throw new HoodieIOException("Reading parquet inlining failed ", e);
    }
  }

  @Override
  protected void deserializeRecords() throws IOException {
    throw new IOException("Not implemented");
  }
} 