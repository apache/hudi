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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

/**
 * HoodieParquetDataBlock contains a list of records serialized using Parquet.
 */
public class HoodieParquetDataBlock extends HoodieDataBlock {

  private final Option<String> compressionCodecName;
  private final Option<Double> expectedCompressionRatio;
  private final Option<Boolean> useDictionaryEncoding;

  public HoodieParquetDataBlock(Supplier<SeekableDataInputStream> inputStreamSupplier,
                                Option<byte[]> content,
                                boolean readBlockLazily,
                                HoodieLogBlockContentLocation logBlockContentLocation,
                                Option<Schema> readerSchema,
                                Map<HeaderMetadataType, String> header,
                                Map<HeaderMetadataType, String> footer,
                                String keyField) {
    super(content, inputStreamSupplier, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, keyField, false);

    this.compressionCodecName = Option.empty();
    this.expectedCompressionRatio = Option.empty();
    this.useDictionaryEncoding = Option.empty();
  }

  public HoodieParquetDataBlock(List<HoodieRecord> records,
                                Map<HeaderMetadataType, String> header,
                                String keyField,
                                String compressionCodecName,
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
  protected byte[] serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(PARQUET_COMPRESSION_CODEC_NAME.key(), compressionCodecName.get());
    paramsMap.put(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(expectedCompressionRatio.get()));
    paramsMap.put(PARQUET_DICTIONARY_ENABLED.key(), String.valueOf(useDictionaryEncoding.get()));
    Schema writerSchema = new Schema.Parser().parse(
        super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA));

    return HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(PARQUET)
        .serializeRecordsToLogBlock(
            storage, records, writerSchema, getSchema(), getKeyFieldName(), paramsMap);
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
    StorageConfiguration<?> inlineConf = blockContentLoc.getStorage().getConf().getInline();
    StoragePath inlineLogFilePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().toUri().getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());

    HoodieStorage inlineStorage = getBlockContentLocation().get().getStorage().newInstance(inlineLogFilePath, inlineConf);
    Schema writerSchema = new Schema.Parser().parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    ClosableIterator<HoodieRecord<T>> iterator = HoodieIOFactory.getIOFactory(inlineStorage)
        .getReaderFactory(type)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, inlineLogFilePath, PARQUET, Option.empty())
        .getRecordIterator(writerSchema, readerSchema);
    return iterator;
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    throw new UnsupportedOperationException("Should not be invoked");
  }
}
