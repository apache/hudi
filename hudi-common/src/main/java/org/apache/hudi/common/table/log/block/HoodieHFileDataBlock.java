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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieHFileDataBlock contains a list of records stored inside an HFile format. It is used with the HFile
 * base file format.
 */
public class HoodieHFileDataBlock extends HoodieDataBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHFileDataBlock.class);

  private final Option<String> compressionCodec;
  // This path is used for constructing HFile reader context, which should not be
  // interpreted as the actual file path for the HFile data blocks
  private final StoragePath pathForReader;

  public HoodieHFileDataBlock(Supplier<SeekableDataInputStream> inputStreamSupplier,
                              Option<byte[]> content,
                              boolean readBlockLazily,
                              HoodieLogBlockContentLocation logBlockContentLocation,
                              Option<Schema> readerSchema,
                              Map<HeaderMetadataType, String> header,
                              Map<FooterMetadataType, String> footer,
                              boolean enablePointLookups,
                              StoragePath pathForReader) {
    super(content, inputStreamSupplier, readBlockLazily, Option.of(logBlockContentLocation), readerSchema,
        header, footer, HoodieAvroHFileReaderImplBase.KEY_FIELD_NAME, enablePointLookups);
    this.compressionCodec = Option.empty();
    this.pathForReader = pathForReader;
  }

  public HoodieHFileDataBlock(List<HoodieRecord> records,
                              Map<HeaderMetadataType, String> header,
                              String compressionCodec,
                              StoragePath pathForReader) {
    super(records, header, new HashMap<>(), HoodieAvroHFileReaderImplBase.KEY_FIELD_NAME);
    if (!this.records.isEmpty()) {
      this.records.get().sort(Comparator.comparing(HoodieRecord::getRecordKey));
    }
    this.compressionCodec = Option.of(compressionCodec);
    this.pathForReader = pathForReader;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.HFILE_DATA_BLOCK;
  }

  @Override
  protected ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    Schema writerSchema = new Schema.Parser().parse(
        super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA));
    return HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.HFILE)
        .serializeRecordsToLogBlock(
            storage, records, writerSchema, getSchema(), getKeyFieldName(),
            Collections.singletonMap(HFILE_COMPRESSION_ALGORITHM_NAME.key(), compressionCodec.get()));
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    checkState(readerSchema != null, "Reader's schema has to be non-null");

    StorageConfiguration<?> storageConf = getBlockContentLocation().get().getStorage().getConf().getInline();
    HoodieStorage inlineStorage = getBlockContentLocation().get().getStorage().newInstance(pathForReader, storageConf);
    // Read the content
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(inlineStorage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getContentReader(ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER, pathForReader, HoodieFileFormat.HFILE,
            inlineStorage, content, Option.of(getSchemaFromHeader()))) {
      return unsafeCast(reader.getRecordIterator(readerSchema));
    }
  }

  /**
   * Streaming deserialization of records.
   *
   * @param inputStream The input stream from which to read the records.
   * @param contentLocation The location within the input stream where the content starts.
   * @param bufferSize The size of the buffer to use for reading the records.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws IOException If there is an error reading or deserializing the records.
   */
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(
      SeekableDataInputStream inputStream,
      HoodieLogBlockContentLocation contentLocation,
      HoodieRecordType type,
      int bufferSize
  ) throws IOException {
    throw new UnsupportedOperationException("Streaming deserialization is not supported for HoodieHFileDataBlock");
  }

  @Override
  protected <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException {
    checkState(readerSchema != null, "Reader's schema has to be non-null");

    StorageConfiguration<?> storageConf = getBlockContentLocation().get().getStorage().getConf().getInline();
    HoodieStorage inlineStorage = getBlockContentLocation().get().getStorage().newInstance(pathForReader, storageConf);
    // Read the content
    try (HoodieAvroHFileReaderImplBase reader = (HoodieAvroHFileReaderImplBase)
        HoodieIOFactory.getIOFactory(inlineStorage).getReaderFactory(HoodieRecordType.AVRO).getContentReader(
            readerContext.getHoodieReaderConfig(), pathForReader, HoodieFileFormat.HFILE, inlineStorage, content,
            Option.of(getSchemaFromHeader()))) {
      return unsafeCast(reader.getIndexedRecordIterator(readerSchema, readerSchema));
    }
  }

  // TODO abstract this w/in HoodieDataBlock
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> lookupRecords(List<String> sortedKeys, boolean fullKey) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    StorageConfiguration<?> inlineConf = getBlockContentLocation().get().getStorage().getConf().getInline();
    StoragePath inlinePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().toUri().getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());
    HoodieStorage inlineStorage = getBlockContentLocation().get().getStorage().newInstance(inlinePath, inlineConf);

    try (final HoodieAvroHFileReaderImplBase reader = (HoodieAvroHFileReaderImplBase) HoodieIOFactory
        .getIOFactory(inlineStorage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER, inlinePath, HoodieFileFormat.HFILE, Option.of(getSchemaFromHeader()))) {
      // Get writer's schema from the header
      final ClosableIterator<HoodieRecord<IndexedRecord>> recordIterator =
          fullKey ? reader.getRecordsByKeysIterator(sortedKeys, readerSchema) : reader.getRecordsByKeyPrefixIterator(sortedKeys, readerSchema);

      return new CloseableMappingIterator<>(recordIterator, data -> (HoodieRecord<T>) data);
    }
  }
}
