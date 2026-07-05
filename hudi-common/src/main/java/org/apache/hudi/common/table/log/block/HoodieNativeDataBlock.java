/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

/**
 * Data block backed by a native log file.
 */
public class HoodieNativeDataBlock extends HoodieDataBlock {

  private final HoodieStorage storage;
  private final HoodieLogFile logFile;
  private final HoodieFileFormat fileFormat;

  public HoodieNativeDataBlock(HoodieStorage storage,
                               HoodieLogFile logFile,
                               HoodieFileFormat fileFormat,
                               Option<HoodieSchema> readerSchema,
                               Map<HeaderMetadataType, String> header,
                               Map<FooterMetadataType, String> footer) {
    super(Option.empty(), null, true, getContentLocation(storage, logFile), readerSchema,
        header, footer, HoodieRecord.RECORD_KEY_METADATA_FIELD, false);
    this.storage = storage;
    this.logFile = logFile;
    this.fileFormat = fileFormat;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    // This is a synthetic reader-side block for native log files. Returning PARQUET_DATA_BLOCK
    // keeps the existing data-block processing/statistics path; native file decoding is driven
    // by fileFormat and readRecordsFromBlockPayload, not by this block type.
    return HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecord.HoodieRecordType type) throws IOException {
    StoragePath path = logFile.getPath();
    return HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(type)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, path, fileFormat, Option.empty())
        .getRecordIterator(getSchemaFromHeader(), readerSchema);
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecord.HoodieRecordType type, int ignoredBufferSize) throws IOException {
    return readRecordsFromBlockPayload(type);
  }

  @Override
  protected <T> ClosableIterator<T> readRecordsFromBlockPayload(HoodieReaderContext<T> readerContext) throws IOException {
    return readerContext.getFileRecordIterator(
        logFile.getPath(), 0, FSUtils.getFileSize(storage, logFile),
        getSchemaFromHeader(),
        readerSchema,
        storage);
  }

  @Override
  protected <T> ClosableIterator<T> lookupEngineRecords(HoodieReaderContext<T> readerContext, List<String> keys, boolean fullKey) throws IOException {
    return FilteringEngineRecordIterator.getInstance(
        readRecordsFromBlockPayload(readerContext),
        new HashSet<>(keys),
        fullKey,
        record -> Option.ofNullable(readerContext.getRecordContext().getRecordKey(record, readerSchema)));
  }

  @Override
  protected ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) {
    throw new UnsupportedOperationException("Native log data blocks are read-only");
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecord.HoodieRecordType type) {
    throw new UnsupportedOperationException("Native log data blocks read records directly from native files");
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(
      SeekableDataInputStream inputStream,
      HoodieLogBlockContentLocation contentLocation,
      HoodieRecord.HoodieRecordType type,
      int bufferSize) {
    throw new UnsupportedOperationException("Native log data blocks do not use inline log block content");
  }

  @Override
  protected <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) {
    throw new UnsupportedOperationException("Native log data blocks read records directly from native files");
  }

  private static Option<HoodieLogBlockContentLocation> getContentLocation(HoodieStorage storage, HoodieLogFile logFile) {
    long fileSize = FSUtils.getFileSize(storage, logFile);
    return Option.of(new HoodieLogBlockContentLocation(storage, logFile, 0, fileSize, fileSize));
  }
}
