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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.hudi.common.model.HoodieFileFormat.LANCE;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

public class HoodieLanceDataBlock extends HoodieDataBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLanceDataBlock.class);

  public HoodieLanceDataBlock(List<HoodieRecord> records, Map<HeaderMetadataType, String> header, Map<FooterMetadataType, String> footer, String keyFieldName) {
    super(records, header, footer, keyFieldName);
  }

  public HoodieLanceDataBlock(Supplier<SeekableDataInputStream> inputStreamSupplier,
                              Option<byte[]> content,
                              boolean readBlockLazily,
                              HoodieLogBlockContentLocation logBlockContentLocation,
                              Option<HoodieSchema> readerSchema,
                              Map<HeaderMetadataType, String> header,
                              Map<FooterMetadataType, String> footer,
                              String keyField) {
    super(content, inputStreamSupplier, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, keyField, false);
  }

  @Override
  protected ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException {
    HoodieSchema writerSchema = HoodieSchemaCache.intern(HoodieSchema.parse(super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA)));
    return HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(LANCE)
        .serializeRecordsToLogBlock(storage, records, writerSchema, getSchema(), getKeyFieldName(), Collections.emptyMap());
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecord.HoodieRecordType type) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();
    HoodieSchema writerSchema = HoodieSchema.parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // Create temporary file from log block content
    File tempFile = createTempFile(content);
    StoragePath tempFilePath = new StoragePath(tempFile.toURI());
    HoodieStorage storage = blockContentLoc.getStorage();

    // Read from temporary file using Lance reader
    ClosableIterator<HoodieRecord<T>> iterator = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(type)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, tempFilePath, LANCE, Option.empty())
        .getRecordIterator(writerSchema, readerSchema);

    return fileCleanupIterator(iterator, tempFile);
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
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(SeekableDataInputStream inputStream,
                                                                     HoodieLogBlockContentLocation contentLocation,
                                                                     HoodieRecord.HoodieRecordType type,
                                                                     int bufferSize) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();
    HoodieSchema writerSchema = HoodieSchema.parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // Create temporary file from log block content
    File tempFile = createTempFile(inputStream, contentLocation, bufferSize);
    StoragePath tempFilePath = new StoragePath(tempFile.toURI());
    HoodieStorage storage = blockContentLoc.getStorage();

    // Read from temporary file using Lance reader
    ClosableIterator<HoodieRecord<T>> iterator = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(type)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, tempFilePath, LANCE, Option.empty())
        .getRecordIterator(writerSchema, readerSchema);

    return fileCleanupIterator(iterator, tempFile);
  }

  @Override
  protected <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException {
    HoodieSchema writerSchema = HoodieSchema.parse(this.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    // Create temporary file from log block content
    File tempFilePath = createTempFile(content);
    StoragePath storagePath = new StoragePath(tempFilePath.toURI());

    // Read from temporary file
    ClosableIterator<T> iterator = readerContext.getFileRecordIterator(
        storagePath, 0, content.length,
        writerSchema,
        readerSchema,
        getBlockContentLocation().get().getStorage());
    return fileCleanupIterator(iterator, tempFilePath);
  }

  private static <R> ClosableIterator<R> fileCleanupIterator(ClosableIterator<R> delegate, File tempFile) {
    return new ClosableIterator<R>() {
      @Override
      public void close() {
        try {
          delegate.close();
        } finally {
          boolean success = false;
          try {
            success = tempFile.delete();
          } catch (Exception e) {
            // Log but don't fail - temp file cleanup is best effort
            // Temp files will be cleaned up by OS eventually
          }
          if (!success) {
            LOG.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
          }
        }
      }

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public R next() {
        return delegate.next();
      }
    };
  }

  /**
   * Creates a temporary file from byte array content.
   * Lance library requires actual file paths and doesn't support InlineFS or stream-based reading.
   *
   * @param content The byte array content to write to the temp file
   * @return Java File object for the temporary file
   * @throws IOException if writing fails
   */
  private File createTempFile(byte[] content) throws IOException {
    File tempFile = createTempFile();
    // Write content to temp file
    try (OutputStream outputStream = Files.newOutputStream(tempFile.toPath())) {
      outputStream.write(content);
    }

    return tempFile;
  }

  private static File createTempFile() throws IOException {
    // Create temporary file with .lance extension
    File tempFile = File.createTempFile("lance-log-block-" + UUID.randomUUID(), ".lance");
    tempFile.deleteOnExit();
    return tempFile;
  }

  /**
   * Creates a temporary file by reading content from a stream.
   * Lance library requires actual file paths and doesn't support InlineFS or stream-based reading.
   *
   * @param inputStream The input stream to read from
   * @param contentLocation The location within the stream (offset and size)
   * @param bufferSize The size of the buffer to use for reading
   * @return Java File object for the temporary file
   * @throws IOException if reading or writing fails
   */
  private File createTempFile(SeekableDataInputStream inputStream,
                              HoodieLogBlockContentLocation contentLocation,
                              int bufferSize) throws IOException {
    File tempFile = createTempFile();
    inputStream.seek(contentLocation.getContentPositionInLogFile());
    try (OutputStream outputStream = Files.newOutputStream(tempFile.toPath())) {
      byte[] buffer = new byte[bufferSize];
      long bytesToRead = contentLocation.getBlockSize();
      while (bytesToRead > 0) {
        int bytesRead = inputStream.read(buffer, 0, (int) Math.min(bufferSize, bytesToRead));
        if (bytesRead == -1) {
          break; // EOF
        }
        outputStream.write(buffer, 0, bytesRead);
        bytesToRead -= bytesRead;
      }
    }
    return tempFile;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.LANCE_DATA_BLOCK;
  }
}
