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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.FileGroupReaderState;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CachingIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
public final class HoodieFileGroupReader<T> implements Closeable {
  private final HoodieReaderContext<T> readerContext;
  private final FileGroupReaderState readerState;
  private final Option<HoodieBaseFile> hoodieBaseFileOption;
  private final List<HoodieLogFile> logFiles;
  private final HoodieStorage storage;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;
  // Core structure to store and process records.
  private final HoodieFileGroupRecordBuffer<T> recordBuffer;
  private ClosableIterator<T> baseFileIterator;
  private final Option<UnaryOperator<T>> outputConverter;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               HoodieStorage storage,
                               String latestCommitTime,
                               FileSlice fileSlice,
                               Schema dataSchema,
                               Schema requestedSchema,
                               Option<InternalSchema> internalSchemaOpt,
                               HoodieTableMetaClient hoodieTableMetaClient,
                               TypedProperties props,
                               long start,
                               long length,
                               boolean shouldUseRecordPosition) {
    this.readerContext = readerContext;
    this.readerState = new FileGroupReaderState();
    readerContext.setHoodieReaderState(readerState);
    this.storage = storage;
    this.hoodieBaseFileOption = fileSlice.getBaseFile();
    this.logFiles = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    this.start = start;
    this.length = length;
    HoodieTableConfig tableConfig = hoodieTableMetaClient.getTableConfig();
    readerState.setProps(props);
    readerState.setMetaClient(hoodieTableMetaClient);
    readerState.setRecordMerger(readerContext.getRecordMerger(tableConfig.getRecordMergerStrategy()));
    readerState.setLatestCommitTime(latestCommitTime);
    readerState.setShouldMergeUseRecordPosition(shouldUseRecordPosition);
    readerState.setHasLogFiles(!this.logFiles.isEmpty());
    if (readerState.getHasLogFiles() && start != 0) {
      throw new IllegalArgumentException("Filegroup reader is doing log file merge but not reading from the start of the base file");
    }
    readerState.setHasBootstrapBaseFile(hoodieBaseFileOption.isPresent() && hoodieBaseFileOption.get().getBootstrapBaseFile().isPresent());
    readerState.setSchemaHandler(readerContext.supportsParquetRowIndex()
        ? new HoodiePositionBasedSchemaHandler(readerState, dataSchema, requestedSchema, internalSchemaOpt)
        : new HoodieFileGroupReaderSchemaHandler(readerState, dataSchema, requestedSchema, internalSchemaOpt));
    this.outputConverter = readerState.getRequestedSchema().equals(readerState.getRequiredSchema())
        ? Option.empty()
        : Option.of(readerContext.projectRecord(readerState.getRequiredSchema(), readerState.getRequestedSchema()));
    this.recordBuffer = this.logFiles.isEmpty()
        ? null
        : shouldUseRecordPosition
        ? new HoodiePositionBasedFileGroupRecordBuffer<>(readerContext, readerState)
        : new HoodieKeyBasedFileGroupRecordBuffer<>(readerContext, readerState);
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  public void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();
    if (logFiles.isEmpty()) {
      this.baseFileIterator = CachingIterator.wrap(iter, readerContext);
    } else {
      this.baseFileIterator = iter;
      scanLogFiles();
      recordBuffer.setBaseFileIterator(baseFileIterator);
    }
  }

  private ClosableIterator<T> makeBaseFileIterator() throws IOException {
    if (!hoodieBaseFileOption.isPresent()) {
      return new EmptyIterator<>();
    }

    HoodieBaseFile baseFile = hoodieBaseFileOption.get();
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      return makeBootstrapBaseFileIterator(baseFile);
    }

    StoragePathInfo baseFileStoragePathInfo = baseFile.getPathInfo();
    if (baseFileStoragePathInfo != null) {
      return readerContext.getFileRecordIterator(
          baseFileStoragePathInfo, start, length,
          readerState.getDataSchema(),
          readerState.getRequiredSchema(), storage);
    } else {
      return readerContext.getFileRecordIterator(
          baseFile.getStoragePath(), start, length,
          readerState.getDataSchema(),
          readerState.getRequiredSchema(), storage);
    }
  }

  private ClosableIterator<T> makeBootstrapBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile dataFile = baseFile.getBootstrapBaseFile().get();
    Pair<List<Schema.Field>, List<Schema.Field>> requiredFields = readerState.getSchemaHandler().getBootstrapRequiredFields();
    Pair<List<Schema.Field>, List<Schema.Field>> allFields = readerState.getSchemaHandler().getBootstrapDataFields();
    Option<Pair<ClosableIterator<T>, Schema>> dataFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getRight(), allFields.getRight(), dataFile);
    Option<Pair<ClosableIterator<T>, Schema>> skeletonFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getLeft(), allFields.getLeft(), baseFile);
    if (!dataFileIterator.isPresent() && !skeletonFileIterator.isPresent()) {
      throw new IllegalStateException("should not be here if only partition cols are required");
    } else if (!dataFileIterator.isPresent()) {
      return skeletonFileIterator.get().getLeft();
    } else if (!skeletonFileIterator.isPresent()) {
      return dataFileIterator.get().getLeft();
    } else {
      if (start != 0) {
        throw new IllegalArgumentException("Filegroup reader is doing bootstrap merge but we are not reading from the start of the base file");
      }
      return readerContext.mergeBootstrapReaders(skeletonFileIterator.get().getLeft(), skeletonFileIterator.get().getRight(),
          dataFileIterator.get().getLeft(), dataFileIterator.get().getRight());
    }
  }

  /**
   * Creates file record iterator to read bootstrap skeleton or data file
   *
   * @param requiredFields list of fields that are expected to be read from the file
   * @param allFields      list of all fields in the data file to be read
   * @param file           file to be read
   * @return pair of the record iterator of the file, and the schema of the data being read
   */
  private Option<Pair<ClosableIterator<T>, Schema>> makeBootstrapBaseFileIteratorHelper(List<Schema.Field> requiredFields,
                                                                                        List<Schema.Field> allFields,
                                                                                        BaseFile file) throws IOException {
    if (requiredFields.isEmpty()) {
      return Option.empty();
    }
    Schema requiredSchema = readerState.getSchemaHandler().createSchemaFromFields(requiredFields);
    StoragePathInfo fileStoragePathInfo = file.getPathInfo();
    if (fileStoragePathInfo != null) {
      return Option.of(Pair.of(readerContext.getFileRecordIterator(fileStoragePathInfo, 0, file.getFileLen(),
          readerState.getSchemaHandler().createSchemaFromFields(allFields), requiredSchema, storage), requiredSchema));
    } else {
      return Option.of(Pair.of(readerContext.getFileRecordIterator(file.getStoragePath(), 0, file.getFileLen(),
          readerState.getSchemaHandler().createSchemaFromFields(allFields), requiredSchema, storage), requiredSchema));
    }
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  public boolean hasNext() throws IOException {
    if (recordBuffer == null) {
      return baseFileIterator.hasNext();
    } else {
      return recordBuffer.hasNext();
    }
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  public T next() {
    T nextVal = recordBuffer == null ? baseFileIterator.next() : recordBuffer.next();
    if (outputConverter.isPresent()) {
      return outputConverter.get().apply(nextVal);
    }
    return nextVal;
  }

  private void scanLogFiles() {
    StoragePath basePath = readerState.getMetaClient().getBasePath();
    HoodieMergedLogRecordReader logRecordReader = HoodieMergedLogRecordReader.newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withBasePath(basePath.toString())
        .withLogFiles(logFiles)
        .withLatestInstantTime(readerState.getLatestCommitTime())
        .withReaderSchema(readerState.getRequestedSchema())
        .withInternalSchema(readerState.getInternalSchema())
        .withReverseReader(false)
        .withBufferSize(readerState.getBufferSize())
        .withPartition(getRelativePartitionPath(
           basePath, logFiles.get(0).getPath().getParent()))
        .withRecordMerger(readerState.getRecordMerger())
        .withRecordMergeMode(readerState.getRecordMergeMode())
        .withRecordBuffer(recordBuffer)
        .build();
    logRecordReader.close();
  }

  @Override
  public void close() throws IOException {
    if (baseFileIterator != null) {
      baseFileIterator.close();
    }
    if (recordBuffer != null) {
      recordBuffer.close();
    }
  }

  public HoodieFileGroupReaderIterator<T> getClosableIterator() {
    return new HoodieFileGroupReaderIterator<>(this);
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<T> {
    private HoodieFileGroupReader<T> reader;

    public HoodieFileGroupReaderIterator(HoodieFileGroupReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read record", e);
      }
    }

    @Override
    public T next() {
      return reader.next();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close the reader", e);
      } finally {
        this.reader = null;
      }
    }
  }
}
