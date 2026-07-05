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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Record reader for RFC-103 LSM file groups backed by native parquet log files.
 *
 * <p>This reader is intentionally separate from {@code HoodieFileGroupReader}. Callers should use it
 * only when the file group follows pure LSM sorted-file semantics: the optional base file is treated
 * as the L1 sorted run and native parquet log files are treated as L0 sorted runs. Mixed legacy log
 * file groups should continue to use {@code HoodieFileGroupReader}.
 *
 * <p>The reader owns file-group level setup that mirrors {@code HoodieFileGroupReader}: schema
 * handling, merge properties, iterator mode, output projection, read stats, and update callbacks.
 * The actual sorted k-way merge is delegated to {@link LsmFileGroupRecordIterator}.
 */
public final class HoodieLsmFileGroupReader<T> implements HoodieRecordReader<T> {

  private final HoodieReaderContext<T> readerContext;
  private final HoodieTableMetaClient metaClient;
  private final InputSplit inputSplit;
  private final List<String> orderingFieldNames;
  private final HoodieStorage storage;
  private final TypedProperties props;
  private final ReaderParameters readerParameters;
  private final Option<UnaryOperator<T>> outputConverter;
  private final Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback;
  private ClosableIterator<BufferedRecord<T>> lsmRecordIterator;
  @Getter
  private final HoodieReadStats readStats;

  @Builder(setterPrefix = "with")
  private HoodieLsmFileGroupReader(
      HoodieReaderContext<T> readerContext,
      String latestCommitTime,
      HoodieSchema dataSchema,
      HoodieSchema requestedSchema,
      Option<InternalSchema> internalSchemaOpt,
      HoodieTableMetaClient hoodieTableMetaClient,
      TypedProperties props,
      Option<HoodieBaseFile> baseFileOption,
      Stream<HoodieLogFile> logFiles,
      Iterator<? extends HoodieRecord> recordIterator,
      String partitionPath,
      Long start,
      Long length,
      Boolean allowInflightInstants,
      Boolean emitDelete,
      Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {

    ValidationUtils.checkArgument(readerContext != null, "Reader context is required");
    ValidationUtils.checkArgument(hoodieTableMetaClient != null, "Hoodie table meta client is required");
    ValidationUtils.checkArgument(latestCommitTime != null, "Latest commit time is required");
    ValidationUtils.checkArgument(dataSchema != null, "Data schema is required");
    ValidationUtils.checkArgument(requestedSchema != null, "Requested schema is required");
    ValidationUtils.checkArgument(props != null, "Props is required");
    ValidationUtils.checkArgument(partitionPath != null, "Partition path is required");
    ValidationUtils.checkArgument(logFiles == null || hoodieTableMetaClient.getTableConfig().getLogFileFormat() == HoodieFileFormat.PARQUET,
        "LSM file group reader expects parquet log files");

    if (internalSchemaOpt == null) {
      internalSchemaOpt = Option.empty();
    }
    if (baseFileOption == null) {
      baseFileOption = Option.empty();
    }
    if (start == null) {
      start = 0L;
    }
    if (length == null) {
      length = Long.MAX_VALUE;
    }
    if (allowInflightInstants == null) {
      allowInflightInstants = false;
    }
    if (emitDelete == null) {
      emitDelete = false;
    }
    if (fileGroupUpdateCallback == null) {
      fileGroupUpdateCallback = Option.empty();
    }

    String tablePath = hoodieTableMetaClient.getBasePath().toString();
    HoodieStorage storage = hoodieTableMetaClient.getStorage().newInstance(new StoragePath(tablePath), readerContext.getStorageConfiguration());

    this.readerParameters = ReaderParameters.builder()
        .shouldUseRecordPosition(false)
        .emitDeletes(emitDelete)
        .sortOutputs(false)
        .inflightInstantsAllowed(allowInflightInstants)
        .build();
    this.inputSplit = InputSplit.builder()
        .baseFileOption(baseFileOption)
        .logFileStream(logFiles)
        .recordIterator((Iterator<HoodieRecord>) recordIterator)
        .partitionPath(partitionPath)
        .start(start)
        .length(length)
        .build();

    this.readerContext = readerContext;
    this.fileGroupUpdateCallback = fileGroupUpdateCallback;
    this.metaClient = hoodieTableMetaClient;
    this.storage = storage;

    readerContext.setHasLogFiles(this.inputSplit.hasLogFiles());
    readerContext.getRecordContext().setPartitionPath(inputSplit.getPartitionPath());
    if (readerContext.getHasLogFiles() && inputSplit.getStart() != 0) {
      throw new IllegalArgumentException("LSM file group reader is doing log file merge but not reading from the start of the base file");
    }
    HoodieTableConfig tableConfig = hoodieTableMetaClient.getTableConfig();
    this.props = ConfigUtils.getMergeProps(props, tableConfig);
    readerContext.initRecordMerger(props);
    readerContext.setTablePath(tablePath);
    readerContext.setLatestCommitTime(latestCommitTime);
    readerContext.setShouldMergeUseRecordPosition(false);
    readerContext.setHasBootstrapBaseFile(inputSplit.getBaseFileOption().flatMap(HoodieBaseFile::getBootstrapBaseFile).isPresent());
    readerContext.setSchemaHandler(readerContext.getRecordContext().supportsParquetRowIndex()
        ? new ParquetRowIndexBasedSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, props, metaClient)
        : new FileGroupReaderSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, props, metaClient));
    this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
    this.orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode(), hoodieTableMetaClient);
    this.readStats = new HoodieReadStats();
  }

  /**
   * Creates a buffered iterator in the requested output mode.
   *
   * <p>{@code includeBaseFile} controls whether the L1/base sorted run participates in the merge.
   * Log-only consumers, such as compaction-style readers, pass {@code false} so only native parquet
   * log files are scanned.
   */
  private ClosableIterator<BufferedRecord<T>> getBufferedRecordIterator(IteratorMode iteratorMode,
                                                                        boolean includeBaseFile) throws IOException {
    this.readerContext.setIteratorMode(iteratorMode);
    if (lsmRecordIterator != null) {
      lsmRecordIterator.close();
    }
    this.lsmRecordIterator = new LsmFileGroupRecordIterator<>(
        readerContext, storage, inputSplit, orderingFieldNames, metaClient, props, readerParameters, readStats, fileGroupUpdateCallback, includeBaseFile);
    return new HoodieLsmFileGroupReaderIterator<>(this);
  }

  @Override
  public ClosableIterator<BufferedRecord<T>> getClosableBufferedRecordIterator() throws IOException {
    return getBufferedRecordIterator(IteratorMode.HOODIE_RECORD, true);
  }

  @Override
  public ClosableIterator<T> getClosableIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.ENGINE_RECORD, true), BufferedRecord::getRecord);
  }

  @Override
  public ClosableIterator<HoodieRecord<T>> getClosableHoodieRecordIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.HOODIE_RECORD, true),
        bufferedRecord -> readerContext.getRecordContext().constructFinalHoodieRecord(bufferedRecord));
  }

  @Override
  public ClosableIterator<String> getClosableKeyIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.RECORD_KEY, true), BufferedRecord::getRecordKey);
  }

  @Override
  public ClosableIterator<BufferedRecord<T>> getLogRecordsOnly() throws IOException {
    return getBufferedRecordIterator(IteratorMode.HOODIE_RECORD, false);
  }

  boolean hasNext() {
    return lsmRecordIterator.hasNext();
  }

  BufferedRecord<T> next() {
    BufferedRecord<T> nextVal = lsmRecordIterator.next();
    if (outputConverter.isPresent()) {
      return nextVal.project(outputConverter.get());
    }
    return nextVal;
  }

  @Override
  public void onWriteFailure(String recordKey) {
    this.fileGroupUpdateCallback.ifPresent(callback -> callback.onFailure(recordKey));
  }

  @Override
  public void close() throws IOException {
    if (lsmRecordIterator != null) {
      lsmRecordIterator.close();
    }
  }

  private static class HoodieLsmFileGroupReaderIterator<T> implements ClosableIterator<BufferedRecord<T>> {
    private HoodieLsmFileGroupReader<T> reader;

    private HoodieLsmFileGroupReaderIterator(HoodieLsmFileGroupReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      return reader.hasNext();
    }

    @Override
    public BufferedRecord<T> next() {
      return reader.next();
    }

    @Override
    public void close() {
      if (reader != null) {
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
}
