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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A scanner used to scan hoodie unmerged log records.
 */
public class HoodieUnMergedLogRecordScanner extends AbstractHoodieLogRecordScanner {

  private final LogRecordScannerCallback callback;
  private final RecordDeletionCallback recordDeletionCallback;

  private HoodieUnMergedLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                                         String latestInstantTime, boolean reverseReader, int bufferSize,
                                         LogRecordScannerCallback callback, RecordDeletionCallback recordDeletionCallback,
                                         Option<InstantRange> instantRange, InternalSchema internalSchema,
                                         boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger,
                                         Option<HoodieTableMetaClient> hoodieTableMetaClientOption) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange,
        false, true, Option.empty(), internalSchema, Option.empty(), enableOptimizedLogBlocksScan, recordMerger,
         hoodieTableMetaClientOption);
    this.callback = callback;
    this.recordDeletionCallback = recordDeletionCallback;
  }

  /**
   * Scans delta-log files processing blocks
   */
  public final void scan() {
    scan(false);
  }

  public final void scan(boolean skipProcessingBlocks) {
    scanInternal(Option.empty(), skipProcessingBlocks);
  }

  /**
   * Returns the builder for {@code HoodieUnMergedLogRecordScanner}.
   */
  public static HoodieUnMergedLogRecordScanner.Builder newBuilder() {
    return new Builder();
  }

  @Override
  protected <T> void processNextRecord(HoodieRecord<T> hoodieRecord) throws Exception {
    // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
    //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
    //       it since these records will be put into queue of BoundedInMemoryExecutor.
    // Just call callback without merging
    if (callback != null) {
      callback.apply(hoodieRecord.copy());
    }
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    if (recordDeletionCallback != null) {
      recordDeletionCallback.apply(deleteRecord.getHoodieKey());
    }
  }

  /**
   * A callback for log record scanner.
   */
  @FunctionalInterface
  public interface LogRecordScannerCallback {

    void apply(HoodieRecord<?> record) throws Exception;
  }

  /**
   * A callback for log record scanner to consume deleted HoodieKeys.
   */
  @FunctionalInterface
  public interface RecordDeletionCallback {
    void apply(HoodieKey deletedKey);
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder extends AbstractHoodieLogRecordScanner.Builder {
    private HoodieStorage storage;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private InternalSchema internalSchema;
    private String latestInstantTime;
    private boolean reverseReader;
    private int bufferSize;
    private Option<InstantRange> instantRange = Option.empty();
    // specific configurations
    private LogRecordScannerCallback callback;
    private RecordDeletionCallback recordDeletionCallback;
    private boolean enableOptimizedLogBlocksScan;
    private HoodieRecordMerger recordMerger = new HoodiePreCombineAvroRecordMerger();
    private HoodieTableMetaClient hoodieTableMetaClient;

    public Builder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public Builder withBasePath(StoragePath basePath) {
      this.basePath = basePath.toString();
      return this;
    }

    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    public Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public Builder withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    public Builder withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    public Builder withLogRecordScannerCallback(LogRecordScannerCallback callback) {
      this.callback = callback;
      return this;
    }

    public Builder withRecordDeletionCallback(RecordDeletionCallback recordDeletionCallback) {
      this.recordDeletionCallback = recordDeletionCallback;
      return this;
    }

    @Override
    public Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    @Override
    public HoodieUnMergedLogRecordScanner.Builder withTableMetaClient(
        HoodieTableMetaClient hoodieTableMetaClient) {
      this.hoodieTableMetaClient = hoodieTableMetaClient;
      return this;
    }

    @Override
    public HoodieUnMergedLogRecordScanner build() {
      ValidationUtils.checkArgument(recordMerger != null);

      return new HoodieUnMergedLogRecordScanner(storage, basePath, logFilePaths, readerSchema,
          latestInstantTime, reverseReader, bufferSize, callback, recordDeletionCallback, instantRange,
          internalSchema, enableOptimizedLogBlocksScan, recordMerger, Option.ofNullable(hoodieTableMetaClient));
    }
  }
}
