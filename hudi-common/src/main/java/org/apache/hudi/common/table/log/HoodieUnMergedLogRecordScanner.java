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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.internal.schema.InternalSchema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A scanner used to scan hoodie unmerged log records.
 */
public class HoodieUnMergedLogRecordScanner extends AbstractHoodieLogRecordReader {

  private final LogRecordScannerCallback callback;

  private HoodieUnMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
                                         String latestInstantTime, boolean readBlocksLazily, boolean reverseReader, int bufferSize,
                                         LogRecordScannerCallback callback, Option<InstantRange> instantRange, boolean useScanV2) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize, instantRange,
        false, true, Option.empty(), InternalSchema.getEmptyInternalSchema(), useScanV2);
    this.callback = callback;
  }

  /**
   * Returns the builder for {@code HoodieUnMergedLogRecordScanner}.
   */
  public static HoodieUnMergedLogRecordScanner.Builder newBuilder() {
    return new Builder();
  }

  @Override
  protected void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) throws Exception {
    // Just call callback without merging
    callback.apply(hoodieRecord);
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    throw new IllegalStateException("Not expected to see delete records in this log-scan mode. Check Job Config");
  }

  /**
   * A callback for log record scanner.
   */
  @FunctionalInterface
  public interface LogRecordScannerCallback {

    void apply(HoodieRecord<? extends HoodieRecordPayload> record) throws Exception;
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder extends AbstractHoodieLogRecordReader.Builder {
    private FileSystem fs;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private String latestInstantTime;
    private boolean readBlocksLazily;
    private boolean reverseReader;
    private int bufferSize;
    private Option<InstantRange> instantRange = Option.empty();
    // specific configurations
    private LogRecordScannerCallback callback;
    private boolean useScanV2;

    public Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
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

    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    public Builder withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
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

    @Override
    public Builder withUseScanV2(boolean useScanV2) {
      this.useScanV2 = useScanV2;
      return this;
    }

    @Override
    public HoodieUnMergedLogRecordScanner build() {
      return new HoodieUnMergedLogRecordScanner(fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, readBlocksLazily, reverseReader, bufferSize, callback, instantRange, useScanV2);
    }
  }
}
