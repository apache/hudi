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

package org.apache.hudi.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;

/**
 * A {@code HoodieMergedLogRecordScanner} implementation which only merged records matching providing keys. This is
 * useful in limiting memory usage when only a small subset of updates records are to be read.
 */
public class HoodieMetadataMergedLogRecordScanner extends HoodieMergedLogRecordScanner {
  // Set of all record keys that are to be read in memory
  private Set<String> mergeKeyFilter;

  private HoodieMetadataMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths,
                                              Schema readerSchema, String latestInstantTime, Long maxMemorySizeInBytes, int bufferSize,
                                              String spillableMapBasePath, Set<String> mergeKeyFilter) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, maxMemorySizeInBytes, false, false, bufferSize,
        spillableMapBasePath, Option.empty(), false);
    this.mergeKeyFilter = mergeKeyFilter;

    performScan();
  }

  @Override
  protected void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) throws IOException {
    if (mergeKeyFilter.isEmpty() || mergeKeyFilter.contains(hoodieRecord.getRecordKey())) {
      super.processNextRecord(hoodieRecord);
    }
  }

  @Override
  protected void processNextDeletedKey(HoodieKey hoodieKey) {
    if (mergeKeyFilter.isEmpty() || mergeKeyFilter.contains(hoodieKey.getRecordKey())) {
      super.processNextDeletedKey(hoodieKey);
    }
  }

  /**
   * Returns the builder for {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static HoodieMetadataMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieMetadataMergedLogRecordScanner.Builder();
  }

  /**
   * Retrieve a record given its key.
   *
   * @param key Key of the record to retrieve
   * @return {@code HoodieRecord} if key was found else {@code Option.empty()}
   */
  public Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key) {
    return Option.ofNullable((HoodieRecord) records.get(key));
  }

  /**
   * Builder used to build {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static class Builder extends HoodieMergedLogRecordScanner.Builder {
    private Set<String> mergeKeyFilter = Collections.emptySet();

    public Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths;
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
      throw new UnsupportedOperationException();
    }

    public Builder withReverseReader(boolean reverseReader) {
      throw new UnsupportedOperationException();
    }

    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      return this;
    }

    public Builder withSpillableMapBasePath(String spillableMapBasePath) {
      this.spillableMapBasePath = spillableMapBasePath;
      return this;
    }

    public Builder withMergeKeyFilter(Set<String> mergeKeyFilter) {
      this.mergeKeyFilter = mergeKeyFilter;
      return this;
    }

    @Override
    public HoodieMetadataMergedLogRecordScanner build() {
      return new HoodieMetadataMergedLogRecordScanner(fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, bufferSize, spillableMapBasePath, mergeKeyFilter);
    }
  }
}
