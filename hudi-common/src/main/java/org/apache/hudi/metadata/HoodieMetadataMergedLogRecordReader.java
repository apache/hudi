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

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * A {@code HoodieMergedLogRecordScanner} implementation which only merged records matching providing keys. This is
 * useful in limiting memory usage when only a small subset of updates records are to be read.
 */
public class HoodieMetadataMergedLogRecordReader extends HoodieMergedLogRecordScanner {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataMergedLogRecordReader.class);

  private HoodieMetadataMergedLogRecordReader(FileSystem fs, String basePath, String partitionName,
                                              List<String> logFilePaths,
                                              Schema readerSchema, String latestInstantTime,
                                              Long maxMemorySizeInBytes, int bufferSize,
                                              String spillableMapBasePath,
                                              ExternalSpillableMap.DiskMapType diskMapType,
                                              boolean isBitCaskDiskMapCompressionEnabled,
                                              Option<InstantRange> instantRange, boolean allowFullScan) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, maxMemorySizeInBytes, true, false, bufferSize,
        spillableMapBasePath, instantRange, diskMapType, isBitCaskDiskMapCompressionEnabled, false, allowFullScan, Option.of(partitionName),
        InternalSchema.getEmptyInternalSchema(), HoodieRecordType.AVRO);
  }

  /**
   * Returns the builder for {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static HoodieMetadataMergedLogRecordReader.Builder newBuilder() {
    return new HoodieMetadataMergedLogRecordReader.Builder();
  }

  /**
   * Retrieve a record given its key.
   *
   * @param key Key of the record to retrieve
   * @return {@code HoodieRecord} if key was found else {@code Option.empty()}
   */
  public synchronized List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordByKey(String key) {
    checkState(forceFullScan, "Record reader has to be in full-scan mode to use this API");
    return Collections.singletonList(Pair.of(key, Option.ofNullable((HoodieRecord) records.get(key))));
  }

  @SuppressWarnings("unchecked")
  public List<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> keyPrefixes) {
    // Following operations have to be atomic, otherwise concurrent
    // readers would race with each other and could crash when
    // processing log block records as part of scan.
    synchronized (this) {
      records.clear();
      scanInternal(Option.of(new KeySpec(keyPrefixes, false)));
      return records.values().stream()
          .filter(Objects::nonNull)
          .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
          .collect(Collectors.toList());
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordsByKeys(List<String> keys) {
    // Following operations have to be atomic, otherwise concurrent
    // readers would race with each other and could crash when
    // processing log block records as part of scan.
    synchronized (this) {
      records.clear();
      scan(keys);
      return keys.stream()
          .map(key -> Pair.of(key, Option.ofNullable((HoodieRecord<HoodieMetadataPayload>) records.get(key))))
          .collect(Collectors.toList());
    }
  }

  @Override
  protected String getKeyField() {
    return HoodieMetadataPayload.KEY_FIELD_NAME;
  }

  /**
   * Builder used to build {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static class Builder extends HoodieMergedLogRecordScanner.Builder {
    private boolean allowFullScan = HoodieMetadataConfig.ENABLE_FULL_SCAN_LOG_FILES.defaultValue();

    @Override
    public Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    @Override
    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths;
      return this;
    }

    @Override
    public Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    @Override
    public Builder withReadBlocksLazily(boolean readBlocksLazily) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withReverseReader(boolean reverseReader) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public Builder withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      return this;
    }

    @Override
    public Builder withSpillableMapBasePath(String spillableMapBasePath) {
      this.spillableMapBasePath = spillableMapBasePath;
      return this;
    }

    @Override
    public Builder withDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      this.diskMapType = diskMapType;
      return this;
    }

    @Override
    public Builder withBitCaskDiskMapCompressionEnabled(boolean isBitCaskDiskMapCompressionEnabled) {
      this.isBitCaskDiskMapCompressionEnabled = isBitCaskDiskMapCompressionEnabled;
      return this;
    }

    public Builder withLogBlockTimestamps(Set<String> validLogBlockTimestamps) {
      withInstantRange(Option.of(new ExplicitMatchRange(validLogBlockTimestamps)));
      return this;
    }

    public Builder allowFullScan(boolean enableFullScan) {
      this.allowFullScan = enableFullScan;
      return this;
    }

    @Override
    public HoodieMetadataMergedLogRecordReader build() {
      return new HoodieMetadataMergedLogRecordReader(fs, basePath, partitionName, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, bufferSize, spillableMapBasePath,
          diskMapType, isBitCaskDiskMapCompressionEnabled, instantRange, allowFullScan);
    }
  }

  /**
   * Class to assist in checking if an instant is part of a set of instants.
   */
  private static class ExplicitMatchRange extends InstantRange {
    Set<String> instants;

    public ExplicitMatchRange(Set<String> instants) {
      super(Collections.min(instants), Collections.max(instants));
      this.instants = instants;
    }

    @Override
    public boolean isInRange(String instant) {
      return this.instants.contains(instant);
    }
  }
}
