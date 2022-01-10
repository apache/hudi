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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;

/**
 * A {@code HoodieMergedLogRecordScanner} implementation which only merged records matching providing keys. This is
 * useful in limiting memory usage when only a small subset of updates records are to be read.
 */
public class HoodieMetadataMergedLogRecordReader extends HoodieMergedLogRecordScanner {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataMergedLogRecordReader.class);

  // Set of all record keys that are to be read in memory
  private Set<String> mergeKeyFilter;

  private HoodieMetadataMergedLogRecordReader(FileSystem fs, String basePath, String partitionName,
                                              List<String> logFilePaths,
                                              Schema readerSchema, String latestInstantTime,
                                              Long maxMemorySizeInBytes, int bufferSize,
                                              String spillableMapBasePath, Set<String> mergeKeyFilter,
                                              ExternalSpillableMap.DiskMapType diskMapType,
                                              boolean isBitCaskDiskMapCompressionEnabled,
                                              Option<InstantRange> instantRange, boolean enableFullScan) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, maxMemorySizeInBytes, false, false, bufferSize,
        spillableMapBasePath, instantRange, false, diskMapType, isBitCaskDiskMapCompressionEnabled, false,
        enableFullScan, Option.of(partitionName));
    this.mergeKeyFilter = mergeKeyFilter;
    if (enableFullScan) {
      performScan();
    }
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

  @Override
  protected HoodieRecord<?> createHoodieRecord(final IndexedRecord rec, final HoodieTableConfig hoodieTableConfig,
                                               final String payloadClassFQN, final String preCombineField,
                                               final boolean withOperationField,
                                               final Option<Pair<String, String>> simpleKeyGenFields,
                                               final Option<String> partitionName) {
    if (hoodieTableConfig.populateMetaFields()) {
      return super.createHoodieRecord(rec, hoodieTableConfig, payloadClassFQN, preCombineField, withOperationField,
          simpleKeyGenFields, partitionName);
    }

    // When meta fields are not available, create the record using the
    // preset key field and the known partition name
    return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) rec, payloadClassFQN,
        preCombineField, simpleKeyGenFields.get(), withOperationField, partitionName);
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
  public List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordByKey(String key) {
    return Collections.singletonList(Pair.of(key, Option.ofNullable((HoodieRecord) records.get(key))));
  }

  public synchronized List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordsByKeys(List<String> keys) {
    // Following operations have to be atomic, otherwise concurrent
    // readers would race with each other and could crash when
    // processing log block records as part of scan.
    records.clear();
    scan(Option.of(keys));
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> metadataRecords = new ArrayList<>();
    keys.forEach(entry -> {
      if (records.containsKey(entry)) {
        metadataRecords.add(Pair.of(entry, Option.ofNullable((HoodieRecord) records.get(entry))));
      } else {
        metadataRecords.add(Pair.of(entry, Option.empty()));
      }
    });
    return metadataRecords;
  }

  @Override
  protected String getKeyField() {
    return HoodieMetadataPayload.SCHEMA_FIELD_ID_KEY;
  }

  /**
   * Builder used to build {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static class Builder extends HoodieMergedLogRecordScanner.Builder {
    private Set<String> mergeKeyFilter = Collections.emptySet();
    private boolean enableFullScan = HoodieMetadataConfig.ENABLE_FULL_SCAN_LOG_FILES.defaultValue();
    private boolean enableInlineReading;

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

    public Builder withMergeKeyFilter(Set<String> mergeKeyFilter) {
      this.mergeKeyFilter = mergeKeyFilter;
      return this;
    }

    public Builder withLogBlockTimestamps(Set<String> validLogBlockTimestamps) {
      withInstantRange(Option.of(new ExplicitMatchRange(validLogBlockTimestamps)));
      return this;
    }

    public Builder enableFullScan(boolean enableFullScan) {
      this.enableFullScan = enableFullScan;
      return this;
    }

    @Override
    public HoodieMetadataMergedLogRecordReader build() {
      return new HoodieMetadataMergedLogRecordReader(fs, basePath, partitionName, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, bufferSize, spillableMapBasePath, mergeKeyFilter,
          diskMapType, isBitCaskDiskMapCompressionEnabled, instantRange, enableFullScan);
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
