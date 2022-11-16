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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@code HoodieMergedLogRecordScanner} implementation which only merged records matching providing keys. This is
 * useful in limiting memory usage when only a small subset of updates records are to be read.
 */
public class HoodieMetadataLogRecordReader implements Closeable {

  private final HoodieMergedLogRecordScanner logRecordScanner;

  private HoodieMetadataLogRecordReader(HoodieMergedLogRecordScanner logRecordScanner) {
    this.logRecordScanner = logRecordScanner;
  }

  /**
   * Returns the builder for {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static HoodieMetadataLogRecordReader.Builder newBuilder() {
    return new HoodieMetadataLogRecordReader.Builder();
  }

  @SuppressWarnings("unchecked")
  public List<HoodieRecord<HoodieMetadataPayload>> getRecords() {
    // TODO remove useless locking (need to address getRecords thread-safety)
    synchronized (this) {
      logRecordScanner.scan();
      return logRecordScanner.getRecords().values()
          .stream()
          .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
          .collect(Collectors.toList());
    }
  }

  @SuppressWarnings("unchecked")
  public List<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> keyPrefixes) {
    if (keyPrefixes.isEmpty()) {
      return Collections.emptyList();
    }

    // TODO add caching for queried prefixes
    // TODO remove useless locking (need to address getRecords thread-safety)
    synchronized (this) {
      logRecordScanner.scanByKeyPrefixes(keyPrefixes);
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> allRecords = logRecordScanner.getRecords();

      Predicate<String> p = createPrefixMatchingPredicate(keyPrefixes);
      return allRecords.entrySet()
          .stream()
          .filter(r -> r != null && p.test(r.getKey()))
          .map(r -> (HoodieRecord<HoodieMetadataPayload>) r.getValue())
          .collect(Collectors.toList());
    }
  }

  @SuppressWarnings("unchecked")
  public List<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(List<String> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }

    // TODO remove useless locking (need to address getRecords thread-safety)
    synchronized (this) {
      logRecordScanner.scanByFullKeys(keys);
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> allRecords = logRecordScanner.getRecords();
      return keys.stream()
          .map(key -> (HoodieRecord<HoodieMetadataPayload>) allRecords.get(key))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }

  // TODO remove this method
  public HoodieMergedLogRecordScanner getLogRecordScanner() {
    return logRecordScanner;
  }

  @Override
  public void close() throws IOException {
    logRecordScanner.close();
  }

  private static Predicate<String> createPrefixMatchingPredicate(List<String> keyPrefixes) {
    if (keyPrefixes.size() == 1) {
      String keyPrefix = keyPrefixes.get(0);
      return key -> key.startsWith(keyPrefix);
    }

    return key -> keyPrefixes.stream().anyMatch(key::startsWith);
  }

  /**
   * Builder used to build {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static class Builder {
    private final HoodieMergedLogRecordScanner.Builder scannerBuilder =
        new HoodieMergedLogRecordScanner.Builder()
            .withKeyFiledOverride(HoodieMetadataPayload.KEY_FIELD_NAME)
            .withReadBlocksLazily(true)
            .withReverseReader(false)
            .withOperationField(false);

    public Builder withFileSystem(FileSystem fs) {
      scannerBuilder.withFileSystem(fs);
      return this;
    }

    public Builder withBasePath(String basePath) {
      scannerBuilder.withBasePath(basePath);
      return this;
    }

    public Builder withLogFilePaths(List<String> logFilePaths) {
      scannerBuilder.withLogFilePaths(logFilePaths);
      return this;
    }

    public Builder withReaderSchema(Schema schema) {
      scannerBuilder.withReaderSchema(schema);
      return this;
    }

    public Builder withLatestInstantTime(String latestInstantTime) {
      scannerBuilder.withLatestInstantTime(latestInstantTime);
      return this;
    }

    public Builder withBufferSize(int bufferSize) {
      scannerBuilder.withBufferSize(bufferSize);
      return this;
    }

    public Builder withPartition(String partitionName) {
      scannerBuilder.withPartition(partitionName);
      return this;
    }

    public Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      scannerBuilder.withMaxMemorySizeInBytes(maxMemorySizeInBytes);
      return this;
    }

    public Builder withSpillableMapBasePath(String spillableMapBasePath) {
      scannerBuilder.withSpillableMapBasePath(spillableMapBasePath);
      return this;
    }

    public Builder withDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      scannerBuilder.withDiskMapType(diskMapType);
      return this;
    }

    public Builder withBitCaskDiskMapCompressionEnabled(boolean isBitCaskDiskMapCompressionEnabled) {
      scannerBuilder.withBitCaskDiskMapCompressionEnabled(isBitCaskDiskMapCompressionEnabled);
      return this;
    }

    public Builder withLogBlockTimestamps(Set<String> validLogBlockTimestamps) {
      scannerBuilder.withInstantRange(Option.of(new ExplicitMatchRange(validLogBlockTimestamps)));
      return this;
    }

    public Builder enableFullScan(boolean enableFullScan) {
      scannerBuilder.withForceFullScan(enableFullScan);
      return this;
    }

    public Builder withUseScanV2(boolean useScanV2) {
      scannerBuilder.withUseScanV2(useScanV2);
      return this;
    }

    public HoodieMetadataLogRecordReader build() {
      return new HoodieMetadataLogRecordReader(scannerBuilder.build());
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
