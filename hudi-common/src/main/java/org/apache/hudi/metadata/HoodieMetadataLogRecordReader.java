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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.hudi.storage.StoragePath;

import javax.annotation.concurrent.ThreadSafe;

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
 * Metadata log-block records reading implementation, internally relying on
 * {@link HoodieMergedLogRecordScanner} to merge corresponding Metadata Table's delta log-blocks
 * sequence
 */
@ThreadSafe
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
    // NOTE: Locking is necessary since we're accessing [[HoodieMetadataLogRecordReader]]
    //       materialized state, to make sure there's no concurrent access
    synchronized (this) {
      logRecordScanner.scan();
      return logRecordScanner.getRecords().values()
          .stream()
          .map(record -> (HoodieRecord<HoodieMetadataPayload>) record)
          .collect(Collectors.toList());
    }
  }

  @SuppressWarnings("unchecked")
  public Map<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> sortedKeyPrefixes) {
    if (sortedKeyPrefixes.isEmpty()) {
      return Collections.emptyMap();
    }

    // NOTE: Locking is necessary since we're accessing [[HoodieMetadataLogRecordReader]]
    //       materialized state, to make sure there's no concurrent access
    synchronized (this) {
      logRecordScanner.scanByKeyPrefixes(sortedKeyPrefixes);
      Predicate<String> p = createPrefixMatchingPredicate(sortedKeyPrefixes);
      return logRecordScanner.getRecords().entrySet()
          .stream()
          .filter(r -> r != null && p.test(r.getKey()))
          .map(r -> (HoodieRecord<HoodieMetadataPayload>) r.getValue())
          .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> r));
    }
  }

  /**
   * Fetches records identified by the provided list of keys in case these are present in
   * the delta-log blocks
   */
  @SuppressWarnings("unchecked")
  public Map<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(List<String> sortedKeys) {
    if (sortedKeys.isEmpty()) {
      return Collections.emptyMap();
    }

    // NOTE: Locking is necessary since we're accessing [[HoodieMetadataLogRecordReader]]
    //       materialized state, to make sure there's no concurrent access
    synchronized (this) {
      logRecordScanner.scanByFullKeys(sortedKeys);
      Map<String, HoodieRecord> allRecords = logRecordScanner.getRecords();
      return sortedKeys.stream()
          .map(key -> (HoodieRecord<HoodieMetadataPayload>) allRecords.get(key))
          .filter(Objects::nonNull)
          .collect(Collectors.toMap(HoodieRecord::getRecordKey, r -> r));
    }
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
            // NOTE: Merging of Metadata Table's records is currently handled using {@code HoodiePreCombineAvroRecordMerger}
            //       for compatibility purposes; In the future it {@code HoodieMetadataPayload} semantic
            //       will be migrated to its own custom instance of {@code RecordMerger}
            .withReverseReader(false)
            .withOperationField(false);

    public Builder withStorage(HoodieStorage storage) {
      scannerBuilder.withStorage(storage);
      return this;
    }

    public Builder withBasePath(String basePath) {
      scannerBuilder.withBasePath(basePath);
      return this;
    }

    public Builder withBasePath(StoragePath basePath) {
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
      InstantRange instantRange = InstantRange.builder()
          .rangeType(InstantRange.RangeType.EXPLICIT_MATCH)
          .explicitInstants(validLogBlockTimestamps).build();
      scannerBuilder.withInstantRange(Option.of(instantRange));
      return this;
    }

    public Builder enableFullScan(boolean enableFullScan) {
      scannerBuilder.withForceFullScan(enableFullScan);
      return this;
    }

    public Builder withEnableOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      scannerBuilder.withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan);
      return this;
    }

    public Builder withTableMetaClient(HoodieTableMetaClient hoodieTableMetaClient) {
      scannerBuilder.withTableMetaClient(hoodieTableMetaClient);
      return this;
    }

    public HoodieMetadataLogRecordReader build() {
      return new HoodieMetadataLogRecordReader(scannerBuilder.build());
    }
  }
}
