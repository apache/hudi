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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;

public class ScannerStorage implements StorageForScanners {
  HoodieUnMergedLogRecordScanner scanner;
  protected final ExternalSpillableMap<String, HoodieRecord> records;
  protected Integer count = 0;

  public ScannerStorage(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                        String latestInstantTime, Long maxMemorySizeInBytes,
                        boolean reverseReader, int bufferSize, String spillableMapBasePath,
                        Option<InstantRange> instantRange,
                        ExternalSpillableMap.DiskMapType diskMapType,
                        boolean isBitCaskDiskMapCompressionEnabled,
                        InternalSchema internalSchema,
                        boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger,
                        Option<String> partitionNameOpt) throws IOException {
    this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
        new HoodieRecordSizeEstimator(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);
    this.scanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(latestInstantTime)
        .withReverseReader(reverseReader)
        .withBufferSize(bufferSize)
        .withInstantRange(instantRange)
        .withInternalSchema(internalSchema)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
        .withRecordMerger(recordMerger)
        .withPartition(partitionNameOpt.orElse(null))
        .withLogRecordScannerCallback(record -> records.put(String.valueOf(count++), record))
        .build();
    this.scanner.scan();
  }

  @Override
  public Map<String, HoodieRecord> getRecords() {
    return records;
  }

  @Override
  public void close() throws IOException {
    if (records != null) {
      records.close();
    }
  }

  public static ScannerStorage.Builder newBuilder() {
    return new ScannerStorage.Builder();
  }


  /**
   * Builder used to build {@code ScannerStorage}.
   */
  public static class Builder extends AbstractHoodieLogRecordReader.Builder {
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
    private HoodieUnMergedLogRecordScanner.LogRecordScannerCallback callback;
    private boolean enableOptimizedLogBlocksScan;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;
    private HoodieTableMetaClient hoodieTableMetaClient;
    private Long maxMemorySizeInBytes;
    private String spillableMapBasePath;
    private ExternalSpillableMap.DiskMapType diskMapType = SPILLABLE_DISK_MAP_TYPE.defaultValue();
    private boolean isBitCaskDiskMapCompressionEnabled = DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue();
    private boolean shouldMerge = true;
    private String partitionName;

    public ScannerStorage.Builder withShouldMerge(boolean shouldMerge) {
      this.shouldMerge = shouldMerge;
      return this;
    }

    public ScannerStorage.Builder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public ScannerStorage.Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public ScannerStorage.Builder withBasePath(StoragePath basePath) {
      this.basePath = basePath.toString();
      return this;
    }

    public ScannerStorage.Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    public ScannerStorage.Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public ScannerStorage.Builder withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    public ScannerStorage.Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    public ScannerStorage.Builder withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    public ScannerStorage.Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public ScannerStorage.Builder withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    public ScannerStorage.Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      return this;
    }

    public ScannerStorage.Builder withSpillableMapBasePath(String spillableMapBasePath) {
      this.spillableMapBasePath = spillableMapBasePath;
      return this;
    }

    public ScannerStorage.Builder withDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      this.diskMapType = diskMapType;
      return this;
    }

    public ScannerStorage.Builder withBitCaskDiskMapCompressionEnabled(boolean isBitCaskDiskMapCompressionEnabled) {
      this.isBitCaskDiskMapCompressionEnabled = isBitCaskDiskMapCompressionEnabled;
      return this;
    }

    @Override
    public ScannerStorage.Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    @Override
    public ScannerStorage.Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    @Override
    public ScannerStorage.Builder withTableMetaClient(
        HoodieTableMetaClient hoodieTableMetaClient) {
      this.hoodieTableMetaClient = hoodieTableMetaClient;
      return this;
    }

    @Override
    public ScannerStorage.Builder withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public AbstractHoodieLogRecordReader build() {
      throw new UnsupportedOperationException();
    }

    public StorageForScanners buildStorageForScanners() {
      try {
        if (shouldMerge) {
          HoodieMergedLogRecordScanner.Builder builder = HoodieMergedLogRecordScanner.newBuilder()
              .withStorage(storage)
              .withBasePath(basePath)
              .withLogFilePaths(logFilePaths)
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(latestInstantTime)
              .withReverseReader(reverseReader)
              .withInternalSchema(internalSchema)
              .withBufferSize(bufferSize)
              .withMaxMemorySizeInBytes(maxMemorySizeInBytes)
              .withSpillableMapBasePath(spillableMapBasePath)
              .withDiskMapType(diskMapType)
              .withBitCaskDiskMapCompressionEnabled(isBitCaskDiskMapCompressionEnabled)
              .withInstantRange(instantRange)
              .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
              .withRecordMerger(recordMerger);
          if (!logFilePaths.isEmpty()) {
            builder.withPartition(partitionName);
          }
          return builder.build();
        } else {
          return new ScannerStorage(storage, basePath, logFilePaths,
              readerSchema, latestInstantTime, maxMemorySizeInBytes, reverseReader,
              bufferSize, spillableMapBasePath, instantRange, diskMapType, isBitCaskDiskMapCompressionEnabled,
              internalSchema, enableOptimizedLogBlocksScan,  recordMerger, Option.ofNullable(partitionName));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
