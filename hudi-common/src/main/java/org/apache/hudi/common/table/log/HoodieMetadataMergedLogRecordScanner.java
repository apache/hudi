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

import org.apache.hudi.common.model.HoodieMetadataRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Merged log record scanner for metadata table using {@link HoodieMetadataRecordMerger}.
 */
public class HoodieMetadataMergedLogRecordScanner extends BaseHoodieMergedLogRecordScanner<String> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataMergedLogRecordScanner.class);

  private HoodieMetadataMergedLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                                               String latestInstantTime, Long maxMemorySizeInBytes,
                                               boolean reverseReader, int bufferSize, String spillableMapBasePath,
                                               Option<InstantRange> instantRange,
                                               ExternalSpillableMap.DiskMapType diskMapType,
                                               boolean isBitCaskDiskMapCompressionEnabled,
                                               boolean withOperationField, boolean forceFullScan,
                                               Option<String> partitionName,
                                               InternalSchema internalSchema,
                                               Option<String> keyFieldOverride,
                                               boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger,
                                               Option<HoodieTableMetaClient> hoodieTableMetaClientOption) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, maxMemorySizeInBytes, reverseReader, bufferSize, spillableMapBasePath, instantRange, diskMapType,
        isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger,
        hoodieTableMetaClientOption);
  }

  @Override
  public Map<String, HoodieRecord> getRecords() {
    return records;
  }

  @Override
  public <T> void processNextRecord(HoodieRecord<T> newRecord) throws IOException {
    // Merge the new record with the existing record in the map
    HoodieRecord<T> oldRecord = (HoodieRecord<T>) records.get(newRecord.getRecordKey());
    if (oldRecord != null) {
      LOG.debug("Merging new record with existing record in the map. Key: {}", newRecord.getRecordKey());
      recordMerger.fullOuterMerge(oldRecord, readerSchema, newRecord, readerSchema, this.getPayloadProps()).forEach(
          mergedRecord -> {
            HoodieRecord<T> combinedRecord = mergedRecord.getLeft();
            if (combinedRecord.getData() != oldRecord.getData()) {
              HoodieRecord latestHoodieRecord = getLatestHoodieRecord(newRecord, combinedRecord, newRecord.getRecordKey());
              records.put(newRecord.getRecordKey(), latestHoodieRecord.copy());
            }
          });
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      records.put(newRecord.getRecordKey(), newRecord.copy());
    }
  }

  /**
   * Returns the builder for {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static HoodieMetadataMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieMetadataMergedLogRecordScanner.Builder();
  }

  /**
   * Builder used to build {@code HoodieMetadataMergedLogRecordScanner}.
   */
  public static class Builder extends BaseHoodieMergedLogRecordScanner.Builder {
    private HoodieRecordMerger recordMerger = HoodieMetadataRecordMerger.INSTANCE;

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = recordMerger;
      return this;
    }

    @Override
    public HoodieMetadataMergedLogRecordScanner getScanner() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(basePath), new StoragePath(this.logFilePaths.get(0)).getParent());
      }
      checkArgument(recordMerger != null);
      checkArgument(nonEmpty(partitionName), "Partition name is required for HoodieMetadataMergedLogRecordScanner.");

      return new HoodieMetadataMergedLogRecordScanner(storage, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, reverseReader,
          bufferSize, spillableMapBasePath, instantRange,
          diskMapType, isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan,
          Option.of(partitionName), internalSchema, Option.ofNullable(keyFieldOverride), enableOptimizedLogBlocksScan, recordMerger,
          Option.ofNullable(hoodieTableMetaClient));
    }
  }
}
