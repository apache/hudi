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

import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Scans through all the blocks in a list of HoodieLogFile and builds up a compacted/merged list of records which will
 * be used as a lookup table when merging the base columnar file with the redo log file.
 * <p>
 * NOTE: If readBlockLazily is turned on, does not merge, instead keeps reading log blocks and merges everything at once
 * This is an optimization to avoid seek() back and forth to read new block (forward seek()) and lazily read content of
 * seen block (reverse and forward seek()) during merge | | Read Block 1 Metadata | | Read Block 1 Data | | | Read Block
 * 2 Metadata | | Read Block 2 Data | | I/O Pass 1 | ..................... | I/O Pass 2 | ................. | | | Read
 * Block N Metadata | | Read Block N Data |
 * <p>
 * This results in two I/O passes over the log file.
 */
@NotThreadSafe
public class HoodieMergedLogRecordScanner extends BaseHoodieMergedLogRecordScanner<String> {

  @SuppressWarnings("unchecked")
  protected HoodieMergedLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
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
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, maxMemorySizeInBytes, reverseReader, bufferSize,
        spillableMapBasePath, instantRange, diskMapType, isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan,
        partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger, hoodieTableMetaClientOption);
  }

  @Override
  public Map<String, HoodieRecord> getRecords() {
    return records;
  }

  @Override
  public <T> void processNextRecord(HoodieRecord<T> newRecord) throws IOException {
    String key = newRecord.getRecordKey();
    HoodieRecord<T> prevRecord = records.get(key);
    if (prevRecord != null) {
      // Merge and store the combined record
      HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(prevRecord, readerSchema,
          newRecord, readerSchema, this.getPayloadProps()).get().getLeft();
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != prevRecord.getData()) {
        HoodieRecord latestHoodieRecord = getLatestHoodieRecord(newRecord, combinedRecord, key);

        // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
        //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
        //       it since these records will be put into records(Map).
        records.put(key, latestHoodieRecord.copy());
      }
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      records.put(key, newRecord.copy());
    }
  }

  /**
   * Returns the builder for {@code HoodieMergedLogRecordScanner}.
   */
  public static HoodieMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieMergedLogRecordScanner.Builder();
  }

  /**
   * Builder used to build {@code HoodieMergedLogRecordScanner}.
   */
  public static class Builder extends BaseHoodieMergedLogRecordScanner.Builder {
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    @Override
    public HoodieMergedLogRecordScanner getScanner() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(basePath), new StoragePath(this.logFilePaths.get(0)).getParent());
      }
      checkArgument(recordMerger != null);

      return new HoodieMergedLogRecordScanner(storage, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, reverseReader,
          bufferSize, spillableMapBasePath, instantRange,
          diskMapType, isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan,
          Option.ofNullable(partitionName), internalSchema, Option.ofNullable(keyFieldOverride), enableOptimizedLogBlocksScan, recordMerger,
          Option.ofNullable(hoodieTableMetaClient));
    }
  }
}
