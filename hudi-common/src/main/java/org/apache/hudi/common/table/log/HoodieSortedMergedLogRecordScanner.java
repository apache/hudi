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
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.compaction.SortMergeCompactionHelper;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SampleEstimator;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.CombineFunc;
import org.apache.hudi.common.util.collection.SortedAppendOnlyExternalSpillableMap;
import org.apache.hudi.common.util.collection.SortEngine;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_LOGFILE_SUFFIX;

public class HoodieSortedMergedLogRecordScanner extends HoodieLogRecordScanner {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSortedMergedLogRecordScanner.class);

  private final SortedAppendOnlyExternalSpillableMap<String/*record key*/, WrapNaturalOrderHoodieRecord> map;
  private final CombineFunc<String, WrapNaturalOrderHoodieRecord, WrapNaturalOrderHoodieRecord> combineFunc;

  private final Comparator<HoodieRecord> hoodieRecordComparator;
  private final Comparator<WrapNaturalOrderHoodieRecord> wrapNaturalOrderHoodieRecordComparator;
  private long maxMemoryUsageForSorting;
  private long totalLogRecords;

  protected HoodieSortedMergedLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                                               String latestInstantTime, boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                               boolean withOperationField, boolean forceFullScan, Option<String> partitionNameOverride,
                                               InternalSchema internalSchema, Option<String> keyFieldOverride,
                                               boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger,
                                               Option<HoodieTableMetaClient> hoodieTableMetaClientOption,
                                               Option<Comparator<HoodieRecord>> comparator, long maxMemoryUsageForSorting, String externalSorterBasePath, SortEngine sortEngine) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange, withOperationField, forceFullScan, partitionNameOverride, internalSchema,
        keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger, hoodieTableMetaClientOption);
    this.hoodieRecordComparator = comparator.orElse(SortMergeCompactionHelper.DEFAULT_RECORD_COMPACTOR);
    this.wrapNaturalOrderHoodieRecordComparator = (o1, o2) -> {
      int compare = hoodieRecordComparator.compare(o1.getRecord(), o2.getRecord());
      if (compare != 0) {
        return compare;
      }
      return Long.compare(o1.getNaturalOrder(), o2.getNaturalOrder());
    };
    this.maxMemoryUsageForSorting = maxMemoryUsageForSorting;
    try {
      SizeEstimator valueSizeEstimator = new SampleEstimator<>(new HoodieRecordSizeEstimator<>(readerSchema));
      SizeEstimator keySizeEstimator = new SampleEstimator<>(new HoodieRecordSizeEstimator<>(readerSchema));
      this.combineFunc = new PreCombineFunc();
      Function<WrapNaturalOrderHoodieRecord, String> keyGetterFromValue = (wrapRecord) -> wrapRecord.getRecord().getRecordKey();
      this.map = new SortedAppendOnlyExternalSpillableMap<>(externalSorterBasePath, maxMemoryUsageForSorting,
          wrapNaturalOrderHoodieRecordComparator, keySizeEstimator, valueSizeEstimator, sortEngine, keyGetterFromValue, Option.of(this.combineFunc));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize ", e);
    }
    // TODO: always enable full scan for now
    // scan all records but don't merge them
    performScan();
  }

  class PreCombineFunc implements CombineFunc<String, WrapNaturalOrderHoodieRecord, WrapNaturalOrderHoodieRecord> {

    @Override
    public WrapNaturalOrderHoodieRecord combine(String key, WrapNaturalOrderHoodieRecord value, WrapNaturalOrderHoodieRecord oldValue) {
      if (value.getRecord() instanceof MarkDeleteRecord) {
        return combineDeleteRecord(key, value, oldValue);
      }
      try {
        return combineDataRecord(key, value, oldValue);
      } catch (IOException e) {
        LOG.error("Failed to merge income record: {} with existing record: {}", value.getRecord(), oldValue.getRecord(), e);
        throw new HoodieIOException("Failed to merge records", e);
      }
    }

    private WrapNaturalOrderHoodieRecord combineDeleteRecord(String key, WrapNaturalOrderHoodieRecord value, WrapNaturalOrderHoodieRecord oldValue) {
      HoodieRecord oldRecord = oldValue.getRecord();
      MarkDeleteRecord deleteRecord = (MarkDeleteRecord) value.getRecord();
      // Merge and store the merged record. The ordering val is taken to decide whether the same key record
      // should be deleted or be kept. The old record is kept only if the DELETE record has smaller ordering val.
      // For same ordering values, uses the natural order(arrival time semantics).

      Comparable curOrderingVal = oldRecord.getOrderingValue(readerSchema, hoodieTableMetaClient.getTableConfig().getProps());
      Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
      // Checks the ordering value does not equal to 0
      // because we use 0 as the default value which means natural order
      boolean choosePrev = !deleteOrderingVal.equals(0)
          && ReflectionUtils.isSameClass(curOrderingVal, deleteOrderingVal)
          && curOrderingVal.compareTo(deleteOrderingVal) > 0;
      if (choosePrev) {
        // The DELETE message is obsolete if the old message has greater orderingVal.
        return oldValue;
      }
      HoodieRecord record;
      if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
        record = SpillableMapUtils.generateEmptyPayload(key,
            deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN());
      } else {
        record = new HoodieEmptyRecord<>(
            new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      }
      return WrapNaturalOrderHoodieRecord.of(value.getNaturalOrder(), record);
    }

    private WrapNaturalOrderHoodieRecord combineDataRecord(String key, WrapNaturalOrderHoodieRecord value, WrapNaturalOrderHoodieRecord oldValue) throws IOException {
      HoodieRecord prevRecord = oldValue.getRecord();
      HoodieRecord newRecord = value.getRecord();
      // Merge and store the combined record
      HoodieRecord combinedRecord = (HoodieRecord) recordMerger.merge(prevRecord, readerSchema,
          newRecord, readerSchema, getPayloadProps()).get().getLeft();
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != prevRecord.getData()) {
        HoodieRecord latestHoodieRecord = getLatestHoodieRecord(newRecord, combinedRecord, key);

        // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
        //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
        //       it since these records will be put into records(Map).
        return WrapNaturalOrderHoodieRecord.of(value.getNaturalOrder(), latestHoodieRecord);
      }
      // keep old value
      return oldValue;
    }

    @Override
    public WrapNaturalOrderHoodieRecord initCombine(String key, WrapNaturalOrderHoodieRecord value) {
      if (value.getRecord() instanceof MarkDeleteRecord) {
        return initCombineDeleteRecord(key, value);
      }
      return initCombineDataRecord(key, value);
    }

    private WrapNaturalOrderHoodieRecord initCombineDeleteRecord(String key, WrapNaturalOrderHoodieRecord value) {
      MarkDeleteRecord deleteRecord = (MarkDeleteRecord) value.getRecord();
      HoodieRecord record;
      if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
        record = SpillableMapUtils.generateEmptyPayload(key,
            deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN());
      } else {
        record = new HoodieEmptyRecord<>(
            new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      }
      return WrapNaturalOrderHoodieRecord.of(value.getNaturalOrder(), record);
    }

    private WrapNaturalOrderHoodieRecord initCombineDataRecord(String key, WrapNaturalOrderHoodieRecord value) {
      return value;
    }
  }

  /**
   * Used for replacing #DeleteRecord
   */
  static class MarkDeleteRecord extends HoodieEmptyRecord {

    public MarkDeleteRecord(HoodieKey key, HoodieRecordType type) {
      super(key, type);
    }

    public MarkDeleteRecord(HoodieKey key, HoodieOperation operation, Comparable orderingVal, HoodieRecordType type) {
      super(key, operation, orderingVal, type);
    }
  }

  static class WrapNaturalOrderHoodieRecord implements Serializable {
    private long naturalOrder;
    private HoodieRecord record;

    private WrapNaturalOrderHoodieRecord(long naturalOrder, HoodieRecord record) {
      this.naturalOrder = naturalOrder;
      this.record = record;
    }

    public HoodieRecord getRecord() {
      return record;
    }

    public long getNaturalOrder() {
      return naturalOrder;
    }

    public static WrapNaturalOrderHoodieRecord of(long naturalOrder, HoodieRecord record) {
      return new WrapNaturalOrderHoodieRecord(naturalOrder, record);
    }

    public WrapNaturalOrderHoodieRecord copy() {
      return new WrapNaturalOrderHoodieRecord(naturalOrder, record.copy());
    }
  }

  private void performScan() {
    scanStart();
    scanInternal(Option.empty(), false);
    scanEnd();
    LOG.info("Scanned {} log files with stats: MaxMemoryForScan => {}, TotalTimeTakenToScan => {}\n"
        + "SortedAppendOnlyExternalSpillableMap info: {}",
        logFilePaths.size(), maxMemoryUsageForSorting, totalTimeTakenToScanRecords, map.generateLogInfo());
  }

  public long getMaxMemoryUsageForSorting() {
    return maxMemoryUsageForSorting;
  }

  @Override
  protected <T> void processNextRecord(HoodieRecord<T> hoodieRecord) {
    map.put(hoodieRecord.getRecordKey(), WrapNaturalOrderHoodieRecord.of(totalLogRecords++, hoodieRecord.copy()));
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    HoodieRecord record = new MarkDeleteRecord(new HoodieKey(deleteRecord.getRecordKey(), deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
    map.put(deleteRecord.getRecordKey(), WrapNaturalOrderHoodieRecord.of(totalLogRecords++, record));
  }

  @Override
  public Iterator<HoodieRecord> iterator() {
    return new CloseableMappingIterator<WrapNaturalOrderHoodieRecord, HoodieRecord>(map.iterator(), WrapNaturalOrderHoodieRecord::getRecord);
  }

  @Override
  public void close() throws IOException {
    this.map.close();
  }

  public static HoodieSortedMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieSortedMergedLogRecordScanner.Builder();
  }

  public static class Builder extends AbstractHoodieLogRecordScanner.Builder {
    private HoodieStorage storage;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();
    private String latestInstantTime;
    private boolean reverseReader;
    private int bufferSize;
    // specific configurations
    private Long maxMemorySizeInBytes;
    private String partitionName;
    private Option<InstantRange> instantRange = Option.empty();
    private boolean withOperationField = false;
    private Option<String> keyFieldOverride = Option.empty();
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;
    private Option<Comparator<HoodieRecord>> comparator = Option.empty();
    protected HoodieTableMetaClient hoodieTableMetaClient;
    private String externalSorterBasePath;
    private SortEngine sortEngine = SortEngine.HEAP;

    @Override
    public Builder withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    @Override
    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public Builder withBasePath(StoragePath basePath) {
      this.basePath = basePath.toString();
      return this;
    }

    @Override
    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public Builder withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    @Override
    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    @Override
    public Builder withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    @Override
    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public Builder withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    public Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      return this;
    }

    public Builder withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
      return this;
    }

    public Builder withKeyFieldOverride(String keyFieldOverride) {
      this.keyFieldOverride = Option.of(keyFieldOverride);
      return this;
    }

    public Builder withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    public Builder withComparator(Comparator<HoodieRecord> comparator) {
      this.comparator = Option.of(comparator);
      return this;
    }

    @Override
    public Builder withTableMetaClient(HoodieTableMetaClient hoodieTableMetaClient) {
      this.hoodieTableMetaClient = hoodieTableMetaClient;
      return this;
    }

    @Override
    public Builder withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    public Builder withExternalSorterBasePath(String externalSorterBasePath) {
      this.externalSorterBasePath = externalSorterBasePath;
      return this;
    }

    public Builder withSortEngine(SortEngine sortEngine) {
      this.sortEngine = sortEngine;
      return this;
    }

    @Override
    public HoodieSortedMergedLogRecordScanner build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(basePath), new StoragePath(this.logFilePaths.get(0)).getParent());
      }
      return new HoodieSortedMergedLogRecordScanner(
          storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange,
          withOperationField, forceFullScan, Option.ofNullable(partitionName), internalSchema, keyFieldOverride,
          enableOptimizedLogBlocksScan, recordMerger, Option.ofNullable(hoodieTableMetaClient), comparator, maxMemorySizeInBytes, externalSorterBasePath,
          sortEngine);
    }
  }
}
