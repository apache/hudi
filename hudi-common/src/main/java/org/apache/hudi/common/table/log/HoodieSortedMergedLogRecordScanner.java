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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.sorter.ExternalSorter;
import org.apache.hudi.common.util.sorter.ExternalSorterFactory;
import org.apache.hudi.common.util.sorter.ExternalSorterType;
import org.apache.hudi.common.util.sorter.SortEngine;
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
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_LOGFILE_SUFFIX;

public class HoodieSortedMergedLogRecordScanner extends AbstractHoodieLogRecordScanner {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSortedMergedLogRecordScanner.class);

  private final ExternalSorter<WrapNaturalOrderHoodieRecord> records;

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
                                               Option<Comparator<HoodieRecord>> comparator, long maxMemoryUsageForSorting, String externalSorterBasePath, ExternalSorterType sorterType,
                                               SortEngine sortEngine) {
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
      SizeEstimator sizeEstimator = new SampleEstimator<>(new HoodieRecordSizeEstimator<>(readerSchema));
      this.records = ExternalSorterFactory.create(sorterType, externalSorterBasePath, maxMemoryUsageForSorting,
          wrapNaturalOrderHoodieRecordComparator, sizeEstimator, sortEngine);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize external sorter", e);
    }
    // TODO: always enable full scan for now
    // scan all records but don't merge them
    performScan();
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
  }

  private void performScan() {
    scanStart();
    scanInternal(Option.empty(), false);
    this.records.finish();
    scanEnd();
    LOG.info("Scanned {} log files with stats: MaxMemoryForScan => {}, TimeTakenToInsertAndWriteRecord => {}, TotalTimeTakenToScan => {}",
        logFilePaths.size(), maxMemoryUsageForSorting, records.getTimeTakenToInsertAndWriteRecord(), totalTimeTakenToScanRecords);
  }

  public long getMaxMemoryUsageForSorting() {
    return maxMemoryUsageForSorting;
  }

  @Override
  protected <T> void processNextRecord(HoodieRecord<T> hoodieRecord) {
    records.add(WrapNaturalOrderHoodieRecord.of(totalLogRecords++, hoodieRecord.copy()));
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    HoodieRecord record = new HoodieEmptyRecord(new HoodieKey(deleteRecord.getRecordKey(), deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
    records.add(WrapNaturalOrderHoodieRecord.of(totalLogRecords++, record));
  }

  @Override
  public Iterator<HoodieRecord> iterator() {
    // TODO: push down pre-combine logic to external sorter
    CloseableMappingIterator<WrapNaturalOrderHoodieRecord, HoodieRecord> iterator =
        new CloseableMappingIterator<>(records.getIterator(), WrapNaturalOrderHoodieRecord::getRecord);
    return new PreCombinedRecordIterator(iterator);
  }

  class PreCombinedRecordIterator implements ClosableIterator<HoodieRecord> {

    private final ClosableIterator<HoodieRecord> rawIterator;

    private Option<HoodieRecord> currentRecord = Option.empty();

    PreCombinedRecordIterator(ClosableIterator<HoodieRecord> rawIterator) {
      this.rawIterator = rawIterator;
      init();
    }

    private void init() {
      if (rawIterator.hasNext()) {
        processRecord(rawIterator.next());
      }
    }

    private HoodieRecord processRecord(HoodieRecord next) {
      // run pre combine
      if (next instanceof HoodieEmptyRecord) {
        return processNextDeleteRecord((HoodieEmptyRecord) next);
      } else {
        try {
          return processNextRecord(next);
        } catch (IOException e) {
          LOG.error("Failed to merge income record: {} with existing record: {}", next, currentRecord.get(), e);
          throw new HoodieIOException("Failed to merge records", e);
        }
      }
    }

    // TODO: extract this method and same logic in HoodieMergedLogRecordScanner to a common method when this process is well validated
    private HoodieRecord processNextDeleteRecord(HoodieEmptyRecord deleteRecord) {
      String key = deleteRecord.getRecordKey();
      if (currentRecord.isPresent() && currentRecord.get().getRecordKey().equals(key)) {
        HoodieRecord oldRecord = currentRecord.get();
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
          return null;
        }
      }
      HoodieRecord outputRecord = currentRecord.isPresent() && !currentRecord.get().getRecordKey().equals(key) ? currentRecord.get() : null;
      // Put the DELETE record
      if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
        currentRecord = Option.of(SpillableMapUtils.generateEmptyPayload(key,
            deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN()));
      } else {
        HoodieEmptyRecord record = new HoodieEmptyRecord<>(
            new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
        currentRecord = Option.of(record);
      }
      return outputRecord;
    }

    private HoodieRecord processNextRecord(HoodieRecord newRecord) throws IOException {
      String key = newRecord.getRecordKey();
      if (currentRecord.isPresent() && currentRecord.get().getRecordKey().equals(key)) {
        HoodieRecord prevRecord = currentRecord.get();
        // Merge and store the combined record
        HoodieRecord combinedRecord = recordMerger.merge(prevRecord, readerSchema,
            newRecord, readerSchema, getPayloadProps()).get().getLeft();
        // If pre-combine returns existing record, no need to update it
        if (combinedRecord.getData() != prevRecord.getData()) {
          HoodieRecord latestHoodieRecord = getLatestHoodieRecord(newRecord, combinedRecord, key);

          // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
          //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
          //       it since these records will be put into records(Map).
          currentRecord = Option.of(latestHoodieRecord.copy());
        }
        return null;
      }
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      HoodieRecord outputRecord = currentRecord.orElse(null);
      currentRecord = Option.of(newRecord.copy());
      return outputRecord;
    }

    @Override
    public void close() {
      rawIterator.close();
    }

    @Override
    public boolean hasNext() {
      return currentRecord.isPresent();
    }

    @Override
    public HoodieRecord next() {
      if (!hasNext()) {
        return null;
      }
      HoodieRecord outputRecord = null;
      while (rawIterator.hasNext() && outputRecord == null) {
        outputRecord = processRecord(rawIterator.next());
      }
      if (outputRecord == null && currentRecord.isPresent()) {
        outputRecord = currentRecord.get();
        currentRecord = Option.empty();
      }
      return outputRecord;
    }
  }

  @Override
  public void close() throws IOException {
    this.records.close();
  }

  public static HoodieSortedMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieSortedMergedLogRecordScanner.Builder();
  }

  public static class Builder extends AbstractHoodieLogRecordReader.Builder {
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
    private ExternalSorterType sorterType = ExternalSorterType.SORT_ON_READ;
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

    public Builder withSorterType(ExternalSorterType sorterType) {
      this.sorterType = sorterType;
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
          sorterType, sortEngine);
    }
  }
}
