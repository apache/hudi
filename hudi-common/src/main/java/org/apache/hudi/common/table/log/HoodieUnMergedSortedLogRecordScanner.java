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
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ExternalSorter;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_LOGFILE_SUFFIX;

public class HoodieUnMergedSortedLogRecordScanner extends AbstractHoodieLogRecordScanner {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieUnMergedSortedLogRecordScanner.class);

  public static final Comparator<HoodieKey> DEFAULT_KEY_COMPARATOR = Comparator.comparing(HoodieKey::getRecordKey);

  private final Comparator<HoodieRecord> defaultComparator = (o1, o2) -> {

    // Compare by key first
    int keyCompareResult = DEFAULT_KEY_COMPARATOR.compare(o1.getKey(), o2.getKey());
    if (keyCompareResult != 0) {
      return keyCompareResult;
    }

    // For same key, compare by ordering value
    // TODO: consider different Comparable types
    Comparable order1 = o1.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps());
    Comparable order2 = o2.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps());
    return order1.compareTo(order2);
  };

  private final ExternalSorter<HoodieRecord> records;

  private final Comparator<HoodieRecord> hoodieRecordComparator;
  private long maxMemoryUsageForSorting;

  protected HoodieUnMergedSortedLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                                                 String latestInstantTime, boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                                 boolean withOperationField, boolean forceFullScan, Option<String> partitionNameOverride,
                                                 InternalSchema internalSchema, Option<String> keyFieldOverride,
                                                 boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger,
                                                 Option<HoodieTableMetaClient> hoodieTableMetaClientOption,
                                                 Option<Comparator<HoodieRecord>> comparator, long maxMemoryUsageForSorting, String externalSorterBasePath) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange, withOperationField, forceFullScan, partitionNameOverride, internalSchema,
        keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger, hoodieTableMetaClientOption);
    this.hoodieRecordComparator = comparator.orElse(defaultComparator);
    this.maxMemoryUsageForSorting = maxMemoryUsageForSorting;
    try {
      this.records = new ExternalSorter<HoodieRecord>(externalSorterBasePath, maxMemoryUsageForSorting, this.hoodieRecordComparator, new HoodieRecordSizeEstimator<>(readerSchema));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize external sorter", e);
    }
    // TODO: use external-sorter
    // scan all records but don't merge them
    performScan();
  }

  private void performScan() {
    scanStart();
    scanInternal(Option.empty(), false);
    // sort it
    this.records.sort();
    scanEnd();
    if (LOG.isInfoEnabled()) {
      LOG.info("Scanned {} log files with stats: RecordsNum => {}, took {} ms ", logFilePaths.size(), records.getTotalEntryCount(), totalTimeTakenToScanRecords);
    }
  }

  public long getMaxMemoryUsageForSorting() {
    return maxMemoryUsageForSorting;
  }

  @Override
  protected <T> void processNextRecord(HoodieRecord<T> hoodieRecord) {
    records.add(hoodieRecord);
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    HoodieRecord record;
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
      record = SpillableMapUtils.generateEmptyPayload(deleteRecord.getRecordKey(), deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN());
    } else {
      record = new HoodieEmptyRecord(new HoodieKey(deleteRecord.getRecordKey(), deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
    }
    records.add(record);
  }

  @NotNull
  @Override
  public Iterator<HoodieRecord> iterator() {
    return this.records.iterator();
  }

  @Override
  public void close() throws IOException {
    this.records.close();
  }

  public static HoodieUnMergedSortedLogRecordScanner.Builder newBuilder() {
    return new HoodieUnMergedSortedLogRecordScanner.Builder();
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
      this.recordMerger = recordMerger;
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


    @Override
    public HoodieUnMergedSortedLogRecordScanner build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(basePath), new StoragePath(this.logFilePaths.get(0)).getParent());
      }
      return new HoodieUnMergedSortedLogRecordScanner(
          storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange,
          withOperationField, forceFullScan, Option.ofNullable(partitionName), internalSchema, keyFieldOverride,
          enableOptimizedLogBlocksScan, recordMerger, Option.ofNullable(hoodieTableMetaClient), comparator, maxMemorySizeInBytes, externalSorterBasePath);
    }
  }
}
