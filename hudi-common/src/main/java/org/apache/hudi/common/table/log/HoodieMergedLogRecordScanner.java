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
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_LOGFILE_SUFFIX;
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
public class HoodieMergedLogRecordScanner extends AbstractHoodieLogRecordScanner
    implements Iterable<HoodieRecord>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergedLogRecordScanner.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // Map of compacted/merged records
  private final ExternalSpillableMap<String, HoodieRecord> records;
  // Set of already scanned prefixes allowing us to avoid scanning same prefixes again
  private final Set<String> scannedPrefixes;
  // count of merged records in log
  private long numMergedRecordsInLog;
  private final long maxMemorySizeInBytes;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;
  private final String[] orderingFields;

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
                                         Option<HoodieTableMetaClient> hoodieTableMetaClientOption,
                                         boolean allowInflightInstants) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger,
        hoodieTableMetaClientOption);
    try {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(readerSchema), diskMapType, new DefaultSerializer<>(), isBitCaskDiskMapCompressionEnabled, getClass().getSimpleName());
      this.scannedPrefixes = new HashSet<>();
      this.allowInflightInstants = allowInflightInstants;
      this.orderingFields = ConfigUtils.getOrderingFields(this.hoodieTableMetaClient.getTableConfig().getProps());
    } catch (IOException e) {
      throw new HoodieIOException("IOException when creating ExternalSpillableMap at " + spillableMapBasePath, e);
    }

    if (forceFullScan) {
      performScan();
    }
  }

  /**
   * Scans delta-log files processing blocks
   */
  public final void scan() {
    scan(false);
  }

  public final void scan(boolean skipProcessingBlocks) {
    if (forceFullScan) {
      // NOTE: When full-scan is enforced, scanning is invoked upfront (during initialization)
      return;
    }

    scanInternal(Option.empty(), skipProcessingBlocks);
  }

  /**
   * Provides incremental scanning capability where only provided keys will be looked
   * up in the delta-log files, scanned and subsequently materialized into the internal
   * cache
   *
   * @param keys to be looked up
   */
  public void scanByFullKeys(List<String> keys) {
    // We can skip scanning in case reader is in full-scan mode, in which case all blocks
    // are processed upfront (no additional scanning is necessary)
    if (forceFullScan) {
      return; // no-op
    }

    List<String> missingKeys = keys.stream()
        .filter(key -> !records.containsKey(key))
        .collect(Collectors.toList());

    if (missingKeys.isEmpty()) {
      // All the required records are already fetched, no-op
      return;
    }

    scanInternal(Option.of(AbstractHoodieLogRecordScanner.KeySpec.fullKeySpec(missingKeys)), false);
  }

  /**
   * Provides incremental scanning capability where only keys matching provided key-prefixes
   * will be looked up in the delta-log files, scanned and subsequently materialized into
   * the internal cache
   *
   * @param keyPrefixes to be looked up
   */
  public void scanByKeyPrefixes(List<String> keyPrefixes) {
    // We can skip scanning in case reader is in full-scan mode, in which case all blocks
    // are processed upfront (no additional scanning is necessary)
    if (forceFullScan) {
      return;
    }

    List<String> missingKeyPrefixes = keyPrefixes.stream()
        .filter(keyPrefix ->
            // NOTE: We can skip scanning the prefixes that have already
            //       been covered by the previous scans
            scannedPrefixes.stream().noneMatch(keyPrefix::startsWith))
        .collect(Collectors.toList());

    if (missingKeyPrefixes.isEmpty()) {
      // All the required records are already fetched, no-op
      return;
    }

    // NOTE: When looking up by key-prefixes unfortunately we can't short-circuit
    //       and will have to scan every time as we can't know (based on just
    //       the records cached) whether particular prefix was scanned or just records
    //       matching the prefix looked up (by [[scanByFullKeys]] API)
    scanInternal(Option.of(AbstractHoodieLogRecordScanner.KeySpec.prefixKeySpec(missingKeyPrefixes)), false);
    scannedPrefixes.addAll(missingKeyPrefixes);
  }

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    scanInternal(Option.empty(), false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = records.size();

    if (LOG.isInfoEnabled()) {
      LOG.info("Scanned {} log files with stats: MaxMemoryInBytes => {}, MemoryBasedMap => {} entries, {} total bytes, DiskBasedMap => {} entries, {} total bytes",
          logFilePaths.size(), maxMemorySizeInBytes, records.getInMemoryMapNumEntries(), records.getCurrentInMemoryMapSize(),
          records.getDiskBasedMapNumEntries(), records.getSizeOfFileOnDiskInBytes());
    }
  }

  @Override
  public Iterator<HoodieRecord> iterator() {
    return records.iterator();
  }

  public Map<String, HoodieRecord> getRecords() {
    return records;
  }

  public HoodieRecord.HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  public long getNumMergedRecordsInLog() {
    return numMergedRecordsInLog;
  }

  /**
   * Returns the builder for {@code HoodieMergedLogRecordScanner}.
   */
  public static HoodieMergedLogRecordScanner.Builder newBuilder() {
    return new Builder();
  }

  @Override
  protected <T> void processNextRecord(HoodieRecord<T> newRecord) throws IOException {
    String key = newRecord.getRecordKey();
    HoodieRecord<T> prevRecord = records.get(key);
    if (prevRecord != null) {
      // Merge and store the combined record
      HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(prevRecord, readerSchema,
          newRecord, readerSchema, this.getPayloadProps()).getLeft();
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

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    String key = deleteRecord.getRecordKey();
    HoodieRecord oldRecord = records.get(key);
    if (oldRecord != null) {
      // Merge and store the merged record. The ordering val is taken to decide whether the same key record
      // should be deleted or be kept. The old record is kept only if the DELETE record has smaller ordering val.
      // For same ordering values, uses the natural order(arrival time semantics).

      Comparable curOrderingVal = oldRecord.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps(), orderingFields);
      Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
      // Checks the ordering value does not equal to 0
      // because we use 0 as the default value which means natural order
      boolean choosePrev = !OrderingValues.isDefault(deleteOrderingVal)
          && OrderingValues.isSameClass(curOrderingVal, deleteOrderingVal)
          && curOrderingVal.compareTo(deleteOrderingVal) > 0;
      if (choosePrev) {
        // The DELETE message is obsolete if the old message has greater orderingVal.
        return;
      }
    }
    // Put the DELETE record
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
      records.put(key, HoodieRecordUtils.generateEmptyPayload(key,
          deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN()));
    } else {
      HoodieEmptyRecord record = new HoodieEmptyRecord<>(new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      records.put(key, record);
    }
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    if (records != null) {
      records.close();
    }
  }

  private static <T> HoodieRecord getLatestHoodieRecord(HoodieRecord<T> newRecord, HoodieRecord<T> combinedRecord, String key) {
    HoodieRecord latestHoodieRecord =
        combinedRecord.newInstance(new HoodieKey(key, newRecord.getPartitionPath()), newRecord.getOperation());

    latestHoodieRecord.unseal();
    latestHoodieRecord.setCurrentLocation(newRecord.getCurrentLocation());
    latestHoodieRecord.seal();
    return latestHoodieRecord;
  }

  /**
   * Builder used to build {@code HoodieMergedLogRecordScanner}.
   */
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
    private String spillableMapBasePath;
    private ExternalSpillableMap.DiskMapType diskMapType = SPILLABLE_DISK_MAP_TYPE.defaultValue();
    private boolean isBitCaskDiskMapCompressionEnabled = DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue();
    // incremental filtering
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    // operation field default false
    private boolean withOperationField = false;
    private String keyFieldOverride;
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    protected boolean allowInflightInstants = false;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;
    protected HoodieTableMetaClient hoodieTableMetaClient;

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

    public Builder withSpillableMapBasePath(String spillableMapBasePath) {
      this.spillableMapBasePath = spillableMapBasePath;
      return this;
    }

    public Builder withDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      this.diskMapType = diskMapType;
      return this;
    }

    public Builder withBitCaskDiskMapCompressionEnabled(boolean isBitCaskDiskMapCompressionEnabled) {
      this.isBitCaskDiskMapCompressionEnabled = isBitCaskDiskMapCompressionEnabled;
      return this;
    }

    @Override
    public Builder withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    public Builder withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
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

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    public Builder withKeyFieldOverride(String keyFieldOverride) {
      this.keyFieldOverride = requireNonNull(keyFieldOverride);
      return this;
    }

    public Builder withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    @Override
    public Builder withTableMetaClient(HoodieTableMetaClient hoodieTableMetaClient) {
      this.hoodieTableMetaClient = hoodieTableMetaClient;
      return this;
    }

    public Builder withAllowInflightInstants(boolean allowInflightInstants) {
      this.allowInflightInstants = allowInflightInstants;
      return this;
    }

    @Override
    public HoodieMergedLogRecordScanner build() {
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
          Option.ofNullable(hoodieTableMetaClient), allowInflightInstants);
    }
  }
}
