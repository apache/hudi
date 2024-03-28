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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
public class HoodieMergedLogRecordScanner extends AbstractHoodieLogRecordReader
    implements Iterable<HoodieRecord>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergedLogRecordScanner.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // Map of compacted/merged records
  private final ExternalSpillableMap<String, HoodieRecord> records;

  private final ExternalSpillableMap<String, HashMap<String, HoodieRecord>> nonUniqueKeyRecords;

  // Set of already scanned prefixes allowing us to avoid scanning same prefixes again
  private final Set<String> scannedPrefixes;
  // count of merged records in log
  private long numMergedRecordsInLog;
  private final long maxMemorySizeInBytes;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  private final boolean logContainsNonUniqueKeys;

  @SuppressWarnings("unchecked")
  private HoodieMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
                                       String latestInstantTime, Long maxMemorySizeInBytes, boolean readBlocksLazily,
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
                                       boolean logContainsNonUniqueKeys) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger,
        hoodieTableMetaClientOption);
    try {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);
      this.nonUniqueKeyRecords = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);

      if (logFilePaths.size() > 0 && HoodieTableMetadata.isMetadataTableSecondaryIndexPartition(basePath, partitionName)) {
        this.logContainsNonUniqueKeys = true;
      } else {
        this.logContainsNonUniqueKeys = false;
      }

      // this.logContainsNonUniqueKeys = logContainsNonUniqueKeys;
      this.scannedPrefixes = new HashSet<>();
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

    scanInternal(Option.of(KeySpec.fullKeySpec(missingKeys)), false);
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
    scanInternal(Option.of(KeySpec.prefixKeySpec(missingKeyPrefixes)), false);
    scannedPrefixes.addAll(missingKeyPrefixes);
  }

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    scanInternal(Option.empty(), false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = records.size();

    LOG.info("Number of log files scanned => " + logFilePaths.size());
    LOG.info("MaxMemoryInBytes allowed for compaction => " + maxMemorySizeInBytes);
    LOG.info("Number of entries in MemoryBasedMap in ExternalSpillableMap => " + records.getInMemoryMapNumEntries());
    LOG.info("Total size in bytes of MemoryBasedMap in ExternalSpillableMap => " + records.getCurrentInMemoryMapSize());
    LOG.info("Number of entries in DiskBasedMap in ExternalSpillableMap => " + records.getDiskBasedMapNumEntries());
    LOG.info("Size of file spilled to disk => " + records.getSizeOfFileOnDiskInBytes());
  }

  @Override
  public Iterator<HoodieRecord> iterator() {
    if (!logContainsNonUniqueKeys) {
      return records.iterator();
    }
    ClosableIterator<HashMap<String, HoodieRecord>> recordIterator = ClosableIterator.wrap(nonUniqueKeyRecords.values().iterator());
    return new ClosableIterator<HoodieRecord>() {
      private Iterator<HoodieRecord> nextKeyRecords;

      @Override
      public void close() {
        recordIterator.close();
      }

      @Override
      public boolean hasNext() {
        if (nextKeyRecords == null) {
          if (!recordIterator.hasNext()) {
            return false;
          }
          nextKeyRecords = recordIterator.next().values().iterator();
        }

        if (nextKeyRecords.hasNext()) {
          return true;
        }

        if (!recordIterator.hasNext()) {
          return false;
        }

        nextKeyRecords = recordIterator.next().values().iterator();
        return nextKeyRecords.hasNext();
      }

      @Override
      public HoodieRecord next() {
        return nextKeyRecords.next();
      }
    };
  }

  public Map<String, HoodieRecord> getRecords() {
    checkArgument(!logContainsNonUniqueKeys, "Cannot get records when the log contains non-unique keys");
    return records;
  }

  public Collection<String> getKeySet() {
    if (!logContainsNonUniqueKeys) {
      return records.keySet();
    }

    return nonUniqueKeyRecords.keySet();
  }

  public Map<String, HashMap<String, HoodieRecord>> getNonUniqueRecordsMap() {
    return nonUniqueKeyRecords;
  }

  public HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  public boolean getLogContainsNonUniqueKeys() {
    return logContainsNonUniqueKeys;
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
  public <T> void processNextRecord(HoodieRecord<T> newRecord) throws IOException {
    if (logContainsNonUniqueKeys) {
      processNextNonUniqueKeyRecord(newRecord);
      return;
    }

    String key = newRecord.getRecordKey();
    HoodieRecord<T> prevRecord = records.get(key);
    if (prevRecord != null) {
      // Merge and store the combined record
      HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(prevRecord, readerSchema,
          newRecord, readerSchema, this.getPayloadProps()).get().getLeft();
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != prevRecord.getData()) {
        HoodieRecord latestHoodieRecord =
            combinedRecord.newInstance(new HoodieKey(key, newRecord.getPartitionPath()), newRecord.getOperation());

        latestHoodieRecord.unseal();
        latestHoodieRecord.setCurrentLocation(newRecord.getCurrentLocation());
        latestHoodieRecord.seal();

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

  private <T> void processNextNonUniqueKeyRecord(HoodieRecord<T> newRecord) throws IOException {
    String key = newRecord.getRecordKey();
    HoodieMetadataPayload newPayload = (HoodieMetadataPayload) newRecord.getData();

    // The rules for merging the prevRecord and the latestRecord is noted below. Note that this only applies for SecondaryIndex
    // records in the metadata table (which is the only user of this API as of this implementation)
    // 1. Iff latestRecord is deleted (i.e it is a tombstone) AND prevRecord is null (i.e not buffered), then retain the latestRecord
    //    The rationale here is that there could be a 'prev record' in the base-file that needs to be merged at a later stage
    // 2. Iff latestRecord is deleted AND prevRecord is non-null, then remove prevRecord from the buffer AND discard the latestRecord
    // 3. Iff latestRecord is not deleted AND prevRecord is non-null, then remove the prevRecord from the buffer AND retain the latestRecord
    //    The rationale is that the most recent record is always retained (based on arrival time). TODO: verify this logic
    // 4. Iff latestRecord is not deleted AND prevRecord is null, then retain the latestRecord (same rationale as #1)

    HashMap<String, HoodieRecord> prevRecords = nonUniqueKeyRecords.get(key);
    if (prevRecords == null) {
      // Case #1 and #4
      HashMap<String, HoodieRecord> recordsMap = new HashMap<>();
      recordsMap.put(newPayload.getRecordKeyFromSecondaryIndex(), newRecord.copy());
      nonUniqueKeyRecords.put(key, recordsMap);
      return;
    }

    String newRecordKey = newPayload.getRecordKeyFromSecondaryIndex();
    HoodieRecord prevRecord = prevRecords.get(newRecordKey);
    if (prevRecord == null) {
      // Case #1 and #4
      prevRecords.put(newRecordKey, newRecord.copy());
      nonUniqueKeyRecords.put(key, prevRecords);
      return;
    }

    HoodieMetadataPayload prevPayload = (HoodieMetadataPayload) prevRecord.getData();
    assert prevPayload.getRecordKeyFromSecondaryIndex().equals(newPayload.getRecordKeyFromSecondaryIndex());

    // TODO: Merger need not be called here as the merging logic is handled explicitly in this function.
    // Retain until Secondary Index feature is tested and stabilized
    HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(prevRecord, readerSchema,
        newRecord, readerSchema, this.getPayloadProps()).get().getLeft();

    if (combinedRecord.getData() != prevRecord.getData()) {
      HoodieRecord latestHoodieRecord =
          combinedRecord.newInstance(new HoodieKey(key, newRecord.getPartitionPath()), newRecord.getOperation());

      latestHoodieRecord.unseal();
      latestHoodieRecord.setCurrentLocation(newRecord.getCurrentLocation());
      latestHoodieRecord.seal();

      HoodieMetadataPayload latestPayload = (HoodieMetadataPayload) latestHoodieRecord.getData();

      if (latestPayload.isSecondaryIndexDeleted()) {
        // If latestPayload is a tombstone record, then remove the prevRecord and discard the current record
        // Case #1
        prevRecords.remove(newRecordKey);
      } else {
        // Retain the latest (merged) record and discard the previous record
        // Case #3
        prevRecords.put(latestPayload.getRecordKeyFromSecondaryIndex(), latestHoodieRecord.copy());
        nonUniqueKeyRecords.put(key, prevRecords);
      }
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

      Comparable curOrderingVal = oldRecord.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps());
      Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
      // Checks the ordering value does not equal to 0
      // because we use 0 as the default value which means natural order
      boolean choosePrev = !deleteOrderingVal.equals(0)
          && ReflectionUtils.isSameClass(curOrderingVal, deleteOrderingVal)
          && curOrderingVal.compareTo(deleteOrderingVal) > 0;
      if (choosePrev) {
        // The DELETE message is obsolete if the old message has greater orderingVal.
        return;
      }
    }
    // Put the DELETE record
    if (recordType == HoodieRecordType.AVRO) {
      records.put(key, SpillableMapUtils.generateEmptyPayload(key,
          deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN()));
    } else {
      HoodieEmptyRecord record = new HoodieEmptyRecord<>(new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      records.put(key, record);
    }
  }

  Option<HoodieRecord> remove(HoodieRecord record) {
    if (!logContainsNonUniqueKeys) {
      return Option.ofNullable(records.remove(record.getRecordKey()));
    }

    HoodieMetadataPayload payload = (HoodieMetadataPayload) record.getData();
    String secondaryKey = record.getRecordKey();
    String recordKey = payload.getRecordKeyFromSecondaryIndex();

    HashMap<String, HoodieRecord> secondaryKeyRecords = nonUniqueKeyRecords.get(secondaryKey);
    if (secondaryKeyRecords == null) {
      return Option.empty();
    }

    HoodieRecord secondaryKeyRecord = secondaryKeyRecords.remove(recordKey);
    if (secondaryKeyRecords.isEmpty()) {
      nonUniqueKeyRecords.remove(secondaryKey);
    }
    return Option.ofNullable(secondaryKeyRecord);
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

  public boolean hasKey(String key) {
    if (!logContainsNonUniqueKeys) {
      return records.containsKey(key);
    }

    return nonUniqueKeyRecords.containsKey(key);
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder extends AbstractHoodieLogRecordReader.Builder {
    private FileSystem fs;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();
    private String latestInstantTime;
    private boolean readBlocksLazily;
    private boolean reverseReader;
    private int bufferSize;
    // specific configurations
    private Long maxMemorySizeInBytes;
    private String spillableMapBasePath;
    private ExternalSpillableMap.DiskMapType diskMapType = HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue();
    private boolean isBitCaskDiskMapCompressionEnabled = HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue();
    // incremental filtering
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    // operation field default false
    private boolean withOperationField = false;
    private String keyFieldOverride;
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;
    protected HoodieTableMetaClient hoodieTableMetaClient;

    private boolean logContainsNonUniqueKeys = false;

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
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
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
    public Builder withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
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

    public Builder withKeyFiledOverride(String keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride);
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

    public Builder withLogContainsNonUniqueKeys(boolean logContainsNonUniqueKeys) {
      this.logContainsNonUniqueKeys = logContainsNonUniqueKeys;
      return this;
    }

    @Override
    public HoodieMergedLogRecordScanner build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(new Path(basePath), new Path(this.logFilePaths.get(0)).getParent());
      }
      ValidationUtils.checkArgument(recordMerger != null);

      return new HoodieMergedLogRecordScanner(fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, readBlocksLazily, reverseReader,
          bufferSize, spillableMapBasePath, instantRange,
          diskMapType, isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan,
          Option.ofNullable(partitionName), internalSchema, Option.ofNullable(keyFieldOverride), enableOptimizedLogBlocksScan, recordMerger,
          Option.ofNullable(hoodieTableMetaClient), logContainsNonUniqueKeys);
    }
  }
}

