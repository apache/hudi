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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

public class HoodiePositionBasedMergedLogRecordReader<T> extends BaseHoodieLogRecordReader<T>
    implements Iterable<Pair<Option<T>, Map<String, Object>>>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePositionBasedMergedLogRecordReader.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // Map of compacted/merged records
  private final Map<String, Pair<Option<T>, Map<String, Object>>> records;
  private final Set<Long> deletePositions = new HashSet<>();
  // Set of already scanned prefixes allowing us to avoid scanning same prefixes again
  private final Set<String> scannedPrefixes;
  // count of merged records in log
  private long numMergedRecordsInLog;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  protected HoodiePositionBasedMergedLogRecordReader(HoodieReaderContext<T> readerContext,
                                                     FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
                                                     String latestInstantTime, boolean readBlocksLazily,
                                                     boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                                     boolean withOperationField, boolean forceFullScan,
                                                     Option<String> partitionName,
                                                     InternalSchema internalSchema,
                                                     Option<String> keyFieldOverride,
                                                     boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger) {
    super(readerContext, fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, true, recordMerger);
    this.records = new HashMap<>();
    this.scannedPrefixes = new HashSet<>();

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

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    scanInternal(Option.empty(), false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = records.size();
  }

  @Override
  public Iterator<Pair<Option<T>, Map<String, Object>>> iterator() {
    return records.values().iterator();
  }

  public Map<String, Pair<Option<T>, Map<String, Object>>> getRecords() {
    return records;
  }

  public HoodieRecord.HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  public static HoodieMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieMergedLogRecordScanner.Builder();
  }

  @Override
  public void processNextRecord(T record, Map<String, Object> metadata) throws Exception {
    // NOTE: This method is not used in this class. It is for key-based merging.
  }

  @Override
  public void processNextRecord(T record, Map<String, Object> metadata, Option<Long> position) throws IOException {
    String key = (String) metadata.get(HoodieReaderContext.INTERNAL_META_RECORD_KEY);
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(key);
    // Assume position is present
    long pos = position.get();
    if (pos >= 0) {
      HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(
          readerContext.constructHoodieRecord(Option.of(record), metadata, readerSchema), readerSchema,
          readerContext.constructHoodieRecord(existingRecordMetadataPair.getLeft(), existingRecordMetadataPair.getRight(), readerSchema),
          readerSchema, this.getPayloadProps()).get().getLeft();
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecordMetadataPair.getLeft().get()) {
        records.put(key, Pair.of(Option.ofNullable(readerContext.seal(combinedRecord.getData())), metadata));
      }
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      records.put(key, Pair.of(Option.of(readerContext.seal(record)), metadata));
    }
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    // NOTE: This method is not used in this class. It is for key-based merging.
  }

  @Override
  protected void processNextDeletePosition(long position) {
    deletePositions.add(position);
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    if (records != null) {
      records.clear();
    }
  }

  public static class Builder<T> extends BaseHoodieLogRecordReader.Builder<T> {
    private HoodieReaderContext<T> readerContext;
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

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withHoodieReaderContext(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withKeyFiledOverride(String keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride);
      return this;
    }

    public HoodiePositionBasedMergedLogRecordReader.Builder<T> withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordReader<T> build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(new Path(basePath), new Path(this.logFilePaths.get(0)).getParent());
      }
      ValidationUtils.checkArgument(recordMerger != null);

      return new HoodiePositionBasedMergedLogRecordReader<>(readerContext, fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, readBlocksLazily, reverseReader,
          bufferSize, instantRange,
          withOperationField, forceFullScan,
          Option.ofNullable(partitionName), internalSchema, Option.ofNullable(keyFieldOverride), enableOptimizedLogBlocksScan, recordMerger);
    }
  }
}
