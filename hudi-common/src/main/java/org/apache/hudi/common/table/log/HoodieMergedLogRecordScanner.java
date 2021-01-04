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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Scans through all the blocks in a list of HoodieLogFile and builds up a compacted/merged list of records which will
 * be used as a lookup table when merging the base columnar file with the redo log file.
 *
 * NOTE: If readBlockLazily is turned on, does not merge, instead keeps reading log blocks and merges everything at once
 * This is an optimization to avoid seek() back and forth to read new block (forward seek()) and lazily read content of
 * seen block (reverse and forward seek()) during merge | | Read Block 1 Metadata | | Read Block 1 Data | | | Read Block
 * 2 Metadata | | Read Block 2 Data | | I/O Pass 1 | ..................... | I/O Pass 2 | ................. | | | Read
 * Block N Metadata | | Read Block N Data |
 * <p>
 * This results in two I/O passes over the log file.
 */

public class HoodieMergedLogRecordScanner extends AbstractHoodieLogRecordScanner
    implements Iterable<HoodieRecord<? extends HoodieRecordPayload>> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergedLogRecordScanner.class);

  // Final map of compacted/merged records
  private final ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records;

  // count of merged records in log
  private long numMergedRecordsInLog;

  // Stores the total time taken to perform reading and merging of log blocks
  private final long totalTimeTakenToReadAndMergeBlocks;
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = new HoodieTimer();

  @SuppressWarnings("unchecked")
  public HoodieMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
      String latestInstantTime, Long maxMemorySizeInBytes, boolean readBlocksLazily, boolean reverseReader,
      int bufferSize, String spillableMapBasePath) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize);
    try {
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(readerSchema));
      // Do the scan and merge
      timer.startTimer();
      scan();
      this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
      this.numMergedRecordsInLog = records.size();
      LOG.info("MaxMemoryInBytes allowed for compaction => " + maxMemorySizeInBytes);
      LOG.info("Number of entries in MemoryBasedMap in ExternalSpillableMap => " + records.getInMemoryMapNumEntries());
      LOG.info(
          "Total size in bytes of MemoryBasedMap in ExternalSpillableMap => " + records.getCurrentInMemoryMapSize());
      LOG.info("Number of entries in DiskBasedMap in ExternalSpillableMap => " + records.getDiskBasedMapNumEntries());
      LOG.info("Size of file spilled to disk => " + records.getSizeOfFileOnDiskInBytes());
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading log file ", e);
    }
  }

  @Override
  public Iterator<HoodieRecord<? extends HoodieRecordPayload>> iterator() {
    return records.iterator();
  }

  public Map<String, HoodieRecord<? extends HoodieRecordPayload>> getRecords() {
    return records;
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
  protected void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) throws IOException {
    String key = hoodieRecord.getRecordKey();
    if (records.containsKey(key)) {
      // Merge and store the merged record. The HoodieRecordPayload implementation is free to decide what should be
      // done when a delete (empty payload) is encountered before or after an insert/update.
      HoodieRecordPayload combinedValue = hoodieRecord.getData().preCombine(records.get(key).getData());
      records.put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()), combinedValue));
    } else {
      // Put the record as is
      records.put(key, hoodieRecord);
    }
  }

  @Override
  protected void processNextDeletedKey(HoodieKey hoodieKey) {
    records.put(hoodieKey.getRecordKey(), SpillableMapUtils.generateEmptyPayload(hoodieKey.getRecordKey(),
        hoodieKey.getPartitionPath(), getPayloadClassFQN()));
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder extends AbstractHoodieLogRecordScanner.Builder {
    private FileSystem fs;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private String latestInstantTime;
    private boolean readBlocksLazily;
    private boolean reverseReader;
    private int bufferSize;
    // specific configurations
    private Long maxMemorySizeInBytes;
    private String spillableMapBasePath;

    public Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths;
      return this;
    }

    public Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    public Builder withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
      return this;
    }

    public Builder withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
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

    @Override
    public HoodieMergedLogRecordScanner build() {
      return new HoodieMergedLogRecordScanner(fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, readBlocksLazily, reverseReader,
          bufferSize, spillableMapBasePath);
    }
  }
}

