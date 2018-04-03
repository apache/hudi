/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log;

import static com.uber.hoodie.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static com.uber.hoodie.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.HoodieTimer;
import com.uber.hoodie.common.util.SpillableMapUtils;
import com.uber.hoodie.common.util.collection.ExternalSpillableMap;
import com.uber.hoodie.common.util.collection.converter.HoodieRecordConverter;
import com.uber.hoodie.common.util.collection.converter.StringConverter;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Scans through all the blocks in a list of HoodieLogFile and builds up a compacted/merged list of records which will
 * be used as a lookup table when merging the base columnar file with the redo log file. NOTE:  If readBlockLazily is
 * turned on, does not merge, instead keeps reading log blocks and merges everything at once This is an optimization to
 * avoid seek() back and forth to read new block (forward seek()) and lazily read content of seen block (reverse and
 * forward seek()) during merge |            | Read Block 1 Metadata |            | Read Block 1 Data | | | Read Block 2
 * Metadata |            | Read Block 2 Data | | I/O Pass 1 | ..................... | I/O Pass 2 | ................. | |
 * | Read Block N Metadata | | Read Block N Data | <p> This results in two I/O passes over the log file.
 */

public class HoodieCompactedLogRecordScanner implements
    Iterable<HoodieRecord<? extends HoodieRecordPayload>> {

  private static final Logger log = LogManager.getLogger(HoodieCompactedLogRecordScanner.class);

  // Final map of compacted/merged records
  private final ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records;
  // Reader schema for the records
  private final Schema readerSchema;
  // Total log files read - for metrics
  private AtomicLong totalLogFiles = new AtomicLong(0);
  // Total log blocks read - for metrics
  private AtomicLong totalLogBlocks = new AtomicLong(0);
  // Total log records read - for metrics
  private AtomicLong totalLogRecords = new AtomicLong(0);
  // Total number of rollbacks written across all log files
  private AtomicLong totalRollbacks = new AtomicLong(0);
  // Total number of corrupt blocks written across all log files
  private AtomicLong totalCorruptBlocks = new AtomicLong(0);
  // Total final list of compacted/merged records
  private long totalRecordsToUpdate;
  // Latest valid instant time
  private String latestInstantTime;
  private HoodieTableMetaClient hoodieTableMetaClient;
  // Merge strategy to use when combining records from log
  private String payloadClassFQN;
  // Store the last instant log blocks (needed to implement rollback)
  private Deque<HoodieLogBlock> currentInstantLogBlocks = new ArrayDeque<>();
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks = 0L;
  // A timer for calculating elapsed time in millis
  public HoodieTimer timer = new HoodieTimer();

  public HoodieCompactedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths,
      Schema readerSchema, String latestInstantTime, Long maxMemorySizeInBytes,
      boolean readBlocksLazily, boolean reverseReader, int bufferSize, String spillableMapBasePath) {
    this.readerSchema = readerSchema;
    this.latestInstantTime = latestInstantTime;
    this.hoodieTableMetaClient = new HoodieTableMetaClient(fs.getConf(), basePath);
    // load class from the payload fully qualified class name
    this.payloadClassFQN = this.hoodieTableMetaClient.getTableConfig().getPayloadClass();
    this.totalLogFiles.addAndGet(logFilePaths.size());
    timer.startTimer();

    try {
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath,
          new StringConverter(), new HoodieRecordConverter(readerSchema, payloadClassFQN));
      // iterate over the paths
      HoodieLogFormatReader logFormatReaderWrapper =
          new HoodieLogFormatReader(fs,
              logFilePaths.stream().map(logFile -> new HoodieLogFile(new Path(logFile)))
                  .collect(Collectors.toList()), readerSchema, readBlocksLazily, reverseReader, bufferSize);
      HoodieLogFile logFile;
      while (logFormatReaderWrapper.hasNext()) {
        logFile = logFormatReaderWrapper.getLogFile();
        log.info("Scanning log file " + logFile);
        // Use the HoodieLogFileReader to iterate through the blocks in the log file
        HoodieLogBlock r = logFormatReaderWrapper.next();
        if (r.getBlockType() != CORRUPT_BLOCK
            && !HoodieTimeline.compareTimestamps(r.getLogBlockHeader().get(INSTANT_TIME),
            this.latestInstantTime,
            HoodieTimeline.LESSER_OR_EQUAL)) {
          //hit a block with instant time greater than should be processed, stop processing further
          break;
        }
        switch (r.getBlockType()) {
          case AVRO_DATA_BLOCK:
            log.info("Reading a data block from file " + logFile.getPath());
            if (isNewInstantBlock(r) && !readBlocksLazily) {
              // If this is an avro data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              merge(records, currentInstantLogBlocks);
            }
            // store the current block
            currentInstantLogBlocks.push(r);
            break;
          case DELETE_BLOCK:
            log.info("Reading a delete block from file " + logFile.getPath());
            if (isNewInstantBlock(r) && !readBlocksLazily) {
              // If this is a delete data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              merge(records, currentInstantLogBlocks);
            }
            // store deletes so can be rolled back
            currentInstantLogBlocks.push(r);
            break;
          case COMMAND_BLOCK:
            // Consider the following scenario
            // (Time 0, C1, Task T1) -> Running
            // (Time 1, C1, Task T1) -> Failed (Wrote either a corrupt block or a correct
            //                                  DataBlock (B1) with commitTime C1
            // (Time 2, C1, Task T1.2) -> Running (Task T1 was retried and the attempt number is 2)
            // (Time 3, C1, Task T1.2) -> Finished (Wrote a correct DataBlock B2)
            // Now a logFile L1 can have 2 correct Datablocks (B1 and B2) which are the same.
            // Say, commit C1 eventually failed and a rollback is triggered.
            // Rollback will write only 1 rollback block (R1) since it assumes one block is
            // written per ingestion batch for a file but in reality we need to rollback (B1 & B2)
            // The following code ensures the same rollback block (R1) is used to rollback
            // both B1 & B2
            log.info("Reading a command block from file " + logFile.getPath());
            // This is a command block - take appropriate action based on the command
            HoodieCommandBlock commandBlock = (HoodieCommandBlock) r;
            String targetInstantForCommandBlock = r.getLogBlockHeader()
                .get(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME);
            switch (commandBlock.getType()) { // there can be different types of command blocks
              case ROLLBACK_PREVIOUS_BLOCK:
                // Rollback the last read log block
                // Get commit time from last record block, compare with targetCommitTime,
                // rollback only if equal, this is required in scenarios of invalid/extra
                // rollback blocks written due to failures during the rollback operation itself
                // and ensures the same rollback block (R1) is used to rollback both B1 & B2 with
                // same instant_time
                int numBlocksRolledBack = 0;
                totalRollbacks.incrementAndGet();
                while (!currentInstantLogBlocks.isEmpty()) {
                  HoodieLogBlock lastBlock = currentInstantLogBlocks.peek();
                  // handle corrupt blocks separately since they may not have metadata
                  if (lastBlock.getBlockType() == CORRUPT_BLOCK) {
                    log.info(
                        "Rolling back the last corrupted log block read in " + logFile.getPath());
                    currentInstantLogBlocks.pop();
                    numBlocksRolledBack++;
                  } else if (lastBlock.getBlockType() != CORRUPT_BLOCK
                      && targetInstantForCommandBlock
                      .contentEquals(lastBlock.getLogBlockHeader().get(INSTANT_TIME))) {
                    // rollback last data block or delete block
                    log.info("Rolling back the last log block read in " + logFile.getPath());
                    currentInstantLogBlocks.pop();
                    numBlocksRolledBack++;
                  } else if (!targetInstantForCommandBlock
                      .contentEquals(
                          currentInstantLogBlocks.peek().getLogBlockHeader().get(INSTANT_TIME))) {
                    // invalid or extra rollback block
                    log.warn("TargetInstantTime " + targetInstantForCommandBlock
                        + " invalid or extra rollback command block in " + logFile.getPath());
                    break;
                  } else {
                    // this should not happen ideally
                    log.warn("Unable to apply rollback command block in " + logFile.getPath());
                  }
                }
                log.info("Number of applied rollback blocks " + numBlocksRolledBack);
                break;
              default:
                throw new UnsupportedOperationException("Command type not yet supported.");

            }
            break;
          case CORRUPT_BLOCK:
            log.info("Found a corrupt block in " + logFile.getPath());
            totalCorruptBlocks.incrementAndGet();
            // If there is a corrupt block - we will assume that this was the next data block
            currentInstantLogBlocks.push(r);
            break;
          default:
            throw new UnsupportedOperationException("Block type not supported yet");
        }
      }
      // merge the last read block when all the blocks are done reading
      if (!currentInstantLogBlocks.isEmpty()) {
        log.info("Merging the final data blocks");
        merge(records, currentInstantLogBlocks);
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading log file ");
    }
    this.totalRecordsToUpdate = records.size();
    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    log.info("MaxMemoryInBytes allowed for compaction => " + maxMemorySizeInBytes);
    log.info("Number of entries in MemoryBasedMap in ExternalSpillableMap => " + records.getInMemoryMapNumEntries());
    log.info("Total size in bytes of MemoryBasedMap in ExternalSpillableMap => " + records.getCurrentInMemoryMapSize());
    log.info("Number of entries in DiskBasedMap in ExternalSpillableMap => " + records.getDiskBasedMapNumEntries());
    log.info("Size of file spilled to disk => " + records.getSizeOfFileOnDiskInBytes());
    log.debug("Total time taken for scanning and compacting log files => " + totalTimeTakenToReadAndMergeBlocks);
  }

  /**
   * Checks if the current logblock belongs to a later instant
   */
  private boolean isNewInstantBlock(HoodieLogBlock logBlock) {
    return currentInstantLogBlocks.size() > 0
        && currentInstantLogBlocks.peek().getBlockType() != CORRUPT_BLOCK
        && !logBlock.getLogBlockHeader().get(INSTANT_TIME)
        .contentEquals(currentInstantLogBlocks.peek().getLogBlockHeader().get(INSTANT_TIME));
  }

  /**
   * Iterate over the GenericRecord in the block, read the hoodie key and partition path and merge with the application
   * specific payload if the same key was found before. Sufficient to just merge the log records since the base data is
   * merged on previous compaction. Finally, merge this log block with the accumulated records
   */
  private Map<String, HoodieRecord<? extends HoodieRecordPayload>> merge(
      HoodieAvroDataBlock dataBlock) throws IOException {
    // TODO (NA) - Implemnt getRecordItr() in HoodieAvroDataBlock and use that here
    List<IndexedRecord> recs = dataBlock.getRecords();
    totalLogRecords.addAndGet(recs.size());
    recs.forEach(rec -> {
      String key = ((GenericRecord) rec).get(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          .toString();
      HoodieRecord<? extends HoodieRecordPayload> hoodieRecord =
          SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) rec, this.payloadClassFQN);
      if (records.containsKey(key)) {
        // Merge and store the merged record
        HoodieRecordPayload combinedValue = records.get(key).getData()
            .preCombine(hoodieRecord.getData());
        records
            .put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()),
                combinedValue));
      } else {
        // Put the record as is
        records.put(key, hoodieRecord);
      }
    });
    return records;
  }

  /**
   * Merge the last seen log blocks with the accumulated records
   */
  private void merge(Map<String, HoodieRecord<? extends HoodieRecordPayload>> records,
      Deque<HoodieLogBlock> lastBlocks) throws IOException {
    while (!lastBlocks.isEmpty()) {
      log.info("Number of remaining logblocks to merge " + lastBlocks.size());
      // poll the element at the bottom of the stack since that's the order it was inserted
      HoodieLogBlock lastBlock = lastBlocks.pollLast();
      switch (lastBlock.getBlockType()) {
        case AVRO_DATA_BLOCK:
          merge((HoodieAvroDataBlock) lastBlock);
          break;
        case DELETE_BLOCK:
          // TODO : If delete is the only block written and/or records are present in parquet file
          // TODO : Mark as tombstone (optional.empty()) for data instead of deleting the entry
          Arrays.stream(((HoodieDeleteBlock) lastBlock).getKeysToDelete()).forEach(records::remove);
          break;
        case CORRUPT_BLOCK:
          log.warn("Found a corrupt block which was not rolled back");
          break;
        default:
          //TODO <vb> : Need to understand if COMMAND_BLOCK has to be handled?
          break;
      }
    }
  }

  @Override
  public Iterator<HoodieRecord<? extends HoodieRecordPayload>> iterator() {
    return records.iterator();
  }

  public long getTotalLogFiles() {
    return totalLogFiles.get();
  }

  public long getTotalLogRecords() {
    return totalLogRecords.get();
  }

  public long getTotalLogBlocks() {
    return totalLogBlocks.get();
  }

  public Map<String, HoodieRecord<? extends HoodieRecordPayload>> getRecords() {
    return records;
  }

  public long getTotalRecordsToUpdate() {
    return totalRecordsToUpdate;
  }

  public long getTotalRollbacks() {
    return totalRollbacks.get();
  }

  public long getTotalCorruptBlocks() {
    return totalCorruptBlocks.get();
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }
}

