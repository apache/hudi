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

import static com.uber.hoodie.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK;
import static com.uber.hoodie.common.table.log.block.HoodieLogBlock.LogMetadataType.INSTANT_TIME;

import com.google.common.collect.Maps;
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
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Scans through all the blocks in a list of HoodieLogFile and builds up a compacted/merged list of
 * records which will be used as a lookup table when merging the base columnar file with the redo
 * log file.
 */
public class HoodieCompactedLogRecordScanner implements
    Iterable<HoodieRecord<? extends HoodieRecordPayload>> {

  private final static Logger log = LogManager.getLogger(HoodieCompactedLogRecordScanner.class);

  // Final list of compacted/merged records to iterate
  private final Collection<HoodieRecord<? extends HoodieRecordPayload>> logRecords;
  // Reader schema for the records
  private final Schema readerSchema;
  // Total log files read - for metrics
  private AtomicLong totalLogFiles = new AtomicLong(0);
  // Total log records read - for metrics
  private AtomicLong totalLogRecords = new AtomicLong(0);
  // Total final list of compacted/merged records
  private long totalRecordsToUpdate;
  // Latest valid instant time
  private String latestInstantTime;
  private HoodieTableMetaClient hoodieTableMetaClient;
  // Merge strategy to use when combining records from log
  private String payloadClassFQN;
  // Store only the last log blocks (needed to implement rollback)
  Deque<HoodieLogBlock> lastBlocks = new ArrayDeque<>();

  public HoodieCompactedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths,
      Schema readerSchema, String latestInstantTime) {
    this.readerSchema = readerSchema;
    this.latestInstantTime = latestInstantTime;
    this.hoodieTableMetaClient = new HoodieTableMetaClient(fs.getConf(), basePath);
    // load class from the payload fully qualified class name
    this.payloadClassFQN = this.hoodieTableMetaClient.getTableConfig().getPayloadClass();

    // Store merged records for all versions for this log file
    Map<String, HoodieRecord<? extends HoodieRecordPayload>> records = Maps.newHashMap();
    // iterate over the paths
    Iterator<String> logFilePathsItr = logFilePaths.iterator();
    while (logFilePathsItr.hasNext()) {
      HoodieLogFile logFile = new HoodieLogFile(new Path(logFilePathsItr.next()));
      log.info("Scanning log file " + logFile.getPath());
      totalLogFiles.incrementAndGet();
      try {
        // Use the HoodieLogFormatReader to iterate through the blocks in the log file
        HoodieLogFormatReader reader = new HoodieLogFormatReader(fs, logFile, readerSchema, true);
        while (reader.hasNext()) {
          HoodieLogBlock r = reader.next();
          String blockInstantTime = r.getLogMetadata().get(INSTANT_TIME);
          if (!HoodieTimeline.compareTimestamps(blockInstantTime, this.latestInstantTime,
              HoodieTimeline.LESSER_OR_EQUAL)) {
            //hit a block with instant time greater than should be processed, stop processing further
            break;
          }
          switch (r.getBlockType()) {
            case AVRO_DATA_BLOCK:
              log.info("Reading a data block from file " + logFile.getPath());
              // If this is a avro data block, then merge the last block records into the main result
              merge(records, lastBlocks);
              // store the last block
              lastBlocks.push(r);
              break;
            case DELETE_BLOCK:
              log.info("Reading a delete block from file " + logFile.getPath());
              String lastBlockInstantTime = lastBlocks.peek().getLogMetadata().get(INSTANT_TIME);
              if (!lastBlockInstantTime.equals(blockInstantTime)) {
                // Block with the keys listed as to be deleted, data and delete blocks written in different batches
                // so it is safe to merge
                // This is a delete block, so lets merge any records from previous data block
                merge(records, lastBlocks);
              }
              // store deletes so can be rolled back
              lastBlocks.push(r);
              break;
            case COMMAND_BLOCK:
              log.info("Reading a command block from file " + logFile.getPath());
              // This is a command block - take appropriate action based on the command
              HoodieCommandBlock commandBlock = (HoodieCommandBlock) r;
              String targetInstantForCommandBlock = r.getLogMetadata()
                  .get(HoodieLogBlock.LogMetadataType.TARGET_INSTANT_TIME);
              switch (commandBlock.getType()) { // there can be different types of command blocks
                case ROLLBACK_PREVIOUS_BLOCK:
                  // Rollback the last read log block
                  // Get commit time from last record block, compare with targetCommitTime, rollback only if equal,
                  // this is required in scenarios of invalid/extra rollback blocks written due to failures during
                  // the rollback operation itself
                  HoodieLogBlock lastBlock = lastBlocks.peek();
                  if (lastBlock != null && lastBlock.getBlockType() != CORRUPT_BLOCK &&
                      targetInstantForCommandBlock
                          .contentEquals(lastBlock.getLogMetadata().get(INSTANT_TIME))) {
                    log.info("Rolling back the last log block read in " + logFile.getPath());
                    lastBlocks.pop();
                  } else if (lastBlock != null && lastBlock.getBlockType() == CORRUPT_BLOCK) {
                    // handle corrupt blocks separately since they may not have metadata
                    log.info(
                        "Rolling back the last corrupted log block read in " + logFile.getPath());
                    lastBlocks.pop();
                  } else {
                    log.warn("Invalid or extra rollback command block in " + logFile.getPath());
                  }
                  break;
              }
              break;
            case CORRUPT_BLOCK:
              log.info("Found a corrupt block in " + logFile.getPath());
              // If there is a corrupt block - we will assume that this was the next data block
              lastBlocks.push(r);
              break;
          }
        }

      } catch (IOException e) {
        throw new HoodieIOException("IOException when reading log file " + logFile);
      }
      // merge the last read block when all the blocks are done reading
      if (!lastBlocks.isEmpty()) {
        log.info("Merging the final data blocks in " + logFile.getPath());
        merge(records, lastBlocks);
      }
    }
    this.logRecords = Collections.unmodifiableCollection(records.values());
    this.totalRecordsToUpdate = records.size();
  }

  /**
   * Iterate over the GenericRecord in the block, read the hoodie key and partition path and merge
   * with the application specific payload if the same key was found before Sufficient to just merge
   * the log records since the base data is merged on previous compaction
   */
  private Map<String, HoodieRecord<? extends HoodieRecordPayload>> loadRecordsFromBlock(
      HoodieAvroDataBlock dataBlock) {
    Map<String, HoodieRecord<? extends HoodieRecordPayload>> recordsFromLastBlock = Maps
        .newHashMap();
    List<IndexedRecord> recs = dataBlock.getRecords();
    totalLogRecords.addAndGet(recs.size());
    recs.forEach(rec -> {
      String key = ((GenericRecord) rec).get(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          .toString();
      String partitionPath =
          ((GenericRecord) rec).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
              .toString();
      HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieRecord<>(
          new HoodieKey(key, partitionPath),
          ReflectionUtils
              .loadPayload(this.payloadClassFQN, new Object[]{Optional.of(rec)}, Optional.class));
      if (recordsFromLastBlock.containsKey(key)) {
        // Merge and store the merged record
        HoodieRecordPayload combinedValue = recordsFromLastBlock.get(key).getData()
            .preCombine(hoodieRecord.getData());
        recordsFromLastBlock
            .put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()),
                combinedValue));
      } else {
        // Put the record as is
        recordsFromLastBlock.put(key, hoodieRecord);
      }
    });
    return recordsFromLastBlock;
  }

  /**
   * Merge the last seen log blocks with the accumulated records
   */
  private void merge(Map<String, HoodieRecord<? extends HoodieRecordPayload>> records,
      Deque<HoodieLogBlock> lastBlocks) {
    while (!lastBlocks.isEmpty()) {
      HoodieLogBlock lastBlock = lastBlocks.pop();
      switch (lastBlock.getBlockType()) {
        case AVRO_DATA_BLOCK:
          merge(records, loadRecordsFromBlock((HoodieAvroDataBlock) lastBlock));
          break;
        case DELETE_BLOCK:
          Arrays.stream(((HoodieDeleteBlock) lastBlock).getKeysToDelete()).forEach(records::remove);
          break;
        case CORRUPT_BLOCK:
          break;
      }
    }
  }

  /**
   * Merge the records read from a single data block with the accumulated records
   */
  private void merge(Map<String, HoodieRecord<? extends HoodieRecordPayload>> records,
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> recordsFromLastBlock) {
    recordsFromLastBlock.forEach((key, hoodieRecord) -> {
      if (records.containsKey(key)) {
        // Merge and store the merged record
        HoodieRecordPayload combinedValue = records.get(key).getData()
            .preCombine(hoodieRecord.getData());
        records.put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()),
            combinedValue));
      } else {
        // Put the record as is
        records.put(key, hoodieRecord);
      }
    });
  }

  @Override
  public Iterator<HoodieRecord<? extends HoodieRecordPayload>> iterator() {
    return logRecords.iterator();
  }

  public long getTotalLogFiles() {
    return totalLogFiles.get();
  }

  public long getTotalLogRecords() {
    return totalLogRecords.get();
  }

  public long getTotalRecordsToUpdate() {
    return totalRecordsToUpdate;
  }
}

