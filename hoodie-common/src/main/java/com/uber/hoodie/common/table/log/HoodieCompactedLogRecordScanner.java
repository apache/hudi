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

import com.google.common.collect.Maps;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
 * Scans through all the blocks in a list of HoodieLogFile and builds up a compacted/merged
 * list of records which will be used as a lookup table when merging the base columnar file
 * with the redo log file.
 *
 * TODO(FIX) - Does not apply application specific merge logic - defaults to HoodieAvroPayload
 */
public class HoodieCompactedLogRecordScanner implements Iterable<HoodieRecord<HoodieAvroPayload>> {
  private final static Logger log = LogManager.getLogger(HoodieCompactedLogRecordScanner.class);

  // Final list of compacted/merged records to iterate
  private final Collection<HoodieRecord<HoodieAvroPayload>> logRecords;
  // Reader schema for the records
  private final Schema readerSchema;
  // Total log files read - for metrics
  private AtomicLong totalLogFiles = new AtomicLong(0);
  // Total log records read - for metrics
  private AtomicLong totalLogRecords = new AtomicLong(0);
  // Total final list of compacted/merged records
  private long totalRecordsToUpdate;

  public HoodieCompactedLogRecordScanner(FileSystem fs, List<String> logFilePaths,
      Schema readerSchema) {
    this.readerSchema = readerSchema;

    Map<String, HoodieRecord<HoodieAvroPayload>> records = Maps.newHashMap();
    // iterate over the paths
    logFilePaths.stream().map(s -> new HoodieLogFile(new Path(s))).forEach(s -> {
      log.info("Scanning log file " + s.getPath());
      totalLogFiles.incrementAndGet();
      try {
        // Use the HoodieLogFormatReader to iterate through the blocks in the log file
        HoodieLogFormatReader reader = new HoodieLogFormatReader(fs, s, readerSchema);
        // Store the records loaded from the last data block (needed to implement rollback)
        Map<String, HoodieRecord<HoodieAvroPayload>> recordsFromLastBlock = Maps.newHashMap();
        reader.forEachRemaining(r -> {
          switch (r.getBlockType()) {
            case AVRO_DATA_BLOCK:
              log.info("Reading a data block from file " + s.getPath());
              // If this is a avro data block, then merge the last block records into the main result
              merge(records, recordsFromLastBlock);
              // Load the merged records into recordsFromLastBlock
              HoodieAvroDataBlock dataBlock = (HoodieAvroDataBlock) r;
              loadRecordsFromBlock(dataBlock, recordsFromLastBlock);
              break;
            case DELETE_BLOCK:
              log.info("Reading a delete block from file " + s.getPath());
              // This is a delete block, so lets merge any records from previous data block
              merge(records, recordsFromLastBlock);
              // Delete the keys listed as to be deleted
              HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) r;
              Arrays.stream(deleteBlock.getKeysToDelete()).forEach(records::remove);
              break;
            case COMMAND_BLOCK:
              log.info("Reading a command block from file " + s.getPath());
              // This is a command block - take appropriate action based on the command
              HoodieCommandBlock commandBlock = (HoodieCommandBlock) r;
              if (commandBlock.getType() == HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK) {
                log.info("Rolling back the last data block read in " + s.getPath());
                // rollback the last read data block
                recordsFromLastBlock.clear();
              }
              break;
            case CORRUPT_BLOCK:
              log.info("Found a corrupt block in " + s.getPath());
              // If there is a corrupt block - we will assume that this was the next data block
              // so merge the last block records (TODO - handle when the corrupted block was a tombstone written partially?)
              merge(records, recordsFromLastBlock);
              recordsFromLastBlock.clear();
              break;
          }
        });

        // merge the last read block when all the blocks are done reading
        if (!recordsFromLastBlock.isEmpty()) {
          log.info("Merging the final data block in " + s.getPath());
          merge(records, recordsFromLastBlock);
        }

      } catch (IOException e) {
        throw new HoodieIOException("IOException when reading log file " + s);
      }
    });
    this.logRecords = Collections.unmodifiableCollection(records.values());
    this.totalRecordsToUpdate = records.size();
  }

  /**
   * Iterate over the GenericRecord in the block, read the hoodie key and partition path
   * and merge with the HoodieAvroPayload if the same key was found before
   *
   * @param dataBlock
   * @param recordsFromLastBlock
   */
  private void loadRecordsFromBlock(
      HoodieAvroDataBlock dataBlock,
      Map<String, HoodieRecord<HoodieAvroPayload>> recordsFromLastBlock) {
    recordsFromLastBlock.clear();
    List<IndexedRecord> recs = dataBlock.getRecords();
    totalLogRecords.addAndGet(recs.size());
    recs.forEach(rec -> {
      String key = ((GenericRecord) rec).get(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          .toString();
      String partitionPath =
          ((GenericRecord) rec).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
              .toString();
      HoodieRecord<HoodieAvroPayload> hoodieRecord = new HoodieRecord<>(
          new HoodieKey(key, partitionPath),
          new HoodieAvroPayload(Optional.of(((GenericRecord) rec))));
      if (recordsFromLastBlock.containsKey(key)) {
        // Merge and store the merged record
        HoodieAvroPayload combinedValue = recordsFromLastBlock.get(key).getData()
            .preCombine(hoodieRecord.getData());
        recordsFromLastBlock
            .put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()),
                combinedValue));
      } else {
        // Put the record as is
        recordsFromLastBlock.put(key, hoodieRecord);
      }
    });
  }

  /**
   * Merge the records read from a single data block with the accumulated records
   *
   * @param records
   * @param recordsFromLastBlock
   */
  private void merge(Map<String, HoodieRecord<HoodieAvroPayload>> records,
      Map<String, HoodieRecord<HoodieAvroPayload>> recordsFromLastBlock) {
    recordsFromLastBlock.forEach((key, hoodieRecord) -> {
      if (records.containsKey(key)) {
        // Merge and store the merged record
        HoodieAvroPayload combinedValue = records.get(key).getData()
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
  public Iterator<HoodieRecord<HoodieAvroPayload>> iterator() {
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

