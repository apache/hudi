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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodieFileGroupRecordBuffer;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.CachingPath;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.BLOCK_IDENTIFIER;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK;

public class HoodieUnMergedLogRecordReader<T> extends BaseHoodieLogRecordReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieUnMergedLogRecordReader.class);
  private HoodieUnMergedLogRecordReader(
      HoodieReaderContext<T> readerContext,
      FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
      String latestInstantTime, boolean readBlocksLazily,
      boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
      boolean withOperationField, boolean forceFullScan,
      Option<String> partitionName,
      InternalSchema internalSchema,
      Option<String> keyFieldOverride,
      boolean enableOptimizedLogBlocksScan,
      HoodieRecordMerger recordMerger,
      HoodieFileGroupRecordBuffer<T> recordBuffer) {
    super(
        readerContext, fs, basePath, logFilePaths, readerSchema,
        latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName,
        internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan,
        recordMerger, recordBuffer);

    this.fs = fs;
    this.bufferSize = bufferSize;
    this.instantRange = instantRange;
    this.internalSchema = internalSchema;
    this.recordKeyField = keyFieldOverride.get();
  }

  private Deque<HoodieLogBlock> currentInstantLogBlocks = new ArrayDeque<>();
  List<HoodieLogBlock> validLogBlockInstants = new ArrayList<>();
  Map<String, Map<Long, List<Pair<Integer, HoodieLogBlock>>>> blockSequenceMapPerCommit = new HashMap<>();

  float progress = 0.0f;
  AtomicLong totalLogFiles = new AtomicLong(0);
  AtomicLong totalRollbacks = new AtomicLong(0);
  AtomicLong totalCorruptBlocks = new AtomicLong(0);
  AtomicLong totalLogBlocks = new AtomicLong(0);
  AtomicLong totalLogRecords = new AtomicLong(0);
  HoodieLogFormatReverseReader logFormatReaderWrapper = null;
  HoodieTimeline commitsTimeline = this.hoodieTableMetaClient.getCommitsTimeline();
  HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
  HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();
  final FileSystem fs;
  final int bufferSize;
  final Option<InstantRange> instantRange;
  final String recordKeyField;

  HoodieLogBlock currentBlock;
  InternalSchema internalSchema;
  Set<HoodieLogFile> scannedLogFiles = new HashSet<>();

  public void initialize() throws IOException {
    logFormatReaderWrapper = new HoodieLogFormatReverseReader(
        fs,
        logFilePaths.stream().map(logFile -> new HoodieLogFile(new CachingPath(logFile))).collect(Collectors.toList()),
        readerSchema, true, false, bufferSize, shouldLookupRecords(), recordKeyField, internalSchema);
    currentBlock = null;
  }

  public void close() throws IOException {
    if (currentBlock != null) {
      currentBlock = null;
    }
    if (logFormatReaderWrapper != null) {
      logFormatReaderWrapper.close();
      logFormatReaderWrapper = null;
    }
  }

  public void fillBuffer() {
    try {
      if (!currentInstantLogBlocks.isEmpty()) {
        currentBlock = currentInstantLogBlocks.getFirst();
        processCurrentBlock();
        currentBlock = null;
        return;
      }

      while (logFormatReaderWrapper.hasNext()) {
        HoodieLogFile logFile = logFormatReaderWrapper.getLogFile();

        // Read one log file at a time to reduce memory footprint.
        if (scannedLogFiles.isEmpty()) {
          scannedLogFiles.add(logFile);
        } else if (!scannedLogFiles.contains(logFile)) {
          scannedLogFiles.add(logFile);
          return;
        }

        HoodieLogBlock logBlock = logFormatReaderWrapper.next();
        final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);
        final String blockSequenceNumberStr = logBlock.getLogBlockHeader().getOrDefault(BLOCK_IDENTIFIER, "");
        int blockSeqNo = -1;
        long attemptNo = -1L;
        if (!StringUtils.isNullOrEmpty(blockSequenceNumberStr)) {
          String[] parts = blockSequenceNumberStr.split(",");
          attemptNo = Long.parseLong(parts[0]);
          blockSeqNo = Integer.parseInt(parts[1]);
        }
        totalLogBlocks.incrementAndGet();

        if (logBlock.getBlockType() != CORRUPT_BLOCK
            && !HoodieTimeline.compareTimestamps(
                logBlock.getLogBlockHeader().get(INSTANT_TIME),
            HoodieTimeline.LESSER_THAN_OR_EQUALS, this.latestInstantTime)
        ) {
          // hit a block with instant time greater than should be processed, stop processing further
          continue;
        }

        if (logBlock.getBlockType() != CORRUPT_BLOCK && logBlock.getBlockType() != COMMAND_BLOCK) {
          if (!completedInstantsTimeline.containsOrBeforeTimelineStarts(instantTime)
              || inflightInstantsTimeline.containsInstant(instantTime)) {
            // hit an uncommitted block possibly from a failed write, move to the next one and skip processing this one
            continue;
          }
          if (instantRange.isPresent() && !instantRange.get().isInRange(instantTime)) {
            // filter the log block by instant range
            continue;
          }
        }

        switch (logBlock.getBlockType()) {
          case HFILE_DATA_BLOCK:
          case AVRO_DATA_BLOCK:
          case PARQUET_DATA_BLOCK:
            currentInstantLogBlocks.push(logBlock);
            validLogBlockInstants.add(logBlock);
            updateBlockSequenceTracker(logBlock, instantTime, blockSeqNo, attemptNo, blockSequenceMapPerCommit);
            break;

          case COMMAND_BLOCK:
            // Consider the following scenario
            // (Time 0, C1, Task T1) -> Running
            // (Time 1, C1, Task T1) -> Failed (Wrote either a corrupt block or a correct
            // DataBlock (B1) with commitTime C1
            // (Time 2, C1, Task T1.2) -> Running (Task T1 was retried and the attempt number is 2)
            // (Time 3, C1, Task T1.2) -> Finished (Wrote a correct DataBlock B2)
            // Now a logFile L1 can have 2 correct Datablocks (B1 and B2) which are the same.
            // Say, commit C1 eventually failed and a rollback is triggered.
            // Rollback will write only 1 rollback block (R1) since it assumes one block is
            // written per ingestion batch for a file but in reality we need to rollback (B1 & B2)
            // The following code ensures the same rollback block (R1) is used to rollback
            // both B1 & B2
            // This is a command block - take appropriate action based on the command
            HoodieCommandBlock commandBlock = (HoodieCommandBlock) logBlock;
            String targetInstantForCommandBlock =
                logBlock.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME);
            switch (commandBlock.getType()) { // there can be different types of command blocks
              case ROLLBACK_BLOCK:
                // Rollback older read log block(s)
                // Get commit time from older record blocks, compare with targetCommitTime,
                // rollback only if equal, this is required in scenarios of invalid/extra
                // rollback blocks written due to failures during the rollback operation itself
                // and ensures the same rollback block (R1) is used to rollback both B1 & B2 with
                // same instant_time.
                final int instantLogBlockSizeBeforeRollback = currentInstantLogBlocks.size();
                currentInstantLogBlocks.removeIf(block -> {
                  // handle corrupt blocks separately since they may not have metadata
                  if (block.getBlockType() == CORRUPT_BLOCK) {
                    return true;
                  }
                  if (targetInstantForCommandBlock.contentEquals(block.getLogBlockHeader().get(INSTANT_TIME))) {
                    // rollback older data block or delete block
                    return true;
                  }
                  return false;
                });

                // remove entire entry from blockSequenceTracker
                blockSequenceMapPerCommit.remove(targetInstantForCommandBlock);

                /// remove all matching log blocks from valid list tracked so far
                validLogBlockInstants = validLogBlockInstants.stream().filter(block -> {
                  // handle corrupt blocks separately since they may not have metadata
                  if (block.getBlockType() == CORRUPT_BLOCK) {
                    return true;
                  }
                  if (targetInstantForCommandBlock.contentEquals(block.getLogBlockHeader().get(INSTANT_TIME))) {
                    // rollback older data block or delete block
                    return false;
                  }
                  return true;
                }).collect(Collectors.toList());

                final int numBlocksRolledBack = instantLogBlockSizeBeforeRollback - currentInstantLogBlocks.size();
                totalRollbacks.addAndGet(numBlocksRolledBack);
                if (numBlocksRolledBack == 0) {
                  LOG.warn(String.format("TargetInstantTime %s invalid or extra rollback command block in %s",
                      targetInstantForCommandBlock, logFile.getPath()));
                }
                break;
              default:
                throw new UnsupportedOperationException("Command type not yet supported.");
            }
            break;
          case CORRUPT_BLOCK:
            LOG.info("Found a corrupt block in " + logFile.getPath());
            totalCorruptBlocks.incrementAndGet();
            // If there is a corrupt block - we will assume that this was the next data block
            currentInstantLogBlocks.push(logBlock);
            validLogBlockInstants.add(logBlock);
            // we don't need to update the block sequence tracker here, since the block sequence tracker is meant to remove additional/spurious valid logblocks.
            // anyway, contents of corrupt blocks are not read.
            break;
          default:
            throw new UnsupportedOperationException("Block type not supported yet");
        }

        if (!currentInstantLogBlocks.isEmpty()) {
          // TODO: Need to add dedup logic if needed.
          currentBlock = currentInstantLogBlocks.getFirst();
          processCurrentBlock();
          currentBlock = null;
          return;
        }
      }
    } catch (Exception e) {
      throw new HoodieException("Exception when reading log file ", e);
    }
  }

  public void processCurrentBlock() throws IOException {
    switch (currentBlock.getBlockType()) {
      case AVRO_DATA_BLOCK:
      case HFILE_DATA_BLOCK:
      case PARQUET_DATA_BLOCK:
        recordBuffer.processDataBlock((HoodieDataBlock) currentBlock, Option.empty());
        break;
      case DELETE_BLOCK:
        recordBuffer.processDeleteBlock((HoodieDeleteBlock) currentBlock);
        break;
      case CORRUPT_BLOCK:
        LOG.warn("Found a corrupt block which was not rolled back");
        break;
      default:
        break;
    }
  }

  public boolean hasNext() throws IOException {
    if (recordBuffer.hasNext()) {
      return true;
    }

    fillBuffer();
    return recordBuffer.hasNext();
  }

  public T next() {
    return recordBuffer.next();
  }
}
