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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.ClosableIteratorWithSchema;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Implements logic to scan log blocks and expose valid and deleted log records to subclass implementation. Subclass is
 * free to either apply merging or expose raw data back to the caller.
 *
 * NOTE: If readBlockLazily is turned on, does not merge, instead keeps reading log blocks and merges everything at once
 * This is an optimization to avoid seek() back and forth to read new block (forward seek()) and lazily read content of
 * seen block (reverse and forward seek()) during merge | | Read Block 1 Metadata | | Read Block 1 Data | | | Read Block
 * 2 Metadata | | Read Block 2 Data | | I/O Pass 1 | ..................... | I/O Pass 2 | ................. | | | Read
 * Block N Metadata | | Read Block N Data |
 * <p>
 * This results in two I/O passes over the log file.
 */
public abstract class AbstractHoodieLogRecordReader {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieLogRecordReader.class);

  // Reader schema for the records
  protected final Schema readerSchema;
  // Latest valid instant time
  // Log-Blocks belonging to inflight delta-instants are filtered-out using this high-watermark.
  private final String latestInstantTime;
  protected final HoodieTableMetaClient hoodieTableMetaClient;
  // Merge strategy to use when combining records from log
  private final String payloadClassFQN;
  // Record's key/partition-path fields
  private final String recordKeyField;
  private final Option<String> partitionPathFieldOpt;
  // Partition name override
  private final Option<String> partitionNameOverrideOpt;
  // Pre-combining field
  protected final String preCombineField;
  // Stateless component for merging records
  protected final HoodieRecordMerger recordMerger;
  private final TypedProperties payloadProps;
  // Log File Paths
  protected final List<String> logFilePaths;
  // Read Lazily flag
  private final boolean readBlocksLazily;
  // Reverse reader - Not implemented yet (NA -> Why do we need ?)
  // but present here for plumbing for future implementation
  private final boolean reverseReader;
  // Buffer Size for log file reader
  private final int bufferSize;
  // optional instant range for incremental block filtering
  private final Option<InstantRange> instantRange;
  // Read the operation metadata field from the avro record
  private final boolean withOperationField;
  // FileSystem
  private final FileSystem fs;
  // Total log files read - for metrics
  private AtomicLong totalLogFiles = new AtomicLong(0);
  // Internal schema, used to support full schema evolution.
  private final InternalSchema internalSchema;
  // Total log blocks read - for metrics
  private AtomicLong totalLogBlocks = new AtomicLong(0);
  // Total log records read - for metrics
  private AtomicLong totalLogRecords = new AtomicLong(0);
  // Total number of rollbacks written across all log files
  private AtomicLong totalRollbacks = new AtomicLong(0);
  // Total number of corrupt blocks written across all log files
  private AtomicLong totalCorruptBlocks = new AtomicLong(0);
  // Store the last instant log blocks (needed to implement rollback)
  private Deque<HoodieLogBlock> currentInstantLogBlocks = new ArrayDeque<>();
  // Enables full scan of log records
  protected final boolean forceFullScan;
  // Progress
  private float progress = 0.0f;
  // Populate meta fields for the records
  private final boolean populateMetaFields;
  // Record type read from log block
  protected final HoodieRecordType recordType;
  // Collect all the block instants after scanning all the log files.
  private final List<String> validBlockInstants = new ArrayList<>();
  // Use scanV2 method.
  private final boolean enableOptimizedLogBlocksScan;

  protected AbstractHoodieLogRecordReader(FileSystem fs, String basePath, List<String> logFilePaths,
                                          Schema readerSchema, String latestInstantTime, boolean readBlocksLazily,
                                          boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                          boolean withOperationField, boolean forceFullScan,
                                          Option<String> partitionNameOverride,
                                          InternalSchema internalSchema,
                                          Option<String> keyFieldOverride,
                                          boolean enableOptimizedLogBlocksScan,
                                          HoodieRecordMerger recordMerger) {
    this.readerSchema = readerSchema;
    this.latestInstantTime = latestInstantTime;
    this.hoodieTableMetaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).build();
    // load class from the payload fully qualified class name
    HoodieTableConfig tableConfig = this.hoodieTableMetaClient.getTableConfig();
    this.payloadClassFQN = tableConfig.getPayloadClass();
    this.preCombineField = tableConfig.getPreCombineField();
    // Log scanner merge log with precombine
    TypedProperties props = HoodieAvroRecordMerger.Config.withLegacyOperatingModePreCombining(new Properties());
    if (this.preCombineField != null) {
      props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, this.preCombineField);
    }
    this.payloadProps = props;
    this.recordMerger = recordMerger;
    this.totalLogFiles.addAndGet(logFilePaths.size());
    this.logFilePaths = logFilePaths;
    this.reverseReader = reverseReader;
    this.readBlocksLazily = readBlocksLazily;
    this.fs = fs;
    this.bufferSize = bufferSize;
    this.instantRange = instantRange;
    this.withOperationField = withOperationField;
    this.forceFullScan = forceFullScan;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;

    if (keyFieldOverride.isPresent()) {
      // NOTE: This branch specifically is leveraged handling Metadata Table
      //       log-block merging sequence. Here we do
      //         - Override the record-key field (which isn't configured t/h table-config)
      //         - Override partition-path value w/ static "partition-name" (in MT all partitions
      //         are static, like "files", "col_stats", etc)
      checkState(partitionNameOverride.isPresent());

      this.populateMetaFields = false;
      this.recordKeyField = keyFieldOverride.get();
      this.partitionPathFieldOpt = Option.empty();
    } else if (tableConfig.populateMetaFields()) {
      this.populateMetaFields = true;
      this.recordKeyField = HoodieRecord.RECORD_KEY_METADATA_FIELD;
      this.partitionPathFieldOpt = Option.of(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    } else {
      this.populateMetaFields = false;
      this.recordKeyField = tableConfig.getRecordKeyFieldProp();
      this.partitionPathFieldOpt = Option.of(tableConfig.getPartitionFieldProp());
    }

    this.partitionNameOverrideOpt = partitionNameOverride;
    this.recordType = recordMerger.getRecordType();
  }

  /**
   * @param keySpecOpt specifies target set of keys to be scanned
   * @param skipProcessingBlocks controls, whether (delta) blocks have to actually be processed
   */
  protected final void scanInternal(Option<KeySpec> keySpecOpt, boolean skipProcessingBlocks) {
    synchronized (this) {
      if (enableOptimizedLogBlocksScan) {
        scanInternalV2(keySpecOpt, skipProcessingBlocks);
      } else {
        scanInternalV1(keySpecOpt);
      }
    }
  }

  private void scanInternalV1(Option<KeySpec> keySpecOpt) {
    currentInstantLogBlocks = new ArrayDeque<>();
    progress = 0.0f;
    totalLogFiles = new AtomicLong(0);
    totalRollbacks = new AtomicLong(0);
    totalCorruptBlocks = new AtomicLong(0);
    totalLogBlocks = new AtomicLong(0);
    totalLogRecords = new AtomicLong(0);
    HoodieLogFormatReader logFormatReaderWrapper = null;
    HoodieTimeline commitsTimeline = this.hoodieTableMetaClient.getCommitsTimeline();
    HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
    HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();
    try {
      // Iterate over the paths
      logFormatReaderWrapper = new HoodieLogFormatReader(fs,
          logFilePaths.stream().map(logFile -> new HoodieLogFile(new Path(logFile))).collect(Collectors.toList()),
          readerSchema, readBlocksLazily, reverseReader, bufferSize, shouldLookupRecords(), recordKeyField, internalSchema);

      Set<HoodieLogFile> scannedLogFiles = new HashSet<>();
      while (logFormatReaderWrapper.hasNext()) {
        HoodieLogFile logFile = logFormatReaderWrapper.getLogFile();
        LOG.info("Scanning log file " + logFile);
        scannedLogFiles.add(logFile);
        totalLogFiles.set(scannedLogFiles.size());
        // Use the HoodieLogFileReader to iterate through the blocks in the log file
        HoodieLogBlock logBlock = logFormatReaderWrapper.next();
        final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);
        totalLogBlocks.incrementAndGet();
        if (logBlock.getBlockType() != CORRUPT_BLOCK
            && !HoodieTimeline.compareTimestamps(logBlock.getLogBlockHeader().get(INSTANT_TIME), HoodieTimeline.LESSER_THAN_OR_EQUALS, this.latestInstantTime
        )) {
          // hit a block with instant time greater than should be processed, stop processing further
          break;
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
            LOG.info("Reading a data block from file " + logFile.getPath() + " at instant "
                + logBlock.getLogBlockHeader().get(INSTANT_TIME));
            if (isNewInstantBlock(logBlock) && !readBlocksLazily) {
              // If this is an avro data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keySpecOpt);
            }
            // store the current block
            currentInstantLogBlocks.push(logBlock);
            break;
          case DELETE_BLOCK:
            LOG.info("Reading a delete block from file " + logFile.getPath());
            if (isNewInstantBlock(logBlock) && !readBlocksLazily) {
              // If this is a delete data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keySpecOpt);
            }
            // store deletes so can be rolled back
            currentInstantLogBlocks.push(logBlock);
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
            LOG.info("Reading a command block from file " + logFile.getPath());
            // This is a command block - take appropriate action based on the command
            HoodieCommandBlock commandBlock = (HoodieCommandBlock) logBlock;
            String targetInstantForCommandBlock =
                logBlock.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME);
            switch (commandBlock.getType()) { // there can be different types of command blocks
              case ROLLBACK_BLOCK:
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
                    LOG.info("Rolling back the last corrupted log block read in " + logFile.getPath());
                    currentInstantLogBlocks.pop();
                    numBlocksRolledBack++;
                  } else if (targetInstantForCommandBlock.contentEquals(lastBlock.getLogBlockHeader().get(INSTANT_TIME))) {
                    // rollback last data block or delete block
                    LOG.info("Rolling back the last log block read in " + logFile.getPath());
                    currentInstantLogBlocks.pop();
                    numBlocksRolledBack++;
                  } else if (!targetInstantForCommandBlock
                      .contentEquals(currentInstantLogBlocks.peek().getLogBlockHeader().get(INSTANT_TIME))) {
                    // invalid or extra rollback block
                    LOG.warn("TargetInstantTime " + targetInstantForCommandBlock
                        + " invalid or extra rollback command block in " + logFile.getPath());
                    break;
                  } else {
                    // this should not happen ideally
                    LOG.warn("Unable to apply rollback command block in " + logFile.getPath());
                  }
                }
                LOG.info("Number of applied rollback blocks " + numBlocksRolledBack);
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
            break;
          default:
            throw new UnsupportedOperationException("Block type not supported yet");
        }
      }
      // merge the last read block when all the blocks are done reading
      if (!currentInstantLogBlocks.isEmpty()) {
        LOG.info("Merging the final data blocks");
        processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keySpecOpt);
      }
      // Done
      progress = 1.0f;
    } catch (IOException e) {
      LOG.error("Got IOException when reading log file", e);
      throw new HoodieIOException("IOException when reading log file ", e);
    } catch (Exception e) {
      LOG.error("Got exception when reading log file", e);
      throw new HoodieException("Exception when reading log file ", e);
    } finally {
      try {
        if (null != logFormatReaderWrapper) {
          logFormatReaderWrapper.close();
        }
      } catch (IOException ioe) {
        // Eat exception as we do not want to mask the original exception that can happen
        LOG.error("Unable to close log format reader", ioe);
      }
    }
  }

  private void scanInternalV2(Option<KeySpec> keySpecOption, boolean skipProcessingBlocks) {
    currentInstantLogBlocks = new ArrayDeque<>();
    progress = 0.0f;
    totalLogFiles = new AtomicLong(0);
    totalRollbacks = new AtomicLong(0);
    totalCorruptBlocks = new AtomicLong(0);
    totalLogBlocks = new AtomicLong(0);
    totalLogRecords = new AtomicLong(0);
    HoodieLogFormatReader logFormatReaderWrapper = null;
    HoodieTimeline commitsTimeline = this.hoodieTableMetaClient.getCommitsTimeline();
    HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
    HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();
    try {
      // Iterate over the paths
      logFormatReaderWrapper = new HoodieLogFormatReader(fs,
          logFilePaths.stream().map(logFile -> new HoodieLogFile(new Path(logFile))).collect(Collectors.toList()),
          readerSchema, readBlocksLazily, reverseReader, bufferSize, shouldLookupRecords(), recordKeyField, internalSchema);

      /**
       * Scanning log blocks and placing the compacted blocks at the right place require two traversals.
       * First traversal to identify the rollback blocks and valid data and compacted blocks.
       *
       * Scanning blocks is easy to do in single writer mode, where the rollback block is right after the effected data blocks.
       * With multiwriter mode the blocks can be out of sync. An example scenario.
       * B1, B2, B3, B4, R1(B3), B5
       * In this case, rollback block R1 is invalidating the B3 which is not the previous block.
       * This becomes more complicated if we have compacted blocks, which are data blocks created using log compaction.
       *
       * To solve this, run a single traversal, collect all the valid blocks that are not corrupted
       * along with the block instant times and rollback block's target instant times.
       *
       * As part of second traversal iterate block instant times in reverse order.
       * While iterating in reverse order keep a track of final compacted instant times for each block.
       * In doing so, when a data block is seen include the final compacted block if it is not already added.
       *
       * find the final compacted block which contains the merged contents.
       * For example B1 and B2 are merged and created a compacted block called M1 and now M1, B3 and B4 are merged and
       * created another compacted block called M2. So, now M2 is the final block which contains all the changes of B1,B2,B3,B4.
       * So, blockTimeToCompactionBlockTimeMap will look like
       * (B1 -> M2), (B2 -> M2), (B3 -> M2), (B4 -> M2), (M1 -> M2)
       * This map is updated while iterating and is used to place the compacted blocks in the correct position.
       * This way we can have multiple layers of merge blocks and still be able to find the correct positions of merged blocks.
       */

      // Collect targetRollbackInstants, using which we can determine which blocks are invalid.
      Set<String> targetRollbackInstants = new HashSet<>();

      // This holds block instant time to list of blocks. Note here the log blocks can be normal data blocks or compacted log blocks.
      Map<String, List<HoodieLogBlock>> instantToBlocksMap = new HashMap<>();

      // Order of Instants.
      List<String> orderedInstantsList = new ArrayList<>();

      Set<HoodieLogFile> scannedLogFiles = new HashSet<>();

      /*
       * 1. First step to traverse in forward direction. While traversing the log blocks collect following,
       *    a. instant times
       *    b. instant to logblocks map.
       *    c. targetRollbackInstants.
       */
      while (logFormatReaderWrapper.hasNext()) {
        HoodieLogFile logFile = logFormatReaderWrapper.getLogFile();
        LOG.info("Scanning log file " + logFile);
        scannedLogFiles.add(logFile);
        totalLogFiles.set(scannedLogFiles.size());
        // Use the HoodieLogFileReader to iterate through the blocks in the log file
        HoodieLogBlock logBlock = logFormatReaderWrapper.next();
        final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);
        totalLogBlocks.incrementAndGet();
        // Ignore the corrupt blocks. No further handling is required for them.
        if (logBlock.getBlockType().equals(CORRUPT_BLOCK)) {
          LOG.info("Found a corrupt block in " + logFile.getPath());
          totalCorruptBlocks.incrementAndGet();
          continue;
        }
        if (!HoodieTimeline.compareTimestamps(logBlock.getLogBlockHeader().get(INSTANT_TIME),
            HoodieTimeline.LESSER_THAN_OR_EQUALS, this.latestInstantTime)) {
          // hit a block with instant time greater than should be processed, stop processing further
          break;
        }
        if (logBlock.getBlockType() != COMMAND_BLOCK) {
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
          case DELETE_BLOCK:
            List<HoodieLogBlock> logBlocksList = instantToBlocksMap.getOrDefault(instantTime, new ArrayList<>());
            if (logBlocksList.size() == 0) {
              // Keep a track of instant Times in the order of arrival.
              orderedInstantsList.add(instantTime);
            }
            logBlocksList.add(logBlock);
            instantToBlocksMap.put(instantTime, logBlocksList);
            break;
          case COMMAND_BLOCK:
            LOG.info("Reading a command block from file " + logFile.getPath());
            // This is a command block - take appropriate action based on the command
            HoodieCommandBlock commandBlock = (HoodieCommandBlock) logBlock;

            // Rollback blocks contain information of instants that are failed, collect them in a set..
            if (commandBlock.getType().equals(ROLLBACK_BLOCK)) {
              totalRollbacks.incrementAndGet();
              String targetInstantForCommandBlock =
                  logBlock.getLogBlockHeader().get(TARGET_INSTANT_TIME);
              targetRollbackInstants.add(targetInstantForCommandBlock);
            } else {
              throw new UnsupportedOperationException("Command type not yet supported.");
            }
            break;
          default:
            throw new UnsupportedOperationException("Block type not yet supported.");
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Ordered instant times seen " + orderedInstantsList);
      }

      int numBlocksRolledBack = 0;

      // All the block's instants time that are added to the queue are collected in this set.
      Set<String> instantTimesIncluded = new HashSet<>();

      // Key will have details related to instant time and value will be empty if that instant is not compacted.
      // Ex: B1(i1), B2(i2), CB(i3,[i1,i2]) entries will be like i1 -> i3, i2 -> i3.
      Map<String, String> blockTimeToCompactionBlockTimeMap = new HashMap<>();

      /*
       * 2. Iterate the instants list in reverse order to get the latest instants first.
       *    While iterating update the blockTimeToCompactionBlockTimesMap and include the compacted blocks in right position.
       */
      for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {
        String instantTime = orderedInstantsList.get(i);

        // Exclude the blocks which are included in targetRollbackInstants set.
        // Here, rollback can include instants affiliated to deltacommits or log compaction commits.
        if (targetRollbackInstants.contains(instantTime)) {
          numBlocksRolledBack += instantToBlocksMap.get(instantTime).size();
          continue;
        }
        List<HoodieLogBlock> instantsBlocks = instantToBlocksMap.get(instantTime);
        if (instantsBlocks.size() == 0) {
          throw new HoodieException("Data corrupted while writing. Found zero blocks for an instant " + instantTime);
        }
        HoodieLogBlock firstBlock = instantsBlocks.get(0);

        // For compacted blocks COMPACTED_BLOCK_TIMES entry is present under its headers.
        if (firstBlock.getLogBlockHeader().containsKey(COMPACTED_BLOCK_TIMES)) {
          // When compacted blocks are seen update the blockTimeToCompactionBlockTimeMap.
          Arrays.stream(firstBlock.getLogBlockHeader().get(COMPACTED_BLOCK_TIMES).split(","))
              .forEach(originalInstant -> {
                String finalInstant = blockTimeToCompactionBlockTimeMap.getOrDefault(instantTime, instantTime);
                blockTimeToCompactionBlockTimeMap.put(originalInstant, finalInstant);
              });
        } else {
          // When a data block is found check if it is already compacted.
          String compactedFinalInstantTime = blockTimeToCompactionBlockTimeMap.get(instantTime);
          if (compactedFinalInstantTime == null) {
            // If it is not compacted then add the blocks related to the instant time at the end of the queue and continue.
            List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(instantTime);
            Collections.reverse(logBlocks);
            logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
            instantTimesIncluded.add(instantTime);
            validBlockInstants.add(instantTime);
            continue;
          }
          // If the compacted block exists and it is already included in the dequeue then ignore and continue.
          if (instantTimesIncluded.contains(compactedFinalInstantTime)) {
            continue;
          }
          // If the compacted block exists and it is not already added then add all the blocks related to that instant time.
          List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(compactedFinalInstantTime);
          Collections.reverse(logBlocks);
          logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
          instantTimesIncluded.add(compactedFinalInstantTime);
          validBlockInstants.add(compactedFinalInstantTime);
        }
      }
      LOG.info("Number of applied rollback blocks " + numBlocksRolledBack);

      if (LOG.isDebugEnabled()) {
        LOG.info("Final view of the Block time to compactionBlockMap " + blockTimeToCompactionBlockTimeMap);
      }

      // merge the last read block when all the blocks are done reading
      if (!currentInstantLogBlocks.isEmpty() && !skipProcessingBlocks) {
        LOG.info("Merging the final data blocks");
        processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keySpecOption);
      }
      // Done
      progress = 1.0f;
    } catch (IOException e) {
      LOG.error("Got IOException when reading log file", e);
      throw new HoodieIOException("IOException when reading log file ", e);
    } catch (Exception e) {
      LOG.error("Got exception when reading log file", e);
      throw new HoodieException("Exception when reading log file ", e);
    } finally {
      try {
        if (null != logFormatReaderWrapper) {
          logFormatReaderWrapper.close();
        }
      } catch (IOException ioe) {
        // Eat exception as we do not want to mask the original exception that can happen
        LOG.error("Unable to close log format reader", ioe);
      }
    }
  }

  /**
   * Checks if the current logblock belongs to a later instant.
   */
  private boolean isNewInstantBlock(HoodieLogBlock logBlock) {
    return currentInstantLogBlocks.size() > 0 && currentInstantLogBlocks.peek().getBlockType() != CORRUPT_BLOCK
        && !logBlock.getLogBlockHeader().get(INSTANT_TIME)
        .contentEquals(currentInstantLogBlocks.peek().getLogBlockHeader().get(INSTANT_TIME));
  }

  /**
   * Iterate over the GenericRecord in the block, read the hoodie key and partition path and call subclass processors to
   * handle it.
   */
  private void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws Exception {
    checkState(partitionNameOverrideOpt.isPresent() || partitionPathFieldOpt.isPresent(),
        "Either partition-name override or partition-path field had to be present");

    Option<Pair<String, String>> recordKeyPartitionPathFieldPair = populateMetaFields
        ? Option.empty()
        : Option.of(Pair.of(recordKeyField, partitionPathFieldOpt.orElse(null)));

    try (ClosableIteratorWithSchema<HoodieRecord> recordIterator = getRecordsIterator(dataBlock, keySpecOpt)) {
      while (recordIterator.hasNext()) {
        HoodieRecord completedRecord = recordIterator.next()
            .wrapIntoHoodieRecordPayloadWithParams(recordIterator.getSchema(),
                hoodieTableMetaClient.getTableConfig().getProps(),
                recordKeyPartitionPathFieldPair,
                this.withOperationField,
                this.partitionNameOverrideOpt,
                populateMetaFields);
        processNextRecord(completedRecord);
        totalLogRecords.incrementAndGet();
      }
    }
  }

  /**
   * Process next record.
   *
   * @param hoodieRecord Hoodie Record to process
   */
  protected abstract <T> void processNextRecord(HoodieRecord<T> hoodieRecord) throws Exception;

  /**
   * Process next deleted record.
   *
   * @param deleteRecord Deleted record(hoodie key and ordering value)
   */
  protected abstract void processNextDeletedRecord(DeleteRecord deleteRecord);

  /**
   * Process the set of log blocks belonging to the last instant which is read fully.
   */
  private void processQueuedBlocksForInstant(Deque<HoodieLogBlock> logBlocks, int numLogFilesSeen,
                                             Option<KeySpec> keySpecOpt) throws Exception {
    while (!logBlocks.isEmpty()) {
      LOG.info("Number of remaining logblocks to merge " + logBlocks.size());
      // poll the element at the bottom of the stack since that's the order it was inserted
      HoodieLogBlock lastBlock = logBlocks.pollLast();
      switch (lastBlock.getBlockType()) {
        case AVRO_DATA_BLOCK:
        case HFILE_DATA_BLOCK:
        case PARQUET_DATA_BLOCK:
          processDataBlock((HoodieDataBlock) lastBlock, keySpecOpt);
          break;
        case DELETE_BLOCK:
          Arrays.stream(((HoodieDeleteBlock) lastBlock).getRecordsToDelete()).forEach(this::processNextDeletedRecord);
          break;
        case CORRUPT_BLOCK:
          LOG.warn("Found a corrupt block which was not rolled back");
          break;
        default:
          break;
      }
    }
    // At this step the lastBlocks are consumed. We track approximate progress by number of log-files seen
    progress = (numLogFilesSeen - 1) / logFilePaths.size();
  }

  private boolean shouldLookupRecords() {
    // NOTE: Point-wise record lookups are only enabled when scanner is not in
    //       a full-scan mode
    return !forceFullScan;
  }

  /**
   * Return progress of scanning as a float between 0.0 to 1.0.
   */
  public float getProgress() {
    return progress;
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

  protected String getPayloadClassFQN() {
    return payloadClassFQN;
  }

  public Option<String> getPartitionNameOverride() {
    return partitionNameOverrideOpt;
  }

  public long getTotalRollbacks() {
    return totalRollbacks.get();
  }

  public long getTotalCorruptBlocks() {
    return totalCorruptBlocks.get();
  }

  public boolean isWithOperationField() {
    return withOperationField;
  }

  protected TypedProperties getPayloadProps() {
    return payloadProps;
  }

  /**
   * Key specification with a list of column names.
   */
  protected interface KeySpec {
    List<String> getKeys();

    boolean isFullKey();

    static KeySpec fullKeySpec(List<String> keys) {
      return new FullKeySpec(keys);
    }

    static KeySpec prefixKeySpec(List<String> keyPrefixes) {
      return new PrefixKeySpec(keyPrefixes);
    }
  }

  private static class FullKeySpec implements KeySpec {
    private final List<String> keys;
    private FullKeySpec(List<String> keys) {
      this.keys = keys;
    }

    @Override
    public List<String> getKeys() {
      return keys;
    }

    @Override
    public boolean isFullKey() {
      return true;
    }
  }

  private static class PrefixKeySpec implements KeySpec {
    private final List<String> keysPrefixes;

    private PrefixKeySpec(List<String> keysPrefixes) {
      this.keysPrefixes = keysPrefixes;
    }

    @Override
    public List<String> getKeys() {
      return keysPrefixes;
    }

    @Override
    public boolean isFullKey() {
      return false;
    }
  }

  public Deque<HoodieLogBlock> getCurrentInstantLogBlocks() {
    return currentInstantLogBlocks;
  }

  public List<String> getValidBlockInstants() {
    return validBlockInstants;
  }

  private ClosableIteratorWithSchema<HoodieRecord> getRecordsIterator(
      HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    ClosableIterator<HoodieRecord> blockRecordsIterator;
    if (keySpecOpt.isPresent()) {
      KeySpec keySpec = keySpecOpt.get();
      blockRecordsIterator = (ClosableIterator) dataBlock
          .getRecordIterator(keySpec.getKeys(), keySpec.isFullKey(), recordType);
    } else {
      blockRecordsIterator = (ClosableIterator) dataBlock.getRecordIterator(recordType);
    }

    Option<Pair<Function<HoodieRecord, HoodieRecord>, Schema>> schemaEvolutionTransformerOpt =
        composeEvolvedSchemaTransformer(dataBlock);
    // In case when schema has been evolved original persisted records will have to be
    // transformed to adhere to the new schema
    if (schemaEvolutionTransformerOpt.isPresent()) {
      return ClosableIteratorWithSchema.newInstance(
          new CloseableMappingIterator<>(blockRecordsIterator,
              schemaEvolutionTransformerOpt.get().getLeft()),
          schemaEvolutionTransformerOpt.get().getRight());
    } else {
      return ClosableIteratorWithSchema.newInstance(blockRecordsIterator, dataBlock.getSchema());
    }
  }

  /**
   * Get final Read Schema for support evolution.
   * step1: find the fileSchema for current dataBlock.
   * step2: determine whether fileSchema is compatible with the final read internalSchema.
   * step3: merge fileSchema and read internalSchema to produce final read schema.
   *
   * @param dataBlock current processed block
   * @return final read schema.
   */
  private Option<Pair<Function<HoodieRecord, HoodieRecord>, Schema>> composeEvolvedSchemaTransformer(
      HoodieDataBlock dataBlock) {
    if (internalSchema.isEmptySchema()) {
      return Option.empty();
    }

    long currentInstantTime = Long.parseLong(dataBlock.getLogBlockHeader().get(INSTANT_TIME));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(currentInstantTime,
        hoodieTableMetaClient, false);
    InternalSchema mergedInternalSchema = new InternalSchemaMerger(fileSchema, internalSchema,
        true, false).mergeSchema();
    Schema mergedAvroSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, readerSchema.getFullName());

    return Option.of(Pair.of((record) -> {
      return record.rewriteRecordWithNewSchema(
          dataBlock.getSchema(),
          this.hoodieTableMetaClient.getTableConfig().getProps(),
          mergedAvroSchema,
          Collections.emptyMap());
    }, mergedAvroSchema));
  }

  /**
   * Builder used to build {@code AbstractHoodieLogRecordScanner}.
   */
  public abstract static class Builder {

    public abstract Builder withFileSystem(FileSystem fs);

    public abstract Builder withBasePath(String basePath);

    public abstract Builder withLogFilePaths(List<String> logFilePaths);

    public abstract Builder withReaderSchema(Schema schema);

    public abstract Builder withInternalSchema(InternalSchema internalSchema);

    public abstract Builder withLatestInstantTime(String latestInstantTime);

    public abstract Builder withReadBlocksLazily(boolean readBlocksLazily);

    public abstract Builder withReverseReader(boolean reverseReader);

    public abstract Builder withBufferSize(int bufferSize);

    public Builder withPartition(String partitionName) {
      throw new UnsupportedOperationException();
    }

    public Builder withInstantRange(Option<InstantRange> instantRange) {
      throw new UnsupportedOperationException();
    }

    public Builder withOperationField(boolean withOperationField) {
      throw new UnsupportedOperationException();
    }

    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      throw new UnsupportedOperationException();
    }

    public Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      throw new UnsupportedOperationException();
    }

    public abstract AbstractHoodieLogRecordReader build();
  }
}
