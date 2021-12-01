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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.CORRUPT_BLOCK;

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
  private final HoodieTableMetaClient hoodieTableMetaClient;
  // Merge strategy to use when combining records from log
  private final String payloadClassFQN;
  // preCombine field
  private final String preCombineField;
  // simple key gen fields
  private Option<Pair<String, String>> simpleKeyGenFields = Option.empty();
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
  protected final boolean enableFullScan;
  private int totalScannedLogFiles;
  // Progress
  private float progress = 0.0f;
  // Partition name
  private Option<String> partitionName;
  // Populate meta fields for the records
  private boolean populateMetaFields = true;

  protected AbstractHoodieLogRecordReader(FileSystem fs, String basePath, List<String> logFilePaths,
                                          Schema readerSchema,
                                          String latestInstantTime, boolean readBlocksLazily, boolean reverseReader,
                                          int bufferSize, Option<InstantRange> instantRange,
                                          boolean withOperationField) {
    this(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, true, Option.empty());
  }

  protected AbstractHoodieLogRecordReader(FileSystem fs, String basePath, List<String> logFilePaths,
                                          Schema readerSchema, String latestInstantTime, boolean readBlocksLazily,
                                          boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                          boolean withOperationField, boolean enableFullScan,
                                          Option<String> partitionName) {
    this.readerSchema = readerSchema;
    this.latestInstantTime = latestInstantTime;
    this.hoodieTableMetaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).build();
    // load class from the payload fully qualified class name
    HoodieTableConfig tableConfig = this.hoodieTableMetaClient.getTableConfig();
    this.payloadClassFQN = tableConfig.getPayloadClass();
    this.preCombineField = tableConfig.getPreCombineField();
    this.totalLogFiles.addAndGet(logFilePaths.size());
    this.logFilePaths = logFilePaths;
    this.reverseReader = reverseReader;
    this.readBlocksLazily = readBlocksLazily;
    this.fs = fs;
    this.bufferSize = bufferSize;
    this.instantRange = instantRange;
    this.withOperationField = withOperationField;
    this.enableFullScan = enableFullScan;

    // Key fields when populate meta fields is disabled (that is, virtual keys enabled)
    if (!tableConfig.populateMetaFields()) {
      this.populateMetaFields = false;
      this.simpleKeyGenFields = Option.of(
          Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
    }
    this.partitionName = partitionName;
  }

  protected String getKeyField() {
    if (this.populateMetaFields) {
      return HoodieRecord.RECORD_KEY_METADATA_FIELD;
    }
    ValidationUtils.checkState(this.simpleKeyGenFields.isPresent());
    return this.simpleKeyGenFields.get().getKey();
  }

  public void scan() {
    scan(Option.empty());
  }

  public void scan(Option<List<String>> keys) {
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

      // Get the key field based on populate meta fields config
      // and the table type
      final String keyField = getKeyField();

      // Iterate over the paths
      logFormatReaderWrapper = new HoodieLogFormatReader(fs,
          logFilePaths.stream().map(logFile -> new HoodieLogFile(new Path(logFile))).collect(Collectors.toList()),
          readerSchema, readBlocksLazily, reverseReader, bufferSize, !enableFullScan, keyField);
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
            LOG.info("Reading a data block from file " + logFile.getPath() + " at instant "
                + logBlock.getLogBlockHeader().get(INSTANT_TIME));
            if (isNewInstantBlock(logBlock) && !readBlocksLazily) {
              // If this is an avro data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keys);
            }
            // store the current block
            currentInstantLogBlocks.push(logBlock);
            break;
          case DELETE_BLOCK:
            LOG.info("Reading a delete block from file " + logFile.getPath());
            if (isNewInstantBlock(logBlock) && !readBlocksLazily) {
              // If this is a delete data block belonging to a different commit/instant,
              // then merge the last blocks and records into the main result
              processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keys);
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
        processQueuedBlocksForInstant(currentInstantLogBlocks, scannedLogFiles.size(), keys);
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
  private void processDataBlock(HoodieDataBlock dataBlock, Option<List<String>> keys) throws Exception {
    // TODO (NA) - Implement getRecordItr() in HoodieAvroDataBlock and use that here
    List<IndexedRecord> recs = new ArrayList<>();
    if (!keys.isPresent()) {
      recs = dataBlock.getRecords();
    } else {
      recs = dataBlock.getRecords(keys.get());
    }
    totalLogRecords.addAndGet(recs.size());
    for (IndexedRecord rec : recs) {
      processNextRecord(createHoodieRecord(rec, this.hoodieTableMetaClient.getTableConfig(), this.payloadClassFQN,
          this.preCombineField, this.withOperationField, this.simpleKeyGenFields, this.partitionName));
    }
  }

  /**
   * Create @{@link HoodieRecord} from the @{@link IndexedRecord}.
   *
   * @param rec                - IndexedRecord to create the HoodieRecord from
   * @param hoodieTableConfig  - Table config
   * @param payloadClassFQN    - Payload class fully qualified name
   * @param preCombineField    - PreCombine field
   * @param withOperationField - Whether operation field is enabled
   * @param simpleKeyGenFields - Key generator fields when populate meta fields is tuened off
   * @param partitionName      - Partition name
   * @return HoodieRecord created from the IndexedRecord
   */
  protected HoodieRecord<?> createHoodieRecord(final IndexedRecord rec, final HoodieTableConfig hoodieTableConfig,
                                               final String payloadClassFQN, final String preCombineField,
                                               final boolean withOperationField,
                                               final Option<Pair<String, String>> simpleKeyGenFields,
                                               final Option<String> partitionName) {
    if (this.populateMetaFields) {
      return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) rec, payloadClassFQN,
          preCombineField, withOperationField);
    } else {
      return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) rec, payloadClassFQN,
          preCombineField, simpleKeyGenFields.get(), withOperationField, partitionName);
    }
  }

  /**
   * Process next record.
   *
   * @param hoodieRecord Hoodie Record to process
   */
  protected abstract void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) throws Exception;

  /**
   * Process next deleted key.
   *
   * @param key Deleted record key
   */
  protected abstract void processNextDeletedKey(HoodieKey key);

  /**
   * Process the set of log blocks belonging to the last instant which is read fully.
   */
  private void processQueuedBlocksForInstant(Deque<HoodieLogBlock> logBlocks, int numLogFilesSeen,
                                             Option<List<String>> keys) throws Exception {
    while (!logBlocks.isEmpty()) {
      LOG.info("Number of remaining logblocks to merge " + logBlocks.size());
      // poll the element at the bottom of the stack since that's the order it was inserted
      HoodieLogBlock lastBlock = logBlocks.pollLast();
      switch (lastBlock.getBlockType()) {
        case AVRO_DATA_BLOCK:
          processDataBlock((HoodieAvroDataBlock) lastBlock, keys);
          break;
        case HFILE_DATA_BLOCK:
          processDataBlock((HoodieHFileDataBlock) lastBlock, keys);
          break;
        case DELETE_BLOCK:
          Arrays.stream(((HoodieDeleteBlock) lastBlock).getKeysToDelete()).forEach(this::processNextDeletedKey);
          break;
        case CORRUPT_BLOCK:
          LOG.warn("Found a corrupt block which was not rolled back");
          break;
        default:
          break;
      }
    }
    // At this step the lastBlocks are consumed. We track approximate progress by number of log-files seen
    progress = numLogFilesSeen - 1 / logFilePaths.size();
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

  protected Option<String> getPartitionName() {
    return partitionName;
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

  /**
   * Builder used to build {@code AbstractHoodieLogRecordScanner}.
   */
  public abstract static class Builder {

    public abstract Builder withFileSystem(FileSystem fs);

    public abstract Builder withBasePath(String basePath);

    public abstract Builder withLogFilePaths(List<String> logFilePaths);

    public abstract Builder withReaderSchema(Schema schema);

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

    public abstract AbstractHoodieLogRecordReader build();
  }
}
