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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
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
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
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

import static org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME;
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
  private InternalSchema internalSchema;
  // Hoodie table path.
  private final String path;
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
  private int totalScannedLogFiles;
  // Progress
  private float progress = 0.0f;
  // Partition name
  private Option<String> partitionName;
  // Populate meta fields for the records
  private boolean populateMetaFields = true;

  protected AbstractHoodieLogRecordReader(FileSystem fs, String basePath, List<String> logFilePaths,
                                          Schema readerSchema, String latestInstantTime,
                                          int bufferSize, Option<InstantRange> instantRange,
                                          boolean withOperationField) {
    this(fs, basePath, logFilePaths, readerSchema, latestInstantTime, bufferSize,
        instantRange, withOperationField, true, Option.empty(), InternalSchema.getEmptyInternalSchema());
  }

  protected AbstractHoodieLogRecordReader(FileSystem fs, String basePath, List<String> logFilePaths,
                                          Schema readerSchema, String latestInstantTime, int bufferSize,
                                          Option<InstantRange> instantRange, boolean withOperationField,
                                          boolean forceFullScan, Option<String> partitionName,
                                          InternalSchema internalSchema) {
    this.readerSchema = readerSchema;
    this.latestInstantTime = latestInstantTime;
    this.hoodieTableMetaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).build();
    // load class from the payload fully qualified class name
    HoodieTableConfig tableConfig = this.hoodieTableMetaClient.getTableConfig();
    this.payloadClassFQN = tableConfig.getPayloadClass();
    this.preCombineField = tableConfig.getPreCombineField();
    this.totalLogFiles.addAndGet(logFilePaths.size());
    this.logFilePaths = logFilePaths;
    this.fs = fs;
    this.bufferSize = bufferSize;
    this.instantRange = instantRange;
    this.withOperationField = withOperationField;
    this.forceFullScan = forceFullScan;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    this.path = basePath;

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

  public synchronized void scan() {
    scanInternal(Option.empty());
  }

  public synchronized void scan(List<String> keys) {
    scanInternal(Option.of(new KeySpec(keys, true)));
  }

  protected synchronized void scanInternal(Option<KeySpec> keySpecOpt) {
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
      boolean enableRecordLookups = !forceFullScan;
      logFormatReaderWrapper = new HoodieLogFormatReader(fs,
          logFilePaths.stream().map(logFile -> new HoodieLogFile(new Path(logFile))).collect(Collectors.toList()),
          readerSchema, bufferSize, enableRecordLookups, keyField, internalSchema);

      /**
       * Scanning log blocks require two traversals on the log blocks.
       * First traversal to identify the rollback blocks and
       *
       *  Scanning blocks is easy to do in single writer mode, where the rollback block is right after the effected data blocks.
       *  With multiwriter mode the blocks can be out of sync. An example scenario.
       *  B1, B2, B3, B4, R1(B3), B5
       *  In this case, rollback block R1 is invalidating the B3 which is not the previous block.
       *  This becomes more complicated if we have compacted blocks, which are data blocks created using log compaction.
       *  TODO: Include support for log compacted blocks. https://issues.apache.org/jira/browse/HUDI-3580
       *
       *  To solve this need to do traversal twice.
       *  In first traversal, collect all the valid data and delete blocks that are not corrupted along with the rollback block's target instant times.
       *  For second traversal, traverse on the collected data blocks by considering the rollback instants.
       */

      // Collect targetRollbackInstants, using which we can determine which blocks are invalid.
      Set<String> targetRollbackInstants = new HashSet<>();
      // This will only contain data and delete blocks, corrupt blocks will be ignored and
      // target instants from rollback block are collected in targetRolbackInstants set.
      List<HoodieLogBlock> dataAndDeleteBlocks = new ArrayList<>();

      Set<HoodieLogFile> scannedLogFiles = new HashSet<>();

      // Do a forward traversal for all files and blocks.
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

        // First traversal to collect data and delete blocks and rollback block's target instant times.
        switch (logBlock.getBlockType()) {
          case HFILE_DATA_BLOCK:
          case AVRO_DATA_BLOCK:
          case PARQUET_DATA_BLOCK:
          case DELETE_BLOCK:
            dataAndDeleteBlocks.add(logBlock);
            break;
          case COMMAND_BLOCK:
            LOG.info("Reading a command block from file " + logFile.getPath());
            // This is a command block - take appropriate action based on the command
            HoodieCommandBlock commandBlock = (HoodieCommandBlock) logBlock;
            if (commandBlock.getType().equals(ROLLBACK_BLOCK)) {
              totalRollbacks.incrementAndGet();
              String targetInstantForCommandBlock =
                  logBlock.getLogBlockHeader().get(TARGET_INSTANT_TIME);
              // Rollback blocks contain information of instants that are failed, collect them in a set..
              targetRollbackInstants.add(targetInstantForCommandBlock);
            } else {
              throw new UnsupportedOperationException("Command type not yet supported.");
            }
            break;
          case CORRUPT_BLOCK:
            LOG.info("Found a corrupt block in " + logFile.getPath());
            totalCorruptBlocks.incrementAndGet();
            break;
          default:
            throw new UnsupportedOperationException("Block type not yet supported.");
        }
      }

      int numBlocksRolledBack = 0;
      // Second traversal to filter out the blocks whose block instant times are part of targetRollbackInstants set.
      for (HoodieLogBlock logBlock : dataAndDeleteBlocks) {
        String blockInstantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);

        // Exclude the blocks for rollback blocks exist.
        // Here, rollback can include instants affiliated to deltacommits or log compaction commits.
        if (targetRollbackInstants.contains(blockInstantTime)) {
          numBlocksRolledBack++;
          continue;
        }
        currentInstantLogBlocks.push(logBlock);
      }
      LOG.info("Number of applied rollback blocks " + numBlocksRolledBack);

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

  /**
   * Iterate over the GenericRecord in the block, read the hoodie key and partition path and call subclass processors to
   * handle it.
   */
  private void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws Exception {
    try (ClosableIterator<IndexedRecord> recordIterator = getRecordsIterator(dataBlock, keySpecOpt)) {
      Option<Schema> schemaOption = getMergedSchema(dataBlock);
      while (recordIterator.hasNext()) {
        IndexedRecord currentRecord = recordIterator.next();
        IndexedRecord record = schemaOption.isPresent() ? HoodieAvroUtils.rewriteRecordWithNewSchema(currentRecord, schemaOption.get()) : currentRecord;
        processNextRecord(createHoodieRecord(record, this.hoodieTableMetaClient.getTableConfig(), this.payloadClassFQN,
            this.preCombineField, this.withOperationField, this.simpleKeyGenFields, this.partitionName));
        totalLogRecords.incrementAndGet();
      }
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
  private Option<Schema> getMergedSchema(HoodieDataBlock dataBlock) {
    Option<Schema> result = Option.empty();
    if (!internalSchema.isEmptySchema()) {
      Long currentInstantTime = Long.parseLong(dataBlock.getLogBlockHeader().get(INSTANT_TIME));
      InternalSchema fileSchema = InternalSchemaCache
          .searchSchemaAndCache(currentInstantTime, hoodieTableMetaClient, false);
      Schema mergeSchema = AvroInternalSchemaConverter
          .convert(new InternalSchemaMerger(fileSchema, internalSchema, true, false).mergeSchema(), readerSchema.getName());
      result = Option.of(mergeSchema);
    }
    return result;
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
  protected HoodieAvroRecord<?> createHoodieRecord(final IndexedRecord rec, final HoodieTableConfig hoodieTableConfig,
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
          processDataBlock((HoodieAvroDataBlock) lastBlock, keySpecOpt);
          break;
        case HFILE_DATA_BLOCK:
          processDataBlock((HoodieHFileDataBlock) lastBlock, keySpecOpt);
          break;
        case PARQUET_DATA_BLOCK:
          processDataBlock((HoodieParquetDataBlock) lastBlock, keySpecOpt);
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
    progress = numLogFilesSeen - 1 / logFilePaths.size();
  }

  private ClosableIterator<IndexedRecord> getRecordsIterator(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    if (keySpecOpt.isPresent()) {
      KeySpec keySpec = keySpecOpt.get();
      return dataBlock.getRecordIterator(keySpec.keys, keySpec.fullKey);
    }

    return dataBlock.getRecordIterator();
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

  public Option<String> getPartitionName() {
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

  protected static class KeySpec {
    private final List<String> keys;
    private final boolean fullKey;

    public KeySpec(List<String> keys, boolean fullKey) {
      this.keys = keys;
      this.fullKey = fullKey;
    }
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
