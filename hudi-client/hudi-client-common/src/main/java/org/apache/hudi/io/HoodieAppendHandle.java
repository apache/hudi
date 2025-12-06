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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.collectColumnRangeMetadata;

/**
 * IO Operation to append data onto an existing file.
 */
public class HoodieAppendHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAppendHandle.class);
  // This acts as the sequenceID for records written
  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);
  private static final int NUMBER_OF_RECORDS_TO_ESTIMATE_RECORD_SIZE = 100;

  // Buffer for holding records in memory before they are flushed to disk
  protected final List<HoodieRecord> recordList = new ArrayList<>();
  // Buffer for holding records (to be deleted), along with their position in log block, in memory before they are flushed to disk
  protected final List<Pair<DeleteRecord, Long>> recordsToDeleteWithPositions = new ArrayList<>();
  // Base file instant time of the record positions
  protected final Option<String> baseFileInstantTimeOfPositions;
  // Incoming records to be written to logs.
  protected Iterator<HoodieRecord<T>> recordItr;
  // Writer to log into the file group's latest slice.
  protected Writer writer;

  protected final List<WriteStatus> statuses;
  // Total number of records written during appending
  protected long recordsWritten = 0;
  // Total number of records deleted during appending
  protected long recordsDeleted = 0;
  // Total number of records updated during appending
  protected long updatedRecordsWritten = 0;
  // Total number of new records inserted into the delta file
  protected long insertRecordsWritten = 0;

  // Average record size for a HoodieRecord. This size is updated at the end of every log block flushed to disk
  private long averageRecordSize = 0;
  // Flag used to initialize some metadata
  private boolean doInit = true;
  // Total number of bytes written during this append phase (an estimation)
  protected long estimatedNumberOfBytesWritten;
  // Number of records that must be written to meet the max block size for a log block
  private long numberOfRecords = 0;
  // Max block size to limit to for a log block
  private final long maxBlockSize = config.getLogFileDataBlockMaxSize();
  // Header metadata for a log block
  protected final Map<HeaderMetadataType, String> header = new HashMap<>();
  private final SizeEstimator<HoodieRecord> sizeEstimator;
  // This is used to distinguish between normal append and logcompaction's append operation.
  private boolean isLogCompaction = false;
  // use writer schema for log compaction.
  private boolean useWriterSchema = false;

  private final Properties recordProperties = new Properties();
  private final String[] orderingFields;

  /**
   * This is used by log compaction only.
   */
  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                            TaskContextSupplier taskContextSupplier, Map<HeaderMetadataType, String> header) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, true);
    this.useWriterSchema = true;
    this.isLogCompaction = true;
    this.header.putAll(header);
  }

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, false);
  }

  private HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                             String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable,
        config.shouldWritePartialUpdates()
            // When enabling writing partial updates to the data blocks in log files,
            // i.e., partial update schema is set, the writer schema is the partial
            // schema containing the updated fields only
            ? Option.of(HoodieSchema.parse(config.getPartialUpdateSchema()))
            : Option.empty(),
        taskContextSupplier,
        preserveMetadata);
    this.recordItr = recordItr;
    this.sizeEstimator = getSizeEstimator();
    this.statuses = new ArrayList<>();
    this.recordProperties.putAll(config.getProps());
    this.orderingFields = ConfigUtils.getOrderingFields(recordProperties);
    boolean shouldWriteRecordPositions = config.shouldWriteRecordPositions()
        // record positions supported only from table version 8
        && config.getWriteVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT);
    this.baseFileInstantTimeOfPositions = shouldWriteRecordPositions
        ? getBaseFileInstantTimeOfPositions()
        : Option.empty();
  }

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier sparkTaskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, sparkTaskContextSupplier);
  }

  protected SizeEstimator<HoodieRecord> getSizeEstimator() {
    return new DefaultSizeEstimator<>();
  }

  protected Option<String> getBaseFileInstantTimeOfPositions() {
    return hoodieTable.getHoodieView().getLatestBaseFile(partitionPath, fileId)
        .map(HoodieBaseFile::getCommitTime);
  }

  private Option<FileSlice> populateWriteStatAndFetchFileSlice(HoodieRecord record, HoodieDeltaWriteStat deltaWriteStat) {
    HoodieTableVersion tableVersion = hoodieTable.version();
    String prevCommit;
    String baseFile = "";
    List<String> logFiles = new ArrayList<>();
    Option<FileSlice> fileSlice = Option.empty();

    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      // table versions 8 and greater.
      prevCommit = instantTime;
      if (hoodieTable.getMetaClient().getTableConfig().isCDCEnabled()) {
        // the cdc reader needs the base file metadata to have deterministic update sequence.
        fileSlice = hoodieTable.getSliceView().getLatestFileSlice(partitionPath, fileId);
        if (fileSlice.isPresent()) {
          prevCommit = fileSlice.get().getBaseInstantTime();
          baseFile = fileSlice.get().getBaseFile().map(BaseFile::getFileName).orElse("");
          logFiles = fileSlice.get().getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
        }
      }
    } else {
      // older table versions.
      fileSlice = hoodieTable.getSliceView().getLatestFileSlice(partitionPath, fileId);
      if (fileSlice.isPresent()) {
        prevCommit = fileSlice.get().getBaseInstantTime();
        baseFile = fileSlice.get().getBaseFile().map(BaseFile::getFileName).orElse("");
        logFiles = fileSlice.get().getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
      } else {
        // Set the base commit time as the current instantTime for new inserts into log files
        // Handle log file only case. This is necessary for the concurrent clustering and writer case (e.g., consistent hashing bucket index).
        // NOTE: flink engine use instantTime to mark operation type, check BaseFlinkCommitActionExecutor::execute
        prevCommit = getInstantTimeForLogFile(record);
        // This means there is no base data file, start appending to a new log file
        LOG.info("New file group from append handle for partition {}", partitionPath);
      }
    }

    deltaWriteStat.setPrevCommit(prevCommit);
    deltaWriteStat.setBaseFile(baseFile);
    deltaWriteStat.setLogFiles(logFiles);
    return fileSlice;
  }

  private void init(HoodieRecord record) {
    if (!doInit) {
      return;
    }
    // Prepare the first write status
    HoodieDeltaWriteStat deltaWriteStat = new HoodieDeltaWriteStat();
    writeStatus.setStat(deltaWriteStat);
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    deltaWriteStat.setPartitionPath(partitionPath);
    deltaWriteStat.setFileId(fileId);
    Option<FileSlice> fileSliceOpt = populateWriteStatAndFetchFileSlice(record, deltaWriteStat);
    averageRecordSize = sizeEstimator.sizeEstimate(record);
    try {
      // Save hoodie partition meta in the partition path
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String instantTime = config.getWriteVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
          ? getInstantTimeForLogFile(record) : deltaWriteStat.getPrevCommit();
      this.writer = createLogWriter(instantTime, fileSliceOpt);
    } catch (Exception e) {
      LOG.error("Error in update task at commit " + instantTime, e);
      writeStatus.setGlobalError(e);
      throw new HoodieUpsertException("Failed to initialize HoodieAppendHandle for FileId: " + fileId + " on commit "
          + instantTime + " on storage path " + hoodieTable.getMetaClient().getBasePath() + "/" + partitionPath, e);
    }
    doInit = false;
  }

  /**
   * Returns the instant time to use in the log file name.
   */
  private String getInstantTimeForLogFile(HoodieRecord<?> record) {
    if (config.isConsistentHashingEnabled()) {
      // Handle log file only case. This is necessary for the concurrent clustering and writer case (e.g., consistent hashing bucket index).
      // NOTE: flink engine use instantTime to mark operation type, check BaseFlinkCommitActionExecutor::execute
      String taggedInstant = HoodieRecordUtils.getCurrentLocationInstant(record);
      if (HoodieInstantTimeGenerator.isValidInstantTime(taggedInstant) && !instantTime.equals(taggedInstant)) {
        // the tagged instant is the pending clustering instant, use this instant in the file name so that
        // the dual-write file is shadowed to the reader view.
        return taggedInstant;
      }
    }
    return instantTime;
  }

  /**
   * Returns whether the hoodie record is an UPDATE.
   */
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    // If currentLocation is present, then this is an update
    return hoodieRecord.getCurrentLocation() != null;
  }

  private void bufferRecord(HoodieRecord<T> hoodieRecord) {
    HoodieSchema schema = useWriterSchema ? writeSchemaWithMetaFields : writeSchema;
    Option<Map<String, String>> recordMetadata = getRecordMetadata(hoodieRecord, schema, recordProperties);
    try {
      // Pass the isUpdateRecord to the props for HoodieRecordPayload to judge
      // Whether it is an update or insert record.
      boolean isUpdateRecord = isUpdateRecord(hoodieRecord);
      recordProperties.put(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, String.valueOf(isUpdateRecord));

      // Check for delete
      if (config.allowOperationMetadataField() || !hoodieRecord.isDelete(deleteContext, recordProperties)) {
        bufferInsertAndUpdate(schema, hoodieRecord, isUpdateRecord);
      } else {
        bufferDelete(hoodieRecord);
      }

      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      hoodieRecord.deflate();
    } catch (Exception e) {
      LOG.error("Error writing record {}", hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
  }

  private MetadataValues populateMetadataFields(HoodieRecord<T> hoodieRecord) {
    MetadataValues metadataValues = new MetadataValues();
    if (config.populateMetaFields()) {
      String seqId =
          HoodieRecord.generateSequenceId(instantTime, getPartitionId(), RECORD_COUNTER.getAndIncrement());
      metadataValues.setFileName(fileId);
      metadataValues.setPartitionPath(partitionPath);
      metadataValues.setRecordKey(hoodieRecord.getRecordKey());
      if (!this.isLogCompaction) {
        metadataValues.setCommitTime(instantTime);
        metadataValues.setCommitSeqno(seqId);
      }
    }
    if (config.allowOperationMetadataField()) {
      metadataValues.setOperation(hoodieRecord.getOperation().getName());
    }

    return metadataValues;
  }

  private void initNewStatus() {
    HoodieDeltaWriteStat prevStat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    // Make a new write status and copy basic fields over.
    HoodieDeltaWriteStat stat = prevStat.copy();

    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        hoodieTable.shouldTrackSuccessRecords(), config.getWriteStatusFailureFraction(), hoodieTable.isMetadataTable());
    this.writeStatus.setFileId(fileId);
    this.writeStatus.setPartitionPath(partitionPath);
    this.writeStatus.setStat(stat);
  }

  private String makeFilePath(HoodieLogFile logFile) {
    return partitionPath.isEmpty()
        ? new StoragePath(logFile.getFileName()).toString()
        : new StoragePath(partitionPath, logFile.getFileName()).toString();
  }

  protected void resetWriteCounts() {
    recordsWritten = 0;
    updatedRecordsWritten = 0;
    insertRecordsWritten = 0;
    recordsDeleted = 0;
  }

  private void updateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    stat.setNumWrites(recordsWritten);
    stat.setNumUpdateWrites(updatedRecordsWritten);
    stat.setNumInserts(insertRecordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setTotalWriteBytes(result.size());
  }

  private void accumulateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    stat.setNumWrites(stat.getNumWrites() + recordsWritten);
    stat.setNumUpdateWrites(stat.getNumUpdateWrites() + updatedRecordsWritten);
    stat.setNumInserts(stat.getNumInserts() + insertRecordsWritten);
    stat.setNumDeletes(stat.getNumDeletes() + recordsDeleted);
    stat.setTotalWriteBytes(stat.getTotalWriteBytes() + result.size());
  }

  private void updateWriteStat(HoodieDeltaWriteStat stat, AppendResult result) {
    stat.setPath(makeFilePath(result.logFile()));
    stat.setLogOffset(result.offset());
    stat.setLogVersion(result.logFile().getLogVersion());
    if (!stat.getLogFiles().contains(result.logFile().getFileName())) {
      stat.addLogFiles(result.logFile().getFileName());
    }
    stat.setFileSizeInBytes(result.size());
  }

  private void updateRuntimeStats(HoodieDeltaWriteStat stat) {
    RuntimeStats runtimeStats = new RuntimeStats();
    runtimeStats.setTotalUpsertTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
  }

  private void accumulateRuntimeStats(HoodieDeltaWriteStat stat) {
    RuntimeStats runtimeStats = stat.getRuntimeStats();
    assert runtimeStats != null;
    runtimeStats.setTotalUpsertTime(runtimeStats.getTotalUpsertTime() + timer.endTimer());
  }

  protected void updateWriteStatus(AppendResult result, HoodieDeltaWriteStat stat) {
    if (stat.getPath() == null) {
      // first time writing to this log block.
      updateWriteStat(stat, result);
      updateWriteCounts(stat, result);
      updateRuntimeStats(stat);
      statuses.add(this.writeStatus);
    } else if (stat.getPath().endsWith(result.logFile().getFileName())) {
      // append/continued writing to the same log file
      stat.setLogOffset(Math.min(stat.getLogOffset(), result.offset()));
      stat.setFileSizeInBytes(stat.getFileSizeInBytes() + result.size());
      accumulateWriteCounts(stat, result);
      accumulateRuntimeStats(stat);
    } else {
      // written to a newer log file, due to rollover/otherwise.
      initNewStatus();
      stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();

      updateWriteStat(stat, result);
      updateWriteCounts(stat, result);
      updateRuntimeStats(stat);
      statuses.add(this.writeStatus);
    }
  }

  protected void processAppendResult(AppendResult result, Option<HoodieLogBlock> dataBlock) {
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    updateWriteStatus(result, stat);

    if (config.isMetadataColumnStatsIndexEnabled()) {
      HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, hoodieTable.getMetaClient());
      Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
          .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
              config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
              Option.of(this.recordMerger.getRecordType()), indexVersion).keySet());
      final List<Pair<String, HoodieSchemaField>> fieldsToIndex = columnsToIndexSet.stream()
          .map(fieldName -> HoodieSchemaUtils.getNestedField(writeSchemaWithMetaFields, fieldName))
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
      try {
        Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
            collectColumnRangeMetadata(recordList.iterator(), fieldsToIndex, stat.getPath(), writeSchemaWithMetaFields, storage.getConf(),
                indexVersion);
        stat.putRecordsStats(columnRangeMetadataMap);
      } catch (HoodieException e) {
        throw new HoodieAppendException("Failed to extract append result", e);
      }
    }

    resetWriteCounts();
    assert stat.getRuntimeStats() != null;
    LOG.info("AppendHandle for partitionPath {} filePath {}, took {} ms.", partitionPath,
        stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime());
    timer.startTimer();
  }

  public void doAppend() {
    while (recordItr.hasNext()) {
      HoodieRecord record = recordItr.next();
      init(record);
      flushToDiskIfRequired(record, false);
      writeToBuffer(record);
    }
    appendDataAndDeleteBlocks(header, true);
    estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
  }

  /**
   * Appends data and delete blocks. When appendDeleteBlocks value is false, only data blocks are appended.
   * This is done so that all the data blocks are created first and then a single delete block is added.
   * Otherwise what can end up happening is creation of multiple small delete blocks get added after each data block.
   */
  protected void appendDataAndDeleteBlocks(Map<HeaderMetadataType, String> header, boolean appendDeleteBlocks) {
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchemaWithMetaFields.toString());
      List<HoodieLogBlock> blocks = new ArrayList<>(2);
      HoodieLogBlock dataBlock = null;
      if (!recordList.isEmpty()) {
        String keyField = config.populateMetaFields()
            ? HoodieRecord.RECORD_KEY_METADATA_FIELD
            : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();

        dataBlock = getDataBlock(config, getLogBlockType(), recordList,
            getUpdatedHeader(header, config, baseFileInstantTimeOfPositions), keyField);
        blocks.add(dataBlock);
      }

      if (appendDeleteBlocks && !recordsToDeleteWithPositions.isEmpty()) {
        blocks.add(new HoodieDeleteBlock(
            recordsToDeleteWithPositions,
            getUpdatedHeader(
                header, config, baseFileInstantTimeOfPositions)));
      }

      if (!blocks.isEmpty()) {
        AppendResult appendResult = writer.appendBlocks(blocks);
        processAppendResult(appendResult, Option.ofNullable(dataBlock));
        recordList.clear();
        if (appendDeleteBlocks) {
          recordsToDeleteWithPositions.clear();
        }
      }
    } catch (Exception e) {
      throw new HoodieAppendException("Failed while appending records to " + writer.getLogFile().getPath(), e);
    }
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return config.getParquetMaxFileSize() >= estimatedNumberOfBytesWritten
        * config.getLogFileToParquetCompressionRatio();
  }

  @Override
  protected void doWrite(HoodieRecord record, HoodieSchema schema, TypedProperties props) {
    Option<Map<String, String>> recordMetadata = record.getMetadata();
    try {
      init(record);
      flushToDiskIfRequired(record, false);
      writeToBuffer(record);
    } catch (Throwable t) {
      // Not throwing exception from here, since we don't want to fail the entire job
      // for a single record
      writeStatus.markFailure(record, t, recordMetadata);
      LOG.error("Error writing record " + record, t);
    }
  }

  @Override
  public List<WriteStatus> close() {
    try {
      if (isClosed()) {
        // Handle has already been closed
        return Collections.singletonList(writeStatus);
      }

      markClosed();
      // flush any remaining records to disk
      appendDataAndDeleteBlocks(header, true);
      if (recordItr instanceof Closeable) {
        ((Closeable) recordItr).close();
      }
      recordItr = null;

      if (writer != null) {
        writer.close();
        writer = null;
      }

      // update final size, once for all log files
      // TODO we can actually deduce file size purely from AppendResult (based on offset and size
      //      of the appended block)
      for (WriteStatus status : statuses) {
        long logFileSize = storage.getPathInfo(
            new StoragePath(config.getBasePath(), status.getStat().getPath()))
            .getLength();
        status.getStat().setFileSizeInBytes(logFileSize);
      }

      // generate Secondary index stats if streaming writes is enabled.
      if (isSecondaryIndexStatsStreamingWritesEnabled) {
        // Adds secondary index only for the last log file write status. We do not need to add secondary index stats
        // for every log file written as part of the append handle write. The last write status would update the
        // secondary index considering all the log files.
        SecondaryIndexStreamingTracker.trackSecondaryIndexStats(partitionPath, fileId, getReadFileSlice(),
            statuses.stream().map(status -> status.getStat().getPath()).collect(Collectors.toList()),
            statuses.get(statuses.size() - 1), hoodieTable, secondaryIndexDefns, config, instantTime, writeSchemaWithMetaFields);
      }

      return statuses;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public void write(Map<String, HoodieRecord<T>> recordMap) {
    try {
      for (Map.Entry<String, HoodieRecord<T>> entry: recordMap.entrySet()) {
        HoodieRecord<T> record = entry.getValue();
        init(record);
        flushToDiskIfRequired(record, false);
        writeToBuffer(record);
      }
      appendDataAndDeleteBlocks(header, true);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to compact blocks for fileId " + fileId, e);
    }
  }

  @Override
  public IOType getIOType() {
    return IOType.APPEND;
  }

  public List<WriteStatus> getWriteStatuses() {
    return statuses;
  }

  /**
   * Whether there is need to update the record location.
   */
  protected boolean needsUpdateLocation() {
    return true;
  }

  private void writeToBuffer(HoodieRecord<T> record) {
    if (!partitionPath.equals(record.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
      writeStatus.markFailure(record, failureEx, record.getMetadata());
      return;
    }

    // update the new location of the record, so we know where to find it next
    if (needsUpdateLocation()) {
      record.unseal();
      record.setNewLocation(newRecordLocation);
      record.seal();
    }
    // fetch the ordering val first in case the record was deflated.
    bufferRecord(record);
    numberOfRecords++;
  }

  private void bufferInsertAndUpdate(HoodieSchema schema, HoodieRecord<T> hoodieRecord, boolean isUpdateRecord) throws IOException {
    // Check if the record should be ignored (special case for [[ExpressionPayload]])
    if (hoodieRecord.shouldIgnore(schema.toAvroSchema(), recordProperties)) {
      return;
    }

    // Prepend meta-fields into the record
    MetadataValues metadataValues = populateMetadataFields(hoodieRecord);
    HoodieRecord populatedRecord =
        hoodieRecord.prependMetaFields(schema.toAvroSchema(), writeSchemaWithMetaFields.toAvroSchema(), metadataValues, recordProperties);

    // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
    //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
    //       it since these records will be put into the recordList(List).
    recordList.add(populatedRecord.copy());
    if (isUpdateRecord || isLogCompaction) {
      updatedRecordsWritten++;
    } else {
      insertRecordsWritten++;
    }
    recordsWritten++;
  }

  private void bufferDelete(HoodieRecord<T> hoodieRecord) {
    // Clear the new location as the record was deleted
    hoodieRecord.unseal();
    hoodieRecord.clearNewLocation();
    hoodieRecord.seal();
    recordsDeleted++;

    // store ordering value with Java type.
    final Comparable<?> orderingVal = hoodieRecord.getOrderingValueAsJava(writeSchema.toAvroSchema(), recordProperties, orderingFields);
    long position = baseFileInstantTimeOfPositions.isPresent() ? hoodieRecord.getCurrentPosition() : -1L;
    recordsToDeleteWithPositions.add(Pair.of(DeleteRecord.create(hoodieRecord.getKey(), orderingVal), position));
  }

  /**
   * Checks if the number of records have reached the set threshold and then flushes the records to disk.
   */
  protected void flushToDiskIfRequired(HoodieRecord record, boolean appendDeleteBlocks) {
    if (numberOfRecords >= (int) (maxBlockSize / averageRecordSize)
        || numberOfRecords % NUMBER_OF_RECORDS_TO_ESTIMATE_RECORD_SIZE == 0) {
      averageRecordSize = (long) (averageRecordSize * 0.8 + sizeEstimator.sizeEstimate(record) * 0.2);
    }

    // Append if max number of records reached to achieve block size
    if (numberOfRecords >= (maxBlockSize / averageRecordSize)) {
      // Recompute averageRecordSize before writing a new block and update existing value with
      // avg of new and old
      LOG.info("Flush log block to disk, the current avgRecordSize => " + averageRecordSize);
      // Delete blocks will be appended after appending all the data blocks.
      appendDataAndDeleteBlocks(header, appendDeleteBlocks);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
      numberOfRecords = 0;
    }
  }

  protected HoodieLogBlock.HoodieLogBlockType getLogBlockType() {
    return CommonClientUtils.getLogBlockType(config, hoodieTable.getMetaClient().getTableConfig());
  }

  private static Map<HeaderMetadataType, String> getUpdatedHeader(Map<HeaderMetadataType, String> header,
                                                                  HoodieWriteConfig config,
                                                                  Option<String> baseInstantTimeForPositions) {
    Map<HeaderMetadataType, String> updatedHeader = new HashMap<>(header);
    if (config.shouldWritePartialUpdates()) {
      // When enabling writing partial updates to the data blocks, the "IS_PARTIAL" flag is also
      // written to the block header so that the reader can differentiate partial updates, i.e.,
      // the "SCHEMA" header contains the partial schema.
      updatedHeader.put(
          HeaderMetadataType.IS_PARTIAL, Boolean.toString(true));
    }
    if (baseInstantTimeForPositions.isPresent()) {
      updatedHeader.put(
          HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS,
          baseInstantTimeForPositions.get());
    }
    return updatedHeader;
  }

  protected HoodieLogBlock getDataBlock(HoodieWriteConfig writeConfig,
                                        HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
                                        List<HoodieRecord> records,
                                        Map<HeaderMetadataType, String> header,
                                        String keyField) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(records, header, keyField);
      case HFILE_DATA_BLOCK:
        // Not supporting positions in HFile data blocks
        header.remove(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
        records.sort(Comparator.comparing(HoodieRecord::getRecordKey));
        return new HoodieHFileDataBlock(
            records, header, writeConfig.getHFileCompressionAlgorithm(), new StoragePath(writeConfig.getBasePath()));
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(
            records,
            header,
            keyField,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled());
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

  /**
   * Returns the file slice for reader view.
   */
  private Option<FileSlice> getReadFileSlice() {
    TableFileSystemView.SliceView rtView = hoodieTable.getSliceView();
    return rtView.getLatestMergedFileSliceBeforeOrOn(partitionPath, instantTime, fileId);
  }
}
