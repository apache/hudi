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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Shared base for append handles writing merge-on-read log files.
 *
 * <p>This class owns the common append lifecycle:
 * <ol>
 *   <li>Initialize write status and log writer state for the target file group.</li>
 *   <li>Iterate incoming {@link HoodieRecord}s and route each record as an insert/update or delete.</li>
 *   <li>Track write counters, record locations, write-status success/failure state, and runtime stats.</li>
 *   <li>Flush pending records to one or more physical log files and update {@link HoodieDeltaWriteStat}s.</li>
 *   <li>Close the record iterator and log writer, then publish secondary-index streaming stats when enabled.</li>
 * </ol>
 *
 * <p>Concrete subclasses provide only the physical log representation. Inline-log handles buffer records
 * into Hudi log blocks, while native-log handles stream records into engine-native file writers. Both variants
 * share the same accounting and status update logic through this class.
 */
@Slf4j
public abstract class HoodieAppendHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);

  protected Iterator<HoodieRecord<T>> recordItr;
  protected final List<WriteStatus> statuses = new ArrayList<>();
  protected final Map<HeaderMetadataType, String> header = new HashMap<>();
  protected final Properties recordProperties = new Properties();
  protected final Option<String> baseFileInstantTimeOfPositions;
  protected boolean isLogCompaction = false;
  protected boolean useWriterSchema = false;
  protected boolean doInit = true;
  protected long recordsWritten = 0;
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected Option<HoodieLogBlock> appendDataBlock = Option.empty();

  protected HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                               String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                               TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, false);
  }

  protected HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                               String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                               TaskContextSupplier taskContextSupplier, Map<HeaderMetadataType, String> header) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, true);
    this.header.putAll(header);
  }

  protected HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                               String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, taskContextSupplier);
  }

  protected HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                               String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                               TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable,
        config.shouldWritePartialUpdates()
            ? Option.of(HoodieSchema.parse(config.getPartialUpdateSchema()))
            : Option.empty(),
        taskContextSupplier,
        preserveMetadata);
    this.recordItr = recordItr;
    this.isLogCompaction = preserveMetadata;
    this.useWriterSchema = preserveMetadata;
    this.recordProperties.putAll(config.getProps());
    this.baseFileInstantTimeOfPositions = config.shouldWriteRecordPositions()
        && config.getWriteVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
        ? getBaseFileInstantTimeOfPositions()
        : Option.empty();
  }

  protected HoodieAppendHandle(HoodieWriteConfig config, String instantTime, String partitionPath, String fileId,
                               HoodieTable<T, I, K, O> hoodieTable, Option<HoodieSchema> writerSchema,
                               TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, writerSchema, taskContextSupplier, preserveMetadata);
    this.baseFileInstantTimeOfPositions = config.shouldWriteRecordPositions()
        && config.getWriteVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
        ? getBaseFileInstantTimeOfPositions()
        : Option.empty();
  }

  /**
   * Writes all records from the incoming iterator and flushes remaining pending data.
   */
  public void doAppend() {
    while (recordItr.hasNext()) {
      HoodieRecord<T> record = recordItr.next();
      writeRecord(record);
    }
    flushAppend();
  }

  /**
   * Writes one record for APIs that call the handle directly instead of using {@link #doAppend()}.
   */
  @Override
  protected void doWrite(HoodieRecord record, HoodieSchema schema, TypedProperties props) {
    writeRecord(record);
  }

  /**
   * Writes a keyed record map, primarily used by log compaction paths.
   */
  @Override
  public void write(Map<String, HoodieRecord<T>> recordMap) {
    try {
      for (Map.Entry<String, HoodieRecord<T>> entry : recordMap.entrySet()) {
        HoodieRecord<T> record = entry.getValue();
        writeRecord(record);
      }
      flushAppend();
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to compact blocks for fileId " + fileId, e);
    }
  }

  /**
   * Flushes pending data, closes resources, finalizes log file sizes, and publishes secondary-index stats.
   */
  @Override
  public List<WriteStatus> close() {
    try {
      if (isClosed()) {
        return statuses.isEmpty() ? Collections.singletonList(writeStatus) : statuses;
      }

      markClosed();
      flushAppend();
      if (recordItr instanceof Closeable) {
        ((Closeable) recordItr).close();
      }
      recordItr = null;
      closeLogWriter();

      for (WriteStatus status : statuses) {
        HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) status.getStat();
        long appendedBytes = stat.getFileSizeInBytes();
        stat.setFileSizeInBytes(stat.getLogOffset() + appendedBytes);
      }

      if (isSecondaryIndexStatsStreamingWritesEnabled && !statuses.isEmpty()) {
        SecondaryIndexStreamingTracker.trackSecondaryIndexStats(partitionPath, fileId, getReadFileSlice(),
            statuses.stream().map(status -> status.getStat().getPath()).collect(Collectors.toList()),
            statuses.get(statuses.size() - 1), hoodieTable, secondaryIndexDefns, config, instantTime, writeSchemaWithMetaFields);
      }

      return statuses;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close " + getClass().getSimpleName(), e);
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
   * Lazily initializes the append handle on the first record.
   *
   * <p>Initialization is delayed until the first record so that append paths which receive an externally supplied
   * iterator can still derive file-slice and instant-time details from the actual record being written. The
   * concrete subclass is responsible for creating the physical writer in {@link #createLogWriterForAppend}.
   */
  protected void init(HoodieRecord record) {
    if (!doInit) {
      return;
    }

    HoodieDeltaWriteStat deltaWriteStat = new HoodieDeltaWriteStat();
    writeStatus.setStat(deltaWriteStat);
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    deltaWriteStat.setPartitionPath(partitionPath);
    deltaWriteStat.setFileId(fileId);
    Option<FileSlice> fileSliceOpt = populateWriteStatAndFetchFileSlice(record, deltaWriteStat);
    try {
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String logInstantTime = config.getWriteVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)
          ? getInstantTimeForLogFile(record) : deltaWriteStat.getPrevCommit();
      createLogWriterForAppend(logInstantTime, fileSliceOpt);
    } catch (Exception e) {
      log.error("Error in update task at commit {}", instantTime, e);
      writeStatus.setGlobalError(e);
      throw new HoodieUpsertException("Failed to initialize HoodieAppendHandle for FileId: " + fileId
          + " on commit " + instantTime + " on storage path " + hoodieTable.getMetaClient().getBasePath() + "/" + partitionPath, e);
    }
    doInit = false;
  }

  /**
   * Initializes the first {@link HoodieDeltaWriteStat} for this append handle and fetches the latest file slice.
   *
   * <p>The stat needs the previous commit, base file name, and existing log file names before any new log append
   * happens. Newer table versions use the current instant as the write instant, while older versions still rely
   * on the latest file slice to determine the previous commit.
   */
  protected Option<FileSlice> populateWriteStatAndFetchFileSlice(HoodieRecord record, HoodieDeltaWriteStat deltaWriteStat) {
    HoodieTableVersion tableVersion = hoodieTable.version();
    String prevCommit;
    String baseFile = "";
    List<String> logFiles = new ArrayList<>();
    Option<FileSlice> fileSlice = Option.empty();

    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      prevCommit = instantTime;
      if (hoodieTable.getMetaClient().getTableConfig().isCDCEnabled()) {
        fileSlice = hoodieTable.getSliceView().getLatestFileSlice(partitionPath, fileId);
        if (fileSlice.isPresent()) {
          prevCommit = fileSlice.get().getBaseInstantTime();
          baseFile = fileSlice.get().getBaseFile().map(BaseFile::getFileName).orElse("");
          logFiles = fileSlice.get().getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
        }
      }
    } else {
      fileSlice = hoodieTable.getSliceView().getLatestFileSlice(partitionPath, fileId);
      if (fileSlice.isPresent()) {
        prevCommit = fileSlice.get().getBaseInstantTime();
        baseFile = fileSlice.get().getBaseFile().map(BaseFile::getFileName).orElse("");
        logFiles = fileSlice.get().getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
      } else {
        prevCommit = getInstantTimeForLogFile(record);
        log.info("New file group from append handle for partition {}", partitionPath);
      }
    }

    deltaWriteStat.setPrevCommit(prevCommit);
    deltaWriteStat.setBaseFile(baseFile);
    deltaWriteStat.setLogFiles(logFiles);
    return fileSlice;
  }

  /**
   * Returns the instant time to use in the new log file name.
   *
   * <p>For consistent hashing, records may be tagged with a pending clustering instant. In that case the log file
   * must use the tagged instant so the dual-write file is shadowed correctly in the reader view.
   */
  protected String getInstantTimeForLogFile(HoodieRecord<?> record) {
    if (config.isConsistentHashingEnabled()) {
      String taggedInstant = HoodieRecordUtils.getCurrentLocationInstant(record);
      if (HoodieInstantTimeGenerator.isValidInstantTime(taggedInstant) && !instantTime.equals(taggedInstant)) {
        return taggedInstant;
      }
    }
    return instantTime;
  }

  /**
   * Returns the base-file instant used to encode record positions in log blocks.
   *
   * <p>Record positions are only written for table versions that support them. Subclasses may override this
   * when an engine-specific writer cannot produce compatible position metadata yet.
   */
  protected Option<String> getBaseFileInstantTimeOfPositions() {
    return hoodieTable.getHoodieView().getLatestBaseFile(partitionPath, fileId)
        .map(HoodieBaseFile::getCommitTime);
  }

  /**
   * Returns the file slice visible to readers after this append, used for secondary-index streaming stats.
   */
  protected Option<FileSlice> getReadFileSlice() {
    TableFileSystemView.SliceView rtView = hoodieTable.getSliceView();
    return rtView.getLatestMergedFileSliceBeforeOrOn(partitionPath, instantTime, fileId);
  }

  /**
   * Writes one record through the shared append flow.
   *
   * <p>This method performs partition validation, new-location assignment, update/delete routing, payload update
   * flags, write-status success/failure marking, and record deflation. Subclasses should generally override
   * {@link #writeInsertAndUpdate} and {@link #writeDelete} instead of this method, unless they need to add
   * behavior around the whole record-write operation.
   */
  protected boolean writeRecord(HoodieRecord<T> hoodieRecord) {
    HoodieSchema schema = useWriterSchema ? writeSchemaWithMetaFields : writeSchema;
    Option<Map<String, String>> recordMetadata = getRecordMetadata(hoodieRecord, schema, recordProperties);
    try {
      init(hoodieRecord);
      if (!partitionPath.equals(hoodieRecord.getPartitionPath())) {
        HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
            + hoodieRecord.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
        writeStatus.markFailure(hoodieRecord, failureEx, recordMetadata);
        return false;
      }

      if (needsUpdateLocation()) {
        hoodieRecord.unseal();
        hoodieRecord.setNewLocation(newRecordLocation);
        hoodieRecord.seal();
      }

      boolean isUpdateRecord = isUpdateRecord(hoodieRecord);
      recordProperties.put(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, String.valueOf(isUpdateRecord));
      if (config.allowOperationMetadataField() || !hoodieRecord.isDelete(deleteContext, recordProperties)) {
        writeInsertAndUpdate(schema, hoodieRecord, isUpdateRecord);
      } else {
        writeDelete(schema, hoodieRecord);
      }

      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      hoodieRecord.deflate();
      return true;
    } catch (Exception e) {
      log.error("Error writing record {}", hoodieRecord, e);
      if (!config.getIgnoreWriteFailed() || ExceptionUtil.isCausedBy(e, HoodieEarlyConflictDetectionException.class)) {
        throw new HoodieException(e.getMessage(), e);
      }
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
      return false;
    }
  }

  /**
   * Returns whether a record should be accounted as an update.
   *
   * <p>The default implementation follows the record location. Engines with bucket-level append semantics can
   * override this when update/insert intent is carried by the bucket instead of the individual record.
   */
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    return hoodieRecord.getCurrentLocation() != null;
  }

  /**
   * Returns whether the append flow should assign a new record location before writing.
   */
  protected boolean needsUpdateLocation() {
    return true;
  }

  /**
   * Builds metadata values that should be prepended to a record before it is written.
   *
   * <p>Log compaction preserves commit metadata from input records, while normal append writes generate new
   * commit time and sequence number values for the current instant.
   */
  protected MetadataValues populateMetadataFields(HoodieRecord<T> hoodieRecord) {
    MetadataValues metadataValues = new MetadataValues();
    if (config.populateMetaFields()) {
      String seqId = HoodieRecord.generateSequenceId(instantTime, getPartitionId(), RECORD_COUNTER.getAndIncrement());
      metadataValues.setFileName(fileId);
      metadataValues.setPartitionPath(partitionPath);
      metadataValues.setRecordKey(hoodieRecord.getRecordKey());
      if (!isLogCompaction) {
        metadataValues.setCommitTime(instantTime);
        metadataValues.setCommitSeqno(seqId);
      }
    }
    if (config.allowOperationMetadataField()) {
      metadataValues.setOperation(hoodieRecord.getOperation().getName());
    }
    return metadataValues;
  }

  protected Map<HeaderMetadataType, String> getUpdatedHeader(Map<HeaderMetadataType, String> header) {
    Map<HeaderMetadataType, String> updatedHeader = new HashMap<>(header);
    if (config.shouldWritePartialUpdates()) {
      updatedHeader.put(HeaderMetadataType.IS_PARTIAL, Boolean.toString(true));
    }
    if (baseFileInstantTimeOfPositions.isPresent()) {
      updatedHeader.put(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, baseFileInstantTimeOfPositions.get());
    }
    return updatedHeader;
  }

  protected void processAppendResult(AppendResult result, long elapsedTime) {
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    updateWriteStatus(result, stat, elapsedTime);
    stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    collectColumnStats(stat);
    assert stat.getRuntimeStats() != null;
    log.info("AppendHandle for partitionPath {} filePath {}, took {} ms.", partitionPath,
        stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime());
  }

  /**
   * Applies the result of one physical append to the current write status.
   *
   * <p>A single handle can write multiple physical log files because of rollover. Appends to the same log file
   * accumulate into the current stat; appends to a new log file create a new write status.
   */
  protected void updateWriteStatus(AppendResult result, HoodieDeltaWriteStat stat, long elapsedTime) {
    if (stat.getPath() == null) {
      updateWriteStat(stat, result);
      updateWriteCounts(stat, result);
      updateRuntimeStats(stat, elapsedTime);
      statuses.add(this.writeStatus);
    } else if (stat.getPath().endsWith(result.logFile().getFileName())) {
      stat.setLogOffset(Math.min(stat.getLogOffset(), result.offset()));
      stat.setFileSizeInBytes(stat.getFileSizeInBytes() + result.size());
      accumulateWriteCounts(stat, result);
      accumulateRuntimeStats(stat, elapsedTime);
    } else {
      initNewStatus();
      stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
      updateWriteStat(stat, result);
      updateWriteCounts(stat, result);
      updateRuntimeStats(stat, elapsedTime);
      statuses.add(this.writeStatus);
    }
  }

  /**
   * Starts a new write status when a flush rolls over to another physical log file.
   */
  protected void initNewStatus() {
    HoodieDeltaWriteStat prevStat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    HoodieDeltaWriteStat stat = prevStat.copy();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        hoodieTable.shouldTrackSuccessRecords(), config.getWriteStatusFailureFraction(), hoodieTable.isMetadataTable());
    this.writeStatus.setFileId(fileId);
    this.writeStatus.setPartitionPath(partitionPath);
    this.writeStatus.setStat(stat);
  }

  protected void resetWriteCounts() {
    recordsWritten = 0;
    updatedRecordsWritten = 0;
    insertRecordsWritten = 0;
    recordsDeleted = 0;
  }

  /**
   * Allows subclasses to collect column stats after a flush.
   *
   * <p>Inline log handles may compute stats from buffered records or inline block metadata. Native log handles
   * can collect stats from the native file footer and should skip delete-only files.
   */
  protected void collectColumnStats(HoodieDeltaWriteStat stat) {
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

  protected void updateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    stat.setNumWrites(recordsWritten);
    stat.setNumUpdateWrites(updatedRecordsWritten);
    stat.setNumInserts(insertRecordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setTotalWriteBytes(result.size());
  }

  protected void accumulateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    stat.setNumWrites(stat.getNumWrites() + recordsWritten);
    stat.setNumUpdateWrites(stat.getNumUpdateWrites() + updatedRecordsWritten);
    stat.setNumInserts(stat.getNumInserts() + insertRecordsWritten);
    stat.setNumDeletes(stat.getNumDeletes() + recordsDeleted);
    stat.setTotalWriteBytes(stat.getTotalWriteBytes() + result.size());
  }

  private void updateRuntimeStats(HoodieDeltaWriteStat stat, long elapsedTime) {
    RuntimeStats runtimeStats = new RuntimeStats();
    runtimeStats.setTotalUpsertTime(elapsedTime);
    stat.setRuntimeStats(runtimeStats);
  }

  private void accumulateRuntimeStats(HoodieDeltaWriteStat stat, long elapsedTime) {
    RuntimeStats runtimeStats = stat.getRuntimeStats();
    assert runtimeStats != null;
    runtimeStats.setTotalUpsertTime(runtimeStats.getTotalUpsertTime() + elapsedTime);
  }

  private String makeFilePath(HoodieLogFile logFile) {
    return partitionPath.isEmpty()
        ? new StoragePath(logFile.getFileName()).toString()
        : new StoragePath(partitionPath, logFile.getFileName()).toString();
  }

  /**
   * Creates the concrete log writer for this append handle.
   */
  protected abstract void createLogWriterForAppend(String instantTime, Option<FileSlice> fileSliceOpt) throws IOException;

  /**
   * Writes an insert or update record to the concrete log representation.
   */
  protected abstract void writeInsertAndUpdate(HoodieSchema schema, HoodieRecord<T> hoodieRecord, boolean isUpdateRecord) throws IOException;

  /**
   * Writes a delete record to the concrete log representation.
   */
  protected abstract void writeDelete(HoodieSchema schema, HoodieRecord<T> hoodieRecord) throws IOException;

  /**
   * Flushes any pending data/delete records and updates write stats for the flushed append result.
   */
  protected abstract void flushAppend();

  /**
   * Closes the concrete log writer.
   */
  protected abstract void closeLogWriter() throws IOException;
}
