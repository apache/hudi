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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.collectColumnRangeMetadata;

/**
 * IO Operation to append data onto an existing file.
 */
public class HoodieAppendHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieAppendHandle.class);
  // This acts as the sequenceID for records written
  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);

  protected final String fileId;
  // Buffer for holding records in memory before they are flushed to disk
  private final List<IndexedRecord> recordList = new ArrayList<>();
  // Buffer for holding records (to be deleted) in memory before they are flushed to disk
  private final List<DeleteRecord> recordsToDelete = new ArrayList<>();
  // Incoming records to be written to logs.
  protected Iterator<HoodieRecord<T>> recordItr;
  // Writer to log into the file group's latest slice.
  protected Writer writer;

  protected final List<WriteStatus> statuses;
  // Total number of records written during an append
  protected long recordsWritten = 0;
  // Total number of records deleted during an append
  protected long recordsDeleted = 0;
  // Total number of records updated during an append
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
  private int numberOfRecords = 0;
  // Max block size to limit to for a log block
  private final int maxBlockSize = config.getLogFileDataBlockMaxSize();
  // Header metadata for a log block
  protected final Map<HeaderMetadataType, String> header = new HashMap<>();
  private SizeEstimator<HoodieRecord> sizeEstimator;

  private Properties recordProperties = new Properties();

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    this.fileId = fileId;
    this.recordItr = recordItr;
    sizeEstimator = new DefaultSizeEstimator();
    this.statuses = new ArrayList<>();
    this.recordProperties.putAll(config.getProps());
  }

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier sparkTaskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, sparkTaskContextSupplier);
  }

  private void init(HoodieRecord record) {
    if (doInit) {
      // extract some information from the first record
      SliceView rtView = hoodieTable.getSliceView();
      Option<FileSlice> fileSlice = rtView.getLatestFileSlice(partitionPath, fileId);
      // Set the base commit time as the current instantTime for new inserts into log files
      String baseInstantTime;
      String baseFile = "";
      List<String> logFiles = new ArrayList<>();
      if (fileSlice.isPresent()) {
        baseInstantTime = fileSlice.get().getBaseInstantTime();
        baseFile = fileSlice.get().getBaseFile().map(BaseFile::getFileName).orElse("");
        logFiles = fileSlice.get().getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
      } else {
        baseInstantTime = instantTime;
        // Handle log file only case. This is necessary for the concurrent clustering and writer case (e.g., consistent hashing bucket index).
        // NOTE: flink engine use instantTime to mark operation type, check BaseFlinkCommitActionExecutor::execute
        if (record.getCurrentLocation() != null && HoodieInstantTimeGenerator.isValidInstantTime(record.getCurrentLocation().getInstantTime())) {
          baseInstantTime = record.getCurrentLocation().getInstantTime();
        }
        // This means there is no base data file, start appending to a new log file
        fileSlice = Option.of(new FileSlice(partitionPath, baseInstantTime, this.fileId));
        LOG.info("New AppendHandle for partition :" + partitionPath);
      }

      // Prepare the first write status
      writeStatus.setStat(new HoodieDeltaWriteStat());
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      averageRecordSize = sizeEstimator.sizeEstimate(record);

      HoodieDeltaWriteStat deltaWriteStat = (HoodieDeltaWriteStat) writeStatus.getStat();
      deltaWriteStat.setPrevCommit(baseInstantTime);
      deltaWriteStat.setPartitionPath(partitionPath);
      deltaWriteStat.setFileId(fileId);
      deltaWriteStat.setBaseFile(baseFile);
      deltaWriteStat.setLogFiles(logFiles);

      try {
        // Save hoodie partition meta in the partition path
        HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, baseInstantTime,
            new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath),
            hoodieTable.getPartitionMetafileFormat());
        partitionMetadata.trySave(getPartitionId());

        // Since the actual log file written to can be different based on when rollover happens, we use the
        // base file to denote some log appends happened on a slice. writeToken will still fence concurrent
        // writers.
        // https://issues.apache.org/jira/browse/HUDI-1517
        createMarkerFile(partitionPath, FSUtils.makeBaseFileName(baseInstantTime, writeToken, fileId, hoodieTable.getBaseFileExtension()));

        this.writer = createLogWriter(fileSlice, baseInstantTime);
      } catch (Exception e) {
        LOG.error("Error in update task at commit " + instantTime, e);
        writeStatus.setGlobalError(e);
        throw new HoodieUpsertException("Failed to initialize HoodieAppendHandle for FileId: " + fileId + " on commit "
            + instantTime + " on HDFS path " + hoodieTable.getMetaClient().getBasePath() + "/" + partitionPath, e);
      }
      doInit = false;
    }
  }

  /**
   * Returns whether the hoodie record is an UPDATE.
   */
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    // If currentLocation is present, then this is an update
    return hoodieRecord.getCurrentLocation() != null;
  }

  private Option<IndexedRecord> getIndexedRecord(HoodieRecord<T> hoodieRecord) {
    Option<Map<String, String>> recordMetadata = hoodieRecord.getData().getMetadata();
    try {
      // Pass the isUpdateRecord to the props for HoodieRecordPayload to judge
      // Whether it is an update or insert record.
      boolean isUpdateRecord = isUpdateRecord(hoodieRecord);
      // If the format can not record the operation field, nullify the DELETE payload manually.
      boolean nullifyPayload = HoodieOperation.isDelete(hoodieRecord.getOperation()) && !config.allowOperationMetadataField();
      recordProperties.put(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, String.valueOf(isUpdateRecord));
      Option<IndexedRecord> avroRecord = nullifyPayload ? Option.empty() : hoodieRecord.getData().getInsertValue(tableSchema, recordProperties);
      if (avroRecord.isPresent()) {
        if (avroRecord.get().equals(IGNORE_RECORD)) {
          return avroRecord;
        }
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        GenericRecord rewriteRecord = rewriteRecord((GenericRecord) avroRecord.get());
        avroRecord = Option.of(rewriteRecord);
        String seqId =
            HoodieRecord.generateSequenceId(instantTime, getPartitionId(), RECORD_COUNTER.getAndIncrement());
        if (config.populateMetaFields()) {
          HoodieAvroUtils.addHoodieKeyToRecord(rewriteRecord, hoodieRecord.getRecordKey(),
              hoodieRecord.getPartitionPath(), fileId);
          HoodieAvroUtils.addCommitMetadataToRecord(rewriteRecord, instantTime, seqId);
        }
        if (config.allowOperationMetadataField()) {
          HoodieAvroUtils.addOperationToRecord(rewriteRecord, hoodieRecord.getOperation());
        }
        if (isUpdateRecord) {
          updatedRecordsWritten++;
        } else {
          insertRecordsWritten++;
        }
        recordsWritten++;
      } else {
        recordsDeleted++;
      }

      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      hoodieRecord.deflate();
      return avroRecord;
    } catch (Exception e) {
      LOG.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return Option.empty();
  }

  private void initNewStatus() {
    HoodieDeltaWriteStat prevStat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    // Make a new write status and copy basic fields over.
    HoodieDeltaWriteStat stat = new HoodieDeltaWriteStat();
    stat.setFileId(fileId);
    stat.setPartitionPath(partitionPath);
    stat.setPrevCommit(prevStat.getPrevCommit());
    stat.setBaseFile(prevStat.getBaseFile());
    stat.setLogFiles(new ArrayList<>(prevStat.getLogFiles()));

    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        !hoodieTable.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
    this.writeStatus.setFileId(fileId);
    this.writeStatus.setPartitionPath(partitionPath);
    this.writeStatus.setStat(stat);
  }

  private String makeFilePath(HoodieLogFile logFile) {
    return partitionPath.length() == 0
        ? new Path(logFile.getFileName()).toString()
        : new Path(partitionPath, logFile.getFileName()).toString();
  }

  private void resetWriteCounts() {
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

  private void updateWriteStatus(HoodieDeltaWriteStat stat, AppendResult result) {
    updateWriteStat(stat, result);
    updateWriteCounts(stat, result);
    updateRuntimeStats(stat);
    statuses.add(this.writeStatus);
  }

  private void processAppendResult(AppendResult result, List<IndexedRecord> recordList) {
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();

    if (stat.getPath() == null) {
      // first time writing to this log block.
      updateWriteStatus(stat, result);
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
      updateWriteStatus(stat, result);
    }

    if (config.isMetadataColumnStatsIndexEnabled()) {
      final List<Schema.Field> fieldsToIndex;
      // If column stats index is enabled but columns not configured then we assume that
      // all columns should be indexed
      if (config.getColumnsEnabledForColumnStatsIndex().isEmpty()) {
        fieldsToIndex = writeSchemaWithMetaFields.getFields();
      } else {
        Set<String> columnsToIndexSet = new HashSet<>(config.getColumnsEnabledForColumnStatsIndex());

        fieldsToIndex = writeSchemaWithMetaFields.getFields().stream()
            .filter(field -> columnsToIndexSet.contains(field.name()))
            .collect(Collectors.toList());
      }

      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangesMetadataMap =
          collectColumnRangeMetadata(recordList, fieldsToIndex, stat.getPath());

      stat.setRecordsStats(columnRangesMetadataMap);
    }

    resetWriteCounts();
    assert stat.getRuntimeStats() != null;
    LOG.info(String.format("AppendHandle for partitionPath %s filePath %s, took %d ms.", partitionPath,
        stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime()));
    timer.startTimer();
  }

  public void doAppend() {
    while (recordItr.hasNext()) {
      HoodieRecord record = recordItr.next();
      init(record);
      flushToDiskIfRequired(record);
      writeToBuffer(record);
    }
    appendDataAndDeleteBlocks(header);
    estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
  }

  protected void appendDataAndDeleteBlocks(Map<HeaderMetadataType, String> header) {
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchemaWithMetaFields.toString());
      List<HoodieLogBlock> blocks = new ArrayList<>(2);
      if (recordList.size() > 0) {
        String keyField = config.populateMetaFields()
            ? HoodieRecord.RECORD_KEY_METADATA_FIELD
            : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();

        blocks.add(getBlock(config, pickLogDataBlockFormat(), recordList, header, keyField));
      }

      if (recordsToDelete.size() > 0) {
        blocks.add(new HoodieDeleteBlock(recordsToDelete.toArray(new DeleteRecord[0]), header));
      }

      if (blocks.size() > 0) {
        AppendResult appendResult = writer.appendBlocks(blocks);
        processAppendResult(appendResult, recordList);
        recordList.clear();
        recordsToDelete.clear();
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
  public void write(HoodieRecord record, Option<IndexedRecord> insertValue) {
    Option<Map<String, String>> recordMetadata = ((HoodieRecordPayload) record.getData()).getMetadata();
    try {
      init(record);
      flushToDiskIfRequired(record);
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
      // flush any remaining records to disk
      appendDataAndDeleteBlocks(header);
      recordItr = null;
      if (writer != null) {
        writer.close();
        writer = null;

        // update final size, once for all log files
        // TODO we can actually deduce file size purely from AppendResult (based on offset and size
        //      of the appended block)
        for (WriteStatus status : statuses) {
          long logFileSize = FSUtils.getFileSize(fs, new Path(config.getBasePath(), status.getStat().getPath()));
          status.getStat().setFileSizeInBytes(logFileSize);
        }
      }
      return statuses;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  @Override
  public IOType getIOType() {
    return IOType.APPEND;
  }

  public List<WriteStatus> writeStatuses() {
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
      writeStatus.markFailure(record, failureEx, record.getData().getMetadata());
      return;
    }

    // update the new location of the record, so we know where to find it next
    if (needsUpdateLocation()) {
      record.unseal();
      record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
      record.seal();
    }
    // fetch the ordering val first in case the record was deflated.
    final Comparable<?> orderingVal = record.getData().getOrderingValue();
    Option<IndexedRecord> indexedRecord = getIndexedRecord(record);
    if (indexedRecord.isPresent()) {
      // Skip the ignored record.
      if (!indexedRecord.get().equals(IGNORE_RECORD)) {
        recordList.add(indexedRecord.get());
      }
    } else {
      recordsToDelete.add(DeleteRecord.create(record.getKey(), orderingVal));
    }
    numberOfRecords++;
  }

  /**
   * Checks if the number of records have reached the set threshold and then flushes the records to disk.
   */
  private void flushToDiskIfRequired(HoodieRecord record) {
    // Append if max number of records reached to achieve block size
    if (numberOfRecords >= (int) (maxBlockSize / averageRecordSize)) {
      // Recompute averageRecordSize before writing a new block and update existing value with
      // avg of new and old
      LOG.info("AvgRecordSize => " + averageRecordSize);
      averageRecordSize = (averageRecordSize + sizeEstimator.sizeEstimate(record)) / 2;
      appendDataAndDeleteBlocks(header);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
      numberOfRecords = 0;
    }
  }

  private HoodieLogBlock.HoodieLogBlockType pickLogDataBlockFormat() {
    Option<HoodieLogBlock.HoodieLogBlockType> logBlockTypeOpt = config.getLogDataBlockFormat();
    if (logBlockTypeOpt.isPresent()) {
      return logBlockTypeOpt.get();
    }

    // Fallback to deduce data-block type based on the base file format
    switch (hoodieTable.getBaseFileFormat()) {
      case PARQUET:
      case ORC:
        return HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;
      case HFILE:
        return HoodieLogBlock.HoodieLogBlockType.HFILE_DATA_BLOCK;
      default:
        throw new HoodieException("Base file format " + hoodieTable.getBaseFileFormat()
            + " does not have associated log block type");
    }
  }

  private static HoodieLogBlock getBlock(HoodieWriteConfig writeConfig,
                                         HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
                                         List<IndexedRecord> recordList,
                                         Map<HeaderMetadataType, String> header,
                                         String keyField) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(recordList, header, keyField);
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(
            recordList, header, writeConfig.getHFileCompressionAlgorithm(), new Path(writeConfig.getBasePath()));
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(recordList, header, keyField, writeConfig.getParquetCompressionCodec());
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }
}
