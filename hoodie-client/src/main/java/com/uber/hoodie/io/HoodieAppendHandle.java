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

package com.uber.hoodie.io;

import com.beust.jcommander.internal.Maps;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDeltaWriteStat;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat.RuntimeStats;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieAppendException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.util.SizeEstimator;

/**
 * IO Operation to append data onto an existing file.
 */
public class HoodieAppendHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieAppendHandle.class);
  // This acts as the sequenceID for records written
  private static AtomicLong recordIndex = new AtomicLong(1);
  private final WriteStatus writeStatus;
  private final String fileId;
  // Buffer for holding records in memory before they are flushed to disk
  List<IndexedRecord> recordList = new ArrayList<>();
  // Buffer for holding records (to be deleted) in memory before they are flushed to disk
  List<String> keysToDelete = new ArrayList<>();
  private TableFileSystemView.RealtimeView fileSystemView;
  private String partitionPath;
  private Iterator<HoodieRecord<T>> recordItr;
  // Total number of records written during an append
  private long recordsWritten = 0;
  // Total number of records deleted during an append
  private long recordsDeleted = 0;
  // Average record size for a HoodieRecord. This size is updated at the end of every log block flushed to disk
  private long averageRecordSize = 0;
  private HoodieLogFile currentLogFile;
  private Writer writer;
  // Flag used to initialize some metadata
  private boolean doInit = true;
  // Total number of bytes written during this append phase (an estimation)
  private long estimatedNumberOfBytesWritten;
  // Number of records that must be written to meet the max block size for a log block
  private int numberOfRecords = 0;
  // Max block size to limit to for a log block
  private int maxBlockSize = config.getLogFileDataBlockMaxSize();
  // Header metadata for a log block
  private Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();

  public HoodieAppendHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String fileId, Iterator<HoodieRecord<T>> recordItr) {
    super(config, commitTime, hoodieTable);
    WriteStatus writeStatus = ReflectionUtils.loadClass(config.getWriteStatusClassName());
    writeStatus.setStat(new HoodieDeltaWriteStat());
    this.writeStatus = writeStatus;
    this.fileId = fileId;
    this.fileSystemView = hoodieTable.getRTFileSystemView();
    this.recordItr = recordItr;
  }

  public HoodieAppendHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable) {
    this(config, commitTime, hoodieTable, UUID.randomUUID().toString(), null);
  }

  private void init(HoodieRecord record) {
    if (doInit) {
      this.partitionPath = record.getPartitionPath();
      // extract some information from the first record
      Optional<FileSlice> fileSlice = fileSystemView.getLatestFileSlices(partitionPath)
          .filter(fileSlice1 -> fileSlice1.getFileId().equals(fileId)).findFirst();
      String baseInstantTime = commitTime;
      if (fileSlice.isPresent()) {
        baseInstantTime = fileSlice.get().getBaseInstantTime();
      } else {
        // This means there is no base data file, start appending to a new log file
        fileSlice = Optional.of(new FileSlice(baseInstantTime, this.fileId));
        logger.info("New InsertHandle for partition :" + partitionPath);
      }
      writeStatus.getStat().setPrevCommit(baseInstantTime);
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      averageRecordSize = SizeEstimator.estimate(record);
      try {
        this.writer = createLogWriter(fileSlice, baseInstantTime);
        this.currentLogFile = writer.getLogFile();
        ((HoodieDeltaWriteStat) writeStatus.getStat()).setLogVersion(currentLogFile.getLogVersion());
        ((HoodieDeltaWriteStat) writeStatus.getStat()).setLogOffset(writer.getCurrentSize());
      } catch (Exception e) {
        logger.error("Error in update task at commit " + commitTime, e);
        writeStatus.setGlobalError(e);
        throw new HoodieUpsertException(
            "Failed to initialize HoodieAppendHandle for FileId: " + fileId + " on commit "
                + commitTime + " on HDFS path " + hoodieTable.getMetaClient().getBasePath()
                + partitionPath, e);
      }
      Path path = new Path(partitionPath, writer.getLogFile().getFileName());
      writeStatus.getStat().setPath(path.toString());
      doInit = false;
    }
  }

  private Optional<IndexedRecord> getIndexedRecord(HoodieRecord<T> hoodieRecord) {
    Optional recordMetadata = hoodieRecord.getData().getMetadata();
    try {
      Optional<IndexedRecord> avroRecord = hoodieRecord.getData().getInsertValue(schema);

      if (avroRecord.isPresent()) {
        String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
            recordIndex.getAndIncrement());
        HoodieAvroUtils
            .addHoodieKeyToRecord((GenericRecord) avroRecord.get(), hoodieRecord.getRecordKey(),
                hoodieRecord.getPartitionPath(), fileId);
        HoodieAvroUtils
            .addCommitMetadataToRecord((GenericRecord) avroRecord.get(), commitTime, seqId);
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
      logger.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return Optional.empty();
  }

  // TODO (NA) - Perform a schema check of current input record with the last schema on log file
  // to make sure we don't append records with older (shorter) schema than already appended
  public void doAppend() {
    while (recordItr.hasNext()) {
      HoodieRecord record = recordItr.next();
      init(record);
      flushToDiskIfRequired(record);
      writeToBuffer(record);
    }
    doAppend(header);
    estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
  }

  private void doAppend(Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, commitTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      if (recordList.size() > 0) {
        writer = writer.appendBlock(new HoodieAvroDataBlock(recordList, header));
        recordList.clear();
      }
      if (keysToDelete.size() > 0) {
        writer = writer.appendBlock(
            new HoodieDeleteBlock(keysToDelete.stream().toArray(String[]::new), header));
        keysToDelete.clear();
      }
    } catch (Exception e) {
      throw new HoodieAppendException(
          "Failed while appending records to " + currentLogFile.getPath(), e);
    }
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return config.getParquetMaxFileSize() >= estimatedNumberOfBytesWritten * config
        .getLogFileToParquetCompressionRatio();
  }

  @Override
  public void write(HoodieRecord record, Optional<IndexedRecord> insertValue) {
    Optional recordMetadata = record.getData().getMetadata();
    try {
      init(record);
      flushToDiskIfRequired(record);
      writeToBuffer(record);
    } catch (Throwable t) {
      // Not throwing exception from here, since we don't want to fail the entire job
      // for a single record
      writeStatus.markFailure(record, t, recordMetadata);
      logger.error("Error writing record " + record, t);
    }
  }

  @Override
  public WriteStatus close() {
    try {
      // flush any remaining records to disk
      doAppend(header);
      if (writer != null) {
        writer.close();
      }
      writeStatus.getStat().setPrevCommit(commitTime);
      writeStatus.getStat().setFileId(this.fileId);
      writeStatus.getStat().setNumWrites(recordsWritten);
      writeStatus.getStat().setNumDeletes(recordsDeleted);
      writeStatus.getStat().setTotalWriteBytes(estimatedNumberOfBytesWritten);
      writeStatus.getStat().setTotalWriteErrors(writeStatus.getFailedRecords().size());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      writeStatus.getStat().setRuntimeStats(runtimeStats);
      return writeStatus;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  @Override
  public WriteStatus getWriteStatus() {
    return writeStatus;
  }

  private Writer createLogWriter(Optional<FileSlice> fileSlice, String baseCommitTime)
      throws IOException, InterruptedException {
    return HoodieLogFormat.newWriterBuilder()
        .onParentPath(new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath))
        .withFileId(fileId).overBaseCommit(baseCommitTime).withLogVersion(
            fileSlice.get().getLogFiles().map(logFile -> logFile.getLogVersion())
                .max(Comparator.naturalOrder()).orElse(HoodieLogFile.LOGFILE_BASE_VERSION))
        .withSizeThreshold(config.getLogFileMaxSize()).withFs(fs)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
  }

  private void writeToBuffer(HoodieRecord<T> record) {
    // update the new location of the record, so we know where to find it next
    record.setNewLocation(new HoodieRecordLocation(commitTime, fileId));
    Optional<IndexedRecord> indexedRecord = getIndexedRecord(record);
    if (indexedRecord.isPresent()) {
      recordList.add(indexedRecord.get());
    } else {
      keysToDelete.add(record.getRecordKey());
    }
    numberOfRecords++;
  }

  /**
   * Checks if the number of records have reached the set threshold and then flushes the records to disk
   */
  private void flushToDiskIfRequired(HoodieRecord record) {
    // Append if max number of records reached to achieve block size
    if (numberOfRecords >= (int) (maxBlockSize / averageRecordSize)) {
      // Recompute averageRecordSize before writing a new block and update existing value with
      // avg of new and old
      logger.info("AvgRecordSize => " + averageRecordSize);
      averageRecordSize = (averageRecordSize + SizeEstimator.estimate(record)) / 2;
      doAppend(header);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
      numberOfRecords = 0;
    }
  }

}
