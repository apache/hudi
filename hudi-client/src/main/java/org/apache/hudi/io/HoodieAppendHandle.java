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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.util.SizeEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IO Operation to append data onto an existing file.
 */
public class HoodieAppendHandle<T extends HoodieRecordPayload> extends HoodieWriteHandle<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieAppendHandle.class);
  // This acts as the sequenceID for records written
  private static AtomicLong recordIndex = new AtomicLong(1);
  private final String fileId;
  // Buffer for holding records in memory before they are flushed to disk
  private List<IndexedRecord> recordList = new ArrayList<>();
  // Buffer for holding records (to be deleted) in memory before they are flushed to disk
  private List<HoodieKey> keysToDelete = new ArrayList<>();

  private Iterator<HoodieRecord<T>> recordItr;
  // Total number of records written during an append
  private long recordsWritten = 0;
  // Total number of records deleted during an append
  private long recordsDeleted = 0;
  // Total number of records updated during an append
  private long updatedRecordsWritten = 0;
  // Average record size for a HoodieRecord. This size is updated at the end of every log block flushed to disk
  private long averageRecordSize = 0;
  private HoodieLogFile currentLogFile;
  private Writer writer;
  // Flag used to initialize some metadata
  private boolean doInit = true;
  // Total number of bytes written during this append phase (an estimation)
  private long estimatedNumberOfBytesWritten;
  // Total number of bytes written to file
  private long sizeInBytes = 0;
  // Number of records that must be written to meet the max block size for a log block
  private int numberOfRecords = 0;
  // Max block size to limit to for a log block
  private int maxBlockSize = config.getLogFileDataBlockMaxSize();
  // Header metadata for a log block
  private Map<HeaderMetadataType, String> header = new HashMap<>();
  // Total number of new records inserted into the delta file
  private long insertRecordsWritten = 0;

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, SparkTaskContextSupplier sparkTaskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, sparkTaskContextSupplier);
    writeStatus.setStat(new HoodieDeltaWriteStat());
    this.fileId = fileId;
    this.recordItr = recordItr;
  }

  public HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T> hoodieTable,
                            String partitionPath, String fileId, SparkTaskContextSupplier sparkTaskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, sparkTaskContextSupplier);
  }

  private void init(HoodieRecord record) {
    if (doInit) {
      // extract some information from the first record
      SliceView rtView = hoodieTable.getSliceView();
      Option<FileSlice> fileSlice = rtView.getLatestFileSlice(partitionPath, fileId);
      // Set the base commit time as the current instantTime for new inserts into log files
      String baseInstantTime;
      if (fileSlice.isPresent()) {
        baseInstantTime = fileSlice.get().getBaseInstantTime();
      } else {
        baseInstantTime = instantTime;
        // This means there is no base data file, start appending to a new log file
        fileSlice = Option.of(new FileSlice(partitionPath, baseInstantTime, this.fileId));
        LOG.info("New InsertHandle for partition :" + partitionPath);
      }
      writeStatus.getStat().setPrevCommit(baseInstantTime);
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      averageRecordSize = SizeEstimator.estimate(record);
      try {
        //save hoodie partition meta in the partition path
        HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, baseInstantTime,
            new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
        partitionMetadata.trySave(getPartitionId());

        // Since the actual log file written to can be different based on when rollover happens, we use the
        // base file to denote some log appends happened on a slice. writeToken will still fence concurrent
        // writers.
        createMarkerFile(partitionPath, FSUtils.makeDataFileName(baseInstantTime, writeToken, fileId, hoodieTable.getBaseFileExtension()));

        this.writer = createLogWriter(fileSlice, baseInstantTime);
        this.currentLogFile = writer.getLogFile();
        ((HoodieDeltaWriteStat) writeStatus.getStat()).setLogVersion(currentLogFile.getLogVersion());
        ((HoodieDeltaWriteStat) writeStatus.getStat()).setLogOffset(writer.getCurrentSize());
      } catch (Exception e) {
        LOG.error("Error in update task at commit " + instantTime, e);
        writeStatus.setGlobalError(e);
        throw new HoodieUpsertException("Failed to initialize HoodieAppendHandle for FileId: " + fileId + " on commit "
            + instantTime + " on HDFS path " + hoodieTable.getMetaClient().getBasePath() + partitionPath, e);
      }
      Path path = partitionPath.length() == 0 ? new Path(writer.getLogFile().getFileName())
          : new Path(partitionPath, writer.getLogFile().getFileName());
      writeStatus.getStat().setPath(path.toString());
      doInit = false;
    }
  }

  private Option<IndexedRecord> getIndexedRecord(HoodieRecord<T> hoodieRecord) {
    Option recordMetadata = hoodieRecord.getData().getMetadata();
    try {
      Option<IndexedRecord> avroRecord = hoodieRecord.getData().getInsertValue(writerSchema);
      if (avroRecord.isPresent()) {
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        avroRecord = Option.of(rewriteRecord((GenericRecord) avroRecord.get()));
        String seqId =
            HoodieRecord.generateSequenceId(instantTime, getPartitionId(), recordIndex.getAndIncrement());
        HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord.get(), hoodieRecord.getRecordKey(),
            hoodieRecord.getPartitionPath(), fileId);
        HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord.get(), instantTime, seqId);
        // If currentLocation is present, then this is an update
        if (hoodieRecord.getCurrentLocation() != null) {
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

  private void doAppend(Map<HeaderMetadataType, String> header) {
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writerSchemaWithMetafields.toString());
      if (recordList.size() > 0) {
        writer = writer.appendBlock(HoodieDataBlock.getBlock(hoodieTable.getLogDataBlockFormat(), recordList, header));
        recordList.clear();
      }
      if (keysToDelete.size() > 0) {
        writer = writer.appendBlock(new HoodieDeleteBlock(keysToDelete.toArray(new HoodieKey[keysToDelete.size()]), header));
        keysToDelete.clear();
      }
    } catch (Exception e) {
      throw new HoodieAppendException("Failed while appending records to " + currentLogFile.getPath(), e);
    }
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return config.getParquetMaxFileSize() >= estimatedNumberOfBytesWritten
        * config.getLogFileToParquetCompressionRatio();
  }

  @Override
  public void write(HoodieRecord record, Option<IndexedRecord> insertValue) {
    Option recordMetadata = record.getData().getMetadata();
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
  public WriteStatus close() {
    try {
      // flush any remaining records to disk
      doAppend(header);

      if (writer != null) {
        sizeInBytes = writer.getCurrentSize();
        writer.close();
      }

      HoodieWriteStat stat = writeStatus.getStat();
      stat.setFileId(this.fileId);
      stat.setNumWrites(recordsWritten);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setTotalWriteBytes(estimatedNumberOfBytesWritten);
      stat.setFileSizeInBytes(sizeInBytes);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      LOG.info(String.format("AppendHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));

      return writeStatus;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  @Override
  public WriteStatus getWriteStatus() {
    return writeStatus;
  }

  @Override
  public IOType getIOType() {
    return IOType.APPEND;
  }

  private Writer createLogWriter(Option<FileSlice> fileSlice, String baseCommitTime)
      throws IOException, InterruptedException {
    Option<HoodieLogFile> latestLogFile = fileSlice.get().getLatestLogFile();

    return HoodieLogFormat.newWriterBuilder()
        .onParentPath(FSUtils.getPartitionPath(hoodieTable.getMetaClient().getBasePath(), partitionPath))
        .withFileId(fileId).overBaseCommit(baseCommitTime)
        .withLogVersion(latestLogFile.map(HoodieLogFile::getLogVersion).orElse(HoodieLogFile.LOGFILE_BASE_VERSION))
        .withSizeThreshold(config.getLogFileMaxSize()).withFs(fs)
        .withLogWriteToken(latestLogFile.map(x -> FSUtils.getWriteTokenFromLogPath(x.getPath())).orElse(writeToken))
        .withRolloverLogWriteToken(writeToken)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
  }

  private void writeToBuffer(HoodieRecord<T> record) {
    if (!partitionPath.equals(record.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
      writeStatus.markFailure(record, failureEx, record.getData().getMetadata());
      return;
    }

    // update the new location of the record, so we know where to find it next
    record.unseal();
    record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
    record.seal();
    Option<IndexedRecord> indexedRecord = getIndexedRecord(record);
    if (indexedRecord.isPresent()) {
      recordList.add(indexedRecord.get());
    } else {
      keysToDelete.add(record.getKey());
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
      averageRecordSize = (averageRecordSize + SizeEstimator.estimate(record)) / 2;
      doAppend(header);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
      numberOfRecords = 0;
    }
  }
}
