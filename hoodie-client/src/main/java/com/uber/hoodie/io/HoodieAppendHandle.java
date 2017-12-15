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
import com.clearspring.analytics.util.Lists;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDeltaWriteStat;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieAppendException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

/**
 * IO Operation to append data onto an existing file.
 */
public class HoodieAppendHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieMergeHandle.class);
  private static AtomicLong recordIndex = new AtomicLong(1);

  private TableFileSystemView.RealtimeView fileSystemView;
  private final WriteStatus writeStatus;
  private final String fileId;
  private String partitionPath;
  private List<HoodieRecord<T>> records;
  private long recordsWritten = 0;
  private long recordsDeleted = 0;
  private HoodieLogFile currentLogFile;
  private Writer writer;

  public HoodieAppendHandle(HoodieWriteConfig config,
      String commitTime,
      HoodieTable<T> hoodieTable,
      String fileId,
      Iterator<HoodieRecord<T>> recordItr) {
    super(config, commitTime, hoodieTable);
    WriteStatus writeStatus = ReflectionUtils.loadClass(config.getWriteStatusClassName());
    writeStatus.setStat(new HoodieDeltaWriteStat());
    this.writeStatus = writeStatus;
    this.fileId = fileId;
    this.fileSystemView = hoodieTable.getRTFileSystemView();
    init(recordItr);
  }

  private void init(Iterator<HoodieRecord<T>> recordItr) {
    List<HoodieRecord<T>> records = Lists.newArrayList();
    recordItr.forEachRemaining(record -> {
      records.add(record);
      // extract some information from the first record
      if (partitionPath == null) {
        partitionPath = record.getPartitionPath();
        FileSlice fileSlice = fileSystemView.getLatestFileSlices(record.getPartitionPath())
            .filter(fileSlice1 -> fileSlice1.getDataFile().get().getFileId().equals(fileId))
            .findFirst().get();
        // HACK(vc) This also assumes a base file. It will break, if appending without one.
        String latestValidFilePath = fileSlice.getDataFile().get().getFileName();
        String baseCommitTime = FSUtils.getCommitTime(latestValidFilePath);
        writeStatus.getStat().setPrevCommit(baseCommitTime);
        writeStatus.setFileId(fileId);
        writeStatus.setPartitionPath(record.getPartitionPath());
        writeStatus.getStat().setFileId(fileId);

        try {
          this.writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath))
              .withFileId(fileId).overBaseCommit(baseCommitTime).withLogVersion(fileSlice.getLogFiles()
                  .max(HoodieLogFile.getLogVersionComparator()::compare)
                  .map(logFile -> logFile.getLogVersion()).orElse(HoodieLogFile.LOGFILE_BASE_VERSION))
              .withFs(fs).withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
          this.currentLogFile = writer.getLogFile();
          ((HoodieDeltaWriteStat) writeStatus.getStat())
              .setLogVersion(currentLogFile.getLogVersion());
          ((HoodieDeltaWriteStat) writeStatus.getStat())
              .setLogOffset(writer.getCurrentSize());
        } catch (Exception e) {
          logger.error("Error in update task at commit " + commitTime, e);
          writeStatus.setGlobalError(e);
          throw new HoodieUpsertException(
              "Failed to initialize HoodieUpdateHandle for FileId: " + fileId
                  + " on commit " + commitTime + " on HDFS path " + hoodieTable
                  .getMetaClient().getBasePath() + partitionPath, e);
        }
        Path path = new Path(record.getPartitionPath(),
            FSUtils.makeDataFileName(commitTime, TaskContext.getPartitionId(), fileId));
        writeStatus.getStat().setPath(path.toString());
      }
      // update the new location of the record, so we know where to find it next
      record.setNewLocation(new HoodieRecordLocation(commitTime, fileId));
    });
    this.records = records;
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

      hoodieRecord.deflate();
      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      return avroRecord;
    } catch (Exception e) {
      logger.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return Optional.empty();
  }

  public void doAppend() {

    List<IndexedRecord> recordList = new ArrayList<>();
    List<String> keysToDelete = new ArrayList<>();
    Map<HoodieLogBlock.LogMetadataType, String> metadata = Maps.newHashMap();
    metadata.put(HoodieLogBlock.LogMetadataType.INSTANT_TIME, commitTime);
    Iterator<HoodieRecord<T>> recordsItr = records.iterator();
    while (recordsItr.hasNext()) {
      HoodieRecord record = recordsItr.next();
      Optional<IndexedRecord> indexedRecord = getIndexedRecord(record);
      if (indexedRecord.isPresent()) {
        recordList.add(indexedRecord.get());
      } else {
        keysToDelete.add(record.getRecordKey());
      }
      recordsItr.remove(); //remove entries when IndexedRecord added to new list
    }
    try {
      if (recordList.size() > 0) {
        writer = writer.appendBlock(new HoodieAvroDataBlock(recordList, schema, metadata));
      }
      if (keysToDelete.size() > 0) {
        writer = writer.appendBlock(
            new HoodieDeleteBlock(keysToDelete.stream().toArray(String[]::new), metadata));
      }
    } catch (Exception e) {
      throw new HoodieAppendException(
          "Failed while appending records to " + currentLogFile.getPath(), e);
    }
  }

  public void close() {
    try {
      if (writer != null) {
        writer.close();
      }
      writeStatus.getStat().setNumWrites(recordsWritten);
      writeStatus.getStat().setNumDeletes(recordsDeleted);
      writeStatus.getStat().setTotalWriteErrors(writeStatus.getFailedRecords().size());
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public WriteStatus getWriteStatus() {
    return writeStatus;
  }


}
