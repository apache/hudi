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

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.model.HoodieWriteStat.RuntimeStats;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.util.DefaultSizeEstimator;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieRecordSizeEstimator;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.common.util.collection.ExternalSpillableMap;
import com.uber.hoodie.common.util.collection.converter.HoodieRecordConverter;
import com.uber.hoodie.common.util.collection.converter.StringConverter;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.io.storage.HoodieStorageWriter;
import com.uber.hoodie.io.storage.HoodieStorageWriterFactory;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

@SuppressWarnings("Duplicates")
public class HoodieMergeHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieMergeHandle.class);

  private WriteStatus writeStatus;
  private Map<String, HoodieRecord<T>> keyToNewRecords;
  private Set<String> writtenRecordKeys;
  private HoodieStorageWriter<IndexedRecord> storageWriter;
  private TableFileSystemView.ReadOptimizedView fileSystemView;
  private Path newFilePath;
  private Path oldFilePath;
  private Path tempPath = null;
  private long recordsWritten = 0;
  private long recordsDeleted = 0;
  private long updatedRecordsWritten = 0;

  public HoodieMergeHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      Iterator<HoodieRecord<T>> recordItr, String fileId) {
    super(config, commitTime, hoodieTable);
    this.fileSystemView = hoodieTable.getROFileSystemView();
    init(fileId, init(fileId, recordItr));
  }

  public HoodieMergeHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      Map<String, HoodieRecord<T>> keyToNewRecords, String fileId) {
    super(config, commitTime, hoodieTable);
    this.fileSystemView = hoodieTable.getROFileSystemView();
    this.keyToNewRecords = keyToNewRecords;
    init(fileId, keyToNewRecords.get(keyToNewRecords.keySet().stream().findFirst().get())
        .getPartitionPath());
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus
   */
  private void init(String fileId, String partitionPath) {
    this.writtenRecordKeys = new HashSet<>();

    WriteStatus writeStatus = ReflectionUtils.loadClass(config.getWriteStatusClassName());
    writeStatus.setStat(new HoodieWriteStat());
    this.writeStatus = writeStatus;
    try {
      String latestValidFilePath = fileSystemView.getLatestDataFiles(partitionPath)
          .filter(dataFile -> dataFile.getFileId().equals(fileId)).findFirst().get().getFileName();
      writeStatus.getStat().setPrevCommit(FSUtils.getCommitTime(latestValidFilePath));

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, commitTime,
          new Path(config.getBasePath()), new Path(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(TaskContext.getPartitionId());

      oldFilePath = new Path(
          config.getBasePath() + "/" + partitionPath + "/" + latestValidFilePath);
      String relativePath = new Path(partitionPath + "/" + FSUtils
          .makeDataFileName(commitTime, TaskContext.getPartitionId(), fileId)).toString();
      newFilePath = new Path(config.getBasePath(), relativePath);
      if (config.shouldUseTempFolderForCopyOnWriteForMerge()) {
        this.tempPath = makeTempPath(partitionPath, TaskContext.getPartitionId(), fileId,
            TaskContext.get().stageId(), TaskContext.get().taskAttemptId());
      }

      // handle cases of partial failures, for update task
      if (fs.exists(newFilePath)) {
        fs.delete(newFilePath, false);
      }

      logger.info(String
          .format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
              getStorageWriterPath().toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      writeStatus.getStat().setPaths(new Path(config.getBasePath()), newFilePath, tempPath);
      // Create the writer for writing the new version file
      storageWriter = HoodieStorageWriterFactory
          .getStorageWriter(commitTime, getStorageWriterPath(), hoodieTable, config, schema);
    } catch (IOException io) {
      logger.error("Error in update task at commit " + commitTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException(
          "Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
              + commitTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  /**
   * Load the new incoming records in a map and return partitionPath
   */
  private String init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    try {
      // Load the new records in a map
      logger.info("MaxMemoryPerPartitionMerge => " + config.getMaxMemoryPerPartitionMerge());
      this.keyToNewRecords = new ExternalSpillableMap<>(config.getMaxMemoryPerPartitionMerge(),
          config.getSpillableMapBasePath(), new StringConverter(),
          new HoodieRecordConverter(schema, config.getPayloadClass()),
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(schema));
    } catch (IOException io) {
      throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
    }
    String partitionPath = null;
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      partitionPath = record.getPartitionPath();
      keyToNewRecords.put(record.getRecordKey(), record);
      // update the new location of the record, so we know where to find it next
      record.setNewLocation(new HoodieRecordLocation(commitTime, fileId));
    }
    logger.info("Number of entries in MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries()
        + "Total size in bytes of MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize()
        + "Number of entries in DiskBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries()
        + "Size of file spilled to disk => "
        + ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes());
    return partitionPath;
  }

  private boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord,
      Optional<IndexedRecord> indexedRecord) {
    Optional recordMetadata = hoodieRecord.getData().getMetadata();
    try {
      if (indexedRecord.isPresent()) {
        storageWriter.writeAvroWithMetadata(indexedRecord.get(), hoodieRecord);
        recordsWritten++;
        updatedRecordsWritten++;
      } else {
        recordsDeleted++;
      }

      writeStatus.markSuccess(hoodieRecord, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      hoodieRecord.deflate();
      return true;
    } catch (Exception e) {
      logger.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return false;
  }

  /**
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to
   * the file.
   */
  public void write(GenericRecord oldRecord) {
    String key = oldRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key);
    boolean copyOldRecord = true;
    if (keyToNewRecords.containsKey(key)) {
      try {
        Optional<IndexedRecord> combinedAvroRecord = hoodieRecord.getData()
            .combineAndGetUpdateValue(oldRecord, schema);
        if (writeUpdateRecord(hoodieRecord, combinedAvroRecord)) {
          /* ONLY WHEN
           * 1) we have an update for this key AND
           * 2) We are able to successfully write the the combined new value
           *
           * We no longer need to copy the old record over.
           */
          copyOldRecord = false;
        }
        writtenRecordKeys.add(key);
      } catch (Exception e) {
        throw new HoodieUpsertException(
            "Failed to combine/merge new record with old value in storage, for new record {"
                + keyToNewRecords.get(key) + "}, old value {" + oldRecord + "}", e);
      }
    }

    if (copyOldRecord) {
      // this should work as it is, since this is an existing record
      String errMsg = "Failed to merge old record into new file for key " + key + " from old file "
          + getOldFilePath() + " to new file " + getStorageWriterPath();
      try {
        storageWriter.writeAvro(key, oldRecord);
      } catch (ClassCastException e) {
        logger.error("Schema mismatch when rewriting old record " + oldRecord + " from file "
            + getOldFilePath() + " to file " + getStorageWriterPath() + " with schema " + schema
            .toString(true));
        throw new HoodieUpsertException(errMsg, e);
      } catch (IOException e) {
        logger.error("Failed to merge old record into new file for key " + key + " from old file "
            + getOldFilePath() + " to new file " + getStorageWriterPath(), e);
        throw new HoodieUpsertException(errMsg, e);
      }
      recordsWritten++;
    }
  }

  @Override
  public WriteStatus close() {
    try {
      // write out any pending records (this can happen when inserts are turned into updates)
      Iterator<String> pendingRecordsItr = keyToNewRecords.keySet().iterator();
      while (pendingRecordsItr.hasNext()) {
        String key = pendingRecordsItr.next();
        if (!writtenRecordKeys.contains(key)) {
          HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key);
          writeUpdateRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(schema));
        }
      }
      keyToNewRecords.clear();
      writtenRecordKeys.clear();

      if (storageWriter != null) {
        storageWriter.close();
      }

      writeStatus.getStat().setTotalWriteBytes(FSUtils.getFileSize(fs, getStorageWriterPath()));
      writeStatus.getStat().setNumWrites(recordsWritten);
      writeStatus.getStat().setNumDeletes(recordsDeleted);
      writeStatus.getStat().setNumUpdateWrites(updatedRecordsWritten);
      writeStatus.getStat().setTotalWriteErrors(writeStatus.getFailedRecords().size());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      writeStatus.getStat().setRuntimeStats(runtimeStats);
      return writeStatus;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public Path getOldFilePath() {
    return oldFilePath;
  }

  private Path getStorageWriterPath() {
    // Use tempPath for storage writer if possible
    return (this.tempPath == null) ? this.newFilePath : this.tempPath;
  }

  @Override
  public WriteStatus getWriteStatus() {
    return writeStatus;
  }
}
