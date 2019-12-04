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

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieStorageWriter;
import org.apache.hudi.io.storage.HoodieStorageWriterFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("Duplicates")
public class HoodieMergeHandle<T extends HoodieRecordPayload> extends HoodieWriteHandle<T> {

  private static Logger logger = LogManager.getLogger(HoodieMergeHandle.class);

  private Map<String, HoodieRecord<T>> keyToNewRecords;
  private Set<String> writtenRecordKeys;
  private HoodieStorageWriter<IndexedRecord> storageWriter;
  private Path newFilePath;
  private Path oldFilePath;
  private long recordsWritten = 0;
  private long recordsDeleted = 0;
  private long updatedRecordsWritten = 0;
  private long insertRecordsWritten = 0;
  private boolean useWriterSchema;

  public HoodieMergeHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      Iterator<HoodieRecord<T>> recordItr, String fileId) {
    super(config, commitTime, fileId, hoodieTable);
    String partitionPath = init(fileId, recordItr);
    init(fileId, partitionPath, hoodieTable.getROFileSystemView().getLatestDataFile(partitionPath, fileId).get());
  }

  /**
   * Called by compactor code path.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      Map<String, HoodieRecord<T>> keyToNewRecords, String fileId, HoodieDataFile dataFileToBeMerged) {
    super(config, commitTime, fileId, hoodieTable);
    this.keyToNewRecords = keyToNewRecords;
    this.useWriterSchema = true;
    init(fileId, keyToNewRecords.get(keyToNewRecords.keySet().stream().findFirst().get()).getPartitionPath(),
        dataFileToBeMerged);
  }

  public static Schema createHoodieWriteSchema(Schema originalSchema) {
    return HoodieAvroUtils.addMetadataFields(originalSchema);
  }

  public Path makeNewPath(String partitionPath) {
    Path path = FSUtils.getPartitionPath(config.getBasePath(), partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, fileId));
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  /**
   * Determines whether we can accept the incoming records, into the current file. Depending on
   * <p>
   * - Whether it belongs to the same partitionPath as existing records - Whether the current file written bytes lt max
   * file size
   */
  public boolean canWrite(HoodieRecord record) {
    return false;
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Option<IndexedRecord> insertValue) {
    // NO_OP
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Option<IndexedRecord> avroRecord, Option<Exception> exception) {
    Option recordMetadata = record.getData().getMetadata();
    if (exception.isPresent() && exception.get() instanceof Throwable) {
      // Not throwing exception from here, since we don't want to fail the entire job for a single record
      writeStatus.markFailure(record, exception.get(), recordMetadata);
      logger.error("Error writing record " + record, exception.get());
    } else {
      write(record, avroRecord);
    }
  }

  /**
   * Rewrite the GenericRecord with the Schema containing the Hoodie Metadata fields.
   */
  protected GenericRecord rewriteRecord(GenericRecord record) {
    return HoodieAvroUtils.rewriteRecord(record, writerSchema);
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus.
   */
  private void init(String fileId, String partitionPath, HoodieDataFile dataFileToBeMerged) {
    logger.info("partitionPath:" + partitionPath + ", fileId to be merged:" + fileId);
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    try {
      String latestValidFilePath = dataFileToBeMerged.getFileName();
      writeStatus.getStat().setPrevCommit(FSUtils.getCommitTime(latestValidFilePath));

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(TaskContext.getPartitionId());

      oldFilePath = new Path(config.getBasePath() + "/" + partitionPath + "/" + latestValidFilePath);
      String relativePath = new Path((partitionPath.isEmpty() ? "" : partitionPath + "/")
          + FSUtils.makeDataFileName(instantTime, writeToken, fileId)).toString();
      newFilePath = new Path(config.getBasePath(), relativePath);

      logger.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
          newFilePath.toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      writeStatus.getStat().setPath(new Path(config.getBasePath()), newFilePath);

      // Create Marker file
      createMarkerFile(partitionPath);

      // Create the writer for writing the new version file
      storageWriter =
          HoodieStorageWriterFactory.getStorageWriter(instantTime, newFilePath, hoodieTable, config, writerSchema);
    } catch (IOException io) {
      logger.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  /**
   * Load the new incoming records in a map and return partitionPath.
   */
  private String init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    try {
      // Load the new records in a map
      long memoryForMerge = config.getMaxMemoryPerPartitionMerge();
      logger.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
      this.keyToNewRecords = new ExternalSpillableMap<>(memoryForMerge, config.getSpillableMapBasePath(),
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(originalSchema));
    } catch (IOException io) {
      throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
    }
    String partitionPath = null;
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      partitionPath = record.getPartitionPath();
      // update the new location of the record, so we know where to find it next
      record.unseal();
      record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
      record.seal();
      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      keyToNewRecords.put(record.getRecordKey(), record);
    }
    logger.info("Number of entries in MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries()
        + "Total size in bytes of MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize() + "Number of entries in DiskBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries() + "Size of file spilled to disk => "
        + ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes());
    return partitionPath;
  }

  private boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
    if (indexedRecord.isPresent()) {
      updatedRecordsWritten++;
    }
    return writeRecord(hoodieRecord, indexedRecord);
  }

  private boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
    Option recordMetadata = hoodieRecord.getData().getMetadata();
    try {
      if (indexedRecord.isPresent()) {
        // Convert GenericRecord to GenericRecord with hoodie commit metadata in schema
        IndexedRecord recordWithMetadataInSchema = rewriteRecord((GenericRecord) indexedRecord.get());
        storageWriter.writeAvroWithMetadata(recordWithMetadataInSchema, hoodieRecord);
        recordsWritten++;
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
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to the file.
   */
  public void write(GenericRecord oldRecord) {
    String key = oldRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    boolean copyOldRecord = true;
    if (keyToNewRecords.containsKey(key)) {
      // If we have duplicate records that we are updating, then the hoodie record will be deflated after
      // writing the first record. So make a copy of the record to be merged
      HoodieRecord<T> hoodieRecord = new HoodieRecord<>(keyToNewRecords.get(key));
      try {
        Option<IndexedRecord> combinedAvroRecord =
            hoodieRecord.getData().combineAndGetUpdateValue(oldRecord, useWriterSchema ? writerSchema : originalSchema);
        if (writeUpdateRecord(hoodieRecord, combinedAvroRecord)) {
          /*
           * ONLY WHEN 1) we have an update for this key AND 2) We are able to successfully write the the combined new
           * value
           *
           * We no longer need to copy the old record over.
           */
          copyOldRecord = false;
        }
        writtenRecordKeys.add(key);
      } catch (Exception e) {
        throw new HoodieUpsertException("Failed to combine/merge new record with old value in storage, for new record {"
            + keyToNewRecords.get(key) + "}, old value {" + oldRecord + "}", e);
      }
    }

    if (copyOldRecord) {
      // this should work as it is, since this is an existing record
      String errMsg = "Failed to merge old record into new file for key " + key + " from old file " + getOldFilePath()
          + " to new file " + newFilePath;
      try {
        storageWriter.writeAvro(key, oldRecord);
      } catch (ClassCastException e) {
        logger.error("Schema mismatch when rewriting old record " + oldRecord + " from file " + getOldFilePath()
            + " to file " + newFilePath + " with writerSchema " + writerSchema.toString(true));
        throw new HoodieUpsertException(errMsg, e);
      } catch (IOException e) {
        logger.error("Failed to merge old record into new file for key " + key + " from old file " + getOldFilePath()
            + " to new file " + newFilePath, e);
        throw new HoodieUpsertException(errMsg, e);
      }
      recordsWritten++;
    }
  }

  @Override
  public WriteStatus close() {
    try {
      // write out any pending records (this can happen when inserts are turned into updates)
      for (String key : keyToNewRecords.keySet()) {
        if (!writtenRecordKeys.contains(key)) {
          HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key);
          if (useWriterSchema) {
            writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(writerSchema));
          } else {
            writeRecord(hoodieRecord, hoodieRecord.getData().getInsertValue(originalSchema));
          }
          insertRecordsWritten++;
        }
      }
      keyToNewRecords.clear();
      writtenRecordKeys.clear();

      if (storageWriter != null) {
        storageWriter.close();
      }

      long fileSizeInBytes = FSUtils.getFileSize(fs, newFilePath);
      HoodieWriteStat stat = writeStatus.getStat();

      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      logger.info(String.format("MergeHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));

      return writeStatus;
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public Path getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public WriteStatus getWriteStatus() {
    return writeStatus;
  }
}
