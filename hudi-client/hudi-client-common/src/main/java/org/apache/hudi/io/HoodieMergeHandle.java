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

import org.apache.hadoop.fs.Path;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("Duplicates")
/**
 * Handle to merge incoming records to those in storage.
 * <p>
 * Simplified Logic:
 * For every existing record
 *     Check if there is a new record coming in. If yes, merge two records and write to file
 *     else write the record as is
 * For all pending records from incoming batch, write to file.
 *
 * Illustration with simple data.
 * Incoming data:
 *     rec1_2, rec4_2, rec5_1, rec6_1
 * Existing data:
 *     rec1_1, rec2_1, rec3_1, rec4_1
 *
 * For every existing record, merge w/ incoming if required and write to storage.
 *    => rec1_1 and rec1_2 is merged to write rec1_2 to storage
 *    => rec2_1 is written as is
 *    => rec3_1 is written as is
 *    => rec4_2 and rec4_1 is merged to write rec4_2 to storage
 * Write all pending records from incoming set to storage
 *    => rec5_1 and rec6_1
 *
 * Final snapshot in storage
 * rec1_2, rec2_1, rec3_1, rec4_2, rec5_1, rec6_1
 *
 * </p>
 */
@NotThreadSafe
public class HoodieMergeHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeHandle.class);

  protected Map<String, HoodieRecord<T>> keyToNewRecords;
  protected Set<String> writtenRecordKeys;
  protected HoodieFileWriter<IndexedRecord> fileWriter;
  protected boolean cdcEnabled = false;
  protected HoodieCDCLogger cdcLogger;
  private boolean preserveMetadata = false;

  protected Path newFilePath;
  protected Path oldFilePath;
  protected long recordsWritten = 0;
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected boolean useWriterSchemaForCompaction;
  protected Option<BaseKeyGenerator> keyGeneratorOpt;
  private HoodieBaseFile baseFileToMerge;

  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                           TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    this(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier,
        getLatestBaseFile(hoodieTable, partitionPath, fileId), keyGeneratorOpt);
  }

  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                           TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    init(fileId, recordItr);
    init(fileId, partitionPath, baseFile);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  /**
   * Called by compactor code path.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                           HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    this.keyToNewRecords = keyToNewRecords;
    this.useWriterSchemaForCompaction = true;
    this.preserveMetadata = config.isPreserveHoodieCommitMetadataForCompaction();
    init(fileId, this.partitionPath, dataFileToBeMerged);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  public static HoodieBaseFile getLatestBaseFile(HoodieTable<?, ?, ?, ?> hoodieTable, String partitionPath, String fileId) {
    Option<HoodieBaseFile> baseFileOp = hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId);
    if (!baseFileOp.isPresent()) {
      throw new NoSuchElementException(String.format("FileID %s of partition path %s does not exist.", fileId, partitionPath));
    }
    return baseFileOp.get();
  }

  @Override
  public Schema getWriterSchemaWithMetaFields() {
    return writeSchemaWithMetaFields;
  }

  public Schema getWriterSchema() {
    return writeSchema;
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus.
   */
  private void init(String fileId, String partitionPath, HoodieBaseFile baseFileToMerge) {
    LOG.info("partitionPath:" + partitionPath + ", fileId to be merged:" + fileId);
    this.baseFileToMerge = baseFileToMerge;
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    try {
      String latestValidFilePath = baseFileToMerge.getFileName();
      writeStatus.getStat().setPrevCommit(FSUtils.getCommitTime(latestValidFilePath));

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave(getPartitionId());

      String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
      makeOldAndNewFilePaths(partitionPath, latestValidFilePath, newFileName);

      LOG.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
          newFilePath.toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      setWriteStatusPath();

      // Create Marker file,
      // uses name of `newFilePath` instead of `newFileName`
      // in case the sub-class may roll over the file handle name.
      createMarkerFile(partitionPath, newFilePath.getName());

      // Create the writer for writing the new version file
      fileWriter = createNewFileWriter(instantTime, newFilePath, hoodieTable, config,
        writeSchemaWithMetaFields, taskContextSupplier);

      // init the cdc logger
      this.cdcEnabled = config.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED);
      if (cdcEnabled) {
        this.cdcLogger = new HoodieCDCLogger(
            instantTime,
            config,
            hoodieTable.getMetaClient().getTableConfig(),
            tableSchema,
            createLogWriter(Option.empty(), instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX),
            IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config));
      }
    } catch (IOException io) {
      LOG.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  protected void setWriteStatusPath() {
    writeStatus.getStat().setPath(new Path(config.getBasePath()), newFilePath);
  }

  protected void makeOldAndNewFilePaths(String partitionPath, String oldFileName, String newFileName) {
    oldFilePath = makeNewFilePath(partitionPath, oldFileName);
    newFilePath = makeNewFilePath(partitionPath, newFileName);
  }

  /**
   * Initialize a spillable map for incoming records.
   */
  protected void initializeIncomingRecordsMap() {
    try {
      // Load the new records in a map
      long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config);
      LOG.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
      this.keyToNewRecords = new ExternalSpillableMap<>(memoryForMerge, config.getSpillableMapBasePath(),
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(tableSchema),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
    } catch (IOException io) {
      throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
    }
  }

  /**
   * Whether there is need to update the record location.
   */
  boolean needsUpdateLocation() {
    return true;
  }

  /**
   * Load the new incoming records in a map and return partitionPath.
   */
  protected void init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    initializeIncomingRecordsMap();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      // update the new location of the record, so we know where to find it next
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      keyToNewRecords.put(record.getRecordKey(), record);
    }
    LOG.info("Number of entries in MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries()
        + ", Total size in bytes of MemoryBasedMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize() + ", Number of entries in BitCaskDiskMap => "
        + ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries() + ", Size of file spilled to disk => "
        + ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes());
  }

  private boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord, GenericRecord oldRecord, Option<IndexedRecord> indexedRecord) {
    boolean isDelete = false;
    if (indexedRecord.isPresent()) {
      updatedRecordsWritten++;
      GenericRecord record = (GenericRecord) indexedRecord.get();
      if (oldRecord != record) {
        // the incoming record is chosen
        isDelete = HoodieOperation.isDelete(hoodieRecord.getOperation());
      } else {
        // the incoming record is dropped
        return false;
      }
    }
    boolean result = writeRecord(hoodieRecord, indexedRecord, isDelete);
    if (result && cdcEnabled) {
      cdcLogger.put(hoodieRecord, oldRecord, indexedRecord);
    }
    return result;
  }

  protected void writeInsertRecord(HoodieRecord<T> hoodieRecord) throws IOException {
    Schema schema = useWriterSchemaForCompaction ? tableSchemaWithMetaFields : tableSchema;
    Option<IndexedRecord> insertRecord = hoodieRecord.getData().getInsertValue(schema, config.getProps());
    // just skip the ignored record
    if (insertRecord.isPresent() && insertRecord.get().equals(IGNORE_RECORD)) {
      return;
    }
    if (writeRecord(hoodieRecord, insertRecord, HoodieOperation.isDelete(hoodieRecord.getOperation()))) {
      if (cdcEnabled) {
        cdcLogger.put(hoodieRecord, null, insertRecord);
      }
      insertRecordsWritten++;
    }
  }

  protected boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord) {
    return writeRecord(hoodieRecord, indexedRecord, false);
  }

  protected boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> indexedRecord, boolean isDelete) {
    Option recordMetadata = hoodieRecord.getData().getMetadata();
    if (!partitionPath.equals(hoodieRecord.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + hoodieRecord.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
      writeStatus.markFailure(hoodieRecord, failureEx, recordMetadata);
      return false;
    }
    try {
      if (indexedRecord.isPresent() && !isDelete) {
        writeToFile(hoodieRecord.getKey(), (GenericRecord) indexedRecord.get(), preserveMetadata && useWriterSchemaForCompaction);
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
      LOG.error("Error writing record  " + hoodieRecord, e);
      writeStatus.markFailure(hoodieRecord, e, recordMetadata);
    }
    return false;
  }

  /**
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to the file.
   */
  public void write(GenericRecord oldRecord) {
    String key = KeyGenUtils.getRecordKeyFromGenericRecord(oldRecord, keyGeneratorOpt);
    boolean copyOldRecord = true;
    if (keyToNewRecords.containsKey(key)) {
      // If we have duplicate records that we are updating, then the hoodie record will be deflated after
      // writing the first record. So make a copy of the record to be merged
      HoodieRecord<T> hoodieRecord = keyToNewRecords.get(key).newInstance();
      try {
        Option<IndexedRecord> combinedAvroRecord =
            hoodieRecord.getData().combineAndGetUpdateValue(oldRecord,
              useWriterSchemaForCompaction ? tableSchemaWithMetaFields : tableSchema,
                config.getPayloadConfig().getProps());

        if (combinedAvroRecord.isPresent() && combinedAvroRecord.get().equals(IGNORE_RECORD)) {
          // If it is an IGNORE_RECORD, just copy the old record, and do not update the new record.
          copyOldRecord = true;
        } else if (writeUpdateRecord(hoodieRecord, oldRecord, combinedAvroRecord)) {
          /*
           * ONLY WHEN 1) we have an update for this key AND 2) We are able to successfully
           * write the combined new value
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
      try {
        // NOTE: We're enforcing preservation of the record metadata to keep existing semantic
        writeToFile(new HoodieKey(key, partitionPath), oldRecord, true);
      } catch (IOException | RuntimeException e) {
        String errMsg = String.format("Failed to merge old record into new file for key %s from old file %s to new file %s with writerSchema %s",
                key, getOldFilePath(), newFilePath, writeSchemaWithMetaFields.toString(true));
        LOG.debug("Old record is " + oldRecord);
        throw new HoodieUpsertException(errMsg, e);
      }
      recordsWritten++;
    }
  }

  protected void writeToFile(HoodieKey key, GenericRecord avroRecord, boolean shouldPreserveRecordMetadata) throws IOException {
    if (shouldPreserveRecordMetadata) {
      // NOTE: `FILENAME_METADATA_FIELD` has to be rewritten to correctly point to the
      //       file holding this record even in cases when overall metadata is preserved
      fileWriter.writeAvro(key.getRecordKey(), rewriteRecordWithMetadata(avroRecord, newFilePath.getName()));
    } else {
      fileWriter.writeAvroWithMetadata(key, rewriteRecord(avroRecord));
    }
  }

  protected void writeIncomingRecords() throws IOException {
    // write out any pending records (this can happen when inserts are turned into updates)
    Iterator<HoodieRecord<T>> newRecordsItr = (keyToNewRecords instanceof ExternalSpillableMap)
        ? ((ExternalSpillableMap)keyToNewRecords).iterator() : keyToNewRecords.values().iterator();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> hoodieRecord = newRecordsItr.next();
      if (!writtenRecordKeys.contains(hoodieRecord.getRecordKey())) {
        writeInsertRecord(hoodieRecord);
      }
    }
  }

  @Override
  public List<WriteStatus> close() {
    try {
      HoodieWriteStat stat = writeStatus.getStat();

      writeIncomingRecords();

      if (keyToNewRecords instanceof ExternalSpillableMap) {
        ((ExternalSpillableMap) keyToNewRecords).close();
      } else {
        keyToNewRecords.clear();
      }
      writtenRecordKeys.clear();

      if (fileWriter != null) {
        fileWriter.close();
        fileWriter = null;
      }

      // if there are cdc data written, set the CDC-related information.
      HoodieCDCLogger.setCDCStatIfNeeded(stat, partitionPath, cdcLogger, recordsWritten, insertRecordsWritten, fs);

      long fileSizeInBytes = FSUtils.getFileSize(fs, newFilePath);
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

      performMergeDataValidationCheck(writeStatus);

      LOG.info(String.format("MergeHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public void performMergeDataValidationCheck(WriteStatus writeStatus) {
    if (!config.isMergeDataValidationCheckEnabled()) {
      return;
    }

    long oldNumWrites = 0;
    try {
      HoodieFileReader reader = HoodieFileReaderFactory.getFileReader(hoodieTable.getHadoopConf(), oldFilePath);
      oldNumWrites = reader.getTotalRecords();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to check for merge data validation", e);
    }

    if ((writeStatus.getStat().getNumWrites() + writeStatus.getStat().getNumDeletes()) < oldNumWrites) {
      throw new HoodieCorruptedDataException(
          String.format("Record write count decreased for file: %s, Partition Path: %s (%s:%d + %d < %s:%d)",
              writeStatus.getFileId(), writeStatus.getPartitionPath(),
              instantTime, writeStatus.getStat().getNumWrites(), writeStatus.getStat().getNumDeletes(),
              FSUtils.getCommitTime(oldFilePath.toString()), oldNumWrites));
    }
  }

  public Path getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  public HoodieBaseFile baseFileForMerge() {
    return baseFileToMerge;
  }
}
