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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
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
public class HoodieMergeHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeHandle.class);

  protected Map<String, HoodieRecord<T>> keyToNewRecords;
  protected Set<String> writtenRecordKeys;
  protected HoodieFileWriter fileWriter;
  protected boolean preserveMetadata = false;

  protected StoragePath newFilePath;
  protected StoragePath oldFilePath;
  protected long recordsWritten = 0;
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected Option<BaseKeyGenerator> keyGeneratorOpt;
  protected HoodieBaseFile baseFileToMerge;

  protected Option<String[]> partitionFields = Option.empty();
  protected Object[] partitionValues = new Object[0];

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
    this.preserveMetadata = true;
    init(fileId, this.partitionPath, dataFileToBeMerged);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  /**
   * Used by `HoodieSparkFileGroupReaderBasedMergeHandle`.
   *
   * @param config              Hudi write config
   * @param instantTime         Instant time to use
   * @param partitionPath       Partition path
   * @param fileId              File group ID for the merge handle to operate on
   * @param hoodieTable         {@link HoodieTable} instance
   * @param taskContextSupplier Task context supplier
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                           String fileId, HoodieTable<T, I, K, O> hoodieTable, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
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
      writeStatus.getStat().setPrevCommit(baseFileToMerge.getCommitTime());
      // At the moment, we only support SI for overwrite with latest payload. So, we don't need to embed entire file slice here.
      // HUDI-8518 will be taken up to fix it for any payload during which we might require entire file slice to be set here.
      // Already AppendHandle adds all logs file from current file slice to HoodieDeltaWriteStat.
      writeStatus.getStat().setPrevBaseFile(latestValidFilePath);

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
      makeOldAndNewFilePaths(partitionPath, latestValidFilePath, newFileName);

      LOG.info("Merging new data into oldPath {}, as newPath {}", oldFilePath, newFilePath);
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
      fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, newFilePath, hoodieTable.getStorage(),
          config, writeSchemaWithMetaFields, taskContextSupplier, recordMerger.getRecordType());
    } catch (IOException io) {
      LOG.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  protected void setWriteStatusPath() {
    writeStatus.getStat().setPath(new StoragePath(config.getBasePath()), newFilePath);
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
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(writeSchema),
          config.getCommonConfig().getSpillableDiskMapType(),
          new DefaultSerializer<>(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled(),
          getClass().getSimpleName());
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

  public boolean isEmptyNewRecords() {
    return keyToNewRecords.isEmpty();
  }

  protected boolean writeUpdateRecord(HoodieRecord<T> newRecord, HoodieRecord<T> oldRecord, Option<HoodieRecord> combineRecordOpt, Schema writerSchema) throws IOException {
    boolean isDelete = false;
    if (combineRecordOpt.isPresent()) {
      if (oldRecord.getData() != combineRecordOpt.get().getData()) {
        // the incoming record is chosen
        isDelete = HoodieOperation.isDelete(newRecord.getOperation());
      } else {
        // the incoming record is dropped
        return false;
      }
      updatedRecordsWritten++;
    }
    return writeRecord(newRecord, combineRecordOpt, writerSchema, config.getPayloadConfig().getProps(), isDelete);
  }

  protected void writeInsertRecord(HoodieRecord<T> newRecord) throws IOException {
    Schema schema = preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
    // just skip the ignored record
    if (newRecord.shouldIgnore(schema, config.getProps())) {
      return;
    }
    writeInsertRecord(newRecord, schema, config.getProps());
  }

  protected void writeInsertRecord(HoodieRecord<T> newRecord, Schema schema, Properties prop)
      throws IOException {
    if (writeRecord(newRecord, Option.of(newRecord), schema, prop, HoodieOperation.isDelete(newRecord.getOperation()))) {
      insertRecordsWritten++;
    }
  }

  protected boolean writeRecord(HoodieRecord<T> newRecord, Option<HoodieRecord> combineRecord, Schema schema, Properties prop) throws IOException {
    return writeRecord(newRecord, combineRecord, schema, prop, false);
  }

  private boolean writeRecord(HoodieRecord<T> newRecord, Option<HoodieRecord> combineRecord, Schema schema, Properties prop, boolean isDelete) throws IOException {
    Option recordMetadata = newRecord.getMetadata();
    if (!partitionPath.equals(newRecord.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + newRecord.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
      writeStatus.markFailure(newRecord, failureEx, recordMetadata);
      return false;
    }
    try {
      if (combineRecord.isPresent() && !combineRecord.get().isDelete(schema, config.getProps()) && !isDelete) {
        // Last-minute check.
        boolean decision = recordMerger.shouldFlush(combineRecord.get(), schema, config.getProps());

        if (decision) { // CASE (1): Flush the merged record.
          writeToFile(newRecord.getKey(), combineRecord.get(), schema, prop, preserveMetadata);
          recordsWritten++;
        } else {  // CASE (2): A delete operation.
          recordsDeleted++;
        }
      } else {
        recordsDeleted++;
        // Clear the new location as the record was deleted
        newRecord.unseal();
        newRecord.clearNewLocation();
        newRecord.seal();
      }
      writeStatus.markSuccess(newRecord, recordMetadata);
      // deflate record payload after recording success. This will help users access payload as a
      // part of marking
      // record successful.
      newRecord.deflate();
      return true;
    } catch (Exception e) {
      LOG.error("Error writing record  " + newRecord, e);
      writeStatus.markFailure(newRecord, e, recordMetadata);
    }
    return false;
  }

  /**
   * Go through an old record. Here if we detect a newer version shows up, we write the new one to the file.
   */
  public void write(HoodieRecord<T> oldRecord) {
    // Use schema with metadata files no matter whether 'hoodie.populate.meta.fields' is enabled
    // to avoid unnecessary rewrite. Even with metadata table(whereas the option 'hoodie.populate.meta.fields' is configured as false),
    // the record is deserialized with schema including metadata fields,
    // see HoodieMergeHelper#runMerge for more details.
    Schema oldSchema = writeSchemaWithMetaFields;
    Schema newSchema = preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
    boolean copyOldRecord = true;
    String key = oldRecord.getRecordKey(oldSchema, keyGeneratorOpt);
    TypedProperties props = config.getPayloadConfig().getProps();
    if (keyToNewRecords.containsKey(key)) {
      // If we have duplicate records that we are updating, then the hoodie record will be deflated after
      // writing the first record. So make a copy of the record to be merged
      HoodieRecord<T> newRecord = keyToNewRecords.get(key).newInstance();
      try {
        Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
        Schema combineRecordSchema = mergeResult.map(Pair::getRight).orElse(null);
        Option<HoodieRecord> combinedRecord = mergeResult.map(Pair::getLeft);
        if (combinedRecord.isPresent() && combinedRecord.get().shouldIgnore(combineRecordSchema, props)) {
          // If it is an IGNORE_RECORD, just copy the old record, and do not update the new record.
          copyOldRecord = true;
        } else if (writeUpdateRecord(newRecord, oldRecord, combinedRecord, combineRecordSchema)) {
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
        writeToFile(new HoodieKey(key, partitionPath), oldRecord, oldSchema, props, true);
      } catch (IOException | RuntimeException e) {
        String errMsg = String.format("Failed to merge old record into new file for key %s from old file %s to new file %s with writerSchema %s",
            key, getOldFilePath(), newFilePath, writeSchemaWithMetaFields.toString(true));
        LOG.debug("Old record is {}", oldRecord);
        throw new HoodieUpsertException(errMsg, e);
      }
      recordsWritten++;
    }
  }

  protected void writeToFile(HoodieKey key, HoodieRecord<T> record, Schema schema, Properties prop, boolean shouldPreserveRecordMetadata) throws IOException {
    if (shouldPreserveRecordMetadata) {
      // NOTE: `FILENAME_METADATA_FIELD` has to be rewritten to correctly point to the
      //       file holding this record even in cases when overall metadata is preserved
      HoodieRecord populatedRecord = updateFileName(record, schema, writeSchemaWithMetaFields, newFilePath.getName(), prop);
      fileWriter.write(key.getRecordKey(), populatedRecord, writeSchemaWithMetaFields);
    } else {
      // rewrite the record to include metadata fields in schema, and the values will be set later.
      record = record.prependMetaFields(schema, writeSchemaWithMetaFields, new MetadataValues(), config.getProps());
      fileWriter.writeWithMetadata(key, record, writeSchemaWithMetaFields);
    }
  }

  protected void writeIncomingRecords() throws IOException {
    // write out any pending records (this can happen when inserts are turned into updates)
    Iterator<HoodieRecord<T>> newRecordsItr;
    if (keyToNewRecords instanceof ExternalSpillableMap) {
      newRecordsItr = ((ExternalSpillableMap) keyToNewRecords).iterator(key -> !writtenRecordKeys.contains(key));
    } else {
      newRecordsItr = keyToNewRecords.entrySet().stream()
          .filter(e -> !writtenRecordKeys.contains(e.getKey()))
          .map(Map.Entry::getValue)
          .iterator();
    }
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> hoodieRecord = newRecordsItr.next();
      writeInsertRecord(hoodieRecord);
    }
  }

  protected HoodieRecord<T> updateFileName(HoodieRecord<T> record, Schema schema, Schema targetSchema, String fileName, Properties prop) {
    MetadataValues metadataValues = new MetadataValues().setFileName(fileName);
    return record.prependMetaFields(schema, targetSchema, metadataValues, prop);
  }

  @Override
  public List<WriteStatus> close() {
    try {
      if (isClosed()) {
        // Handle has already been closed
        return Collections.emptyList();
      }

      markClosed();
      writeIncomingRecords();

      if (keyToNewRecords instanceof Closeable) {
        ((Closeable) keyToNewRecords).close();
      }

      keyToNewRecords = null;
      writtenRecordKeys = null;

      fileWriter.close();
      fileWriter = null;

      long fileSizeInBytes = storage.getPathInfo(newFilePath).getLength();
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

      performMergeDataValidationCheck(writeStatus);

      LOG.info("MergeHandle for partitionPath {} fileID {}, took {} ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime());

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public void performMergeDataValidationCheck(WriteStatus writeStatus) {
    if (!config.isMergeDataValidationCheckEnabled() || baseFileToMerge == null) {
      return;
    }

    long oldNumWrites = 0;
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getReaderFactory(this.recordMerger.getRecordType())
        .getFileReader(config, oldFilePath)) {
      oldNumWrites = reader.getTotalRecords();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to check for merge data validation", e);
    }

    if ((writeStatus.getStat().getNumWrites() + writeStatus.getStat().getNumDeletes()) < oldNumWrites) {
      throw new HoodieCorruptedDataException(
          String.format("Record write count decreased for file: %s, Partition Path: %s (%s:%d + %d < %s:%d)",
              writeStatus.getFileId(), writeStatus.getPartitionPath(),
              instantTime, writeStatus.getStat().getNumWrites(), writeStatus.getStat().getNumDeletes(),
              baseFileToMerge.getCommitTime(), oldNumWrites));
    }
  }

  public Iterator<List<WriteStatus>> getWriteStatusesAsIterator() {
    List<WriteStatus> statuses = getWriteStatuses();
    // TODO(vc): This needs to be revisited
    if (getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null {}, {}", getOldFilePath(), statuses);
    }
    return Collections.singletonList(statuses).iterator();
  }

  public StoragePath getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  public HoodieBaseFile baseFileForMerge() {
    return baseFileToMerge;
  }

  public void setPartitionFields(Option<String[]> partitionFields) {
    this.partitionFields = partitionFields;
  }

  public Option<String[]> getPartitionFields() {
    return this.partitionFields;
  }

  public void setPartitionValues(Object[] partitionValues) {
    this.partitionValues = partitionValues;
  }

  public Object[] getPartitionValues() {
    return this.partitionValues;
  }
}
