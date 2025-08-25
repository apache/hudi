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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
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
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Handle to merge incoming records to those in storage row-by-row.
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
@SuppressWarnings("Duplicates")
@NotThreadSafe
public class HoodieWriteMergeHandle<T, I, K, O> extends HoodieAbstractMergeHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteMergeHandle.class);

  protected Map<String, HoodieRecord<T>> keyToNewRecords;
  protected Set<String> writtenRecordKeys;
  protected HoodieFileWriter fileWriter;

  protected long recordsWritten = 0;
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;

  public HoodieWriteMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    this(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier,
        getLatestBaseFile(hoodieTable, partitionPath, fileId), keyGeneratorOpt);
  }

  public HoodieWriteMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, baseFile, keyGeneratorOpt, false);
    populateIncomingRecordsMap(recordItr);
    initMarkerFileAndFileWriter(fileId, partitionPath);
  }

  /**
   * Called by compactor code path.
   */
  public HoodieWriteMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                                HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier,
                                Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, dataFileToBeMerged, keyGeneratorOpt,
        // preserveMetadata is disabled by default for MDT but enabled otherwise
        !HoodieTableMetadata.isMetadataTable(config.getBasePath()));
    this.keyToNewRecords = keyToNewRecords;
    initMarkerFileAndFileWriter(fileId, this.partitionPath);
  }

  /**
   * Used by `FileGroupReaderBasedMergeHandle`.
   *
   * @param config              Hudi write config
   * @param instantTime         Instant time to use
   * @param partitionPath       Partition path
   * @param fileId              File group ID for the merge handle to operate on
   * @param hoodieTable         {@link HoodieTable} instance
   * @param taskContextSupplier Task context supplier
   */
  public HoodieWriteMergeHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                                String fileId, HoodieTable<T, I, K, O> hoodieTable, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, true);
  }

  @Override
  public void doMerge() throws IOException {
    HoodieMergeHelper.newInstance().runMerge(hoodieTable, this);
  }

  /**
   * Initialize marker file and file writer.
   */
  private void initMarkerFileAndFileWriter(String fileId, String partitionPath) {
    this.writtenRecordKeys = new HashSet<>();
    try {
      // Create Marker file,
      // uses name of `newFilePath` instead of `newFileName`
      // in case the subclass may roll over the file handle name.
      createMarkerFile(partitionPath, newFilePath.getName());
      // Create the writer for writing the new version file
      fileWriter = HoodieFileWriterFactory.getFileWriter(
          instantTime, newFilePath, hoodieTable.getStorage(),
          config, writeSchemaWithMetaFields, taskContextSupplier, getRecordType());
    } catch (IOException io) {
      LOG.error("Error in update task at commit {}", instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  protected HoodieRecord.HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  /**
   * Initialize a spillable map for incoming records.
   */
  protected void initIncomingRecordsMap() {
    try {
      // Load the new records in a map
      long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config);
      LOG.info("MaxMemoryPerPartitionMerge => {}", memoryForMerge);
      this.keyToNewRecords = new ExternalSpillableMap<>(memoryForMerge, config.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(), new HoodieRecordSizeEstimator<>(writeSchema),
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
  protected void populateIncomingRecordsMap(Iterator<HoodieRecord<T>> newRecordsItr) {
    initIncomingRecordsMap();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      // update the new location of the record, so we know where to find it next
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(newRecordLocation);
        record.seal();
      }
      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      keyToNewRecords.put(record.getRecordKey(), record);
    }
    if (keyToNewRecords instanceof ExternalSpillableMap) {
      ExternalSpillableMap<String, HoodieRecord<T>> spillableMap = (ExternalSpillableMap<String, HoodieRecord<T>>) keyToNewRecords;
      LOG.info("Number of entries in MemoryBasedMap => {}, Total size in bytes of MemoryBasedMap => {}, "
          + "Number of entries in BitCaskDiskMap => {}, Size of file spilled to disk => {}",
          spillableMap.getInMemoryMapNumEntries(), spillableMap.getCurrentInMemoryMapSize(), spillableMap.getDiskBasedMapNumEntries(), spillableMap.getSizeOfFileOnDiskInBytes());
    }
  }

  public boolean isEmptyNewRecords() {
    return keyToNewRecords.isEmpty();
  }

  protected boolean writeUpdateRecord(HoodieRecord<T> newRecord, HoodieRecord<T> oldRecord, Option<HoodieRecord> combineRecordOpt, Schema writerSchema) throws IOException {
    boolean isDelete = false;
    if (combineRecordOpt.isPresent()) {
      if (oldRecord.getData() != combineRecordOpt.get().getData()) {
        // the incoming record is chosen
        isDelete = isDeleteRecord(newRecord);
      } else {
        // the incoming record is dropped
        return false;
      }
      updatedRecordsWritten++;
    }
    return writeRecord(newRecord, oldRecord, combineRecordOpt, writerSchema, config.getPayloadConfig().getProps(), isDelete);
  }

  protected void writeInsertRecord(HoodieRecord<T> newRecord) throws IOException {
    Schema schema = getNewSchema();
    // just skip the ignored record
    if (newRecord.shouldIgnore(schema, config.getProps())) {
      return;
    }
    writeInsertRecord(newRecord, schema, config.getProps());
  }

  protected void writeInsertRecord(HoodieRecord<T> newRecord, Schema schema, Properties prop)
      throws IOException {
    if (writeRecord(newRecord, null, Option.of(newRecord), schema, prop, isDeleteRecord(newRecord))) {
      insertRecordsWritten++;
    }
  }

  protected boolean writeRecord(HoodieRecord<T> newRecord, Option<HoodieRecord> combineRecord, Schema schema, Properties prop) throws IOException {
    return writeRecord(newRecord, null, combineRecord, schema, prop, false);
  }

  /**
   * Check if a record is a DELETE/UPDATE_BEFORE marked by the '_hoodie_operation' field.
   */
  private boolean isDeleteRecord(HoodieRecord<T> record) {
    HoodieOperation operation = record.getOperation();
    return HoodieOperation.isDelete(operation) || HoodieOperation.isUpdateBefore(operation);
  }

  /**
   * The function takes the different versions of the record - old record, new incoming record and combined record
   * created by merging the old record with the new incoming record. It decides whether the combined record needs to be
   * written to the file and writes the record accordingly.
   *
   * @param newRecord     The new incoming record
   * @param oldRecord     The value of old record
   * @param combineRecord Record created by merging the old record with the new incoming record
   * @param schema        Record schema
   * @param props         Properties
   * @param isDelete      Whether the new record is a delete record
   *
   * @return true if the record was written successfully
   * @throws IOException
   */
  private boolean writeRecord(HoodieRecord<T> newRecord,
                              @Nullable HoodieRecord<T> oldRecord,
                              Option<HoodieRecord> combineRecord,
                              Schema schema,
                              Properties props,
                              boolean isDelete) {
    Option<Map<String, String>> recordMetadata = getRecordMetadata(newRecord, schema, props);
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

        if (decision) {
          // CASE (1): Flush the merged record.
          HoodieKey hoodieKey = newRecord.getKey();
          if (isSecondaryIndexStatsStreamingWritesEnabled) {
            SecondaryIndexStreamingTracker.trackSecondaryIndexStats(hoodieKey, combineRecord, oldRecord, false, writeStatus,
                writeSchemaWithMetaFields, this::getNewSchema, secondaryIndexDefns, keyGeneratorOpt, config);
          }
          writeToFile(hoodieKey, combineRecord.get(), schema, props, preserveMetadata);
          recordsWritten++;
        } else {
          // CASE (2): A delete operation.
          if (isSecondaryIndexStatsStreamingWritesEnabled) {
            SecondaryIndexStreamingTracker.trackSecondaryIndexStats(newRecord.getKey(), combineRecord, oldRecord, true, writeStatus,
                writeSchemaWithMetaFields, this::getNewSchema, secondaryIndexDefns, keyGeneratorOpt, config);
          }
          recordsDeleted++;
        }
      } else {
        if (isSecondaryIndexStatsStreamingWritesEnabled) {
          SecondaryIndexStreamingTracker.trackSecondaryIndexStats(newRecord.getKey(), combineRecord, oldRecord, true, writeStatus,
              writeSchemaWithMetaFields, this::getNewSchema, secondaryIndexDefns, keyGeneratorOpt, config);
        }
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
      LOG.error("Error writing record {}", newRecord, e);
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
    Schema newSchema = getNewSchema();
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
      HoodieRecord populatedRecord = record.updateMetaField(schema, HoodieRecord.FILENAME_META_FIELD_ORD, newFilePath.getName());
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

  private Schema getNewSchema() {
    return preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
  }

  @Override
  public List<WriteStatus> close() {
    try {
      if (isClosed()) {
        // Handle has already been closed
        return Collections.singletonList(writeStatus);
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
}
