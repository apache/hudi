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

import org.apache.hudi.avro.HoodieFileFooterSupport;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.log.HoodieUnMergedSortedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class HoodieSortedMerge2Handle<T, I, K, O> extends HoodieMergeHandle<T, I, K, O> {

  private Set<HoodieKey> writtenKeys = new HashSet<>();

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSortedMerge2Handle.class);

  private final Iterator<HoodieRecord> logRecords;
  private HoodieRecord iterCurrentRecord;
  private Option<HoodieRecord<T>> currentMergedRecord = Option.empty();
  private boolean currentMergedRecordMerged = false;

  private long recordsMerge = 0;

  public HoodieSortedMerge2Handle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                  Iterator<HoodieRecord> recordItr, String partitionPath, String fileId, HoodieBaseFile baseFile,
                                  TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, Collections.emptyMap(), partitionPath, fileId, baseFile, taskContextSupplier, keyGeneratorOpt);
    this.logRecords = recordItr;
    init();
  }

  private void init() {
    if (logRecords.hasNext()) {
      iterCurrentRecord = logRecords.next();
    }
  }

  @Override
  public void write(HoodieRecord<T> oldRecord) {
    while (iterCurrentRecord != null && HoodieUnMergedSortedLogRecordScanner.DEFAULT_KEY_COMPARATOR.compare(iterCurrentRecord.getKey(), oldRecord.getKey()) < 0) {
      // pick the to-be-merged record from logRecords iterator
      merge(iterCurrentRecord);
      iterCurrentRecord = logRecords.hasNext() ? logRecords.next() : null;
    }

    // no more valid log records in the iterator, pick the old record as the merged record
    merge(oldRecord);
  }

  private void merge(HoodieRecord<T> record) {

    // for update record, merge new record with current merged record
    if (currentMergedRecord.isEmpty()) {
      currentMergedRecord = Option.of(record);
      currentMergedRecordMerged = false;
      return;
    }
    if (record.getRecordKey().equals(currentMergedRecord.get().getRecordKey())) {
      // run merge logic when two records have the same key
      Schema oldSchema = writeSchemaWithMetaFields;
      Schema newSchema = preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
      TypedProperties props = config.getPayloadConfig().getProps();
      Option<Pair<HoodieRecord, Schema>> mergeResult;
      try {
        mergeResult = recordMerger.merge(currentMergedRecord.get(), oldSchema, record, newSchema, props);
        Schema combineRecordSchema = mergeResult.map(Pair::getRight).orElse(null);
        Option<HoodieRecord> combinedRecord = mergeResult.map(Pair::getLeft);
        if (combinedRecord.isEmpty() || combinedRecord.get().shouldIgnore(combineRecordSchema, props)) {
          // After merging return an empty record, so just mark currentMergedRecord as empty
          currentMergedRecord = Option.empty();
          currentMergedRecordMerged = false;
          recordsDeleted++;
          return;
        }

        recordsMerge++;
        combinedRecord.get().setKey(currentMergedRecord.get().getKey());
        currentMergedRecord = Option.of(combinedRecord.get());
        currentMergedRecordMerged = true;
        return;
      } catch (Exception e) {
        throw new HoodieUpsertException("Merge failed for key " + record.getRecordKey(), e);
      }
    }

    // different keys, write current merged record and update it with new record
    writeRecord(currentMergedRecord.get(), currentMergedRecordMerged);
    currentMergedRecord = Option.of(record);
    currentMergedRecordMerged = false;
  }

  private void writeRecord(HoodieRecord<T> record, boolean merged) {
    Option recordMetadata = record.getMetadata();
    if (!partitionPath.equals(record.getPartitionPath())) {
      HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
          + record.getPartitionPath() + " but trying to upsert into partition: " + partitionPath);
      writeStatus.markFailure(record, failureEx, recordMetadata);
      return;
    }
    Schema newSchema = preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
    try {
      writeToFile(record.getKey(), record, newSchema, config.getPayloadConfig().getProps(), preserveMetadata);
      writeStatus.markSuccess(record, recordMetadata);
      recordsWritten++;
      if (writtenKeys.contains(record.getKey())) {
        throw new HoodieUpsertException("Duplicate key found in records written out, key :" + record.getKey());
      }
      writtenKeys.add(record.getKey());
      if (merged) {
        // update
        updatedRecordsWritten++;
      } else {
        // insert
        insertRecordsWritten++;
      }
    } catch (Exception e) {
      LOG.error("Error writing record " + record, e);
      writeStatus.markFailure(record, e, recordMetadata);
    }
  }

  @Override
  public List<WriteStatus> close() {
    if (isClosed()) {
      return Collections.emptyList();
    }
    markClosed();
    // write all records still stay in the iterator
    try {
      writeRemainingRecords();
      // add metadata about sorted
      fileWriter.writeFooterMetadata(HoodieFileFooterSupport.HOODIE_BASE_FILE_SORTED, "true");
      // close file writer
      fileWriter.close();
      fileWriter = null;
      long fileSizeInBytes = HadoopFSUtils.getFileSize(fs, new Path(newFilePath.toUri()));
      HoodieWriteStat stat = writeStatus.getStat();

      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      LOG.info("MergeHandle Stats: TotalWrites=" + recordsWritten + ", " + "TotalDeletes=" + recordsDeleted + ", "
          + "TotalUpdateWrites=" + updatedRecordsWritten + ", " + "TotalInserts=" + insertRecordsWritten + ", "
          + "TotalErrors=" + writeStatus.getTotalErrorRecords() + ", " + "TotalBytesWritten=" + fileSizeInBytes + ", "
          + "TotalRecordsSize=" + recordsMerge + ", " + "TotalUpsertTime=" + runtimeStats.getTotalUpsertTime() + " ("
          + runtimeStats.getTotalUpsertTime() + "ms)");
      LOG.info(String.format("SortedMerge2Handle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));
      LOG.info("SortedMerge2Handle successfully merge: {} to a new file: {}", oldFilePath, newFilePath);

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close HoodieSortedMerge2Handle", e);
    }
  }

  private void writeRemainingRecords() throws IOException {
    // write remaining records in the iterator
    while (iterCurrentRecord != null) {
      merge(iterCurrentRecord);
      iterCurrentRecord = logRecords.hasNext() ? logRecords.next() : null;
    }
    // write the last merged record
    if (currentMergedRecord.isPresent()) {
      writeRecord(currentMergedRecord.get(), currentMergedRecordMerged);
      currentMergedRecord = Option.empty();
      currentMergedRecordMerged = false;
    }
  }
}
