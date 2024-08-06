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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class HoodieUnmergedCreateHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieUnmergedCreateHandle.class);

  private Iterator<HoodieRecord> unmergedRecordsIter;

  private Option<HoodieRecord<T>> currentMergedRecord = Option.empty();

  private long recordsMerge = 0;

  private boolean currentMergedRecordMerged = false;

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId,
                                    TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                                    boolean preserveMetadata) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, preserveMetadata);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId,
                                    Option<Schema> overriddenSchema, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, overriddenSchema, taskContextSupplier);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                    String partitionPath, String fileId, Iterator<HoodieRecord> recordItr,
                                    TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, true);
    unmergedRecordsIter = recordItr;
    useWriterSchema = true;
  }

  @Override
  public void write() {
    // Iterate all unmerged records and write them out
    if (!unmergedRecordsIter.hasNext()) {
      LOG.info("No unmerged records to write for partition path " + partitionPath + " and fileId " + fileId);
      return;
    }
    while (unmergedRecordsIter.hasNext()) {
      HoodieRecord unmergedRecord = unmergedRecordsIter.next();
      merge(unmergedRecord);
    }

    // Write out the last merged record
    if (currentMergedRecord.isPresent()) {
      write(currentMergedRecord.get(), writeSchemaWithMetaFields, config.getProps());
    }
  }

  private void merge(HoodieRecord newRecord) {

    // for update record, merge new record with current merged record
    HoodieRecord<T> record = (HoodieRecord<T>) newRecord;
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
    write(currentMergedRecord.get(), writeSchemaWithMetaFields, config.getProps());
    currentMergedRecord = Option.of(record);
    currentMergedRecordMerged = false;
  }
}
