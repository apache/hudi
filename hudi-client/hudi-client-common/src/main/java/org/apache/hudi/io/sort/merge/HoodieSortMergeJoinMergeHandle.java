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

package org.apache.hudi.io.sort.merge;

import org.apache.hudi.avro.HoodieFileFooterSupport;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.compaction.SortMergeCompactionHelper;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.SortMergeReader;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class HoodieSortMergeJoinMergeHandle<T, I, K, O> extends HoodieMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSortMergeJoinMergeHandle.class);

  private final Iterator<HoodieRecord> logRecords;
  private Option<HoodieRecord> currentLogRecord = Option.empty();

  public HoodieSortMergeJoinMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                        Iterator<HoodieRecord> recordItr, String partitionPath, String fileId, HoodieBaseFile baseFile,
                                        TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, Collections.emptyMap(), partitionPath, fileId, baseFile, taskContextSupplier, keyGeneratorOpt);
    this.logRecords = recordItr;
    init();
  }

  private void init() {
    if (logRecords.hasNext()) {
      currentLogRecord = Option.of(logRecords.next());
    }
  }

  @Override
  public void write(HoodieRecord<T> oldRecord) {
    Schema oldSchema = writeSchemaWithMetaFields;
    String oldKey = oldRecord.getRecordKey(oldSchema, keyGeneratorOpt);
    // write all log records which have a key less than the base record
    while (currentLogRecord.isPresent() && SortMergeCompactionHelper.DEFAULT_RECORD_KEY_COMPACTOR.compare(currentLogRecord.get().getRecordKey(), oldKey) < 0) {
      // pick the current log record to write out
      try {
        writeInsertRecord(currentLogRecord.get());
      } catch (IOException e) {
        LOG.error("Error writing record: {} to file: {}", currentLogRecord.get(), newFilePath, e);
        throw new HoodieUpsertException("Failed to write log record into new file", e);
      }
      // roll to next log record
      currentLogRecord = logRecords.hasNext() ? Option.of(logRecords.next()) : Option.empty();
    }
    super.write(oldRecord);
  }

  @Override
  protected void markRecordWritten(String key) {
    if (currentLogRecord.isPresent() && currentLogRecord.get().getRecordKey().equals(key)) {
      // roll to next log record
      currentLogRecord = logRecords.hasNext() ? Option.of(logRecords.next()) : Option.empty();
    }
  }

  @Override
  protected boolean containsKey(String key) {
    return currentLogRecord.isPresent() && currentLogRecord.get().getRecordKey().equals(key);
  }

  @Override
  protected HoodieRecord getRecordByKey(String key) {
    return currentLogRecord.isPresent() && currentLogRecord.get().getKey().getRecordKey().equals(key) ? currentLogRecord.get() : null;
  }

  @Override
  protected void writeIncomingRecords() throws IOException {
    // write out all log records which have a key greater than the last base record
    while (currentLogRecord.isPresent()) {
      writeInsertRecord(currentLogRecord.get());
      currentLogRecord = logRecords.hasNext() ? Option.of(logRecords.next()) : Option.empty();
    }
  }

  @Override
  protected void closeInner() throws IOException {
    // add metadata about sorted
    fileWriter.writeFooterMetadata(HoodieFileFooterSupport.HOODIE_BASE_FILE_SORTED, "true");
    // set merged record in log
    if (logRecords instanceof SortMergeReader) {
      writeStatus.getStat().setTotalUpdatedRecordsCompacted(((SortMergeReader) logRecords).getCombinedRecordsNum());
    }
    if (logRecords instanceof ClosableIterator) {
      ((ClosableIterator) logRecords).close();
    }
    LOG.info("SortedMergeHandle for partitionPath " + partitionPath + " fileID " + fileId + " wrote with sorted records");
  }

}
