/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into a record key based map.
 * The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class KeyBasedFileGroupRecordBuffer<T> extends FileGroupRecordBuffer<T> {

  public KeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                       HoodieTableMetaClient hoodieTableMetaClient,
                                       RecordMergeMode recordMergeMode,
                                       Option<PartialUpdateMode> partialUpdateModeOpt,
                                       TypedProperties props,
                                       List<String> orderingFieldNames,
                                       UpdateProcessor<T> updateProcessor) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    Pair<ClosableIterator<T>, HoodieSchema> recordsIteratorSchemaPair =
        getRecordsIterator(dataBlock, keySpecOpt);
    if (dataBlock.containsPartialUpdates() && !enablePartialMerging) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
      bufferedRecordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          recordMergeMode,
          true,
          recordMerger,
          readerSchema.toAvroSchema(),
          payloadClasses,
          props,
          partialUpdateModeOpt);
    }

    // TODO: Add HoodieSchemaCache#intern after #14374 is merged (this schema is interned originaly)
    HoodieSchema schema = recordsIteratorSchemaPair.getRight();

    RecordContext<T> recordContext = readerContext.getRecordContext();
    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        boolean isDelete = recordContext.isDeleteRecord(nextRecord, deleteContext);
        BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(nextRecord, schema, readerContext.getRecordContext(), orderingFieldNames, isDelete);
        processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      }
    }
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    BufferedRecord<T> existingRecord = records.get(recordKey);
    totalLogRecords++;
    bufferedRecordMerger.deltaMerge(record, existingRecord).ifPresent(bufferedRecord ->
        records.put(recordKey, bufferedRecord.toBinary(readerContext.getRecordContext())));
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
    while (it.hasNext()) {
      DeleteRecord record = it.next();
      processNextDeletedRecord(record, record.getRecordKey());
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordIdentifier) {
    BufferedRecord<T> existingRecord = records.get(recordIdentifier);
    totalLogRecords++;
    Option<DeleteRecord> recordOpt = bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord);
    recordOpt.ifPresent(deleteRec ->
        records.put(recordIdentifier, BufferedRecords.fromDeleteRecord(deleteRec, readerContext.getRecordContext())));
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
    BufferedRecord<T> logRecordInfo = records.remove(recordKey);
    return hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    // Handle merging.
    while (baseFileIterator.hasNext()) {
      if (hasNextBaseRecord(baseFileIterator.next())) {
        return true;
      }
    }

    // Handle records solely from log files.
    return hasNextLogRecord();
  }

  public boolean isPartialMergingEnabled() {
    return enablePartialMerging;
  }
}
