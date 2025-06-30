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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

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
                                       TypedProperties props,
                                       HoodieReadStats readStats,
                                       Option<String> orderingFieldName,
                                       boolean emitDelete) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, props, readStats, orderingFieldName, emitDelete);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    Pair<ClosableIterator<T>, Schema> recordsIteratorSchemaPair =
        getRecordsIterator(dataBlock, keySpecOpt);
    if (dataBlock.containsPartialUpdates()) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
    }

    Schema schema = AvroSchemaCache.intern(recordsIteratorSchemaPair.getRight());

    try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        boolean isDelete = isBuiltInDeleteRecord(nextRecord) || isCustomDeleteRecord(nextRecord) || isDeleteHoodieOperation(nextRecord);
        BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(nextRecord, schema, readerContext, orderingFieldName, isDelete);
        processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      }
    }
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    BufferedRecord<T> existingRecord = records.get(recordKey);
    Option<BufferedRecord<T>> bufferRecord = doProcessNextDataRecord(record, existingRecord);

    if (bufferRecord.isPresent()) {
      records.put(recordKey, bufferRecord.get().toBinary(readerContext));
    }
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
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordKey) {
    BufferedRecord<T> existingRecord = records.get(recordKey);
    Option<DeleteRecord> recordOpt = doProcessNextDeletedRecord(deleteRecord, existingRecord);
    if (recordOpt.isPresent()) {
      Comparable orderingValue = getOrderingValue(readerContext, recordOpt.get());
      records.put(recordKey, BufferedRecord.forDeleteRecord(deleteRecord, orderingValue));
    }
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.containsKey(recordKey);
  }

  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordKey(baseRecord, readerSchema);
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
}
