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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * An extension of {@link KeyBasedFileGroupRecordBuffer} that sorts the log records based on the record key when joining with the base file records.
 * This assumes that the base file records are already sorted by the record key.
 */
class SortedKeyBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T> {

  // when sorting is enabled, this will hold the base file record if it was not used in the previous iteration
  private Option<T> queuedBaseFileRecord = Option.empty();
  private Queue<String> logRecordKeysSorted = null;

  SortedKeyBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                             HoodieTableMetaClient hoodieTableMetaClient,
                                             RecordMergeMode recordMergeMode,
                                             PartialUpdateMode partialUpdateMode,
                                             IteratorMode iteratorMode,
                                             TypedProperties props,
                                             List<String> orderingFieldNames,
                                             UpdateProcessor<T> updateProcessor) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, iteratorMode, props, orderingFieldNames, updateProcessor);
  }

  @Override
  protected void initializeLogRecordIterator() {
    logRecordIterator = records.values().stream().sorted(Comparator.comparing(BufferedRecord::getRecordKey)).iterator();
  }

  @Override
  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
    int comparison = 0;
    while (!getLogRecordKeysSorted().isEmpty() && (comparison = getLogRecordKeysSorted().peek().compareTo(recordKey)) <= 0) {
      String nextLogRecordKey = getLogRecordKeysSorted().poll();
      if (comparison == 0) {
        break; // Log record key matches the base record key, exit loop after removing the key from the queue of log record keys
      }
      // Handle the case where the next record is only present in the log records
      BufferedRecord<T> nextLogRecord = records.remove(nextLogRecordKey);
      nextRecord = updateProcessor.processUpdate(recordKey, null, nextLogRecord, nextLogRecord.isDelete());
      if (nextRecord != null) {
        // If the next log record does not result in a deletion, or we are emitting deletes, we can return it
        // and queue the base record, which is already read from the iterator, for the next iteration
        queuedBaseFileRecord = Option.of(baseRecord);
        return true;
      }
      // Iterate until the next log record key is greater than or equal to the base record key
    }
    // if last comparison shows the keys are equal, we remove the log record info, otherwise we can just assume there is no log record for this key
    BufferedRecord<T> logRecordInfo = comparison == 0 ? records.remove(recordKey) : null;
    return hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    if (queuedBaseFileRecord.isPresent()) {
      T nextRecord = queuedBaseFileRecord.get();
      queuedBaseFileRecord = Option.empty();
      if (hasNextBaseRecord(nextRecord)) {
        return true;
      }
    }
    return super.doHasNext();
  }

  private Queue<String> getLogRecordKeysSorted() {
    if (logRecordKeysSorted == null) {
      logRecordKeysSorted = records.keySet().stream().map(Object::toString).collect(Collectors.toCollection(PriorityQueue::new));
    }
    return logRecordKeysSorted;
  }
}
