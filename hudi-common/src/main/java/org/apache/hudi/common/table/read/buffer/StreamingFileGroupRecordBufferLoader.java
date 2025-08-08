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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Records based {@link FileGroupRecordBuffer} which takes in a map of records to be merged with a base file of interest.
 * This will be used in write paths for COW merge cases.
 * @param <T> Engine native presentation of the record.
 */
public class StreamingFileGroupRecordBufferLoader<T> implements FileGroupRecordBufferLoader<T> {
  private final Schema recordSchema;

  StreamingFileGroupRecordBufferLoader(Schema recordSchema) {
    this.recordSchema = recordSchema;
  }

  @Override
  public Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext, HoodieStorage storage, InputSplit inputSplit,
                                                                            List<String> orderingFieldNames, HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props, ReaderParameters readerParameters, HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.emitDeletes(), fileGroupUpdateCallback);
    FileGroupRecordBuffer<T> recordBuffer;
    if (readerParameters.sortOutputs()) {
      recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldNames, updateProcessor);
    } else {
      recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldNames, updateProcessor);
    }

    RecordContext<T> recordContext = readerContext.getRecordContext();
    Iterator<HoodieRecord> recordIterator = inputSplit.getRecordIterator();
    String[] orderingFieldsArray = orderingFieldNames.toArray(new String[0]);
    while (recordIterator.hasNext()) {
      HoodieRecord hoodieRecord = recordIterator.next();
      T data = recordContext.extractDataFromRecord(hoodieRecord, recordSchema, props);
      try {
        // we use -U operation to represent the record should be ignored during updating index.
        HoodieOperation hoodieOperation = hoodieRecord.getIgnoreIndexUpdate() ? HoodieOperation.UPDATE_BEFORE : hoodieRecord.getOperation();
        BufferedRecord<T> bufferedRecord;
        if (data == null) {
          DeleteRecord deleteRecord = DeleteRecord.create(hoodieRecord.getKey(), hoodieRecord.getOrderingValue(recordSchema, props, orderingFieldsArray));
          bufferedRecord = BufferedRecords.fromDeleteRecord(deleteRecord, deleteRecord.getOrderingValue(), hoodieOperation);
        } else {
          // HoodieRecord#isDelete does not check if a record is a DELETE marked by a custom delete marker,
          // so we use recordContext#isDeleteRecord here if the data field is not null.
          boolean isDelete = recordContext.isDeleteRecord(data, recordBuffer.getDeleteContext());
          bufferedRecord = BufferedRecords.fromEngineRecord(data, recordSchema, recordContext, orderingFieldNames, BufferedRecords.inferOperation(isDelete, hoodieOperation));
        }
        recordBuffer.processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to process next buffered record", e);
      }
    }
    return Pair.of(recordBuffer, Collections.emptyList());
  }
}