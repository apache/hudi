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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Delete block backed by a native delete log file.
 */
public class HoodieNativeDeleteBlock extends HoodieDeleteBlock {

  private final HoodieStorage storage;
  private final HoodieLogFile logFile;
  private final HoodieReaderContext<?> readerContext;
  private final HoodieSchema deleteLogSchema;
  private final List<String> orderingFieldNames;
  private List<BufferedRecord<?>> bufferedRecordsToDelete;

  public HoodieNativeDeleteBlock(HoodieStorage storage,
                                 HoodieLogFile logFile,
                                 HoodieReaderContext<?> readerContext,
                                 HoodieSchema deleteLogSchema,
                                 List<String> orderingFieldNames,
                                 Map<HeaderMetadataType, String> header,
                                 Map<FooterMetadataType, String> footer) {
    super(Option.empty(), null, true, getContentLocation(storage, logFile), header, footer);
    this.storage = storage;
    this.logFile = logFile;
    this.readerContext = readerContext;
    this.deleteLogSchema = deleteLogSchema;
    this.orderingFieldNames = orderingFieldNames;
  }

  @Override
  public DeleteRecord[] getRecordsToDelete() {
    throw new HoodieNotSupportedException("Native delete log files do not support the legacy DeleteRecord[] API. "
        + "Use getRecordsToDelete(RecordContext) instead. Log file: " + logFile);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> List<BufferedRecord<T>> getRecordsToDelete(RecordContext<T> recordContext) {
    if (bufferedRecordsToDelete == null) {
      bufferedRecordsToDelete = (List) readBufferedRecordsToDelete(recordContext);
    }
    return (List) bufferedRecordsToDelete;
  }

  @SuppressWarnings("unchecked")
  private <T> List<BufferedRecord<T>> readBufferedRecordsToDelete(RecordContext<T> recordContext) {
    HoodieReaderContext<T> typedReaderContext = (HoodieReaderContext<T>) readerContext;
    List<BufferedRecord<T>> deleteRecords = new ArrayList<>();
    try (ClosableIterator<T> recordIterator = typedReaderContext.getFileRecordIterator(
        logFile.getPath(), 0, FSUtils.getFileSize(storage, logFile), deleteLogSchema, deleteLogSchema, storage)) {
      while (recordIterator.hasNext()) {
        T record = recordIterator.next();
        Object recordKey = recordContext.getValue(record, deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
        Comparable orderingValue = recordContext.getOrderingValue(record, deleteLogSchema, orderingFieldNames);
        deleteRecords.add(BufferedRecords.createDelete(recordKey.toString(), orderingValue));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read native delete log file " + logFile, e);
    }
    return deleteRecords;
  }

  private static Option<HoodieLogBlockContentLocation> getContentLocation(HoodieStorage storage, HoodieLogFile logFile) {
    long fileSize = FSUtils.getFileSize(storage, logFile);
    return Option.of(new HoodieLogBlockContentLocation(storage, logFile, 0, fileSize, fileSize));
  }
}
