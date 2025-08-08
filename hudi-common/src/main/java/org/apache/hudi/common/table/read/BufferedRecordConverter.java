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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.RecordContext;

import org.apache.avro.Schema;

import java.util.List;

/**
 * The converter used to convert the engine-specific record into {@link BufferedRecord}
 * according to different {@link IteratorMode}s for the file group reader.
 */
public interface BufferedRecordConverter<T> {
  BufferedRecord<T> convert(T record);

  static <T> BufferedRecordConverter<T> createConverter(
      IteratorMode iteratorMode, Schema readerSchema, RecordContext<T> recordContext, List<String> orderingFieldNames) {
    switch (iteratorMode) {
      case ENGINE_RECORD:
        return new BufferedRecordConverter<T>() {
          // Reused BufferedRecord wrapper for engine record iterator to avoid creating that at record level.
          // It's safe because the converter is used in single thread and the iterator does not emit the reused `BufferedRecord`.
          private final BufferedRecord<T> reusedBufferedRecord = new BufferedRecord<>();

          @Override
          public BufferedRecord<T> convert(T record) {
            return reusedBufferedRecord.replaceRecord(record);
          }
        };
      case RECORD_KEY:
        return new BufferedRecordConverter<T>() {
          // Reused BufferedRecord wrapper for record key iterator to avoid creating that at record level.
          // It's safe because the converter is used in single thread and the iterator does not emit the reused `BufferedRecord`.
          private final BufferedRecord<T> reusedBufferedRecord = new BufferedRecord<>();

          @Override
          public BufferedRecord<T> convert(T record) {
            String recordKey = recordContext.getRecordKey(record, readerSchema);
            return reusedBufferedRecord.replaceRecordKey(recordKey);
          }
        };
      default:
        return new BufferedRecordConverter<T>() {
          @Override
          public BufferedRecord<T> convert(T record) {
            return BufferedRecords.forRecordWithContext(record, readerSchema, recordContext, orderingFieldNames, false);
          }
        };
    }
  }
}
