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

public interface BufferedRecordConverter<T> {
  BufferedRecord<T> convert(T record);

  static <T> BufferedRecordConverter<T> createConverter(
      IteratorMode iteratorMode, Schema readerSchema, RecordContext<T> recordContext, List<String> orderingFieldNames) {
    switch (iteratorMode) {
      case ENGINE_RECORD:
        return new BufferedRecordConverter<T>() {
          @Override
          public BufferedRecord<T> convert(T record) {
            return BufferedRecord.forRecord(record);
          }
        };
      case RECORD_KEY:
        return new BufferedRecordConverter<T>() {
          @Override
          public BufferedRecord<T> convert(T record) {
            return BufferedRecord.forRecordKey(record, readerSchema, recordContext);
          }
        };
      default:
        return new BufferedRecordConverter<T>() {
          @Override
          public BufferedRecord<T> convert(T record) {
            return BufferedRecord.forRecordWithContext(record, readerSchema, recordContext, orderingFieldNames, false);
          }
        };
    }
  }
}
