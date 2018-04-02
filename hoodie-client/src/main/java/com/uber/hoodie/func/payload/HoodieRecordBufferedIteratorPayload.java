/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func.payload;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

/**
 * BufferedIteratorPayload that takes HoodieRecord as input and transforms to output Optional<IndexedRecord>
 * @param <T>
 */
public class HoodieRecordBufferedIteratorPayload<T extends HoodieRecordPayload>
    extends AbstractBufferedIteratorPayload<HoodieRecord<T>, Optional<IndexedRecord>> {

  // It caches the exception seen while fetching insert value.
  public Optional<Exception> exception = Optional.empty();

  public HoodieRecordBufferedIteratorPayload(HoodieRecord record, Schema schema) {
    super(record);
    try {
      this.outputPayload = record.getData().getInsertValue(schema);
    } catch (Exception e) {
      this.exception = Optional.of(e);
    }
  }

  public Optional<Exception> getException() {
    return exception;
  }
}
