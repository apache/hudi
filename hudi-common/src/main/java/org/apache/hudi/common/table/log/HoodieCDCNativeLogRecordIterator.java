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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.IndexedRecord;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * CDC log record iterator for native CDC log files read as engine-specific rows.
 *
 * @param <T> Engine-specific record type used by native CDC log files
 */
public class HoodieCDCNativeLogRecordIterator<T> implements HoodieCDCLogRecordIterator<T> {

  private final Iterator<String> cdcFileIterator;
  private final Function<String, ClosableIterator<T>> recordIteratorFunc;
  private final HoodieCDCEngineRecordAccessor<T> recordAccessor;
  private ClosableIterator<T> recordIterator;

  public HoodieCDCNativeLogRecordIterator(
      Iterator<String> cdcFileIterator,
      Function<String, ClosableIterator<T>> recordIteratorFunc,
      HoodieCDCEngineRecordAccessor<T> recordAccessor) {
    this.cdcFileIterator = cdcFileIterator;
    this.recordIteratorFunc = recordIteratorFunc;
    this.recordAccessor = recordAccessor;
  }

  @Override
  public boolean hasNext() {
    while (recordIterator == null || !recordIterator.hasNext()) {
      if (recordIterator != null) {
        recordIterator.close();
        recordIterator = null;
      }
      if (!cdcFileIterator.hasNext()) {
        return false;
      }
      recordIterator = recordIteratorFunc.apply(cdcFileIterator.next());
      ValidationUtils.checkState(recordIterator != null, "Native CDC record iterator must not be null");
    }
    return true;
  }

  @Override
  public HoodieCDCLogRecord<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more CDC log records");
    }
    return new NativeCDCLogRecord<>(recordIterator.next(), recordAccessor);
  }

  @Override
  public void close() {
    if (recordIterator != null) {
      recordIterator.close();
      recordIterator = null;
    }
  }

  private static class NativeCDCLogRecord<T> implements HoodieCDCLogRecord<T> {
    private final T record;
    private final HoodieCDCEngineRecordAccessor<T> recordAccessor;

    private NativeCDCLogRecord(T record, HoodieCDCEngineRecordAccessor<T> recordAccessor) {
      this.record = record;
      this.recordAccessor = recordAccessor;
    }

    @Override
    public String getOperation() {
      return recordAccessor.getOperation(record);
    }

    @Override
    public String getRecordKey() {
      return recordAccessor.getRecordKey(record);
    }

    @Override
    public IndexedRecord getAvroImage(int ordinal) {
      throw new UnsupportedOperationException("Native CDC records do not contain Avro images");
    }

    @Override
    public T getEngineImage(int ordinal, int imageArity) {
      return recordAccessor.getImage(record, ordinal, imageArity);
    }

    @Override
    public boolean isNative() {
      return true;
    }
  }
}
