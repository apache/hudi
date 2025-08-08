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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.util.Option;

/**
 * Interface used within the {@link HoodieFileGroupReader<T>} for processing updates to records in Merge-on-Read tables.
 * Note that the updates are always relative to the base file's current state.
 * @param <T> the engine specific record type
 */
public interface UpdateProcessor<T> {
  /**
   * Processes the update to the record. If the update should not be returned to the caller, the method should return null.
   * @param recordKey the key of the record being updated
   * @param previousRecord the previous version of the record, or null if there is no previous value
   * @param mergedRecord the current version of the record after merging with the existing record, if any exists
   * @param isDelete a flag indicating whether the merge resulted in a delete operation
   * @return the processed record, or null if the record should not be returned to the caller
   */
  T processUpdate(String recordKey, T previousRecord, T mergedRecord, boolean isDelete);

  static <T> UpdateProcessor<T> create(HoodieReadStats readStats, HoodieReaderContext<T> readerContext,
                                       boolean emitDeletes, Option<BaseFileUpdateCallback<T>> updateCallback) {
    UpdateProcessor<T> handler = new StandardUpdateProcessor<>(readStats, readerContext, emitDeletes);
    if (updateCallback.isPresent()) {
      return new CallbackProcessor<>(updateCallback.get(), handler);
    }
    return handler;
  }

  /**
   * A standard update processor that increments the read stats and returns the record if applicable.
   * @param <T> the engine specific record type
   */
  class StandardUpdateProcessor<T> implements UpdateProcessor<T> {
    protected final HoodieReadStats readStats;
    protected final HoodieReaderContext<T> readerContext;
    protected final boolean emitDeletes;

    public StandardUpdateProcessor(HoodieReadStats readStats, HoodieReaderContext<T> readerContext,
                                   boolean emitDeletes) {
      this.readStats = readStats;
      this.readerContext = readerContext;
      this.emitDeletes = emitDeletes;
    }

    @Override
    public T processUpdate(String recordKey, T previousRecord, T mergedRecord, boolean isDelete) {
      if (isDelete) {
        readStats.incrementNumDeletes();
        if (emitDeletes) {
          return readerContext.getRecordContext().getDeleteRow(mergedRecord, recordKey);
        }
        return null;
      } else {
        if (previousRecord != null && previousRecord != mergedRecord) {
          readStats.incrementNumUpdates();
        } else if (previousRecord == null) {
          readStats.incrementNumInserts();
        }
        return readerContext.seal(mergedRecord);
      }
    }
  }

  /**
   * A processor that wraps the standard update processor and invokes a customizable callback for each update.
   * @param <T> the engine specific record type
   */
  class CallbackProcessor<T> implements UpdateProcessor<T> {
    private final BaseFileUpdateCallback<T> callback;
    private final UpdateProcessor<T> delegate;

    public CallbackProcessor(BaseFileUpdateCallback callback, UpdateProcessor<T> delegate) {
      this.callback = callback;
      this.delegate = delegate;
    }

    @Override
    public T processUpdate(String recordKey, T previousRecord, T currentRecord, boolean isDelete) {
      T result = delegate.processUpdate(recordKey, previousRecord, currentRecord, isDelete);

      if (isDelete) {
        callback.onDelete(recordKey, previousRecord);
      } else if (previousRecord != null && currentRecord != null) {
        callback.onUpdate(recordKey, previousRecord, currentRecord);
      } else {
        callback.onInsert(recordKey, currentRecord);
      }
      return result;
    }
  }
}
