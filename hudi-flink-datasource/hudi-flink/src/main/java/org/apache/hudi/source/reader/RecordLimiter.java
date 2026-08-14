/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility that enforces a pushed-down row-count limit across multiple splits.
 *
 * <p>Call {@link #wrap(RecordsWithSplitIds)} to get a decorated batch whose
 * {@code nextRecordFromSplit()} returns {@code null} once the global limit is reached.
 * Use {@link #isLimitReached()} to skip reading any further splits entirely.
 */
public class RecordLimiter {
  public static final long NO_LIMIT = -1L;

  private final long limit;
  private final AtomicLong totalReadCount = new AtomicLong(0);

  public RecordLimiter(long limit) {
    this.limit = limit;
  }

  /** Returns {@code true} when the configured limit has been reached or exceeded. */
  public boolean isLimitReached() {
    return limit != NO_LIMIT && totalReadCount.get() >= limit;
  }

  /**
   * Wraps {@code records} so that each call to {@link RecordsWithSplitIds#nextRecordFromSplit()}
   * increments the internal counter and returns {@code null} once the global limit is reached,
   * causing Flink to treat the current split as exhausted.
   */
  public <T> RecordsWithSplitIds<T> wrap(RecordsWithSplitIds<T> records) {
    return new RecordsWithSplitIds<T>() {
      @Override
      public String nextSplit() {
        return records.nextSplit();
      }

      @Override
      public T nextRecordFromSplit() {
        if (totalReadCount.get() >= limit) {
          return null;
        }
        T record = records.nextRecordFromSplit();
        if (record != null) {
          totalReadCount.incrementAndGet();
        }
        return record;
      }

      @Override
      public Set<String> finishedSplits() {
        return records.finishedSplits();
      }

      @Override
      public void recycle() {
        records.recycle();
      }
    };
  }
}
