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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BatchSupport;
import org.apache.hudi.common.table.read.BufferedRecord;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Finds the buffered log record of each row of a base batch, for the vectorized merge-on-read read.
 *
 * @param <T> the engine row type
 */
public interface BatchMatcher<T> {

  /**
   * The base file column the match reads: the record key or the row index.
   */
  String columnName();

  /**
   * Whether no log record can match a row of the batch, so the batch passes through without a look at each row.
   */
  <B> boolean untouched(BatchSupport<T, B> batchSupport, B batch, int column, int numRows);

  /**
   * Removes and returns the log record of a row, or null when it has none.
   */
  <B> BufferedRecord<T> take(BatchSupport<T, B> batchSupport, B batch, int column, int row);

  /**
   * The log records no base row took.
   */
  Iterator<BufferedRecord<T>> leftovers();

  /**
   * Matches a hash of the record key of each row.
   */
  final class KeyBased<T> implements BatchMatcher<T> {
    private final Map<Object, BufferedRecord<T>> byKey;

    /**
     * @param byKey the log records keyed by {@link BatchSupport#toEngineKey} of their record key
     */
    KeyBased(Map<Object, BufferedRecord<T>> byKey) {
      this.byKey = byKey;
    }

    @Override
    public String columnName() {
      return HoodieRecord.RECORD_KEY_METADATA_FIELD;
    }

    @Override
    public <B> boolean untouched(BatchSupport<T, B> batchSupport, B batch, int column, int numRows) {
      return byKey.isEmpty();
    }

    @Override
    public <B> BufferedRecord<T> take(BatchSupport<T, B> batchSupport, B batch, int column, int row) {
      return batchSupport.isNullAt(batch, column, row) ? null : byKey.remove(batchSupport.getEngineKey(batch, column, row));
    }

    @Override
    public Iterator<BufferedRecord<T>> leftovers() {
      return byKey.values().iterator();
    }
  }

  /**
   * Walks the log records sorted by the base row they replace along the row index of the base batches, which ascends
   * through the file. A batch with no log position between its first and last row index passes through.
   */
  final class PositionBased<T> implements BatchMatcher<T> {
    private final long[] positions;
    private final BufferedRecord<T>[] records;
    private int cursor;

    /**
     * @param positions the base file positions the log records replace, ascending
     * @param records   the log record of each position
     */
    PositionBased(long[] positions, BufferedRecord<T>[] records) {
      this.positions = positions;
      this.records = records;
    }

    @Override
    public String columnName() {
      return PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;
    }

    private void skipBefore(long position) {
      while (cursor < positions.length && positions[cursor] < position) {
        cursor++;
      }
    }

    @Override
    public <B> boolean untouched(BatchSupport<T, B> batchSupport, B batch, int column, int numRows) {
      skipBefore(batchSupport.getLong(batch, column, 0));
      return cursor >= positions.length || positions[cursor] > batchSupport.getLong(batch, column, numRows - 1);
    }

    @Override
    public <B> BufferedRecord<T> take(BatchSupport<T, B> batchSupport, B batch, int column, int row) {
      long position = batchSupport.getLong(batch, column, row);
      skipBefore(position);
      if (cursor < positions.length && positions[cursor] == position) {
        BufferedRecord<T> record = records[cursor];
        records[cursor++] = null;
        return record;
      }
      return null;
    }

    @Override
    public Iterator<BufferedRecord<T>> leftovers() {
      return Arrays.stream(records).filter(Objects::nonNull).iterator();
    }
  }
}
