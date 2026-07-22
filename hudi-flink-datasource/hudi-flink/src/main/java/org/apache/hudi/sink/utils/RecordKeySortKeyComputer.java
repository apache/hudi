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

package org.apache.hudi.sink.utils;

import org.apache.hudi.sink.bulk.RowDataKeyGen;

import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;

/**
 * Computes a normalized prefix for sorting rows by their encoded Hudi record key.
 *
 * <p>The common LSM reader compares encoded record keys with {@link String#compareTo(String)},
 * whose order is based on UTF-16 code units. This computer therefore uses Flink's normalized-key
 * encoding for Java strings, which preserves that order while still using only one byte for the
 * common ASCII case.
 *
 * <p>The normalized prefix is eight bytes for one record-key field and sixteen bytes for two or
 * more fields. It is packed into one or two big-endian {@code long}s so the sort hot path can
 * compare and swap the prefix with long operations instead of processing every byte separately.
 * Short prefixes are padded with zeroes. Truncated prefixes, as well as collisions caused by zero
 * padding, are resolved by {@link RecordKeySortComparator}; therefore the normalized key never
 * fully determines the result.
 */
public class RecordKeySortKeyComputer implements NormalizedKeyComputer {

  private static final int BYTES_PER_RECORD_KEY_FIELD = 8;
  private static final int MAX_NORMALIZED_KEY_BYTES = 16;

  private final RowDataKeyGen keyGen;
  private final int numKeyBytes;
  private final StringComparator stringComparator = new StringComparator(true);

  /**
   * Creates a normalized-key computer using up to sixteen bytes of the encoded record-key prefix.
   *
   * @param keyGen generator for an order-equivalent encoded record key
   * @param recordKeyFieldCount number of fields forming the record key
   */
  public RecordKeySortKeyComputer(RowDataKeyGen keyGen, int recordKeyFieldCount) {
    this.keyGen = keyGen;
    this.numKeyBytes = Math.min(recordKeyFieldCount * BYTES_PER_RECORD_KEY_FIELD, MAX_NORMALIZED_KEY_BYTES);
  }

  @Override
  public void putKey(RowData record, MemorySegment target, int offset) {
    stringComparator.putNormalizedKey(
        keyGen.getRecordKeyForComparison(record), target, offset, numKeyBytes);
  }

  @Override
  public int compareKey(MemorySegment segment1, int offset1, MemorySegment segment2, int offset2) {
    int result = Long.compareUnsigned(
        segment1.getLongBigEndian(offset1), segment2.getLongBigEndian(offset2));
    if (result != 0 || numKeyBytes == Long.BYTES) {
      return result;
    }
    return Long.compareUnsigned(
        segment1.getLongBigEndian(offset1 + Long.BYTES),
        segment2.getLongBigEndian(offset2 + Long.BYTES));
  }

  @Override
  public void swapKey(MemorySegment segment1, int offset1, MemorySegment segment2, int offset2) {
    // Endianness is irrelevant when swapping the complete packed value, so use native long access.
    swapLong(segment1, offset1, segment2, offset2);
    if (numKeyBytes > Long.BYTES) {
      swapLong(segment1, offset1 + Long.BYTES, segment2, offset2 + Long.BYTES);
    }
  }

  private static void swapLong(
      MemorySegment segment1, int offset1, MemorySegment segment2, int offset2) {
    long value = segment1.getLong(offset1);
    segment1.putLong(offset1, segment2.getLong(offset2));
    segment2.putLong(offset2, value);
  }

  @Override
  public int getNumKeyBytes() {
    return numKeyBytes;
  }

  @Override
  public boolean isKeyFullyDetermines() {
    return false;
  }

  @Override
  public boolean invertKey() {
    return false;
  }
}
