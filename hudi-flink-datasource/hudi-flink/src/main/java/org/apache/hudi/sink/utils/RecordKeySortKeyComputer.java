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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;

/**
 * Computes a normalized prefix for sorting rows by their encoded Hudi record key.
 *
 * <p>The common LSM reader compares encoded record keys by their unsigned UTF-8 byte
 * representation. This computer copies a prefix of those bytes into the normalized key so its
 * ordering is consistent with the reader.
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
    int bytesWritten = putUtf8Prefix(
        keyGen.getRecordKeyForComparison(record), target, offset, numKeyBytes);
    for (int i = bytesWritten; i < numKeyBytes; i++) {
      target.put(offset + i, (byte) 0);
    }
  }

  /**
   * Encodes at most {@code maxBytes} of the UTF-8 prefix directly into the target segment.
   *
   * <p>This writes the exact first {@code maxBytes} UTF-8 bytes, including a partial multi-byte
   * sequence at the prefix boundary, without materializing the complete encoded record key.
   *
   * <p>Production record keys contain well-formed UTF-16 because they are derived from UTF-8 input.
   * An unpaired surrogate is encoded with the same replacement byte used by
   * {@link String#getBytes(java.nio.charset.Charset)}.
   */
  private static int putUtf8Prefix(
      String value, MemorySegment target, int offset, int maxBytes) {
    int bytesWritten = 0;
    for (int i = 0; i < value.length() && bytesWritten < maxBytes; i++) {
      char current = value.charAt(i);
      int encoded;
      int encodedBytes;
      // Pack the encoded bytes in big-endian order into the least-significant encodedBytes bytes.
      if (current < 0x80) {
        encoded = current;
        encodedBytes = 1;
      } else if (current < 0x800) {
        encoded = (0xC0 | current >>> 6) << 8
            | (0x80 | current & 0x3F);
        encodedBytes = 2;
      } else if (Character.isHighSurrogate(current)
          && i + 1 < value.length()
          && Character.isLowSurrogate(value.charAt(i + 1))) {
        int codePoint = Character.toCodePoint(current, value.charAt(++i));
        encoded = (0xF0 | codePoint >>> 18) << 24
            | (0x80 | codePoint >>> 12 & 0x3F) << 16
            | (0x80 | codePoint >>> 6 & 0x3F) << 8
            | (0x80 | codePoint & 0x3F);
        encodedBytes = 4;
      } else if (Character.isSurrogate(current)) {
        encoded = '?';
        encodedBytes = 1;
      } else {
        encoded = (0xE0 | current >>> 12) << 16
            | (0x80 | current >>> 6 & 0x3F) << 8
            | (0x80 | current & 0x3F);
        encodedBytes = 3;
      }

      // Writing byte by byte is intentional: the normalized prefix may end inside this sequence.
      for (int shift = (encodedBytes - 1) * Byte.SIZE;
           shift >= 0 && bytesWritten < maxBytes;
           shift -= Byte.SIZE) {
        target.put(offset + bytesWritten++, (byte) (encoded >>> shift));
      }
    }
    return bytesWritten;
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
