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

package org.apache.hudi.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * An abstract implementation of the ByteRange API
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractByteRange implements ByteRange {
  public static final int UNSET_HASH_VALUE = -1;

  // Note to maintainers: Do not make these final, as the intention is to
  // reuse objects of this class

  /**
   * The array containing the bytes in this range. It will be &gt;= length.
   */
  protected byte[] bytes;

  /**
   * The index of the first byte in this range. {@code ByteRange.get(0)} will
   * return bytes[offset].
   */
  protected int offset;

  /**
   * The number of bytes in the range. Offset + length must be &lt;= bytes.length
   */
  protected int length;

  /**
   * Variable for lazy-caching the hashCode of this range. Useful for frequently
   * used ranges, long-lived ranges, or long ranges.
   */
  protected int hash = UNSET_HASH_VALUE;

  //
  // methods for managing the backing array and range viewport
  //
  @Override
  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public ByteRange set(int capacity) {
    return set(new byte[capacity]);
  }

  @Override
  public ByteRange set(byte[] bytes) {
    if (null == bytes) {
      return unset();
    }

    clearHashCache();
    this.bytes = bytes;
    this.offset = 0;
    this.length = bytes.length;
    return this;
  }

  @Override
  public ByteRange set(byte[] bytes, int offset, int length) {
    if (null == bytes) {
      return unset();
    }

    clearHashCache();
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    return this;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public ByteRange setOffset(int offset) {
    clearHashCache();
    this.offset = offset;
    return this;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public ByteRange setLength(int length) {
    clearHashCache();
    this.length = length;
    return this;
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(this);
  }

  /**
   * @return true when {@code range} is of zero length, false otherwise.
   */
  public static boolean isEmpty(ByteRange range) {
    return range == null || range.getLength() == 0;
  }

  //
  // methods for retrieving data
  //

  @Override
  public byte get(int index) {
    return bytes[offset + index];
  }

  @Override
  public ByteRange get(int index, byte[] dst) {
    if (0 == dst.length) {
      return this;
    }

    return get(index, dst, 0, dst.length);
  }

  @Override
  public ByteRange get(int index, byte[] dst, int offset, int length) {
    if (0 == length) {
      return this;
    }

    System.arraycopy(this.bytes, this.offset + index, dst, offset, length);
    return this;
  }

  @Override
  public short getShort(int index) {
    int offset = this.offset + index;
    short n = 0;
    n = (short) ((n ^ bytes[offset]) & 0xFF);
    n = (short) (n << 8);
    n = (short) ((n ^ bytes[offset + 1]) & 0xFF);
    return n;
  }

  @Override
  public int getInt(int index) {
    int offset = this.offset + index;
    int n = 0;
    for (int i = offset; i < (offset + Bytes.SIZEOF_INT); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  @Override
  public long getLong(int index) {
    int offset = this.offset + index;
    long l = 0;
    for (int i = offset; i < offset + Bytes.SIZEOF_LONG; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  // Copied from com.google.protobuf.CodedInputStream v2.5.0 readRawVarint64
  @Override
  public long getVLong(int index) {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = get(index++);
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }
  // end of copied from protobuf

  public static int getVLongSize(long val) {
    int rPos = 0;
    while ((val & ~0x7F) != 0) {
      val >>>= 7;
      rPos++;
    }
    return rPos + 1;
  }

  //
  // methods for duplicating the current instance
  //

  @Override
  public byte[] deepCopyToNewArray() {
    byte[] result = new byte[length];
    System.arraycopy(bytes, offset, result, 0, length);
    return result;
  }

  @Override
  public void deepCopyTo(byte[] destination, int destinationOffset) {
    System.arraycopy(bytes, offset, destination, destinationOffset, length);
  }

  @Override
  public void deepCopySubRangeTo(int innerOffset, int copyLength, byte[] destination,
                                 int destinationOffset) {
    System.arraycopy(bytes, offset + innerOffset, destination, destinationOffset, copyLength);
  }

  //
  // methods used for comparison
  //

  @Override
  public int hashCode() {
    if (isHashCached()) {// hash is already calculated and cached
      return hash;
    }
    if (this.isEmpty()) {// return 0 for empty ByteRange
      hash = 0;
      return hash;
    }
    int off = offset;
    hash = 0;
    for (int i = 0; i < length; i++) {
      hash = 31 * hash + bytes[off++];
    }
    return hash;
  }

  protected boolean isHashCached() {
    return hash != UNSET_HASH_VALUE;
  }

  protected void clearHashCache() {
    hash = UNSET_HASH_VALUE;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ByteRange)) {
      return false;
    }
    return compareTo((ByteRange) obj) == 0;
  }

  /**
   * Bitwise comparison of each byte in the array. Unsigned comparison, not
   * paying attention to java's signed bytes.
   */
  @Override
  public int compareTo(ByteRange other) {
    return Bytes.compareTo(bytes, offset, length, other.getBytes(), other.getOffset(),
        other.getLength());
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(bytes, offset, length);
  }
}
