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

/**
 * Lightweight, reusable class for specifying ranges of byte[]'s.
 * <p>
 * {@code ByteRange} maintains an underlying byte[] and a viewport into that
 * byte[] as a range of bytes. The {@code ByteRange} is a mutable, reusable
 * object, so the underlying byte[] can be modified after instantiation. This
 * is done using the {@link #set(byte[])} and {@link #unset()} methods. Direct
 * access to the byte[] is also available via {@link #getBytes()}. The viewport
 * is defined by an {@code offset} into the byte[] and a {@code length}. The
 * range of bytes is 0-indexed, and is accessed by index via the
 * {@link #get(int)} and {@link #put(int, byte)} methods.
 * </p>
 * <p>
 * This interface differs from ByteBuffer:
 * </p>
 * <ul>
 * <li>On-heap bytes only</li>
 * <li>Raw {@code byte} access only; does not encode other primitives.</li>
 * <li>Implements {@code equals(Object)}, {@code #hashCode()}, and
 * {@code #compareTo(ByteRange)} so that it can be used in standard java
 * Collections. Comparison operations are lexicographic, which is native to
 * HBase.</li>
 * <li>Allows the addition of simple core methods like the deep and shallow
 * copy methods.</li>
 * <li>Can be reused in tight loops like a major compaction which can save
 * significant amounts of garbage. (Without reuse, we throw off garbage like
 * <a href="http://www.youtube.com/watch?v=lkmBH-MjZF4">this thing</a>.)</li>
 * </ul>
 * <p>
 * Mutable, and always evaluates {@code #equals(Object)}, {@code #hashCode()},
 * and {@code #compareTo(ByteRange)} based on the current contents.
 * </p>
 * <p>
 * Can contain convenience methods for comparing, printing, cloning, spawning
 * new arrays, copying to other arrays, etc. Please place non-core methods into
 * {@link ByteRangeUtils}.
 * </p>
 */
@InterfaceAudience.Public
public interface ByteRange extends Comparable<ByteRange> {

  /**
   * The underlying byte[].
   */
  public byte[] getBytes();

  /**
   * Nullifies this ByteRange. That is, it becomes a husk, being a range over
   * no byte[] whatsoever.
   * @return this
   */
  public ByteRange unset();

  /**
   * Reuse this {@code ByteRange} over a new byte[]. {@code offset} is set to
   * 0 and {@code length} is set to {@code capacity}.
   * @param capacity the size of a new byte[].
   * @return this
   */
  public ByteRange set(int capacity);

  /**
   * Reuse this {@code ByteRange} over a new byte[]. {@code offset} is set to
   * 0 and {@code length} is set to {@code bytes.length}. A null {@code bytes}
   * IS supported, in which case this method will behave equivalently to
   * {@link #unset()}.
   * @param bytes the array to wrap.
   * @return this
   */
  public ByteRange set(byte[] bytes);

  /**
   * Reuse this {@code ByteRange} over a new byte[]. A null {@code bytes} IS
   * supported, in which case this method will behave equivalently to
   * {@link #unset()}, regardless of the values of {@code offset} and
   * {@code length}.
   * @param bytes The array to wrap.
   * @param offset The offset into {@code bytes} considered the beginning of
   *            this range.
   * @param length The length of this range.
   * @return this.
   */
  public ByteRange set(byte[] bytes, int offset, int length);

  /**
   * The offset, the index into the underlying byte[] at which this range
   * begins.
   * @see #getBytes()
   */
  public int getOffset();

  /**
   * Update the beginning of this range. {@code offset + length} may not be
   * greater than {@code bytes.length}.
   * @param offset the new start of this range.
   * @return this.
   */
  public ByteRange setOffset(int offset);

  /**
   * The length of the range.
   */
  public int getLength();

  /**
   * Update the length of this range. {@code offset + length} should not be
   * greater than {@code bytes.length}.
   * @param length The new length of this range.
   * @return this.
   */
  public ByteRange setLength(int length);

  /**
   * @return true when this range is of zero length, false otherwise.
   */
  public boolean isEmpty();

  /**
   * Retrieve the byte at {@code index}.
   * @param index zero-based index into this range.
   * @return single byte at index.
   */
  public byte get(int index);

  /**
   * Retrieve the short value at {@code index}
   * @param index zero-based index into this range
   * @return the short value at {@code index}
   */
  public short getShort(int index);

  /**
   * Retrieve the int value at {@code index}
   * @param index zero-based index into this range
   * @return the int value at {@code index}
   */
  public int getInt(int index);

  /**
   * Retrieve the long value at {@code index}
   * @param index zero-based index into this range
   * @return the long value at {@code index}
   */
  public long getLong(int index);

  /**
   * Retrieve the long value at {@code index} which is stored as VLong
   * @param index zero-based index into this range
   * @return the long value at {@code index} which is stored as VLong
   */
  public long getVLong(int index);

  /**
   * Fill {@code dst} with bytes from the range, starting from {@code index}.
   * @param index zero-based index into this range.
   * @param dst the destination of the copy.
   * @return this.
   */
  public ByteRange get(int index, byte[] dst);

  /**
   * Fill {@code dst} with bytes from the range, starting from {@code index}.
   * {@code length} bytes are copied into {@code dst}, starting at {@code offset}.
   * @param index zero-based index into this range.
   * @param dst the destination of the copy.
   * @param offset the offset into {@code dst} to start the copy.
   * @param length the number of bytes to copy into {@code dst}.
   * @return this.
   */
  public ByteRange get(int index, byte[] dst, int offset, int length);

  /**
   * Store {@code val} at {@code index}.
   * @param index the index in the range where {@code val} is stored.
   * @param val the value to store.
   * @return this.
   */
  public ByteRange put(int index, byte val);

  /**
   * Store the short value at {@code index}
   * @param index the index in the range where {@code val} is stored
   * @param val the value to store
   * @return this
   */
  public ByteRange putShort(int index, short val);

  /**
   * Store the int value at {@code index}
   * @param index the index in the range where {@code val} is stored
   * @param val the value to store
   * @return this
   */
  public ByteRange putInt(int index, int val);

  /**
   * Store the long value at {@code index}
   * @param index the index in the range where {@code val} is stored
   * @param val the value to store
   * @return this
   */
  public ByteRange putLong(int index, long val);

  /**
   * Store the long value at {@code index} as a VLong
   * @param index the index in the range where {@code val} is stored
   * @param val the value to store
   * @return number of bytes written
   */
  public int putVLong(int index, long val);

  /**
   * Store {@code val} at {@code index}.
   * @param index the index in the range where {@code val} is stored.
   * @param val the value to store.
   * @return this.
   */
  public ByteRange put(int index, byte[] val);

  /**
   * Store {@code length} bytes from {@code val} into this range, starting at
   * {@code index}. Bytes from {@code val} are copied starting at {@code offset}
   * into the range.
   * @param index position in this range to start the copy.
   * @param val the value to store.
   * @param offset the offset in {@code val} from which to start copying.
   * @param length the number of bytes to copy from {@code val}.
   * @return this.
   */
  public ByteRange put(int index, byte[] val, int offset, int length);

  /**
   * Instantiate a new byte[] with exact length, which is at least 24 bytes +
   * length. Copy the contents of this range into it.
   * @return The newly cloned byte[].
   */
  public byte[] deepCopyToNewArray();

  /**
   * Create a new {@code ByteRange} with new backing byte[] containing a copy
   * of the content from {@code this} range's window.
   * @return Deep copy
   */
  public ByteRange deepCopy();

  /**
   * Wrapper for System.arraycopy. Copy the contents of this range into the
   * provided array.
   * @param destination Copy to this array
   * @param destinationOffset First index in the destination array.
   */
  public void deepCopyTo(byte[] destination, int destinationOffset);

  /**
   * Wrapper for System.arraycopy. Copy the contents of this range into the
   * provided array.
   * @param innerOffset Start copying from this index in this source
   *          ByteRange. First byte copied is bytes[offset + innerOffset]
   * @param copyLength Copy this many bytes
   * @param destination Copy to this array
   * @param destinationOffset First index in the destination array.
   */
  public void deepCopySubRangeTo(int innerOffset, int copyLength, byte[] destination,
                                 int destinationOffset);

  /**
   * Create a new {@code ByteRange} that points at this range's byte[].
   * Modifying the shallowCopy will modify the bytes in this range's array.
   * Pass over the hash code if it is already cached.
   * @return new {@code ByteRange} object referencing this range's byte[].
   */
  public ByteRange shallowCopy();

  /**
   * Create a new {@code ByteRange} that points at this range's byte[]. The new
   * range can have different values for offset and length, but modifying the
   * shallowCopy will modify the bytes in this range's array. Pass over the
   * hash code if it is already cached.
   * @param innerOffset First byte of clone will be this.offset + copyOffset.
   * @param copyLength Number of bytes in the clone.
   * @return new {@code ByteRange} object referencing this range's byte[].
   */
  public ByteRange shallowCopySubRange(int innerOffset, int copyLength);

}
