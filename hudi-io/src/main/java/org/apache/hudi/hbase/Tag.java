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

package org.apache.hudi.hbase;

import java.nio.ByteBuffer;

import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Tags are part of cells and helps to add metadata about them.
 * Metadata could be ACLs, visibility labels, etc.
 * <p>
 * Each Tag is having a type (one byte) and value part. The max value length for a Tag is 65533.
 * <p>
 * See {@link TagType} for reserved tag types.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Tag {

  public final static int TYPE_LENGTH_SIZE = Bytes.SIZEOF_BYTE;
  public final static int TAG_LENGTH_SIZE = Bytes.SIZEOF_SHORT;
  public final static int INFRASTRUCTURE_SIZE = TYPE_LENGTH_SIZE + TAG_LENGTH_SIZE;
  public static final int MAX_TAG_LENGTH = (2 * Short.MAX_VALUE) + 1 - TAG_LENGTH_SIZE;

  /**
   * Custom tags if created are suggested to be above this range. So that
   * it does not overlap with internal tag types
   */
  public static final byte CUSTOM_TAG_TYPE_RANGE = (byte)64;
  /**
   * @return the tag type
   */
  byte getType();

  /**
   * @return Offset of tag value within the backed buffer
   */
  int getValueOffset();

  /**
   * @return Length of tag value within the backed buffer
   */
  int getValueLength();

  /**
   * Tells whether or not this Tag is backed by a byte array.
   * @return true when this Tag is backed by byte array
   */
  boolean hasArray();

  /**
   * @return The array containing the value bytes.
   * @throws UnsupportedOperationException
   *           when {@link #hasArray()} return false. Use {@link #getValueByteBuffer()} in such
   *           situation
   */
  byte[] getValueArray();

  /**
   * @return The {@link java.nio.ByteBuffer} containing the value bytes.
   */
  ByteBuffer getValueByteBuffer();

  /**
   * Returns tag value in a new byte array. Primarily for use client-side. If server-side, use
   * {@link Tag#getValueArray()} with appropriate {@link Tag#getValueOffset()} and
   * {@link Tag#getValueLength()} instead to save on allocations.
   * @param tag The Tag whose value to be returned
   * @return tag value in a new byte array.
   */
  public static byte[] cloneValue(Tag tag) {
    int tagLength = tag.getValueLength();
    byte[] tagArr = new byte[tagLength];
    if (tag.hasArray()) {
      Bytes.putBytes(tagArr, 0, tag.getValueArray(), tag.getValueOffset(), tagLength);
    } else {
      ByteBufferUtils.copyFromBufferToArray(tagArr, tag.getValueByteBuffer(), tag.getValueOffset(),
          0, tagLength);
    }
    return tagArr;
  }

  /**
   * Converts the value bytes of the given tag into a String value
   * @param tag The Tag
   * @return value as String
   */
  public static String getValueAsString(Tag tag) {
    if (tag.hasArray()) {
      return Bytes.toString(tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    }
    return Bytes.toString(cloneValue(tag));
  }

  /**
   * Matches the value part of given tags
   * @param t1 Tag to match the value
   * @param t2 Tag to match the value
   * @return True if values of both tags are same.
   */
  public static boolean matchingValue(Tag t1, Tag t2) {
    if (t1.hasArray() && t2.hasArray()) {
      return Bytes.equals(t1.getValueArray(), t1.getValueOffset(), t1.getValueLength(),
          t2.getValueArray(), t2.getValueOffset(), t2.getValueLength());
    }
    if (t1.hasArray()) {
      return ByteBufferUtils.equals(t2.getValueByteBuffer(), t2.getValueOffset(),
          t2.getValueLength(), t1.getValueArray(), t1.getValueOffset(), t1.getValueLength());
    }
    if (t2.hasArray()) {
      return ByteBufferUtils.equals(t1.getValueByteBuffer(), t1.getValueOffset(),
          t1.getValueLength(), t2.getValueArray(), t2.getValueOffset(), t2.getValueLength());
    }
    return ByteBufferUtils.equals(t1.getValueByteBuffer(), t1.getValueOffset(), t1.getValueLength(),
        t2.getValueByteBuffer(), t2.getValueOffset(), t2.getValueLength());
  }

  /**
   * Copies the tag's value bytes to the given byte array
   * @param tag The Tag
   * @param out The byte array where to copy the Tag value.
   * @param offset The offset within 'out' array where to copy the Tag value.
   */
  public static void copyValueTo(Tag tag, byte[] out, int offset) {
    if (tag.hasArray()) {
      Bytes.putBytes(out, offset, tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    } else {
      ByteBufferUtils.copyFromBufferToArray(out, tag.getValueByteBuffer(), tag.getValueOffset(),
          offset, tag.getValueLength());
    }
  }

  /**
   * Converts the value bytes of the given tag into a long value
   * @param tag The Tag
   * @return value as long
   */
  public static long getValueAsLong(Tag tag) {
    if (tag.hasArray()) {
      return Bytes.toLong(tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    }
    return ByteBufferUtils.toLong(tag.getValueByteBuffer(), tag.getValueOffset());
  }

  /**
   * Converts the value bytes of the given tag into a byte value
   * @param tag The Tag
   * @return value as byte
   */
  public static byte getValueAsByte(Tag tag) {
    if (tag.hasArray()) {
      return tag.getValueArray()[tag.getValueOffset()];
    }
    return ByteBufferUtils.toByte(tag.getValueByteBuffer(), tag.getValueOffset());
  }
}
