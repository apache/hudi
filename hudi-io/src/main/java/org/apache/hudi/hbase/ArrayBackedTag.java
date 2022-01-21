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

import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This is a {@link Tag} implementation in which value is backed by an on heap byte array.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ArrayBackedTag implements Tag {
  private final byte type;// TODO  extra type state needed?
  private final byte[] bytes;
  private int offset = 0;
  private int length = 0;

  /**
   * The special tag will write the length of each tag and that will be
   * followed by the type and then the actual tag.
   * So every time the length part is parsed we need to add + 1 byte to it to
   * get the type and then get the actual tag.
   */
  public ArrayBackedTag(byte tagType, String tag) {
    this(tagType, Bytes.toBytes(tag));
  }

  /**
   * Format for a tag :
   * {@code <length of tag - 2 bytes><type code - 1 byte><tag>} tag length is serialized
   * using 2 bytes only but as this will be unsigned, we can have max tag length of
   * (Short.MAX_SIZE * 2) +1. It includes 1 byte type length and actual tag bytes length.
   */
  public ArrayBackedTag(byte tagType, byte[] tag) {
    int tagLength = tag.length + TYPE_LENGTH_SIZE;
    if (tagLength > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_LENGTH);
    }
    length = TAG_LENGTH_SIZE + tagLength;
    bytes = new byte[length];
    int pos = Bytes.putAsShort(bytes, 0, tagLength);
    pos = Bytes.putByte(bytes, pos, tagType);
    Bytes.putBytes(bytes, pos, tag, 0, tag.length);
    this.type = tagType;
  }

  /**
   * Creates a Tag from the specified byte array and offset. Presumes
   * <code>bytes</code> content starting at <code>offset</code> is formatted as
   * a Tag blob.
   * The bytes to include the tag type, tag length and actual tag bytes.
   * @param offset offset to start of Tag
   */
  public ArrayBackedTag(byte[] bytes, int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  private static int getLength(byte[] bytes, int offset) {
    return TAG_LENGTH_SIZE + Bytes.readAsInt(bytes, offset, TAG_LENGTH_SIZE);
  }

  /**
   * Creates a Tag from the specified byte array, starting at offset, and for length
   * <code>length</code>. Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a Tag blob.
   */
  public ArrayBackedTag(byte[] bytes, int offset, int length) {
    if (length > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_LENGTH);
    }
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    this.type = bytes[offset + TAG_LENGTH_SIZE];
  }

  /**
   * @return The byte array backing this Tag.
   */
  @Override
  public byte[] getValueArray() {
    return this.bytes;
  }

  /**
   * @return the tag type
   */
  @Override
  public byte getType() {
    return this.type;
  }

  /**
   * @return Length of actual tag bytes within the backed buffer
   */
  @Override
  public int getValueLength() {
    return this.length - INFRASTRUCTURE_SIZE;
  }

  /**
   * @return Offset of actual tag bytes within the backed buffer
   */
  @Override
  public int getValueOffset() {
    return this.offset + INFRASTRUCTURE_SIZE;
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public String toString() {
    return "[Tag type : " + this.type + ", value : "
        + Bytes.toStringBinary(bytes, getValueOffset(), getValueLength()) + "]";
  }
}
