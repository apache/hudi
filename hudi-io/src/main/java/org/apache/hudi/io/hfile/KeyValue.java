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

package org.apache.hudi.io.hfile;

import static org.apache.hudi.io.hfile.ByteUtils.readInt;
import static org.apache.hudi.io.hfile.ByteUtils.readShort;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_SHORT;

/**
 * Represents a key-value pair in the data block.
 */
public class KeyValue {
  /** Size of the row length field in bytes */
  public static final int ROW_LENGTH_SIZE = SIZEOF_SHORT;
  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  public static final int ROW_OFFSET =
      SIZEOF_INT /*keylength*/ + SIZEOF_INT /*valuelength*/;

  public static final int ROW_KEY_OFFSET = ROW_OFFSET + ROW_LENGTH_SIZE;

  protected final byte[] bytes;
  protected final int offset;
  protected final int length;

  public KeyValue(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  public KeyValue(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * @return the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * @return Row offset
   */
  public int getRowOffset() {
    return this.offset + ROW_KEY_OFFSET;
  }

  /**
   * @return Length of key portion.
   */
  public int getKeyLength() {
    return readInt(this.bytes, this.offset);
  }

  /**
   * @return Key offset in backing buffer..
   */
  public int getKeyOffset() {
    return this.offset + ROW_OFFSET;
  }

  /**
   * @return Row length
   */
  public short getRowLength() {
    return readShort(this.bytes, getKeyOffset());
  }

  /**
   * @return the value offset
   */
  public int getValueOffset() {
    int voffset = getKeyOffset() + getKeyLength();
    return voffset;
  }

  /**
   * @return Value length
   */
  public int getValueLength() {
    int vlength = readInt(this.bytes, this.offset + SIZEOF_INT);
    return vlength;
  }

  @Override
  public String toString() {
    if (length > 0) {
      return new String(bytes, offset, length);
    }
    // TODO: fix length
    return new String(bytes, offset, 22);
  }
}
