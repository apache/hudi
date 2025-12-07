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

import lombok.Getter;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT32;
import static org.apache.hudi.io.util.IOUtils.readInt;

/**
 * Represents a key-value pair in the data block.
 */
public class KeyValue {
  // Key part starts after the key length (integer) and value length (integer)
  public static final int KEY_OFFSET = SIZEOF_INT32 * 2;
  @Getter
  private final byte[] bytes;
  private final int offset;
  @Getter
  private final Key key;

  public KeyValue(byte[] bytes, int offset) {
    this.bytes = bytes;
    this.offset = offset;
    this.key = new Key(bytes, offset + KEY_OFFSET, readInt(bytes, offset));
  }

  /**
   * @return key content offset.
   */
  public int getKeyContentOffset() {
    return key.getContentOffset();
  }

  /**
   * @return length of key portion.
   */
  public int getKeyLength() {
    return key.getLength();
  }

  /**
   * @return key offset in backing buffer.
   */
  public int getKeyOffset() {
    return key.getOffset();
  }

  /**
   * @return key content length.
   */
  public int getKeyContentLength() {
    return key.getContentLength();
  }

  /**
   * @return the value offset.
   */
  public int getValueOffset() {
    return getKeyOffset() + getKeyLength();
  }

  /**
   * @return value length.
   */
  public int getValueLength() {
    return readInt(this.bytes, this.offset + SIZEOF_INT32);
  }

  @Override
  public String toString() {
    return "KeyValue{key="
        + key.toString()
        + "}";
  }
}
