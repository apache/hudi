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

import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.List;

public abstract class HFileIndexBlock extends HFileBlock {
  protected final List<BlockIndexEntry> entries = new ArrayList<>();
  protected long blockDataSize = -1L;

  protected HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType,
                         byte[] byteBuff,
                         int startOffsetInBuff) {
    super(context, blockType, byteBuff, startOffsetInBuff);
  }

  protected HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType) {
    super(context, blockType, -1L);
  }

  public void add(byte[] firstKey, long offset, int size) {
    Key key = new Key(firstKey);
    entries.add(new BlockIndexEntry(key, Option.empty(), offset, size));
  }

  public int getNumOfEntries() {
    return entries.size();
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  /**
   * Encodes an integer using {@code WritableUtils} variable-length encoding (VInt): the encoding
   * the HFile block index uses for the per-entry key length and that the reader decodes
   * ({@link org.apache.hudi.io.util.IOUtils#readVarLong}). Values in [-112, 127] use a single
   * byte; otherwise the first byte carries the number of following big-endian value bytes and the
   * sign. This is NOT interchangeable with the Protobuf varint
   * ({@link HFileFileInfoBlock#getProtobufVarIntBytes(int)}): the two diverge for values >= 128.
   *
   * @param value the integer value to encode.
   * @return the encoded byte array.
   */
  static byte[] getVarIntBytes(int value) {
    if (value >= -112 && value <= 127) {
      return new byte[] {(byte) value};
    }
    long longValue = value;
    int len = -112;
    if (longValue < 0) {
      longValue ^= -1L;
      len = -120;
    }
    long tmp = longValue;
    while (tmp != 0) {
      tmp >>= 8;
      len--;
    }
    int numBytes = (len < -120) ? -(len + 120) : -(len + 112);
    byte[] result = new byte[1 + numBytes];
    result[0] = (byte) len;
    for (int idx = 0; idx < numBytes; idx++) {
      int shiftBits = (numBytes - idx - 1) * 8;
      result[1 + idx] = (byte) ((longValue >> shiftBits) & 0xFF);
    }
    return result;
  }
}
