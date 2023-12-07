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

import static org.apache.hudi.io.hfile.KeyValue.ROW_OFFSET;

/**
 * Represents a {@link HFileBlockType#DATA} block in the "Scanned block" section.
 */
public class HFileDataBlock extends HFileBlock {
  protected HFileDataBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.DATA, byteBuff, startOffsetInBuff);
  }

  /**
   * Seeks to the key to look up.
   *
   * @param key Key to look up.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or {@link null} if the lookup key does not exist.
   */
  public KeyValue seekTo(KeyValue key) {
    int offset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;
    int endOffset = offset + onDiskSizeWithoutHeader;
    // TODO: check last 4 bytes in the data block
    while (offset + HFILEBLOCK_HEADER_SIZE < endOffset) {
      // Full length is not known yet until parsing
      KeyValue kv = new KeyValue(byteBuff, offset, -1);
      // TODO: Reading long instead of two integers per HBase
      int comp = ByteUtils.compareTo(kv.getBytes(), kv.getRowOffset(), kv.getRowLength(),
          key.getBytes(), key.getRowOffset(), key.getRowLength());
      if (comp == 0) {
        return kv;
      } else if (comp > 0) {
        return null;
      }
      // TODO: check what's the extra byte
      long incr = (long) ROW_OFFSET + (long) kv.getKeyLength() + (long) kv.getValueLength() + 1L;
      offset += incr;
    }
    return null;
  }

  @Override
  public HFileBlock cloneForUnpack() {
    return new HFileDataBlock(context, allocateBuffer(), 0);
  }
}
