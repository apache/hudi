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

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.io.hfile.ByteUtils.copy;
import static org.apache.hudi.io.hfile.ByteUtils.decodeVLongSizeOnDisk;
import static org.apache.hudi.io.hfile.ByteUtils.readInt;
import static org.apache.hudi.io.hfile.ByteUtils.readLong;
import static org.apache.hudi.io.hfile.ByteUtils.readVLong;

/**
 * Represents a {@link HFileBlockType#ROOT_INDEX} block in the "Load-on-open" section.
 */
public class HFileRootIndexBlock extends HFileBlock {
  public HFileRootIndexBlock(HFileContext context,
                             byte[] byteBuff,
                             int startOffsetInBuff) {
    super(context, HFileBlockType.ROOT_INDEX, byteBuff, startOffsetInBuff);
  }

  /**
   * Reads the data index block.
   *
   * @param numEntries The number of entries in the block.
   * @return A list of {@link BlockIndexEntry}.
   */
  public List<BlockIndexEntry> readDataIndex(int numEntries) {
    List<BlockIndexEntry> entryList = new ArrayList<>();
    int buffOffset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;
    for (int i = 0; i < numEntries; i++) {
      long offset = readLong(byteBuff, buffOffset);
      int size = readInt(byteBuff, buffOffset + 8);
      int vLongSizeOnDist = decodeVLongSizeOnDisk(byteBuff, buffOffset + 12);
      int keyLength = (int) readVLong(byteBuff, buffOffset + 12, vLongSizeOnDist);
      byte[] firstKeyBytes = copy(byteBuff, buffOffset + 12 + vLongSizeOnDist, keyLength);
      KeyOnlyKeyValue firstKey = new KeyOnlyKeyValue(firstKeyBytes, 0, firstKeyBytes.length);
      entryList.add(new BlockIndexEntry(firstKey, offset, size));
      buffOffset += (12 + vLongSizeOnDist + keyLength);
    }
    return entryList;
  }

  @Override
  public HFileBlock cloneForUnpack() {
    return new HFileRootIndexBlock(context, allocateBuffer(), 0);
  }
}
