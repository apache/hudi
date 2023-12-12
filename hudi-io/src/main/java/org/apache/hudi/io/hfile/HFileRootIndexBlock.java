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

import java.util.TreeMap;

import static org.apache.hudi.io.util.IOUtils.copy;
import static org.apache.hudi.io.util.IOUtils.decodeVarLongSizeOnDisk;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.apache.hudi.io.util.IOUtils.readLong;
import static org.apache.hudi.io.util.IOUtils.readVarLong;

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
   * Reads the data index block and returns the block index entry to an in-memory {@link TreeMap}
   * for searches.
   *
   * @param numEntries The number of entries in the block.
   * @return A {@link TreeMap} of block index entries.
   */
  public TreeMap<Key, BlockIndexEntry> readDataIndex(int numEntries) {
    TreeMap<Key, BlockIndexEntry> blockIndexEntryMap = new TreeMap<>();
    int buffOffset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;
    for (int i = 0; i < numEntries; i++) {
      long offset = readLong(byteBuff, buffOffset);
      int size = readInt(byteBuff, buffOffset + 8);
      int varLongSizeOnDist = decodeVarLongSizeOnDisk(byteBuff, buffOffset + 12);
      int keyLength = (int) readVarLong(byteBuff, buffOffset + 12, varLongSizeOnDist);
      byte[] firstKeyBytes = copy(byteBuff, buffOffset + 12 + varLongSizeOnDist, keyLength);
      Key firstKey = new Key(firstKeyBytes, 0, firstKeyBytes.length);
      blockIndexEntryMap.put(firstKey, new BlockIndexEntry(firstKey, offset, size));
      buffOffset += (12 + varLongSizeOnDist + keyLength);
    }
    return blockIndexEntryMap;
  }

  @Override
  public HFileBlock cloneForUnpack() {
    return new HFileRootIndexBlock(context, allocateBuffer(), 0);
  }
}
