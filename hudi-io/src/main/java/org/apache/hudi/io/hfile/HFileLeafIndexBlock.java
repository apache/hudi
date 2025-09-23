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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.io.util.IOUtils.copy;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.apache.hudi.io.util.IOUtils.readLong;

/**
 * Represents a {@link HFileBlockType#LEAF_INDEX} block, as
 * part of a multi-level block index.
 */
public class HFileLeafIndexBlock extends HFileBlock {
  protected HFileLeafIndexBlock(HFileContext context,
                                byte[] byteBuff,
                                int startOffsetInBuff) {
    super(context, HFileBlockType.LEAF_INDEX, byteBuff, startOffsetInBuff);
  }

  protected HFileLeafIndexBlock(HFileContext context,
                                HFileBlockType blockType,
                                byte[] byteBuff,
                                int startOffsetInBuff) {
    super(context, blockType, byteBuff, startOffsetInBuff);
  }

  @Override
  protected ByteBuffer getUncompressedBlockDataToWrite() {
    throw new HoodieException("HFile writer does not support leaf index block");
  }

  @Override
  protected int calculateBufferCapacity() {
    throw new HoodieException("HFile writer does not support leaf index block");
  }

  /**
   * Reads the index block and returns the block index entries.
   */
  public List<BlockIndexEntry> readBlockIndex() {
    // 0. Print block magic
    int buffOffset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;

    // 1. Get the number of entries.
    int numEntries = readInt(byteBuff, buffOffset);
    buffOffset += DataSize.SIZEOF_INT32;
    // 2. Parse the secondary index.
    List<Integer> relativeOffsets = new ArrayList<>();
    for (int i = 0; i <= numEntries; i++) {
      relativeOffsets.add(readInt(byteBuff, buffOffset));
      buffOffset += DataSize.SIZEOF_INT32;
    }
    // 3. Read index entries.
    List<BlockIndexEntry> indexEntries = new ArrayList<>();
    int secondIndexAfterOffset = buffOffset;
    for (int i = 0; i < numEntries; i++) {
      ValidationUtils.checkState(buffOffset - secondIndexAfterOffset == relativeOffsets.get(i));
      long offset = readLong(byteBuff, buffOffset);
      int size = readInt(byteBuff, buffOffset + 8);
      // Key parsing requires different logic than that of root index.
      int keyStartOffset = buffOffset + 12;
      int nextEntryStartOffset = secondIndexAfterOffset + relativeOffsets.get(i + 1);
      int keyLength = nextEntryStartOffset - keyStartOffset;
      byte[] keyBytes = copy(byteBuff, buffOffset + 12, keyLength);
      Key key = new Key(keyBytes);
      indexEntries.add(new BlockIndexEntry(key, Option.empty(), offset, size));
      buffOffset += (12 + keyLength);
    }
    return indexEntries;
  }
}
