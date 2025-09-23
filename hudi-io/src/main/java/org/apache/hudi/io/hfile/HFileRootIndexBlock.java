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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;
import static org.apache.hudi.io.util.IOUtils.copy;
import static org.apache.hudi.io.util.IOUtils.decodeVarLongSizeOnDisk;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.apache.hudi.io.util.IOUtils.readLong;
import static org.apache.hudi.io.util.IOUtils.readVarLong;

/**
 * Represents a {@link HFileBlockType#ROOT_INDEX} block.
 */
public class HFileRootIndexBlock extends HFileIndexBlock {
  public HFileRootIndexBlock(HFileContext context,
                             byte[] byteBuff,
                             int startOffsetInBuff) {
    super(context, HFileBlockType.ROOT_INDEX, byteBuff, startOffsetInBuff);
  }

  private HFileRootIndexBlock(HFileContext context) {
    super(context, HFileBlockType.ROOT_INDEX);
  }

  public static HFileRootIndexBlock createRootIndexBlockToWrite(HFileContext context) {
    return new HFileRootIndexBlock(context);
  }

  /**
   * Reads the index block and returns the block index entry to an in-memory {@link TreeMap}
   * for searches.
   *
   * @param numEntries     the number of entries in the block
   * @param contentKeyOnly whether the key part contains content only
   * @return a {@link TreeMap} of block index entries
   */
  public TreeMap<Key, BlockIndexEntry> readBlockIndex(int numEntries, boolean contentKeyOnly) {
    TreeMap<Key, BlockIndexEntry> blockIndexEntryMap = new TreeMap<>();
    List<BlockIndexEntry> indexEntryList = readBlockIndexEntry(numEntries, contentKeyOnly);
    for (int i = 0; i < numEntries; i++) {
      Key key = indexEntryList.get(i).getFirstKey();
      blockIndexEntryMap.put(key, new BlockIndexEntry(
          key,
          i < numEntries - 1 ? Option.of(indexEntryList.get(i + 1).getFirstKey()) : Option.empty(),
          indexEntryList.get(i).getOffset(),
          indexEntryList.get(i).getSize()));
    }
    return blockIndexEntryMap;
  }

  /**
   * Returns the block index entries contained in the root index block.
   *
   * @param numEntries     the number of entries in the block
   * @param contentKeyOnly whether the key part contains content only
   * @return a {@link List} of block index entries
   */
  public List<BlockIndexEntry> readBlockIndexEntry(int numEntries,
                                                   boolean contentKeyOnly) {
    List<BlockIndexEntry> indexEntryList = new ArrayList<>();
    int buffOffset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;
    for (int i = 0; i < numEntries; i++) {
      long offset = readLong(byteBuff, buffOffset);
      int size = readInt(byteBuff, buffOffset + 8);
      int varLongSizeOnDist = decodeVarLongSizeOnDisk(byteBuff, buffOffset + 12);
      int keyLength = (int) readVarLong(byteBuff, buffOffset + 12, varLongSizeOnDist);
      byte[] keyBytes = copy(byteBuff, buffOffset + 12 + varLongSizeOnDist, keyLength);
      Key key = contentKeyOnly ? new UTF8StringKey(keyBytes) : new Key(keyBytes);
      indexEntryList.add(new BlockIndexEntry(key, Option.empty(), offset, size));
      buffOffset += (12 + varLongSizeOnDist + keyLength);
    }
    return indexEntryList;
  }

  @Override
  protected int calculateBufferCapacity() {
    // 2 bytes for length of key length.
    // 5 bytes for maximal key length.
    return longestEntrySize + 2 + 5;
  }

  @Override
  public ByteBuffer getUncompressedBlockDataToWrite() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteBuffer buf = ByteBuffer.allocate(calculateBufferCapacity());
    for (BlockIndexEntry entry : entries) {
      buf.putLong(entry.getOffset());
      buf.putInt(entry.getSize());

      // Key length + 2.
      try {
        byte[] keyLength = getVariableLengthEncodedBytes(
            entry.getFirstKey().getLength() + SIZEOF_INT16);
        buf.put(keyLength);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to serialize number: " + entry.getFirstKey().getLength() + SIZEOF_INT16);
      }
      // Key length.
      buf.putShort((short) entry.getFirstKey().getLength());
      // Key.
      buf.put(entry.getFirstKey().getBytes());
      // Copy to output stream.
      baos.write(buf.array(), 0, buf.position());
      // Clear the buffer.
      buf.clear();
    }

    // Output all data in a buffer.
    byte[] allData = baos.toByteArray();
    blockDataSize = allData.length;
    return ByteBuffer.wrap(allData);
  }
}
