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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_BYTE;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT64;
import static org.apache.hudi.io.util.IOUtils.copy;
import static org.apache.hudi.io.util.IOUtils.decodeVarLongSizeOnDisk;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.apache.hudi.io.util.IOUtils.readLong;
import static org.apache.hudi.io.util.IOUtils.readVarLong;
import static org.apache.hudi.io.util.IOUtils.writeVarInt;

/**
 * Represents a {@link HFileBlockType#ROOT_INDEX} block.
 */
public class HFileRootIndexBlock extends HFileIndexBlock {
  // Each block-index "first key" is written as a full HBase KeyValue key
  // ([2-byte rowLen][row][1-byte cfLen=0][8-byte ts][1-byte type]) so an HBase-based HFile reader
  // can parse and point-look-up the block index, matching the data-block KeyValue encoding. The
  // native reader reads only the row via the leading 2-byte length prefix, so it reads both this
  // and the older row-only format.
  private static final long LATEST_TIMESTAMP = Long.MAX_VALUE;
  private static final byte KEY_TYPE_PUT = (byte) 4;
  // HBase KeyValue key suffix: column-family length (1) + timestamp (8) + key type (1).
  private static final int KEY_METADATA_SUFFIX_LENGTH = SIZEOF_BYTE + SIZEOF_INT64 + SIZEOF_BYTE;

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
  public ByteBuffer getUncompressedBlockDataToWrite() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(context.getBlockSize());
    try (DataOutputStream outputStream = new DataOutputStream(baos)) {
      for (BlockIndexEntry entry : entries) {
        outputStream.writeLong(entry.getOffset());
        outputStream.writeInt(entry.getSize());

        int rowLength = entry.getFirstKey().getLength();
        // 2-byte row-length prefix + row + 10-byte KeyValue suffix.
        // Hadoop WritableUtils VarInt encoding matches HBase's HFile format.
        outputStream.write(writeVarInt(SIZEOF_INT16 + rowLength + KEY_METADATA_SUFFIX_LENGTH));
        outputStream.writeShort((short) rowLength);
        outputStream.write(entry.getFirstKey().getBytes());
        // KeyValue suffix: column-family length (0), timestamp (latest), key type (Put).
        outputStream.write(0);
        outputStream.writeLong(LATEST_TIMESTAMP);
        outputStream.write(KEY_TYPE_PUT);
      }
    }

    // Output all data in a buffer.
    byte[] allData = baos.toByteArray();
    blockDataSize = allData.length;
    return ByteBuffer.wrap(allData);
  }
}
