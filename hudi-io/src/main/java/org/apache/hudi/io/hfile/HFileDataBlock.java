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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_BYTE;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT64;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_BEFORE_BLOCK_FIRST_KEY;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_FOUND;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_IN_RANGE;
import static org.apache.hudi.io.hfile.KeyValue.KEY_OFFSET;

/**
 * Represents a {@link HFileBlockType#DATA} block.
 */
public class HFileDataBlock extends HFileBlock {
  private static final int KEY_LENGTH_LENGTH = SIZEOF_INT16;
  private static final int COLUMN_FAMILY_LENGTH = SIZEOF_BYTE;
  private static final int VERSION_TIMESTAMP_LENGTH = SIZEOF_INT64;
  private static final int KEY_TYPE_LENGTH = SIZEOF_BYTE;
  // Hudi does not use HFile MVCC timestamp version so the version
  // is always 0, thus the byte length of the version is always 1.
  // This assumption is also validated when parsing {@link HFileInfo},
  // i.e., the maximum MVCC timestamp in a HFile must be 0.
  private static final long ZERO_TS_VERSION_BYTE_LENGTH = 1;
  // Hudi does not set version timestamp for key value pairs,
  // so the latest timestamp is used.
  private static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  // End offset of content in the block, relative to the start of the start of the block
  protected final int uncompressedContentEndRelativeOffset;
  private final List<KeyValueEntry> entriesToWrite = new ArrayList<>();

  // For read purpose.
  protected HFileDataBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.DATA, byteBuff, startOffsetInBuff);

    this.uncompressedContentEndRelativeOffset =
        this.uncompressedEndOffset - this.sizeCheckSum - this.startOffsetInBuff;
  }

  // For write purpose.
  private HFileDataBlock(HFileContext context, long previousBlockOffset) {
    super(context, HFileBlockType.DATA, previousBlockOffset);
    // This is not used for write.
    uncompressedContentEndRelativeOffset = -1;
  }

  static HFileDataBlock createDataBlockToWrite(HFileContext context,
                                               long previousBlockOffset) {
    return new HFileDataBlock(context, previousBlockOffset);
  }

  /**
   * Seeks to the key to look up. The key may not have an exact match.
   *
   * @param cursor                 {@link HFileCursor} containing the current position relative
   *                               to the beginning of the HFile (not the block start offset).
   * @param key                    key to look up.
   * @param blockStartOffsetInFile the start offset of the block relative to the beginning of the
   *                               HFile.
   * @return 0 ({@link HFileReader#SEEK_TO_FOUND}) if the block contains the exact same key as
   * the lookup key; the cursor points to the key;
   * 1 ({@link HFileReader#SEEK_TO_IN_RANGE}) if the lookup key does not exist, and the lookup
   * key is lexicographically greater than the actual first key of the data block and
   * lexicographically smaller than the start key of the next data block based on the block index,
   * or lexicographically smaller than or equal to the last key of the file if the data block is
   * the last one; the cursor points to the lexicographically largest key that is smaller
   * than the lookup key;
   * -2 (@link HFileReader#SEEK_TO_BEFORE_BLOCK_FIRST_KEY) if the lookup key does not exist,
   * and the lookup key is lexicographically greater than or equal to the fake first key of the
   * data block based on the block index and lexicographically smaller than the actual first key
   * of the data block; the cursor points to the actual first key of the data block which is
   * lexicographically greater than the lookup key.
   */
  int seekTo(HFileCursor cursor, Key key, int blockStartOffsetInFile) {
    int relativeOffset = cursor.getOffset() - blockStartOffsetInFile;
    int lastRelativeOffset = relativeOffset;
    Option<KeyValue> lastKeyValue = cursor.getKeyValue();
    while (relativeOffset < uncompressedContentEndRelativeOffset) {
      // Full length is not known yet until parsing
      KeyValue kv = readKeyValue(relativeOffset);
      int comp = kv.getKey().compareTo(key);
      if (comp == 0) {
        // The lookup key equals the key `relativeOffset` points to; the key is found.
        // Set the cursor to the current offset that points to the exact match
        cursor.set(relativeOffset + blockStartOffsetInFile, kv);
        return SEEK_TO_FOUND;
      } else if (comp > 0) {
        // There is no matched key (otherwise, the method should already stop there and return 0)
        // and the key `relativeOffset` points to is already greater than the lookup key.
        // So set the cursor to the previous offset, pointing the greatest key in the file that is
        // less than the lookup key.
        if (lastKeyValue.isPresent()) {
          // If the key-value pair is already, cache it
          cursor.set(lastRelativeOffset + blockStartOffsetInFile, lastKeyValue.get());
        } else {
          // Otherwise, defer the read till it's needed
          cursor.setOffset(lastRelativeOffset + blockStartOffsetInFile);
        }
        // If the lookup key is lexicographically smaller than the first key pointed to by
        // the cursor, SEEK_TO_BEFORE_BLOCK_FIRST_KEY should be returned, so the caller
        // know that the cursor is ahead of the lookup key in this case.
        return isAtFirstKey(relativeOffset) ? SEEK_TO_BEFORE_BLOCK_FIRST_KEY : SEEK_TO_IN_RANGE;
      }
      long increment =
          (long) KEY_OFFSET + (long) kv.getKeyLength() + (long) kv.getValueLength()
              + ZERO_TS_VERSION_BYTE_LENGTH;
      lastRelativeOffset = relativeOffset;
      relativeOffset += increment;
      lastKeyValue = Option.of(kv);
    }
    // We reach the end of the block. Set the cursor to the offset of last key.
    // In this case, the lookup key is greater than the last key.
    if (lastKeyValue.isPresent()) {
      cursor.set(lastRelativeOffset + blockStartOffsetInFile, lastKeyValue.get());
    } else {
      cursor.setOffset(lastRelativeOffset + blockStartOffsetInFile);
    }
    return SEEK_TO_IN_RANGE;
  }

  /**
   * Reads the key value at the offset.
   *
   * @param offset offset to read relative to the start of {@code byteBuff}.
   * @return the {@link KeyValue} instance.
   */
  KeyValue readKeyValue(int offset) {
    return new KeyValue(byteBuff, offset);
  }

  /**
   * Moves the cursor to next {@link KeyValue}.
   *
   * @param cursor                 {@link HFileCursor} instance containing the current position.
   * @param blockStartOffsetInFile the start offset of the block relative to the beginning of the
   *                               HFile.
   * @return {@code true} if there is next {@link KeyValue}; {code false} otherwise.
   */
  boolean next(HFileCursor cursor, int blockStartOffsetInFile) {
    int offset = cursor.getOffset() - blockStartOffsetInFile;
    Option<KeyValue> keyValue = cursor.getKeyValue();
    if (!keyValue.isPresent()) {
      keyValue = Option.of(readKeyValue(offset));
    }
    cursor.increment((long) KEY_OFFSET + (long) keyValue.get().getKeyLength()
        + (long) keyValue.get().getValueLength() + ZERO_TS_VERSION_BYTE_LENGTH);
    return cursor.getOffset() - blockStartOffsetInFile < uncompressedContentEndRelativeOffset;
  }

  private boolean isAtFirstKey(int relativeOffset) {
    return relativeOffset == HFILEBLOCK_HEADER_SIZE;
  }

  // ================ Below are for Write ================

  boolean isEmpty() {
    return entriesToWrite.isEmpty();
  }

  void add(byte[] key, byte[] value) {
    KeyValueEntry kv = new KeyValueEntry(key, value);
    // Assume all entries are sorted before write.
    entriesToWrite.add(kv);
    longestEntrySize = Math.max(longestEntrySize, key.length + value.length);
  }

  int getNumOfEntries() {
    return entriesToWrite.size();
  }

  byte[] getFirstKey() {
    return entriesToWrite.get(0).key;
  }

  byte[] getLastKeyContent() {
    if (entriesToWrite.isEmpty()) {
      return new byte[0];
    }
    return entriesToWrite.get(entriesToWrite.size() - 1).key;
  }

  @Override
  protected int calculateBufferCapacity() {
    // Key length = 4,
    // value length = 4,
    // key length length = 2,
    // 10 bytes for column family, timestamp, and key type,
    // 1 byte for MVCC.
    // Sum is 21 bytes.
    // So the capacity of the buffer should be: longestEntrySize + 21.
    return longestEntrySize + 21;
  }

  @Override
  protected ByteBuffer getUncompressedBlockDataToWrite() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteBuffer dataBuf = ByteBuffer.allocate(calculateBufferCapacity());
    for (KeyValueEntry kv : entriesToWrite) {
      // Length of key + length of a short variable indicating length of key.
      // Note that 10 extra bytes are required by hbase reader.
      // That is: 1 byte for column family length, 8 bytes for timestamp, 1 bytes for key type.
      dataBuf.putInt(kv.key.length + KEY_LENGTH_LENGTH + COLUMN_FAMILY_LENGTH + VERSION_TIMESTAMP_LENGTH + KEY_TYPE_LENGTH);
      // Length of value.
      dataBuf.putInt(kv.value.length);
      // Key content length.
      dataBuf.putShort((short)kv.key.length);
      // Key.
      dataBuf.put(kv.key);
      // Column family length: constant 0.
      dataBuf.put((byte)0);
      // Column qualifier: assume 0 bits.
      // Timestamp: using the latest.
      dataBuf.putLong(LATEST_TIMESTAMP);
      // Key type: constant Put (4) in Hudi.
      // Minimum((byte) 0), Put((byte) 4), Delete((byte) 8),
      // DeleteFamilyVersion((byte) 10), DeleteColumn((byte) 12),
      // DeleteFamily((byte) 14), Maximum((byte) 255).
      dataBuf.put((byte)4);
      // Value.
      dataBuf.put(kv.value);
      // MVCC.
      dataBuf.put((byte)0);

      // Copy to output stream.
      baos.write(dataBuf.array(), 0, dataBuf.position());
      // Clear the buffer.
      dataBuf.clear();
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }
}
