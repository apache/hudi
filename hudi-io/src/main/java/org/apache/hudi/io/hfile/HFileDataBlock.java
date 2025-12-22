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

import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_FOUND;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_IN_RANGE;
import static org.apache.hudi.io.hfile.KeyValue.KEY_OFFSET;

/**
 * Represents a {@link HFileBlockType#DATA} block.
 */
public class HFileDataBlock extends HFileBlock {
  // Hudi does not use HFile MVCC timestamp version so the version
  // is always 0, thus the byte length of the version is always 1.
  // This assumption is also validated when parsing {@link HFileInfo},
  // i.e., the maximum MVCC timestamp in a HFile must be 0.
  private static final long ZERO_TS_VERSION_BYTE_LENGTH = 1;

  // End offset of content in the block, relative to the start of the start of the block
  protected final int uncompressedContentEndRelativeOffset;

  protected HFileDataBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.DATA, byteBuff, startOffsetInBuff);

    this.uncompressedContentEndRelativeOffset =
        this.uncompressedEndOffset - this.sizeCheckSum - this.startOffsetInBuff;
  }

  /**
   * Seeks to the key to look up. The key may not have an exact match.
   *
   * @param cursor                 {@link HFileCursor} containing the current position relative
   *                               to the beginning of the HFile (not the block start offset).
   * @param key                    key to look up.
   * @param blockStartOffsetInFile the start offset of the block relative to the beginning of the
   *                               HFile.
   * @return 0 if the block contains the exact same key as the lookup key, and the cursor points
   * to the key; or 1 if the lookup key does not exist, and the cursor points to the
   * lexicographically largest key that is smaller than the lookup key.
   */
  public int seekTo(HFileCursor cursor, Key key, int blockStartOffsetInFile) {
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
        return SEEK_TO_IN_RANGE;
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
  public KeyValue readKeyValue(int offset) {
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
  public boolean next(HFileCursor cursor, int blockStartOffsetInFile) {
    int offset = cursor.getOffset() - blockStartOffsetInFile;
    Option<KeyValue> keyValue = cursor.getKeyValue();
    if (!keyValue.isPresent()) {
      keyValue = Option.of(readKeyValue(offset));
    }
    cursor.increment((long) KEY_OFFSET + (long) keyValue.get().getKeyLength()
        + (long) keyValue.get().getValueLength() + ZERO_TS_VERSION_BYTE_LENGTH);
    return cursor.getOffset() - blockStartOffsetInFile < uncompressedContentEndRelativeOffset;
  }
}
