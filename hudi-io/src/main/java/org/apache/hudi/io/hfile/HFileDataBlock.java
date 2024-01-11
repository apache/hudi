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

  // End offset of content in the block, relative to the start of the byte array {@link byteBuff}
  protected final int uncompressedContentEndOffset;

  protected HFileDataBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.DATA, byteBuff, startOffsetInBuff);

    this.uncompressedContentEndOffset = this.uncompressedEndOffset - this.sizeCheckSum;
  }

  /**
   * Seeks to the key to look up. The key may not have an exact match.
   *
   * @param position               {@link HFilePosition} containing the current position relative
   *                               to the beginning of the HFile (not the block start offset).
   * @param key                    key to look up.
   * @param blockStartOffsetInFile the start offset of the block relative to the beginning of the
   *                               HFile.
   * @return 0 if the block contains the exact same key as the lookup key, and the position points
   * to the key; or 1 if the lookup key does not exist, and the position points to the
   * lexicographically largest key that is smaller than the lookup key.
   */
  public int seekTo(HFilePosition position, Key key, int blockStartOffsetInFile) {
    int offset = position.getOffset() - blockStartOffsetInFile;
    int endOffset = uncompressedContentEndOffset;
    int lastOffset = offset;
    Option<KeyValue> lastKeyValue = position.getKeyValue();
    while (offset < endOffset) {
      // Full length is not known yet until parsing
      KeyValue kv = readKeyValue(offset);
      int comp = kv.getKey().compareTo(key);
      if (comp == 0) {
        position.set(offset + blockStartOffsetInFile, kv);
        return 0;
      } else if (comp > 0) {
        if (lastKeyValue.isPresent()) {
          position.set(lastOffset + blockStartOffsetInFile, lastKeyValue.get());
        } else {
          position.setOffset(lastOffset + blockStartOffsetInFile);
        }
        return 1;
      }
      long increment =
          (long) KEY_OFFSET + (long) kv.getKeyLength() + (long) kv.getValueLength()
              + ZERO_TS_VERSION_BYTE_LENGTH;
      lastOffset = offset;
      offset += increment;
      lastKeyValue = Option.of(kv);
    }
    if (lastKeyValue.isPresent()) {
      position.set(lastOffset + blockStartOffsetInFile, lastKeyValue.get());
    } else {
      position.setOffset(lastOffset + blockStartOffsetInFile);
    }
    return 1;
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
   * Moves the position to next {@link KeyValue}.
   *
   * @param position               {@link HFilePosition} instance containing the current position.
   * @param blockStartOffsetInFile the start offset of the block relative to the beginning of the
   *                               HFile.
   * @return {@code true} if there is next {@link KeyValue}; {code false} otherwise.
   */
  public boolean next(HFilePosition position, int blockStartOffsetInFile) {
    int offset = position.getOffset() - blockStartOffsetInFile;
    Option<KeyValue> keyValue = position.getKeyValue();
    if (!keyValue.isPresent()) {
      keyValue = Option.of(readKeyValue(offset));
    }
    position.increment((long) KEY_OFFSET + (long) keyValue.get().getKeyLength()
        + (long) keyValue.get().getValueLength() + ZERO_TS_VERSION_BYTE_LENGTH);
    return position.getOffset() - blockStartOffsetInFile < uncompressedContentEndOffset;
  }
}
