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

import org.apache.hudi.io.util.IOUtils;
import org.apache.hudi.io.util.Option;

import java.io.IOException;

import static org.apache.hudi.io.hfile.KeyValue.KEY_OFFSET;

/**
 * Represents a {@link HFileBlockType#DATA} block in the "Scanned block" section.
 */
public class HFileDataBlock extends HFileBlock {
  // Hudi does not use HFile MVCC timestamp version so the version
  // is always 0, thus the byte length of the version is always 1.
  private static final long ZERO_TS_VERSION_BYTE_LENGTH = 1;


  protected final int uncompressContentEndOffset;

  protected HFileDataBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.DATA, byteBuff, startOffsetInBuff);

    this.uncompressContentEndOffset = this.uncompressedEndOffset - this.sizeCheckSum;
  }

  /**
   * Seeks to the key to look up.
   *
   * @param key Key to look up.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Option} if the lookup key does not exist.
   */
  public Option<KeyValue> seekTo(Key key) {
    int offset = startOffsetInBuff + HFILEBLOCK_HEADER_SIZE;
    int endOffset = offset + onDiskSizeWithoutHeader;
    while (offset + HFILEBLOCK_HEADER_SIZE < endOffset) {
      // Full length is not known yet until parsing
      KeyValue kv = readKeyValue(offset);
      int comp =
          IOUtils.compareTo(kv.getBytes(), kv.getKeyContentOffset(), kv.getKeyContentLength(),
              key.getBytes(), key.getContentOffset(), key.getContentLength());
      if (comp == 0) {
        return Option.of(kv);
      } else if (comp > 0) {
        return Option.empty();
      }
      long increment =
          (long) KEY_OFFSET + (long) kv.getKeyLength() + (long) kv.getValueLength()
              + ZERO_TS_VERSION_BYTE_LENGTH;
      offset += increment;
    }
    return Option.empty();
  }

  /**
   * Seeks to the key to look up.
   *
   * @param key Key to look up.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Option} if the lookup key does not exist.
   */
  public int seekTo(HFilePosition position, Key key, int blockStartOffsetInFile) {
    int offset = position.getOffset() - blockStartOffsetInFile;
    int endOffset = uncompressContentEndOffset;
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

  public KeyValue readKeyValue(int offset) {
    return new KeyValue(byteBuff, offset);
  }

  public boolean next(HFilePosition position, int blockStartOffsetInFile) throws IOException {
    int offset = position.getOffset() - blockStartOffsetInFile;
    Option<KeyValue> keyValue = position.getKeyValue();
    if (!keyValue.isPresent()) {
      keyValue = Option.of(readKeyValue(offset));
    }
    position.increment((long) KEY_OFFSET + (long) keyValue.get().getKeyLength()
        + (long) keyValue.get().getValueLength() + ZERO_TS_VERSION_BYTE_LENGTH);
    return position.getOffset() - blockStartOffsetInFile < uncompressContentEndOffset;
  }

  @Override
  public HFileBlock cloneForUnpack() {
    return new HFileDataBlock(context, allocateBuffer(), 0);
  }
}
