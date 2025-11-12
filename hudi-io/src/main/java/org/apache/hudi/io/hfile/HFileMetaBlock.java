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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents a {@link HFileBlockType#META} block.
 */
public class HFileMetaBlock extends HFileBlock {
  protected KeyValueEntry entryToWrite;

  protected HFileMetaBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.META, byteBuff, startOffsetInBuff);
  }

  private HFileMetaBlock(HFileContext context, KeyValueEntry keyValueEntry) {
    super(context, HFileBlockType.META, -1L);
    this.entryToWrite = keyValueEntry;
  }

  static HFileMetaBlock createMetaBlockToWrite(HFileContext context,
                                               KeyValueEntry keyValueEntry) {
    return new HFileMetaBlock(context, keyValueEntry);
  }

  public ByteBuffer readContent() {
    return ByteBuffer.wrap(
        getByteBuff(),
        startOffsetInBuff + HFILEBLOCK_HEADER_SIZE, uncompressedSizeWithoutHeader);
  }

  // ================ Below are for Write ================

  public byte[] getFirstKey() {
    return entryToWrite.key;
  }

  @Override
  public ByteBuffer getUncompressedBlockDataToWrite() throws IOException {
    // Note that: only value should be store in the block.
    // The key is stored in the meta index block.
    return ByteBuffer.wrap(entryToWrite.value);
  }
}
