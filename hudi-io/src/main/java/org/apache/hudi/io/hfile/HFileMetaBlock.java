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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a {@link HFileBlockType#META} block.
 */
public class HFileMetaBlock extends HFileBlock {
  protected HFileMetaBlock(HFileContext context,
                           byte[] byteBuff,
                           int startOffsetInBuff) {
    super(context, HFileBlockType.META, byteBuff, startOffsetInBuff);
  }

  public ByteBuffer readContent() {
    return ByteBuffer.wrap(
        getByteBuff(),
        readAttributesOpt.get().startOffsetInBuff + HFILEBLOCK_HEADER_SIZE,
        readAttributesOpt.get().uncompressedSizeWithoutHeader);
  }

  // ================ Below are for Write ================

  protected final List<KeyValueEntry> entries = new ArrayList<>();

  public HFileMetaBlock(HFileContext context) {
    super(context, HFileBlockType.META, -1L);
  }

  public byte[] getFirstKey() {
    return entries.get(0).key;
  }

  public void add(byte[] key, byte[] value) {
    KeyValueEntry kv = new KeyValueEntry(key, value);
    add(kv, false);
  }

  protected void add(KeyValueEntry kv, boolean sorted) {
    entries.add(kv);
    if (sorted) {
      entries.sort(KeyValueEntry::compareTo);
    }
  }

  @Override
  public ByteBuffer getPayload() {
    ByteBuffer dataBuf = ByteBuffer.allocate(context.getBlockSize());
    // Rule 1: there must be only one key-value entry.
    assert (1 == entries.size())
        : "only 1 value is allowed in meta block";
    // Rule 2: only value should be store in the block.
    // The key is stored in the meta index block.
    dataBuf.put(entries.get(0).value);
    dataBuf.flip();
    return dataBuf;
  }
}
