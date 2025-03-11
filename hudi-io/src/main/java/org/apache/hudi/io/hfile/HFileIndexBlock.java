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

import org.apache.hudi.io.hfile.writer.IndexEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;

public class HFileIndexBlock extends HFileBlock {
  protected final List<IndexEntry> entries = new ArrayList<>();
  protected long payloadSize = -1L;

  public HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType,
                         byte[] byteBuff,
                         int startOffsetInBuff) {
    super(context, blockType, byteBuff, startOffsetInBuff);
  }

  public HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType) {
    super(context, blockType);
  }

  public void add(byte[] firstKey, long offset, int size) {
    entries.add(new IndexEntry(firstKey, offset, size));
  }

  public int getNumOfEntries() {
    return entries.size();
  }

  public long getPayloadSize() {
    return payloadSize;
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  @Override
  public ByteBuffer getPayload() {
    ByteBuffer buf = ByteBuffer.allocate(context.getBlockSize() * 2);
    for (IndexEntry entry : entries) {
      buf.putLong(entry.offset);
      buf.putInt(entry.size);
      // Key length + 2.
      try {
        byte[] keyLength = getVariableLengthEncodes(entry.firstKey.length + SIZEOF_INT16);
        buf.put(keyLength);
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize number: " + entry.firstKey.length + SIZEOF_INT16);
      }
      // Key length.
      buf.putShort((short) entry.firstKey.length);
      // Key.
      buf.put(entry.firstKey);
    }
    buf.flip();

    // Set metrics.
    payloadSize = buf.limit();
    return buf;
  }
}
