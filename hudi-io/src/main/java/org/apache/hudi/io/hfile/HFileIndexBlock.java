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

import java.util.ArrayList;
import java.util.List;

public abstract class HFileIndexBlock extends HFileBlock {
  protected final List<BlockIndexEntry> entries = new ArrayList<>();
  protected long blockDataSize = -1L;

  protected HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType,
                         byte[] byteBuff,
                         int startOffsetInBuff) {
    super(context, blockType, byteBuff, startOffsetInBuff);
  }

  protected HFileIndexBlock(HFileContext context,
                         HFileBlockType blockType) {
    super(context, blockType, -1L);
  }

  public void add(byte[] firstKey, long offset, int size) {
    Key key = new Key(firstKey);
    entries.add(new BlockIndexEntry(key, Option.empty(), offset, size));
    // 8 bytes for offset, 4 bytes for size.
    longestEntrySize = Math.max(longestEntrySize, key.getContentLength() + 12);
  }

  public int getNumOfEntries() {
    return entries.size();
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }
}
