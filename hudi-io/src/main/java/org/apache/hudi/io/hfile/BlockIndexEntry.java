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

import lombok.Getter;

/**
 * Represents the index entry of a data block in the Data Index stored in the
 * {@link HFileBlockType#ROOT_INDEX} block.
 * <p>
 * This is completely in-memory representation and does not involve byte parsing.
 * <p>
 * When comparing two {@link BlockIndexEntry} instances, the underlying bytes of the keys
 * are compared in lexicographical order.
 */
@Getter
public class BlockIndexEntry implements Comparable<BlockIndexEntry> {
  // The first key stored in the block index can be shortened to save memory,
  // i.e., a fake first key is stored in the block index entry, and such a fake
  // first key does not exist in the data block. The fake first key is
  // lexicographically greater than the last key of the previous block and
  // lexicographically equal or smaller than the actual first key of the current block.
  private final Key firstKey;
  private final Option<Key> nextBlockFirstKey;
  private final long offset;
  private final int size;

  public BlockIndexEntry(Key firstKey, Option<Key> nextBlockFirstKey,
                         long offset,
                         int size) {
    this.firstKey = firstKey;
    this.nextBlockFirstKey = nextBlockFirstKey;
    this.offset = offset;
    this.size = size;
  }

  @Override
  public int compareTo(BlockIndexEntry o) {
    return firstKey.compareTo(o.getFirstKey());
  }

  @Override
  public String toString() {
    return "BlockIndexEntry{firstKey="
        + firstKey.toString()
        + ", offset="
        + offset
        + ", size="
        + size
        + "}";
  }
}
