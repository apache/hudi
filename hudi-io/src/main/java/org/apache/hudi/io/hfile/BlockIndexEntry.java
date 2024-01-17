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

/**
 * Represents the index entry of a data block in the Data Index stored in the
 * {@link HFileBlockType#ROOT_INDEX} block.
 * <p>
 * This is completely in-memory representation and does not involve byte parsing.
 * <p>
 * When comparing two {@link BlockIndexEntry} instances, the underlying bytes of the keys
 * are compared in lexicographical order.
 */
public class BlockIndexEntry implements Comparable<BlockIndexEntry> {
  private final Key key;
  private final Option<Key> nextBlockKey;
  private final long offset;
  private final int size;

  public BlockIndexEntry(Key key, Option<Key> nextBlockFirstKey,
                         long offset,
                         int size) {
    this.key = key;
    this.nextBlockKey = nextBlockFirstKey;
    this.offset = offset;
    this.size = size;
  }

  public Key getKey() {
    return key;
  }

  public Option<Key> getNextBlockKey() {
    return nextBlockKey;
  }

  public long getOffset() {
    return offset;
  }

  public int getSize() {
    return size;
  }

  @Override
  public int compareTo(BlockIndexEntry o) {
    return key.compareTo(o.getKey());
  }

  @Override
  public String toString() {
    return "BlockIndexEntry{firstKey="
        + key.toString()
        + ", offset="
        + offset
        + ", size="
        + size
        + "}";
  }
}
