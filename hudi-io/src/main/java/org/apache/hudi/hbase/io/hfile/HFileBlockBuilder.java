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

package org.apache.hudi.hbase.io.hfile;

import static javax.swing.Spring.UNSET;
import static org.apache.hudi.hbase.io.ByteBuffAllocator.HEAP;

import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HFileBlockBuilder {

  private BlockType blockType;
  private int onDiskSizeWithoutHeader;
  private int onDiskDataSizeWithHeader;
  private int uncompressedSizeWithoutHeader;
  private long prevBlockOffset;
  private ByteBuff buf;
  private boolean fillHeader = false;
  private long offset = UNSET;
  private int nextBlockOnDiskSize = UNSET;
  private HFileContext fileContext;
  private ByteBuffAllocator allocator = HEAP;
  private boolean isShared;

  public HFileBlockBuilder withBlockType(BlockType blockType) {
    this.blockType = blockType;
    return this;
  }

  public HFileBlockBuilder withOnDiskSizeWithoutHeader(int onDiskSizeWithoutHeader) {
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    return this;
  }

  public HFileBlockBuilder withOnDiskDataSizeWithHeader(int onDiskDataSizeWithHeader) {
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    return this;
  }

  public HFileBlockBuilder withUncompressedSizeWithoutHeader(int uncompressedSizeWithoutHeader) {
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    return this;
  }

  public HFileBlockBuilder withPrevBlockOffset(long prevBlockOffset) {
    this.prevBlockOffset = prevBlockOffset;
    return this;
  }

  public HFileBlockBuilder withByteBuff(ByteBuff buf) {
    this.buf = buf;
    return this;
  }

  public HFileBlockBuilder withFillHeader(boolean fillHeader) {
    this.fillHeader = fillHeader;
    return this;
  }

  public HFileBlockBuilder withOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public HFileBlockBuilder withNextBlockOnDiskSize(int nextBlockOnDiskSize) {
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    return this;
  }

  public HFileBlockBuilder withHFileContext(HFileContext fileContext) {
    this.fileContext = fileContext;
    return this;
  }

  public HFileBlockBuilder withByteBuffAllocator(ByteBuffAllocator allocator) {
    this.allocator = allocator;
    return this;
  }

  public HFileBlockBuilder withShared(boolean isShared) {
    this.isShared = isShared;
    return this;
  }

  public HFileBlock build() {
    if (isShared) {
      return new SharedMemHFileBlock(blockType, onDiskSizeWithoutHeader,
          uncompressedSizeWithoutHeader, prevBlockOffset, buf, fillHeader, offset,
          nextBlockOnDiskSize, onDiskDataSizeWithHeader, fileContext, allocator);
    } else {
      return new ExclusiveMemHFileBlock(blockType, onDiskSizeWithoutHeader,
          uncompressedSizeWithoutHeader, prevBlockOffset, buf, fillHeader, offset,
          nextBlockOnDiskSize, onDiskDataSizeWithHeader, fileContext, allocator);
    }
  }
}
