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

import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The {@link ByteBuffAllocator} won't allocate pooled heap {@link ByteBuff} now; at the same time,
 * if allocate an off-heap {@link ByteBuff} from allocator, then it must be a pooled one. That's to
 * say, an exclusive memory HFileBlock would must be an heap block and a shared memory HFileBlock
 * would must be an off-heap block.
 * @see org.apache.hudi.hbase.io.hfile.ExclusiveMemHFileBlock
 **/
@InterfaceAudience.Private
public class SharedMemHFileBlock extends HFileBlock {

  SharedMemHFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
                      int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuff buf, boolean fillHeader,
                      long offset, int nextBlockOnDiskSize, int onDiskDataSizeWithHeader, HFileContext fileContext,
                      ByteBuffAllocator alloc) {
    super(blockType, onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader, prevBlockOffset, buf,
        fillHeader, offset, nextBlockOnDiskSize, onDiskDataSizeWithHeader, fileContext, alloc);
  }

  @Override
  public boolean isSharedMem() {
    return true;
  }
}
