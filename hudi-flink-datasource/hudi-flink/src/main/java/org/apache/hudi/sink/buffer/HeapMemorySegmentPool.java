/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.LinkedList;
import java.util.List;

/**
 * A heap memory based ${code MemorySegmentPool}.
 */
public class HeapMemorySegmentPool implements MemorySegmentPool {

  private final LinkedList<MemorySegment> cachePages;
  private final int maxPages;
  private final int pageSize;
  private int allocateNum;

  public HeapMemorySegmentPool(int pageSize, long maxSizeInBytes) {
    this.cachePages = new LinkedList<>();
    this.maxPages = (int) (maxSizeInBytes / pageSize);
    this.pageSize = pageSize;
    this.allocateNum = 0;
  }

  @Override
  public int pageSize() {
    return this.pageSize;
  }

  @Override
  public void returnAll(List<MemorySegment> list) {
    cachePages.addAll(list);
  }

  @Override
  public int freePages() {
    return maxPages - allocateNum + cachePages.size();
  }

  @Override
  public MemorySegment nextSegment() {
    if (!cachePages.isEmpty()) {
      return cachePages.poll();
    }
    if (freePages() > 0) {
      allocateNum += 1;
      return MemorySegmentFactory.wrap(new byte[pageSize]);
    }
    return null;
  }
}
