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

package org.apache.hudi.sink.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.SortUtil;

/**
 * An implementation of @{code NormalizedKeyComputer} for {@code BinaryInMemorySortBuffer} in case sort is not needed,
 * and the returned iterator follows natural order as inserted.
 */
public class NaturalOrderKeyComputer implements NormalizedKeyComputer {
  @Override
  public void putKey(RowData rowData, MemorySegment target, int offset) {
    SortUtil.minNormalizedKey(target, offset, 1);
  }

  @Override
  public int compareKey(MemorySegment memorySegment, int i, MemorySegment target, int offset) {
    return 0;
  }

  @Override
  public void swapKey(MemorySegment seg1, int index1, MemorySegment seg2, int index2) {
    // do nothing.
  }

  @Override
  public int getNumKeyBytes() {
    return 1;
  }

  @Override
  public boolean isKeyFullyDetermines() {
    return true;
  }

  @Override
  public boolean invertKey() {
    return false;
  }
}
