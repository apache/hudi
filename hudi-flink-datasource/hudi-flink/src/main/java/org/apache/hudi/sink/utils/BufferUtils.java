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

import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;

import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities to create binary buffer for writing functions.
 */
public class BufferUtils {
  // minimum pages for a BinaryInMemorySortBuffer
  private static final int MIN_REQUIRED_BUFFERS = 3;

  public static BinaryInMemorySortBuffer createBuffer(RowType rowType, MemorySegmentPool memorySegmentPool) {
    return createBuffer(rowType, memorySegmentPool,  new NaturalOrderKeyComputer(), new NaturalOrderRecordComparator());
  }

  public static BinaryInMemorySortBuffer createBuffer(RowType rowType, MemorySegmentPool memorySegmentPool, NormalizedKeyComputer keyComputer, RecordComparator recordComparator) {
    if (memorySegmentPool.freePages() < MIN_REQUIRED_BUFFERS) {
      // there is no enough free pages to create a binary buffer, may need flush first.
      throw new MemoryPagesExhaustedException("Free pages are not enough to create a BinaryInMemorySortBuffer.");
    }
    return BinaryInMemorySortBuffer.createBuffer(
        keyComputer,
        new RowDataSerializer(rowType),
        new BinaryRowDataSerializer(rowType.getFieldCount()),
        recordComparator,
        memorySegmentPool);
  }

  /**
   * Returns whether code-generated typed-field sorting preserves encoded hoodie recordKey ordering.
   *
   * <p>Code-generated sorting uses normalized keys and a generated comparator, and is much more
   * efficient than repeatedly encoding record keys during comparison. Most tables use a single
   * string record key, so this fast path covers the common case.
   *
   * <p>A single string field has the same order as its encoded record key regardless of the
   * complex key generator encoding. With the new encoding the record key is the field value
   * itself; the compatible encoding may produce {@code <field>:<value>}, but {@code <field>:} is
   * identical for every record and therefore does not affect ordering. Numeric and composite keys
   * can have a different typed-field order and must use encoded-key comparison.
   */
  public static boolean canUseCodegenSorting(RowType rowType, String[] recordKeyFields) {
    if (recordKeyFields.length != 1) {
      return false;
    }
    int fieldIndex = rowType.getFieldIndex(recordKeyFields[0]);
    LogicalTypeRoot typeRoot = rowType.getTypeAt(fieldIndex).getTypeRoot();
    return typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR;
  }
}
