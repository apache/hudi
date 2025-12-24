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

import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;

import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities to create binary buffer for writing functions.
 */
public class BufferUtils {
  // minimum pages for a BinaryInMemorySortBuffer
  private static final int MIN_REQUIRED_BUFFERS = 3;

  public static BinaryInMemorySortBuffer createBuffer(RowType rowType,
                                                      MemorySegmentPool memorySegmentPool,
                                                      ClassLoader cl,
                                                      String[] sortColumns) {
    return createBuffer(rowType, memorySegmentPool, cl, sortColumns, new TableConfig());
  }

  public static BinaryInMemorySortBuffer createBuffer(RowType rowType,
                                                      MemorySegmentPool memorySegmentPool,
                                                      ClassLoader cl,
                                                      String[] sortColumns,
                                                      TableConfig tableConfig) {
    if (memorySegmentPool.freePages() < MIN_REQUIRED_BUFFERS) {
      // there is no enough free pages to create a binary buffer, may need flush first.
      throw new MemoryPagesExhaustedException("Free pages are not enough to create a BinaryInMemorySortBuffer.");
    }
    // sort records based on record keys
    NormalizedKeyComputer keyComputer = createSortCodeGenerator(rowType, sortColumns, tableConfig)
        .generateNormalizedKeyComputer("SortComputer").newInstance(cl);
    RecordComparator recordComparator = createSortCodeGenerator(rowType, sortColumns, tableConfig)
        .generateRecordComparator("SortComparator").newInstance(cl);
    return BinaryInMemorySortBuffer.createBuffer(
        keyComputer,
        new RowDataSerializer(rowType),
        new BinaryRowDataSerializer(rowType.getFieldCount()),
        recordComparator,
        memorySegmentPool);
  }

  private static SortCodeGenerator createSortCodeGenerator(RowType rowType, String[] sortColumns, TableConfig tableConfig) {
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortColumns);
    return sortOperatorGen.createSortCodeGenerator(tableConfig);
  }

  public static void sort(BinaryInMemorySortBuffer dataBuffer) {
    new QuickSort().sort(dataBuffer);
  }
}