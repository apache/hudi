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

package org.apache.hudi.sink.append;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.util.MutableIteratorWrapperIterator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sink function to write the data to the underneath filesystem with buffer sort
 * to improve the parquet compression rate.
 *
 * <p>The function writes base files directly for each checkpoint,
 * the file may roll over when it’s size hits the configured threshold.
 *
 * @param <T> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class AppendWriteFunctionWithBufferSort<T> extends AppendWriteFunction<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AppendWriteFunctionWithBufferSort.class);
  private final long writeBufferSize;
  private transient BinaryInMemorySortBuffer buffer;

  public AppendWriteFunctionWithBufferSort(Configuration config, RowType rowType) {
    super(config, rowType);
    this.writeBufferSize = config.get(FlinkOptions.WRITE_BUFFER_SIZE);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    String sortKeys = config.get(FlinkOptions.WRITE_BUFFER_SORT_KEYS);
    if (sortKeys == null) {
      throw new IllegalArgumentException("Sort keys can't be null for append write with buffer sort.");
    }
    List<String> sortKeyList = Arrays.stream(sortKeys.split(",")).map(key -> key.trim()).collect(Collectors.toList());
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortKeyList.toArray(new String[0]));
    SortCodeGenerator codeGenerator = sortOperatorGen.createSortCodeGenerator();
    GeneratedNormalizedKeyComputer keyComputer = codeGenerator.generateNormalizedKeyComputer("SortComputer");
    GeneratedRecordComparator recordComparator = codeGenerator.generateRecordComparator("SortComparator");
    MemorySegmentPool memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
    this.buffer = BufferUtils.createBuffer(rowType,
            memorySegmentPool,
            keyComputer.newInstance(Thread.currentThread().getContextClassLoader()),
            recordComparator.newInstance(Thread.currentThread().getContextClassLoader()));
    LOG.info("{} is initialized successfully.", getClass().getSimpleName());
  }

  @Override
  public void processElement(T value, Context ctx, Collector<RowData> out) throws Exception {
    RowData data = (RowData) value;

    // 1.try to write data into memory pool
    boolean success = buffer.write(data);
    if (!success) {
      // 2. flushes the bucket if the memory pool is full
      sortAndSend();
      // 3. write the row again
      success = buffer.write(data);
      if (!success) {
        throw new HoodieException("Buffer is too small to hold a single record.");
      }
    }

    if (buffer.size() >= writeBufferSize) {
      sortAndSend();
    }
  }

  @Override
  public void snapshotState() {
    try {
      sortAndSend();
    } catch (IOException e) {
      throw new HoodieIOException("Fail to sort and flush data in buffer during snapshot state.", e);
    }
    super.snapshotState();
  }

  @Override
  public void endInput() {
    try {
      sortAndSend();
    } catch (IOException e) {
      throw new HoodieIOException("Fail to sort and flush data in buffer during endInput.", e);
    }
    super.endInput();
  }

  /**
   * For append writing, the flushing can be triggered with the following conditions:
   * 1. Checkpoint trigger, in which the current remaining data in buffer are flushed and committed.
   * 2. Binary buffer is full.
   * 3. `endInput` is called for pipelines with a bounded source.
   */
  private void sortAndSend() throws IOException {
    if (buffer.isEmpty()) {
      return;
    }
    if (this.writerHelper == null) {
      initWriterHelper();
    }
    sort(buffer);
    Iterator<BinaryRowData> iterator =
            new MutableIteratorWrapperIterator<>(
                    buffer.getIterator(), () -> new BinaryRowData(rowType.getFieldCount()));
    while (iterator.hasNext()) {
      writerHelper.write(iterator.next());
    }
    buffer.reset();
  }

  private static void sort(BinaryInMemorySortBuffer dataBuffer) {
    new QuickSort().sort(dataBuffer);
  }
}
