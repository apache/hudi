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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.queue.DisruptorMessageQueue;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.util.MutableIteratorWrapperIterator;

import lombok.extern.slf4j.Slf4j;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Sink function to write the data to the underneath filesystem using LMAX Disruptor
 * as a lock-free ring buffer for better throughput.
 *
 * <p>Uses Flink's native {@link BinaryInMemorySortBuffer} with code-generated comparators
 * for efficient sorting.
 *
 * <p>The function writes base files directly for each checkpoint,
 * the file may roll over when its size hits the configured threshold.
 *
 * @param <T> Type of the input record
 * @see StreamWriteOperatorCoordinator
 * @see BufferType#DISRUPTOR
 */
@Slf4j
public class AppendWriteFunctionWithDisruptorBufferSort<T> extends AppendWriteFunction<T> {

  // writeBufferSize: record count threshold for flushing sort buffer to disk
  private final long writeBufferSize;
  // ringBufferSize: Disruptor ring buffer capacity (queue between producer and consumer threads)
  private final int ringBufferSize;
  private final String waitStrategy;

  private transient MemorySegmentPool memorySegmentPool;
  private transient GeneratedNormalizedKeyComputer keyComputer;
  private transient GeneratedRecordComparator recordComparator;
  private transient DisruptorMessageQueue<RowData, RowData> disruptorQueue;
  private transient BinaryInMemorySortBuffer sortBuffer;
  private transient SortingConsumer sortingConsumer;

  public AppendWriteFunctionWithDisruptorBufferSort(Configuration config, RowType rowType) {
    super(config, rowType);
    this.writeBufferSize = config.get(FlinkOptions.WRITE_BUFFER_SIZE);
    this.ringBufferSize = config.get(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE);
    this.waitStrategy = config.get(FlinkOptions.WRITE_BUFFER_DISRUPTOR_WAIT_STRATEGY);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // Resolve sort keys (defaults to record key if not specified)
    String sortKeys = AppendWriteFunctions.resolveSortKeys(config);
    if (StringUtils.isNullOrEmpty(sortKeys)) {
      throw new IllegalArgumentException("Sort keys can't be null or empty for append write with disruptor sort. "
          + "Either set write.buffer.sort.keys or ensure record key field is configured.");
    }

    List<String> sortKeyList = Arrays.stream(sortKeys.split(","))
        .map(String::trim)
        .collect(Collectors.toList());

    // Create Flink-native sort components
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortKeyList.toArray(new String[0]));
    SortCodeGenerator codeGenerator = sortOperatorGen.createSortCodeGenerator();
    this.keyComputer = codeGenerator.generateNormalizedKeyComputer("SortComputer");
    this.recordComparator = codeGenerator.generateRecordComparator("SortComparator");
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);

    initDisruptorBuffer();

    log.info("{} initialized with disruptor buffer, sort keys: {}, ring size: {}",
        getClass().getSimpleName(), sortKeys, ringBufferSize);
  }

  private void initDisruptorBuffer() throws Exception {
    if (sortBuffer == null) {
      this.sortBuffer = BufferUtils.createBuffer(rowType,
          memorySegmentPool,
          keyComputer.newInstance(Thread.currentThread().getContextClassLoader()),
          recordComparator.newInstance(Thread.currentThread().getContextClassLoader()));
    }

    this.sortingConsumer = new SortingConsumer();

    this.disruptorQueue = new DisruptorMessageQueue<>(
        ringBufferSize,
        Function.identity(),
        waitStrategy,
        1,  // single producer (Flink operator thread)
        null
    );
    disruptorQueue.setHandlers(sortingConsumer);
    disruptorQueue.start();
  }

  @Override
  public void processElement(T value, Context ctx, Collector<RowData> out) throws Exception {
    try {
      disruptorQueue.insertRecord((RowData) value);
    } catch (Exception e) {
      disruptorQueue.markAsFailed(e);
      throw new HoodieException("Failed to insert record into disruptor queue", e);
    }
  }

  @Override
  public void snapshotState() {
    try {
      flushDisruptor();
      reinitDisruptorAfterCheckpoint();
    } catch (Exception e) {
      throw new HoodieException("Fail to flush data during snapshot state.", e);
    }
    super.snapshotState();
  }

  @Override
  public void endInput() {
    try {
      flushDisruptor();
    } catch (Exception e) {
      throw new HoodieException("Fail to flush data during endInput.", e);
    }
    super.endInput();
  }

  private void flushDisruptor() {
    disruptorQueue.close();
    // Check if any errors occurred during event processing
    Throwable error = disruptorQueue.getThrowable();
    if (error != null) {
      throw new HoodieException("Error processing records in disruptor buffer", error);
    }
    sortingConsumer.finish();
  }

  private void reinitDisruptorAfterCheckpoint() throws Exception {
    // sortBuffer is reused - it's already reset after flush via sortAndSend()
    // disruptorQueue cannot be reused once closed, so we create a new one
    initDisruptorBuffer();
  }

  private void sortAndSend(BinaryInMemorySortBuffer buffer) throws IOException {
    if (buffer.isEmpty()) {
      return;
    }
    if (this.writerHelper == null) {
      initWriterHelper();
    }
    new QuickSort().sort(buffer);
    Iterator<BinaryRowData> iterator = new MutableIteratorWrapperIterator<>(
        buffer.getIterator(),
        () -> new BinaryRowData(rowType.getFieldCount()));
    while (iterator.hasNext()) {
      writerHelper.write(iterator.next());
    }
    buffer.reset();
  }

  @Override
  public void close() throws Exception {
    try {
      if (disruptorQueue != null) {
        disruptorQueue.close();
      }
    } finally {
      super.close();
    }
  }

  /**
   * Consumer for disruptor buffer that uses Flink's native sorting.
   * Accumulates records into a sort buffer and flushes when full.
   */
  private class SortingConsumer implements HoodieConsumer<RowData, Void> {

    @Override
    public void consume(RowData record) throws Exception {
      boolean success = sortBuffer.write(record);
      if (!success) {
        sortAndSend(sortBuffer);
        success = sortBuffer.write(record);
        if (!success) {
          throw new HoodieException("Sort buffer is too small to hold a single record.");
        }
      }

      if (sortBuffer.size() >= writeBufferSize) {
        sortAndSend(sortBuffer);
      }
    }

    @Override
    public Void finish() {
      try {
        sortAndSend(sortBuffer);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to flush sort buffer on finish.", e);
      }
      return null;
    }
  }
}
