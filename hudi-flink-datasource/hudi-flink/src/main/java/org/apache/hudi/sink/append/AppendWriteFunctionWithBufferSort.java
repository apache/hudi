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

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.append.sort.RowDataComparator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.utils.BufferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
  private final long writebufferSize;
  private final RowDataComparator rowDataComparator;
  private transient BinaryInMemorySortBuffer buffer;
  private transient MemorySegmentPool memorySegmentPool;

  public AppendWriteFunctionWithBufferSort(Configuration config, RowType rowType) {
    super(config, rowType);
    this.writebufferSize = config.get(FlinkOptions.WRITE_BUFFER_SIZE);
    String sortKeys = config.get(FlinkOptions.WRITE_BUFFER_SORT_KEYS);
    if (sortKeys == null) {
      throw new IllegalArgumentException("Sort keys can't be null for append write with buffer sort.");
    }

    this.rowDataComparator = new RowDataComparator(rowType, sortKeys);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
    this.buffer = BufferUtils.createBuffer(rowType, memorySegmentPool, rowDataComparator);
  }

  @Override
  public void processElement(T value, Context ctx, Collector<Object> out) throws Exception {
    RowData data = (RowData) value;
    buffer.write(data);
    if (buffer.size() >= writebufferSize) {
      sortAndSend();
    }
  }

  @Override
  public void snapshotState() {
    try {
      sortAndSend();
    } catch (IOException e) {
      LOG.error("Fail to sort and flush data in buffer during snapshot state.");
      throw new FlinkRuntimeException(e);
    }
    super.snapshotState();
  }

  private void sortAndSend() throws IOException {
    if (this.writerHelper == null) {
      initWriterHelper();
    }

    MutableObjectIterator<BinaryRowData> iterator =  buffer.getIterator();
    BinaryRowData rowData = new BinaryRowData(rowType.getFieldCount());
    while (rowData != null) {
      rowData = iterator.next(rowData);
      if (rowData != null) {
        writerHelper.write(rowData);
      }
    }
    buffer.reset();
  }
}
