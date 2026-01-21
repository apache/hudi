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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.MutableIteratorWrapperIterator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Flink stream writing function that handles writing index records to the metadata table.
 *
 * <p>This function is responsible for:
 * <ul>
 *   <li>Buffering incoming index rows in memory using a binary buffer</li>
 *   <li>Converting index rows to {@link HoodieRecord} instances</li>
 *   <li>Writing index records to metadata partitions via the write client</li>
 *   <li>Coordinating checkpoint barriers and flush operations</li>
 * </ul>
 *
 * <p>The function operates as part of the Flink sink topology for Hudi tables, specifically
 * handling the index writing phase. It buffers index rows in an in-memory binary buffer and
 * flushes them during snapshot state (checkpoint) or when the memory pool is exhausted. Upon
 * the checkpoint and `endInput` invoking (indicated by {@code lastBatch=true}), it closes
 * the metadata writer for the current instant.
 */
@Slf4j
public class IndexWriteFunction extends AbstractStreamWriteFunction<RowData> {

  private static final long serialVersionUID = 1L;

  private transient MemorySegmentPool memorySegmentPool;

  private transient BinaryInMemorySortBuffer indexDataBuffer;

  /**
   * Hoodie Flink table.
   */
  private transient HoodieFlinkTable<?> flinkTable;

  public IndexWriteFunction(Configuration conf) {
    super(conf);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.flinkTable = this.writeClient.getHoodieTable();
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config, config.get(FlinkOptions.INDEX_RLI_WRITE_BUFFER_SIZE));
    this.indexDataBuffer = BufferUtils.createBuffer(IndexRowUtils.INDEX_ROW_TYPE, memorySegmentPool);
  }

  @Override
  protected void sendWriteMetadataEvent(WriteMetadataEvent metadataEvent) {
    this.correspondent.sendWriteMetadataEvent(metadataEvent);
  }

  @Override
  protected boolean isMetadataTable() {
    return true;
  }

  @Override
  public void processElement(RowData indexRow, Context ctx, Collector<RowData> out) throws Exception {
    boolean success = bufferIndexRow(indexRow);
    if (!success) {
      // flushes the buffer if the memory pool is full
      flushBuffer(false, false);
      // try to write index row again
      bufferIndexRow(indexRow);
    }
  }

  /**
   * Put the index row into the binary buffer.
   *
   * @param indexRow the incoming index row data
   * @return true if the index row is put into the buffer successfully, false otherwise.
   */
  private boolean bufferIndexRow(RowData indexRow) throws IOException {
    try {
      return indexDataBuffer.write(indexRow);
    } catch (MemoryPagesExhaustedException e) {
      log.info("There is no enough free pages in memory pool to create buffer, need flushing first.", e);
      return false;
    }
  }

  @Override
  public void snapshotState() {
    flushBuffer(true, false);
  }

  private void flushBuffer(boolean lastBatch, boolean endInput) {
    // in case of there is no index record during the checkpoint interval, request instant from coordinator.
    this.currentInstant = instantToWrite(true);
    Pair<List<HoodieRecord>, Set<String>> indexRecordsAndPartitions = prepareIndexRecordsAndPartitions(this.indexDataBuffer);
    HoodieData<HoodieRecord> indexRecords = HoodieListData.eager(indexRecordsAndPartitions.getLeft());
    List<WriteStatus> writeStatus = this.writeClient.streamWriteToMetadataPartitions(
        flinkTable, indexRecords, indexRecordsAndPartitions.getRight(), this.currentInstant).collectAsList();

    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .checkpointId(this.checkpointId)
        .instantTime(this.currentInstant)
        .writeStatus(writeStatus)
        .lastBatch(lastBatch)
        .endInput(endInput)
        .metadataTable(true)
        .build();

    this.correspondent.sendWriteMetadataEvent(event);
    this.writeStatuses.addAll(writeStatus);

    // reset buffer for reusing
    this.indexDataBuffer.reset();
    // close the metadata writer if the current instant writing is finished.
    if (lastBatch) {
      try {
        this.writeClient.postStreamWriteToMetadataTable(this.currentInstant);
      } catch (Exception e) {
        throw new HoodieException("Failed to close the metadata writer for instant: " + this.currentInstant);
      }
    }
  }

  private Pair<List<HoodieRecord>, Set<String>> prepareIndexRecordsAndPartitions(BinaryInMemorySortBuffer indexDataBuffer) {
    BinaryRowData reusedRow = new BinaryRowData(IndexRowUtils.INDEX_ROW_TYPE.getFieldCount());
    Iterator<BinaryRowData> rowItr = new MutableIteratorWrapperIterator<>(indexDataBuffer.getIterator(), () -> reusedRow);
    List<HoodieRecord> indexRecords = new ArrayList<>(indexDataBuffer.size());
    Set<String> dataPartitions = new HashSet<>();
    HoodieWriteConfig writeConfig = writeClient.getConfig();
    while (rowItr.hasNext()) {
      RowData indexRow = rowItr.next();
      indexRecords.add(IndexRowUtils.convertToHoodieRecord(this.currentInstant, indexRow, writeConfig));
      dataPartitions.add(IndexRowUtils.getPartition(indexRow));

    }
    return Pair.of(indexRecords, dataPartitions);
  }

  @Override
  public void endInput() {
    // flush index data buffer
    flushBuffer(true, true);
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    // no-op
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public List<HoodieRecord> getDataBuffer() {
    return prepareIndexRecordsAndPartitions(this.indexDataBuffer).getLeft();
  }

  @Override
  public void close() throws Exception {
    this.indexDataBuffer.dispose();
    this.memorySegmentPool.freePages();
    super.close();
  }
}
