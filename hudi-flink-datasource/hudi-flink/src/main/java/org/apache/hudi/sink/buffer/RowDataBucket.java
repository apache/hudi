/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.action.commit.BucketInfo;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * RowData buffer for a specific data bucket, and the buffer is based on {@code BinaryInMemorySortBuffer}
 * from Flink, which is backed by a managed {@code MemorySegmentPool} to minimize GC influence.
 *
 * <p> A separate buffer is used to store the delete records, in case we need building record iterator and
 * delete record iterator separately, for example, they are needed for building data block and delete block
 * during writing MOR log files.
 */
public class RowDataBucket {
  private final BinaryInMemorySortBuffer dataBuffer;
  private final Option<BinaryInMemorySortBuffer> deleteDataBuffer;
  private final BucketInfo bucketInfo;
  private final BufferSizeDetector detector;
  private final String bucketId;

  public RowDataBucket(
      String bucketId,
      BinaryInMemorySortBuffer dataBuffer,
      BinaryInMemorySortBuffer deleteDataBuffer,
      BucketInfo bucketInfo,
      Double batchSize) {
    this.bucketId = bucketId;
    this.dataBuffer = dataBuffer;
    this.deleteDataBuffer = Option.ofNullable(deleteDataBuffer);
    this.bucketInfo = bucketInfo;
    this.detector = new BufferSizeDetector(batchSize);
  }

  public MutableObjectIterator<BinaryRowData> getDataIterator() {
    return dataBuffer.getIterator();
  }

  public MutableObjectIterator<BinaryRowData> getDeleteDataIterator() {
    return deleteDataBuffer.map(BinaryInMemorySortBuffer::getIterator).orElse(null);
  }

  public String getBucketId() {
    return bucketId;
  }

  public boolean writeRow(RowData rowData) throws IOException {
    boolean success;
    if (rowData.getRowKind() == RowKind.DELETE && deleteDataBuffer.isPresent()) {
      success = deleteDataBuffer.get().write(rowData);
    } else {
      success = dataBuffer.write(rowData);
    }
    if (success) {
      detector.detect(rowData);
    }
    return success;
  }

  public BucketInfo getBucketInfo() {
    return bucketInfo;
  }

  public long getBufferSize() {
    return detector.totalSize;
  }

  public boolean isEmpty() {
    return dataBuffer.isEmpty() && (!deleteDataBuffer.isPresent() || deleteDataBuffer.get().isEmpty());
  }

  public boolean isFull() {
    return detector.isFull();
  }

  public long getLastRecordSize() {
    return detector.getLastRecordSize();
  }

  public void dispose() {
    dataBuffer.dispose();
    deleteDataBuffer.ifPresent(BinaryInMemorySortBuffer::dispose);
    detector.reset();
  }
}
