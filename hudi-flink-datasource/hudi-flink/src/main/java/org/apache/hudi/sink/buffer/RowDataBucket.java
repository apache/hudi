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

import org.apache.hudi.table.action.commit.BucketInfo;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * RowData buffer for a specific data bucket, and the buffer is based on {@code BinaryInMemorySortBuffer}
 * from Flink, which is backed by a managed {@code MemorySegmentPool} to minimize GC costs.
 */
public class RowDataBucket {
  private final BinaryInMemorySortBuffer dataBuffer;
  private final BucketInfo bucketInfo;
  private final BufferSizeDetector detector;
  private final String bucketId;

  public RowDataBucket(
      String bucketId,
      BinaryInMemorySortBuffer dataBuffer,
      BucketInfo bucketInfo,
      Double batchSize) {
    this.bucketId = bucketId;
    this.dataBuffer = dataBuffer;
    this.bucketInfo = bucketInfo;
    this.detector = new BufferSizeDetector(batchSize);
  }

  public MutableObjectIterator<BinaryRowData> getDataIterator() {
    return dataBuffer.getIterator();
  }

  public String getBucketId() {
    return bucketId;
  }

  public boolean writeRow(RowData rowData) throws IOException {
    boolean success = dataBuffer.write(rowData);
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
    return dataBuffer.isEmpty();
  }

  public boolean isFull() {
    return detector.isFull();
  }

  public long getLastRecordSize() {
    return detector.getLastRecordSize();
  }

  public void dispose() {
    dataBuffer.dispose();
    detector.reset();
  }
}
