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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.util.MutableObjectIterator;
import java.io.IOException;

/**
 * Data bucket.
 */
public class DataBucket {
  private final BufferSizeDetector detector;
  private final String partitionPath;
  private final String bucketID;
  private final BinaryInMemorySortBuffer dataBuffer;
  private final String fileID;
  public DataBucket(Double batchSize, String partitionPath, String bucketID, BinaryInMemorySortBuffer dataBuffer, String fileID) {
    this.detector = new BufferSizeDetector(batchSize);
    this.partitionPath = partitionPath;
    this.bucketID = bucketID;
    this.dataBuffer = dataBuffer;
    this.fileID = fileID;
  }

  public boolean writeRow(RowData rowData) throws IOException {
    boolean success = dataBuffer.write(rowData);
    if (success) {
      detector.detect(rowData);
    }
    return success;
  }

  public long getBufferSize() {
    return detector.totalSize;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void reset() {
    this.detector.reset();
  }

  public MutableObjectIterator<BinaryRowData> getDataIterator() {
    return dataBuffer.getIterator();
  }

  public MutableObjectIterator<BinaryRowData> getSortedDataIterator() {
    BufferUtils.sort(dataBuffer);
    return dataBuffer.getIterator();
  }

  public String getBucketId() {
    return bucketID;
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

  public String getFileID() {
    return fileID;
  }
}