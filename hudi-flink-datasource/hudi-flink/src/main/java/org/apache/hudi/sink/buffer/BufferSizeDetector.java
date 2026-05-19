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

package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.util.ObjectSizeCalculator;

import lombok.Getter;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.util.Random;

/**
 * A size detector to calculate the total size of the written records in a buffer.
 */
public class BufferSizeDetector {
  private final Random random = new Random(47);
  private static final int DENOMINATOR = 100;

  private final double batchSizeBytes;

  @Getter
  public long lastRecordSize = -1L;
  public long totalSize = 0L;

  public BufferSizeDetector(double batchSizeMb) {
    this.batchSizeBytes = batchSizeMb * 1024 * 1024;
  }

  public boolean detect(Object record) {
    if (record instanceof BinaryRowData) {
      lastRecordSize = ((BinaryRowData) record).getSizeInBytes();
    } else if (lastRecordSize == -1 || sampling()) {
      lastRecordSize = ObjectSizeCalculator.getObjectSize(record);
    }
    totalSize += lastRecordSize;
    return totalSize > this.batchSizeBytes;
  }

  public boolean isFull() {
    return totalSize > batchSizeBytes;
  }

  public boolean sampling() {
    // 0.01 sampling percentage
    return random.nextInt(DENOMINATOR) == 1;
  }

  public void reset() {
    this.lastRecordSize = -1L;
    this.totalSize = 0L;
  }
}
