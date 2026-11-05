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

import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBufferAccountingUtilities {

  @Test
  void testTotalSizeTracerTracksCountdownAndReset() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 103D);
    conf.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, 1);
    TotalSizeTracer tracer = new TotalSizeTracer(conf);
    long oneMb = 1024L * 1024L;

    assertEquals(2 * oneMb, tracer.maxBufferSize);
    assertFalse(tracer.trace(oneMb));
    assertFalse(tracer.trace(oneMb));
    assertTrue(tracer.trace(1));
    tracer.countDown(oneMb + 1);
    assertEquals(oneMb, tracer.bufferSize);
    tracer.reset();
    assertEquals(0, tracer.bufferSize);
  }

  @Test
  void testTotalSizeTracerRejectsInsufficientTaskMemory() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 101D);
    conf.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, 1);

    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> new TotalSizeTracer(conf));
    assertTrue(exception.getMessage().contains(FlinkOptions.WRITE_TASK_MAX_SIZE.key()));
  }

  @Test
  void testBufferSizeDetectorUsesBinaryRowSizeAndResets() {
    BufferSizeDetector detector = new BufferSizeDetector(0.00001);
    BinaryRowData smallRow = new BinaryRowData(0);
    smallRow.pointTo(MemorySegmentFactory.wrap(new byte[8]), 0, 8);

    assertFalse(detector.detect(smallRow));
    assertEquals(8, detector.getLastRecordSize());
    assertTrue(detector.detect(smallRow));
    assertTrue(detector.isFull());

    detector.reset();
    assertEquals(-1, detector.getLastRecordSize());
    assertEquals(0, detector.totalSize);
    assertFalse(detector.isFull());
  }

  @Test
  void testBufferSizeDetectorReusesLastSampleForRegularObjects() {
    BufferSizeDetector detector = new BufferSizeDetector(10);
    assertFalse(detector.detect("first value"));
    long sampledSize = detector.getLastRecordSize();

    assertTrue(sampledSize > 0);
    assertFalse(detector.detect("second value"));
    assertEquals(sampledSize * 2, detector.totalSize);

    // The seeded random makes sampling deterministic and covers both outcomes.
    boolean sampled = false;
    boolean skipped = false;
    for (int i = 0; i < 500 && !(sampled && skipped); i++) {
      if (detector.sampling()) {
        sampled = true;
      } else {
        skipped = true;
      }
    }
    assertTrue(sampled);
    assertTrue(skipped);
  }
}
