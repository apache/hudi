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

package org.apache.hudi.source.reader;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test cases for {@link HoodieRecordEmitter}.
 */
public class TestHoodieRecordEmitter {

  private HoodieRecordEmitter<String> emitter;
  private SourceOutput<String> mockOutput;
  private HoodieSourceSplit mockSplit;

  @BeforeEach
  public void setUp() {
    emitter = new HoodieRecordEmitter<>();
    mockOutput = mock(SourceOutput.class);
    mockSplit = createTestSplit();
  }

  @Test
  public void testEmitRecordCollectsRecord() throws Exception {
    String testRecord = "test-record";
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>(testRecord, 0, 0L);

    emitter.emitRecord(recordWithPosition, mockOutput, mockSplit);

    // Verify the record was collected
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockOutput, times(1)).collect(captor.capture());
    assertEquals(testRecord, captor.getValue());
  }

  @Test
  public void testEmitRecordUpdatesSplitPosition() throws Exception {
    int fileOffset = 5;
    long recordOffset = 100L;
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("record", fileOffset, recordOffset);

    emitter.emitRecord(recordWithPosition, mockOutput, mockSplit);

    // Verify split position was updated
    assertEquals(fileOffset, mockSplit.getFileOffset());
    assertEquals(recordOffset, mockSplit.getConsumed());
  }

  @Test
  public void testEmitMultipleRecords() throws Exception {
    List<HoodieRecordWithPosition<String>> records = new ArrayList<>();
    records.add(new HoodieRecordWithPosition<>("record1", 0, 1L));
    records.add(new HoodieRecordWithPosition<>("record2", 0, 2L));
    records.add(new HoodieRecordWithPosition<>("record3", 0, 3L));

    for (HoodieRecordWithPosition<String> record : records) {
      emitter.emitRecord(record, mockOutput, mockSplit);
    }

    // Verify all records were collected
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockOutput, times(3)).collect(captor.capture());

    List<String> collectedRecords = captor.getAllValues();
    assertEquals(3, collectedRecords.size());
    assertEquals("record1", collectedRecords.get(0));
    assertEquals("record2", collectedRecords.get(1));
    assertEquals("record3", collectedRecords.get(2));

    // Verify final split position
    assertEquals(0, mockSplit.getFileOffset());
    assertEquals(3L, mockSplit.getConsumed());
  }

  @Test
  public void testEmitRecordWithDifferentFileOffsets() throws Exception {
    HoodieRecordWithPosition<String> record1 =
        new HoodieRecordWithPosition<>("record1", 0, 10L);
    HoodieRecordWithPosition<String> record2 =
        new HoodieRecordWithPosition<>("record2", 1, 20L);
    HoodieRecordWithPosition<String> record3 =
        new HoodieRecordWithPosition<>("record3", 2, 30L);

    emitter.emitRecord(record1, mockOutput, mockSplit);
    assertEquals(0, mockSplit.getFileOffset());
    assertEquals(10L, mockSplit.getConsumed());

    emitter.emitRecord(record2, mockOutput, mockSplit);
    assertEquals(1, mockSplit.getFileOffset());
    assertEquals(20L, mockSplit.getConsumed());

    emitter.emitRecord(record3, mockOutput, mockSplit);
    assertEquals(2, mockSplit.getFileOffset());
    assertEquals(30L, mockSplit.getConsumed());
  }

  @Test
  public void testEmitRecordWithNullRecord() throws Exception {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>(null, 0, 0L);

    emitter.emitRecord(recordWithPosition, mockOutput, mockSplit);

    // Verify null record was collected
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockOutput, times(1)).collect(captor.capture());
    assertEquals(null, captor.getValue());
  }

  @Test
  public void testEmitRecordWithDifferentRecordTypes() throws Exception {
    HoodieRecordEmitter<Integer> intEmitter = new HoodieRecordEmitter<>();
    SourceOutput<Integer> intOutput = mock(SourceOutput.class);

    HoodieRecordWithPosition<Integer> intRecord =
        new HoodieRecordWithPosition<>(42, 0, 0L);

    intEmitter.emitRecord(intRecord, intOutput, mockSplit);

    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
    verify(intOutput, times(1)).collect(captor.capture());
    assertEquals(42, captor.getValue());
  }

  @Test
  public void testEmitRecordPositionIncrementsCorrectly() throws Exception {
    // Simulate reading records sequentially with increasing offsets
    for (long i = 1; i <= 10; i++) {
      HoodieRecordWithPosition<String> record =
          new HoodieRecordWithPosition<>("record" + i, 0, i);
      emitter.emitRecord(record, mockOutput, mockSplit);

      assertEquals(0, mockSplit.getFileOffset());
      assertEquals(i, mockSplit.getConsumed());
    }

    verify(mockOutput, times(10)).collect(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  public void testEmitRecordAcrossMultipleFiles() throws Exception {
    // File 0, records 0-2
    emitter.emitRecord(new HoodieRecordWithPosition<>("f0r0", 0, 0L), mockOutput, mockSplit);
    emitter.emitRecord(new HoodieRecordWithPosition<>("f0r1", 0, 1L), mockOutput, mockSplit);
    emitter.emitRecord(new HoodieRecordWithPosition<>("f0r2", 0, 2L), mockOutput, mockSplit);

    assertEquals(0, mockSplit.getFileOffset());
    assertEquals(2L, mockSplit.getConsumed());

    // File 1, records 0-1
    emitter.emitRecord(new HoodieRecordWithPosition<>("f1r0", 1, 0L), mockOutput, mockSplit);
    emitter.emitRecord(new HoodieRecordWithPosition<>("f1r1", 1, 1L), mockOutput, mockSplit);

    assertEquals(1, mockSplit.getFileOffset());
    assertEquals(1L, mockSplit.getConsumed());

    // File 2, record 0
    emitter.emitRecord(new HoodieRecordWithPosition<>("f2r0", 2, 0L), mockOutput, mockSplit);

    assertEquals(2, mockSplit.getFileOffset());
    assertEquals(0L, mockSplit.getConsumed());

    verify(mockOutput, times(6)).collect(org.mockito.ArgumentMatchers.anyString());
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit() {
    return new HoodieSourceSplit(
        1,
        "test-base-path",
        Option.of(Collections.emptyList()),
        "/test/table/path",
        "/test/partition",
        "read_optimized",
        "file-1"
    );
  }
}
