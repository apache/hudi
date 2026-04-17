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

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    emitter = new HoodieRecordEmitter<>(true);
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
    HoodieRecordEmitter<Integer> intEmitter = new HoodieRecordEmitter<>(false);
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

  // -------------------------------------------------------------------------
  // Watermark tests
  // -------------------------------------------------------------------------

  @Test
  public void testWatermarkEmittedForLastRecordInSplit() throws Exception {
    // "19700101000000000" parses to epoch 0 ms in the local timezone of the JVM;
    // we only verify that emitWatermark was invoked exactly once.
    HoodieRecordWithPosition<String> lastRecord = new HoodieRecordWithPosition<>("last", 0, 1L);
    lastRecord.setLastInSplit(true);

    emitter.emitRecord(lastRecord, mockOutput, mockSplit);

    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    // The watermark must be non-null and captured exactly once.
    assertFalse(watermarkCaptor.getAllValues().isEmpty());
  }

  @Test
  public void testNoWatermarkEmittedForNonLastRecord() throws Exception {
    HoodieRecordWithPosition<String> record = new HoodieRecordWithPosition<>("mid", 0, 0L);
    // lastInSplit defaults to false

    emitter.emitRecord(record, mockOutput, mockSplit);

    verify(mockOutput, never()).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testWatermarkEmittedOnlyForLastOfMultipleRecords() throws Exception {
    HoodieRecordWithPosition<String> r1 = new HoodieRecordWithPosition<>("r1", 0, 0L);
    HoodieRecordWithPosition<String> r2 = new HoodieRecordWithPosition<>("r2", 0, 1L);
    HoodieRecordWithPosition<String> r3 = new HoodieRecordWithPosition<>("r3", 0, 2L);
    r3.setLastInSplit(true);

    emitter.emitRecord(r1, mockOutput, mockSplit);
    emitter.emitRecord(r2, mockOutput, mockSplit);
    emitter.emitRecord(r3, mockOutput, mockSplit);

    // Three records collected, but watermark emitted only once (for r3).
    verify(mockOutput, times(3)).collect(org.mockito.ArgumentMatchers.anyString());
    verify(mockOutput, times(1)).emitWatermark(org.mockito.ArgumentMatchers.any(Watermark.class));
  }

  // -------------------------------------------------------------------------
  // Additional coverage tests
  // -------------------------------------------------------------------------

  @Test
  public void testWatermarkNotEmittedWhenFlagDisabled() throws Exception {
    HoodieRecordEmitter<String> noWatermarkEmitter = new HoodieRecordEmitter<>(false);
    HoodieRecordWithPosition<String> lastRecord = new HoodieRecordWithPosition<>("last", 0, 5L);
    lastRecord.setLastInSplit(true);

    noWatermarkEmitter.emitRecord(lastRecord, mockOutput, mockSplit);

    verify(mockOutput, times(1)).collect("last");
    verify(mockOutput, never()).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testWatermarkNotEmittedForUnparsableLatestCommit() throws Exception {
    HoodieSourceSplit badCommitSplit = new HoodieSourceSplit(
        2, "some-base", Option.of(Collections.emptyList()),
        "/table", "/partition", "read_optimized",
        "not-a-valid-instant", "file-2", Option.empty());

    HoodieRecordWithPosition<String> lastRecord = new HoodieRecordWithPosition<>("last", 0, 0L);
    lastRecord.setLastInSplit(true);

    // Must not throw; ParseException is swallowed internally.
    emitter.emitRecord(lastRecord, mockOutput, badCommitSplit);

    verify(mockOutput, times(1)).collect("last");
    verify(mockOutput, never()).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testWatermarkTimestampMatchesLatestCommit() throws Exception {
    long expectedWatermarkMs =
        HoodieInstantTimeGenerator.parseDateFromInstantTime("19700101000000000").getTime();

    HoodieRecordWithPosition<String> lastRecord = new HoodieRecordWithPosition<>("last", 0, 0L);
    lastRecord.setLastInSplit(true);

    emitter.emitRecord(lastRecord, mockOutput, mockSplit);

    ArgumentCaptor<Watermark> captor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(captor.capture());
    assertEquals(expectedWatermarkMs, captor.getValue().getTimestamp());
  }

  @Test
  public void testHoodieRecordWithPositionNoArgConstructorAndSet() throws Exception {
    HoodieRecordWithPosition<String> record = new HoodieRecordWithPosition<>();
    record.set("set-record", 3, 42L);

    emitter.emitRecord(record, mockOutput, mockSplit);

    verify(mockOutput, times(1)).collect("set-record");
    assertEquals(3, mockSplit.getFileOffset());
    assertEquals(42L, mockSplit.getConsumed());
  }

  @Test
  public void testEmitLastRecordBothCollectsAndEmitsWatermark() throws Exception {
    HoodieRecordWithPosition<String> lastRecord =
        new HoodieRecordWithPosition<>("only-record", 0, 1L);
    lastRecord.setLastInSplit(true);

    emitter.emitRecord(lastRecord, mockOutput, mockSplit);

    verify(mockOutput, times(1)).collect("only-record");
    verify(mockOutput, times(1)).emitWatermark(org.mockito.ArgumentMatchers.any(Watermark.class));
    assertEquals(0, mockSplit.getFileOffset());
    assertEquals(1L, mockSplit.getConsumed());
  }

  @Test
  public void testWatermarkEmittedIndependentlyPerSplit() throws Exception {
    HoodieSourceSplit split1 = createTestSplit();
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        2, "base2", Option.of(Collections.emptyList()),
        "/table2", "/partition2", "read_optimized",
        "20210101000000000", "file-2", Option.empty());

    HoodieRecordWithPosition<String> lastOfSplit1 = new HoodieRecordWithPosition<>("s1-last", 0, 1L);
    lastOfSplit1.setLastInSplit(true);
    HoodieRecordWithPosition<String> lastOfSplit2 = new HoodieRecordWithPosition<>("s2-last", 0, 2L);
    lastOfSplit2.setLastInSplit(true);

    emitter.emitRecord(lastOfSplit1, mockOutput, split1);
    emitter.emitRecord(lastOfSplit2, mockOutput, split2);

    verify(mockOutput, times(2)).collect(org.mockito.ArgumentMatchers.anyString());
    verify(mockOutput, times(2)).emitWatermark(org.mockito.ArgumentMatchers.any(Watermark.class));
  }

  @Test
  public void testRecordMethodIncreasesOffsetAndClearsLastInSplit() throws Exception {
    HoodieRecordWithPosition<String> record = new HoodieRecordWithPosition<>("init", 0, 5L);
    record.setLastInSplit(true);

    // record() increments recordOffset by 1 and resets lastInSplit to false.
    record.record("next");

    emitter.emitRecord(record, mockOutput, mockSplit);

    verify(mockOutput, times(1)).collect("next");
    assertEquals(0, mockSplit.getFileOffset());
    assertEquals(6L, mockSplit.getConsumed());
    verify(mockOutput, never()).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testEmitterIsSerializable() throws Exception {
    byte[] bytes;
    try (java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
         java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
      oos.writeObject(emitter);
      bytes = bos.toByteArray();
    }

    HoodieRecordEmitter<?> deserialized;
    try (java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bytes);
         java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bis)) {
      deserialized = (HoodieRecordEmitter<?>) ois.readObject();
    }

    assertNotNull(deserialized);
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
        "19700101000000000",
        "file-1",
            Option.empty()
    );
  }
}
