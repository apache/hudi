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

import org.apache.flink.api.common.eventtime.Watermark;
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
    mockSplit = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");
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
  public void testWatermarkEmissionOnFirstSplitTransition() throws Exception {
    // Create first split with watermark from basePath timestamp 20250129120000001
    HoodieSourceSplit split1 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");

    // Create second split with higher timestamp
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129130000002.parquet",
        "20250129130000002",
        "file-2");

    // Emit record from first split
    emitter.emitRecord(new HoodieRecordWithPosition<>("record1", 0, 1L), mockOutput, split1);

    // No watermark emitted yet (first split, no previous watermark to emit)
    verify(mockOutput, times(0)).emitWatermark(org.mockito.ArgumentMatchers.any());

    // Emit record from second split - should emit watermark from split1
    emitter.emitRecord(new HoodieRecordWithPosition<>("record2", 0, 1L), mockOutput, split2);

    // Verify watermark from split1 was emitted
    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    assertEquals(20250129120000001L, watermarkCaptor.getValue().getTimestamp());
  }

  @Test
  public void testWatermarkEmissionWithMultipleSplits() throws Exception {
    HoodieSourceSplit split1 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129130000002.parquet",
        "20250129130000002",
        "file-2");
    HoodieSourceSplit split3 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129140000003.parquet",
        "20250129140000003",
        "file-3");

    // Process splits sequentially
    emitter.emitRecord(new HoodieRecordWithPosition<>("r1", 0, 1L), mockOutput, split1);
    emitter.emitRecord(new HoodieRecordWithPosition<>("r2", 0, 1L), mockOutput, split2);
    emitter.emitRecord(new HoodieRecordWithPosition<>("r3", 0, 1L), mockOutput, split3);

    // Verify watermarks emitted for split1 and split2
    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(2)).emitWatermark(watermarkCaptor.capture());

    List<Watermark> watermarks = watermarkCaptor.getAllValues();
    assertEquals(20250129120000001L, watermarks.get(0).getTimestamp());
    assertEquals(20250129130000002L, watermarks.get(1).getTimestamp());
  }

  @Test
  public void testWatermarkExtractionFromBasePath() throws Exception {
    HoodieSourceSplit split = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129150000005.parquet",
        "20250129150000005",
        "file-5");

    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split);

    // No watermark emitted on first split
    verify(mockOutput, times(0)).emitWatermark(org.mockito.ArgumentMatchers.any());

    // Process a new split to trigger watermark emission from previous split
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129160000006.parquet",
        "20250129160000006",
        "file-6");
    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split2);

    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    assertEquals(20250129150000005L, watermarkCaptor.getValue().getTimestamp());
  }

  @Test
  public void testWatermarkExtractionFromLogPaths() throws Exception {
    List<String> logPaths = new ArrayList<>();
    logPaths.add("40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.log");
    logPaths.add("40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129130000002.log");
    logPaths.add("40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129140000003.log");

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        null,  // No base path
        Option.of(logPaths),
        "/test/table/path",
        "/test/partition",
        "merge_on_read",
        "20250129140000003",
        "file-1"
    );

    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split);

    // Emit another split to trigger watermark from previous
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129150000004.parquet",
        "20250129150000004",
        "file-2");
    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split2);

    // Verify watermark is max of log paths (20250129140000003)
    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    assertEquals(20250129140000003L, watermarkCaptor.getValue().getTimestamp());
  }

  @Test
  public void testWatermarkExtractionFromBothBaseAndLogPaths() throws Exception {
    List<String> logPaths = new ArrayList<>();
    logPaths.add("40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129130000002.log");
    logPaths.add("40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129160000006.log");  // Higher than base

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        Option.of(logPaths),
        "/test/table/path",
        "/test/partition",
        "merge_on_read",
        "20250129160000006",
        "file-1"
    );

    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split);

    // Emit another split to trigger watermark
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129170000007.parquet",
        "20250129170000007",
        "file-2");
    emitter.emitRecord(new HoodieRecordWithPosition<>("record", 0, 1L), mockOutput, split2);

    // Verify watermark is max of all paths (20250129160000006 from log)
    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    assertEquals(20250129160000006L, watermarkCaptor.getValue().getTimestamp());
  }

  @Test
  public void testNoWatermarkEmittedOnSameSplit() throws Exception {
    HoodieSourceSplit split = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");

    // Emit multiple records from same split
    emitter.emitRecord(new HoodieRecordWithPosition<>("record1", 0, 1L), mockOutput, split);
    emitter.emitRecord(new HoodieRecordWithPosition<>("record2", 0, 2L), mockOutput, split);
    emitter.emitRecord(new HoodieRecordWithPosition<>("record3", 0, 3L), mockOutput, split);

    // No watermarks should be emitted for same split
    verify(mockOutput, times(0)).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testNoWatermarkRegressionWarning() throws Exception {
    // This test verifies the warning case when watermark decreases
    // Start with high watermark
    HoodieSourceSplit split1 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129140000003.parquet",
        "20250129140000003",
        "file-1");

    // Then move to lower watermark
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-2");

    emitter.emitRecord(new HoodieRecordWithPosition<>("record1", 0, 1L), mockOutput, split1);
    emitter.emitRecord(new HoodieRecordWithPosition<>("record2", 0, 1L), mockOutput, split2);

    // No watermark should be emitted when it would go backwards
    verify(mockOutput, times(0)).emitWatermark(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testEmitMultipleRecordsAcrossSplits() throws Exception {
    HoodieSourceSplit split1 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");
    HoodieSourceSplit split2 = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129130000002.parquet",
        "20250129130000002",
        "file-2");

    // Emit multiple records from split1
    emitter.emitRecord(new HoodieRecordWithPosition<>("r1", 0, 1L), mockOutput, split1);
    emitter.emitRecord(new HoodieRecordWithPosition<>("r2", 0, 2L), mockOutput, split1);
    emitter.emitRecord(new HoodieRecordWithPosition<>("r3", 0, 3L), mockOutput, split1);

    // Emit multiple records from split2
    emitter.emitRecord(new HoodieRecordWithPosition<>("r4", 0, 1L), mockOutput, split2);
    emitter.emitRecord(new HoodieRecordWithPosition<>("r5", 0, 2L), mockOutput, split2);

    // Verify all records collected
    verify(mockOutput, times(5)).collect(org.mockito.ArgumentMatchers.anyString());

    // Verify watermark emitted once when transitioning from split1 to split2
    ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);
    verify(mockOutput, times(1)).emitWatermark(watermarkCaptor.capture());
    assertEquals(20250129120000001L, watermarkCaptor.getValue().getTimestamp());
  }

  @Test
  public void testSplitPositionUpdatesCorrectly() throws Exception {
    HoodieSourceSplit split = createTestSplitWithParams(
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20250129120000001.parquet",
        "20250129120000001",
        "file-1");

    emitter.emitRecord(new HoodieRecordWithPosition<>("r1", 0, 10L), mockOutput, split);
    assertEquals(0, split.getFileOffset());
    assertEquals(10L, split.getConsumed());

    emitter.emitRecord(new HoodieRecordWithPosition<>("r2", 1, 20L), mockOutput, split);
    assertEquals(1, split.getFileOffset());
    assertEquals(20L, split.getConsumed());

    emitter.emitRecord(new HoodieRecordWithPosition<>("r3", 2, 30L), mockOutput, split);
    assertEquals(2, split.getFileOffset());
    assertEquals(30L, split.getConsumed());
  }

  /**
   * Helper method to create a test HoodieSourceSplit with specific parameters.
   */
  private HoodieSourceSplit createTestSplitWithParams(String baseFilePath, String latestCommit, String fileId) {
    return new HoodieSourceSplit(
        1,
        baseFilePath,
        Option.of(Collections.emptyList()),
        "/test/table/path",
        "/test/partition",
        "read_optimized",
        latestCommit,
        fileId
    );
  }
}
