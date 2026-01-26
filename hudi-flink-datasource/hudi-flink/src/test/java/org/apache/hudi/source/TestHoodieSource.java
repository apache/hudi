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

package org.apache.hudi.source;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSourceSplitStatus;
import org.apache.hudi.source.split.SerializableComparator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link HoodieSource}.
 */
public class TestHoodieSource {

  @TempDir
  File tempDir;

  private HoodieTableMetaClient mockMetaClient;
  private ScanContext scanContext;
  private SplitReaderFunction<RowData> mockReaderFunction;
  private SerializableComparator<HoodieSourceSplit> mockComparator;
  private HoodieRecordEmitter<RowData> mockRecordEmitter;

  @BeforeEach
  public void setUp() throws Exception {
    // Setup mock meta client
    mockMetaClient = mock(HoodieTableMetaClient.class);
    doReturn(new StoragePath(tempDir.getAbsolutePath())).when(mockMetaClient).getBasePath();
    doReturn(mock(HoodieActiveTimeline.class)).when(mockMetaClient).getActiveTimeline();

    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    doReturn("test_table").when(mockTableConfig).getTableName();
    doReturn(mockTableConfig).when(mockMetaClient).getTableConfig();


    // Setup mock reader function
    mockReaderFunction = mock(SplitReaderFunction.class);

    // Setup mock comparator
    mockComparator = mock(SerializableComparator.class);

    // Setup mock record emitter
    mockRecordEmitter = mock(HoodieRecordEmitter.class);

    // Setup scan context
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tempDir.getAbsolutePath());
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 60);

    scanContext = new ScanContext.Builder()
        .conf(conf)
        .path(new Path(tempDir.getAbsolutePath()))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .endInstant("20240201000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(false)
        .build();
  }

  @Test
  public void testConstructorWithValidArguments() {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    assertNotNull(source);
  }

  @Test
  public void testConstructorWithNullScanContext() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSource<>(
          null,
          mockReaderFunction,
          mockComparator,
          mockMetaClient,
          mockRecordEmitter
      );
    });
  }

  @Test
  public void testConstructorWithNullReaderFunction() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSource<>(
          scanContext,
          null,
          mockComparator,
          mockMetaClient,
          mockRecordEmitter
      );
    });
  }

  @Test
  public void testConstructorWithNullComparator() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSource<>(
          scanContext,
          mockReaderFunction,
          null,
          mockMetaClient,
          mockRecordEmitter
      );
    });
  }

  @Test
  public void testConstructorWithNullMetaClient() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSource<>(
          scanContext,
          mockReaderFunction,
          mockComparator,
          null,
          mockRecordEmitter
      );
    });
  }

  @Test
  public void testConstructorWithNullRecordEmitter() {
    assertThrows(IllegalArgumentException.class, () -> {
      new HoodieSource<>(
          scanContext,
          mockReaderFunction,
          mockComparator,
          mockMetaClient,
          null
      );
    });
  }

  @Test
  public void testGetBoundednessForBatchMode() {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    assertEquals(Boundedness.BOUNDED, source.getBoundedness());
  }

  @Test
  public void testGetBoundednessForStreamingMode() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tempDir.getAbsolutePath());

    ScanContext streamingScanContext = new ScanContext.Builder()
        .conf(conf)
        .path(new Path(tempDir.getAbsolutePath()))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .build();

    HoodieSource<RowData> source = new HoodieSource<>(
        streamingScanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
  }

  @Test
  public void testGetSplitSerializer() {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    SimpleVersionedSerializer<HoodieSourceSplit> serializer = source.getSplitSerializer();
    assertNotNull(serializer);
  }

  @Test
  public void testGetEnumeratorCheckpointSerializer() {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    SimpleVersionedSerializer<HoodieSplitEnumeratorState> serializer =
        source.getEnumeratorCheckpointSerializer();
    assertNotNull(serializer);
  }

  @Test
  public void testCreateEnumeratorWithNullState() throws Exception {

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    // Insert test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();
    ScanContext scanContext = createScanContext(conf);

    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        metaClient,
        mockRecordEmitter
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.createEnumerator(mockEnumContext);

    assertNotNull(enumerator);
  }

  @Test
  public void testRestoreEnumeratorWithState() throws Exception {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    // Create a state with some splits
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");

    List<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.UNASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.ASSIGNED)
    );

    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240101000000"),
        Option.empty()
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.restoreEnumerator(mockEnumContext, state);

    assertNotNull(enumerator);
  }

  @Test
  public void testRestoreEnumeratorWithEmptyState() throws Exception {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.empty(),
        Option.empty()
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.restoreEnumerator(mockEnumContext, state);

    assertNotNull(enumerator);
  }

  @Test
  public void testRestoreEnumeratorWithMultipleSplits() throws Exception {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    List<HoodieSourceSplitState> splitStates = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file-" + i, "/partition-" + i);
      splitStates.add(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED));
    }

    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240101120000"),
        Option.of("offset-123")
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.restoreEnumerator(mockEnumContext, state);

    assertNotNull(enumerator);
  }

  @Test
  public void testCreateStreamingEnumeratorWithNullState() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tempDir.getAbsolutePath());

    ScanContext streamingScanContext = new ScanContext.Builder()
        .conf(conf)
        .path(new Path(tempDir.getAbsolutePath()))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .build();

    HoodieSource<RowData> source = new HoodieSource<>(
        streamingScanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.createEnumerator(mockEnumContext);

    assertNotNull(enumerator);
  }

  @Test
  public void testCreateStreamingEnumeratorWithState() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tempDir.getAbsolutePath());

    ScanContext streamingScanContext = new ScanContext.Builder()
        .conf(conf)
        .path(new Path(tempDir.getAbsolutePath()))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .build();

    HoodieSource<RowData> source = new HoodieSource<>(
        streamingScanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        Collections.singletonList(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED)),
        Option.of("20240101000000"),
        Option.empty()
    );

    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = createMockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.restoreEnumerator(mockEnumContext, state);

    assertNotNull(enumerator);
  }

  @Test
  public void testSplitSerializerCanSerializeAndDeserialize() throws IOException {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    SimpleVersionedSerializer<HoodieSourceSplit> serializer = source.getSplitSerializer();
    HoodieSourceSplit originalSplit = createTestSplit(1, "file1", "/partition1");

    byte[] serialized = serializer.serialize(originalSplit);
    HoodieSourceSplit deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(originalSplit, deserializedSplit);
  }

  @Test
  public void testEnumeratorStateSerializerCanSerializeAndDeserialize() throws IOException {
    HoodieSource<RowData> source = new HoodieSource<>(
        scanContext,
        mockReaderFunction,
        mockComparator,
        mockMetaClient,
        mockRecordEmitter
    );

    SimpleVersionedSerializer<HoodieSplitEnumeratorState> serializer =
        source.getEnumeratorCheckpointSerializer();

    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    HoodieSplitEnumeratorState originalState = new HoodieSplitEnumeratorState(
        Collections.singletonList(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.ASSIGNED)),
        Option.of("20240101000000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(originalState);
    HoodieSplitEnumeratorState deserializedState = serializer.deserialize(
        serializer.getVersion(),
        serialized
    );

    assertEquals(originalState.getPendingSplitStates().size(),
        deserializedState.getPendingSplitStates().size());
    assertEquals(originalState.getLastEnumeratedInstant(),
        deserializedState.getLastEnumeratedInstant());
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit(int splitNum, String fileId, String partitionPath) {
    return new HoodieSourceSplit(
        splitNum,
        "base-path-" + splitNum,
        Option.of(Collections.emptyList()),
        "/test/table",
        partitionPath,
        "read_optimized",
        fileId
    );
  }

  private SplitEnumeratorContext<HoodieSourceSplit> createMockSplitEnumeratorContext() {
    SplitEnumeratorContext<HoodieSourceSplit> mockEnumContext = mock(SplitEnumeratorContext.class);
    SplitEnumeratorMetricGroup metricGroup = UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
    doReturn(metricGroup).when(mockEnumContext).metricGroup();

    return mockEnumContext;
  }

  private ScanContext createScanContext(Configuration conf) throws Exception {
    return createScanContextWithSkipOptions(conf, false, false, false);
  }

  private ScanContext createScanContextWithSkipOptions(
      Configuration conf,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite) throws Exception {
    return new ScanContext.Builder()
        .conf(conf)
        .path(new Path(tempDir.getAbsolutePath()))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant(conf.get(FlinkOptions.READ_START_COMMIT))
        .endInstant("")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .cdcEnabled(false)
        .build();
  }
}
