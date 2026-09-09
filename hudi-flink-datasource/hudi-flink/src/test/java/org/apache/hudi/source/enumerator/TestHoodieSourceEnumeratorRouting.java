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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.HoodieSource;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.function.HoodieSplitReaderFunction;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.GlobalHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitComparator;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSourceSplitStatus;
import org.apache.hudi.source.split.HoodieSplitProvider;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests which split provider {@link HoodieSource} wires into the enumerator.
 *
 * <p>Bounded reads use the shared work-stealing pool ({@link GlobalHoodieSplitProvider}); streaming
 * keeps per-subtask assignment ({@link DefaultHoodieSplitProvider}) so that a file id's successive
 * incremental splits stay affine to one reader. Because {@code HoodieSource.createEnumerator}
 * handles fresh creation and restore in the same method, both paths are covered for every mode.
 *
 * <p>These tests also assert the property that makes work stealing safe for bounded reads: every
 * bounded query mode emits exactly one split per file group, so there is no cross-commit
 * continuation and no ordering relationship between splits that a shared pool could break.
 *
 * <p>Lives in the enumerator package so it can read the package-private
 * {@link AbstractHoodieSplitEnumerator#splitProvider}.
 */
public class TestHoodieSourceEnumeratorRouting {

  @TempDir
  File tempDir;

  private Configuration conf;
  private StoragePath tablePath;
  private HoodieTableMetaClient metaClient;

  /**
   * The bounded query modes {@code HoodieSource.createBatchHoodieSplits()} covers. All of them are
   * routed to the shared pool, so all of them are exercised here.
   *
   * <p>Incremental appears three times on purpose. {@code IncrementalInputSplits.inputSplits()}
   * branches on {@code fullTableScan}, which is true when the query consumes from the earliest
   * instant, and the two sides build their file slice set differently: the full scan lists the
   * table directly, while the other side derives partitions and files from the commit metadata of
   * the instants in range (and, when CDC is on, leaves through the CDC extractor entirely). Only
   * covering {@code earliest} would leave the metadata-driven branch untested.
   */
  private enum BoundedMode {
    COW_SNAPSHOT(HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_SNAPSHOT, false, IncrementalStart.NOT_INCREMENTAL),
    MOR_SNAPSHOT(HoodieTableType.MERGE_ON_READ, FlinkOptions.QUERY_TYPE_SNAPSHOT, false, IncrementalStart.NOT_INCREMENTAL),
    MOR_READ_OPTIMIZED(HoodieTableType.MERGE_ON_READ, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED, false, IncrementalStart.NOT_INCREMENTAL),
    COW_INCREMENTAL(HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_INCREMENTAL, false, IncrementalStart.LAST_COMMIT),
    COW_INCREMENTAL_FROM_EARLIEST(HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_INCREMENTAL, false, IncrementalStart.EARLIEST),
    COW_INCREMENTAL_CDC(HoodieTableType.COPY_ON_WRITE, FlinkOptions.QUERY_TYPE_INCREMENTAL, true, IncrementalStart.LAST_COMMIT);

    private final HoodieTableType tableType;
    private final String queryType;
    private final boolean cdcEnabled;
    private final IncrementalStart incrementalStart;

    BoundedMode(HoodieTableType tableType, String queryType, boolean cdcEnabled, IncrementalStart incrementalStart) {
      this.tableType = tableType;
      this.queryType = queryType;
      this.cdcEnabled = cdcEnabled;
      this.incrementalStart = incrementalStart;
    }

    boolean isIncremental() {
      return incrementalStart != IncrementalStart.NOT_INCREMENTAL;
    }
  }

  /**
   * Where an incremental mode starts reading, which is what decides the {@code fullTableScan}
   * branch: {@code earliest} leaves {@code startInstant} empty and takes the full scan,
   * a real completion time takes the metadata-driven branch.
   */
  private enum IncrementalStart {
    NOT_INCREMENTAL,
    EARLIEST,
    LAST_COMMIT
  }

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    tablePath = new StoragePath(tempDir.getAbsolutePath());
  }

  @ParameterizedTest
  @EnumSource(BoundedMode.class)
  public void testBoundedReadUsesSharedSplitPool(BoundedMode mode) throws Exception {
    HoodieSource<RowData> source = prepareBoundedSource(mode);
    MockSplitEnumeratorContext context = new MockSplitEnumeratorContext();

    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.createEnumerator(context);

    assertInstanceOf(HoodieStaticSplitEnumerator.class, enumerator,
        "Bounded read should use the static enumerator for mode " + mode);
    assertInstanceOf(GlobalHoodieSplitProvider.class, providerOf(enumerator),
        "Bounded read should use the shared work-stealing pool for mode " + mode);
    List<HoodieSourceSplit> splits = pendingSplits(enumerator);
    assertOneSplitPerFileGroup(splits, mode);
    if (mode.incrementalStart == IncrementalStart.LAST_COMMIT) {
      // Guards the parameterization: if the start commit stopped making fullTableScan false, these
      // splits would come from a full table listing and cover par1 through par6, and this mode
      // would silently stop exercising the metadata-driven branch.
      assertEquals(new HashSet<>(Arrays.asList("par5", "par6")),
          splits.stream().map(HoodieSourceSplit::getPartitionPath).collect(Collectors.toSet()),
          "Mode " + mode + " should read only the partitions written by the start commit, "
              + "which is what distinguishes the incremental branch from a full table scan");
    }
    if (mode.cdcEnabled) {
      // Likewise, a full table scan would bypass the CDC extractor and yield plain splits.
      splits.forEach(split -> assertInstanceOf(HoodieCdcSourceSplit.class, split,
          "CDC mode should produce CDC splits"));
    }
  }

  @ParameterizedTest
  @EnumSource(BoundedMode.class)
  public void testBoundedRestoreKeepsSharedSplitPool(BoundedMode mode) throws Exception {
    HoodieSource<RowData> source = prepareBoundedSource(mode);
    // Snapshot the real splits of this mode, then restore from a subset of them.
    List<HoodieSourceSplit> discovered =
        pendingSplits(source.createEnumerator(new MockSplitEnumeratorContext()));
    assertFalse(discovered.isEmpty(), "Expected at least one split for mode " + mode);
    List<HoodieSourceSplit> checkpointed = discovered.subList(0, 1);

    MockSplitEnumeratorContext context = new MockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restored =
        source.restoreEnumerator(context, enumeratorStateOf(checkpointed));

    assertInstanceOf(HoodieStaticSplitEnumerator.class, restored,
        "Restored bounded read should still use the static enumerator for mode " + mode);
    assertInstanceOf(GlobalHoodieSplitProvider.class, providerOf(restored),
        "Restored bounded read should still use the shared work-stealing pool for mode " + mode);
    assertEquals(checkpointed.size(), providerOf(restored).pendingSplitCount(),
        "Restore should replay exactly the checkpointed splits into the shared pool "
            + "and must not re-run split discovery for mode " + mode);
  }

  /**
   * A restored pending split is not owned by any subtask: whichever reader asks for work claims it.
   * Parameterized over the requesting subtask so the assertion is deterministic - under per-subtask
   * pinning only the one subtask the file id hashes to could ever receive it.
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  public void testRestoredSplitIsClaimedByWhicheverSubtaskAsks(int requestingSubtask) throws Exception {
    HoodieSource<RowData> source = prepareBoundedSource(BoundedMode.COW_SNAPSHOT);
    List<HoodieSourceSplit> discovered =
        pendingSplits(source.createEnumerator(new MockSplitEnumeratorContext()));
    List<HoodieSourceSplit> checkpointed = discovered.subList(0, 1);

    MockSplitEnumeratorContext context = new MockSplitEnumeratorContext();
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restored =
        source.restoreEnumerator(context, enumeratorStateOf(checkpointed));
    restored.start();
    for (int subtask = 0; subtask < 4; subtask++) {
      context.registerReader(new ReaderInfo(subtask, "localhost"));
    }

    restored.handleSplitRequest(requestingSubtask, "localhost");

    assertEquals(checkpointed, context.getAssignedSplits().get(requestingSubtask),
        "The restored split should go to whichever subtask asked for work");
    assertFalse(context.getNoMoreSplitsSignaled().contains(requestingSubtask),
        "The requesting subtask received a split, so it should not be told no-more-splits");
  }

  @Test
  public void testStreamingReadKeepsPerSubtaskProvider() throws Exception {
    HoodieSource<RowData> source = prepareStreamingSource();

    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator =
        source.createEnumerator(new MockSplitEnumeratorContext());

    assertInstanceOf(HoodieContinuousSplitEnumerator.class, enumerator,
        "Streaming read should use the continuous enumerator");
    assertInstanceOf(DefaultHoodieSplitProvider.class, providerOf(enumerator),
        "Streaming read must keep per-subtask assignment for file id affinity");
  }

  @Test
  public void testStreamingRestoreKeepsPerSubtaskProvider() throws Exception {
    HoodieSource<RowData> source = prepareStreamingSource();
    List<HoodieSourceSplit> checkpointed = Collections.singletonList(
        new HoodieSourceSplit(0, null, Option.empty(), tablePath.toString(), "par1",
            FlinkOptions.REALTIME_PAYLOAD_COMBINE, "20260126034717000", "file-0", Option.empty()));

    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restored =
        source.restoreEnumerator(new MockSplitEnumeratorContext(), enumeratorStateOf(checkpointed));

    assertInstanceOf(HoodieContinuousSplitEnumerator.class, restored,
        "Restored streaming read should use the continuous enumerator");
    assertInstanceOf(DefaultHoodieSplitProvider.class, providerOf(restored),
        "Restored streaming read must keep per-subtask assignment");
    assertEquals(1, providerOf(restored).pendingSplitCount(),
        "Restored split should be replayed into the per-subtask provider");
  }

  // Helper methods

  private static HoodieSplitProvider providerOf(
      SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator) {
    return ((AbstractHoodieSplitEnumerator) enumerator).splitProvider;
  }

  private static List<HoodieSourceSplit> pendingSplits(
      SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> enumerator) {
    return providerOf(enumerator).state().stream()
        .map(HoodieSourceSplitState::getSplit)
        .collect(Collectors.toList());
  }

  private static HoodieSplitEnumeratorState enumeratorStateOf(List<HoodieSourceSplit> splits) {
    List<HoodieSourceSplitState> states = splits.stream()
        .map(split -> new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
    return new HoodieSplitEnumeratorState(states, Option.empty(), Option.empty());
  }

  /**
   * Asserts the invariant that makes a shared pool safe for a bounded read: one split per file
   * group, hence no cross-commit continuation and no ordering relationship between splits.
   */
  private static void assertOneSplitPerFileGroup(List<HoodieSourceSplit> splits, BoundedMode mode) {
    assertFalse(splits.isEmpty(), "Expected at least one split for mode " + mode);
    Set<String> fileIds = splits.stream()
        .map(HoodieSourceSplit::getFileId)
        .collect(Collectors.toSet());
    assertEquals(splits.size(), fileIds.size(),
        "Mode " + mode + " must emit exactly one split per file group, otherwise splits of the "
            + "same file group could be read concurrently by different readers");
  }

  private HoodieSource<RowData> prepareBoundedSource(BoundedMode mode) throws Exception {
    conf.set(FlinkOptions.TABLE_TYPE, mode.tableType.name());
    conf.set(FlinkOptions.READ_AS_STREAMING, false);
    if (mode.tableType == HoodieTableType.MERGE_ON_READ) {
      // Compact the first commit so the MOR file groups own a base file (a read-optimized read
      // sees nothing otherwise); the second commit below is then written as logs only, so a
      // snapshot read exercises real base + log file slices.
      conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
      conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    }
    if (mode.cdcEnabled) {
      conf.set(FlinkOptions.CDC_ENABLED, true);
      conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true); // for batch update
    }

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    if (mode.tableType == HoodieTableType.MERGE_ON_READ) {
      conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    }
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    if (mode.incrementalStart == IncrementalStart.LAST_COMMIT) {
      // A last commit that only touches par5 and par6, so the partitions of the resulting splits
      // show which branch produced them: the metadata-driven branch derives its read partitions
      // from this commit alone, a full table scan would list par1 through par6.
      TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    }
    metaClient = StreamerUtil.createMetaClient(conf);

    conf.set(FlinkOptions.QUERY_TYPE, mode.queryType);
    if (mode.incrementalStart == IncrementalStart.EARLIEST) {
      conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    } else if (mode.incrementalStart == IncrementalStart.LAST_COMMIT) {
      conf.set(FlinkOptions.READ_START_COMMIT, lastCompletionTime());
    }
    return createSource();
  }

  private HoodieSource<RowData> prepareStreamingSource() throws Exception {
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient = StreamerUtil.createMetaClient(conf);

    return createSource();
  }

  private String lastCompletionTime() {
    List<String> commits = metaClient.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .map(HoodieInstant::getCompletionTime)
        .collect(Collectors.toList());
    assertTrue(commits.size() > 1, "Expected more than one commit to read changes from");
    return commits.get(commits.size() - 1);
  }

  private HoodieSource<RowData> createSource() {
    RowType rowType = TestConfigurations.ROW_TYPE;
    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(tablePath)
        .rowType(rowType)
        .startInstant(conf.get(FlinkOptions.READ_START_COMMIT))
        .endInstant(conf.get(FlinkOptions.READ_END_COMMIT))
        .maxCompactionMemoryInBytes(conf.get(FlinkOptions.COMPACTION_MAX_MEMORY))
        .maxPendingSplits(1000)
        .skipCompaction(conf.get(FlinkOptions.READ_STREAMING_SKIP_COMPACT))
        .skipClustering(conf.get(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING))
        .skipInsertOverwrite(conf.get(FlinkOptions.READ_STREAMING_SKIP_INSERT_OVERWRITE))
        .cdcEnabled(conf.get(FlinkOptions.CDC_ENABLED))
        .isStreaming(conf.get(FlinkOptions.READ_AS_STREAMING))
        .build();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType);
    HadoopStorageConfiguration hadoopConf =
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf));
    InternalSchemaManager internalSchemaManager = InternalSchemaManager.get(hadoopConf, metaClient);

    return new HoodieSource<>(
        scanContext,
        () -> new HoodieSplitReaderFunction(
            conf,
            schema,
            schema,
            internalSchemaManager,
            conf.get(FlinkOptions.MERGE_TYPE),
            Collections.emptyList(),
            false),
        new HoodieSourceSplitComparator(),
        metaClient,
        new HoodieRecordEmitter<>());
  }

  /**
   * Minimal mock of {@link SplitEnumeratorContext} for the wiring assertions above.
   */
  private static class MockSplitEnumeratorContext implements SplitEnumeratorContext<HoodieSourceSplit> {
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<HoodieSourceSplit>> assignedSplits = new HashMap<>();
    private final List<Integer> noMoreSplitsSignaled = new ArrayList<>();

    void registerReader(ReaderInfo readerInfo) {
      registeredReaders.put(readerInfo.getSubtaskId(), readerInfo);
    }

    Map<Integer, List<HoodieSourceSplit>> getAssignedSplits() {
      return assignedSplits;
    }

    List<Integer> getNoMoreSplitsSignaled() {
      return noMoreSplitsSignaled;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
      // No-op for testing
    }

    @Override
    public int currentParallelism() {
      return Math.max(registeredReaders.size(), 1);
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
      return new HashMap<>(registeredReaders);
    }

    @Override
    public void assignSplits(SplitsAssignment<HoodieSourceSplit> newSplitAssignments) {
      newSplitAssignments.assignment().forEach((subtask, splits) ->
          assignedSplits.computeIfAbsent(subtask, k -> new ArrayList<>()).addAll(splits));
    }

    @Override
    public void assignSplit(HoodieSourceSplit split, int subtask) {
      assignedSplits.computeIfAbsent(subtask, k -> new ArrayList<>()).add(split);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
      noMoreSplitsSignaled.add(subtask);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      // No-op: split discovery is not exercised by these wiring tests.
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {
      // No-op: split discovery is not exercised by these wiring tests.
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
      runnable.run();
    }
  }
}
