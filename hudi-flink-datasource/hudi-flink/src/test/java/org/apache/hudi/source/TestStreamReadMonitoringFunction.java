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

package org.apache.hudi.source;

import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link StreamReadMonitoringFunction}.
 */
public class TestStreamReadMonitoringFunction {
  private static final long WAIT_TIME_MILLIS = 10 * 1000L;

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2); // check every 2 seconds

    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  public void testConsumeFromLatestCommit() throws Exception {
    // write 2 commits first， and all the splits should come from the second commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);

      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));

      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");
      String latestCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getLatestCommit().equals(latestCommit)),
          "All the splits should be with latestCommit instant time");

      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testConsumeFromLastCommit() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);

      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");

      Thread.sleep(1000L);

      // reset the source context
      latch = new CountDownLatch(4);
      sourceContext.reset(latch);

      // write another instant and validate
      TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");

      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testConsumeForSpeedLimitWhenEmptyCommitExists() throws Exception {
    // Step1 : create 4 empty commit
    Configuration conf = new Configuration(this.conf);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.setString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), "true");

    TestData.writeData(Collections.EMPTY_LIST, conf);
    TestData.writeData(Collections.EMPTY_LIST, conf);
    TestData.writeData(Collections.EMPTY_LIST, conf);
    TestData.writeData(Collections.EMPTY_LIST, conf);

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(conf.get(FlinkOptions.PATH), HoodieTableType.COPY_ON_WRITE);
    HoodieTimeline commitsTimeline = metaClient.reloadActiveTimeline()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    HoodieInstant firstInstant = commitsTimeline.firstInstant().get();

    // Step2: trigger streaming read from first instant and set READ_COMMITS_LIMIT 2
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING, true);
    conf.set(FlinkOptions.READ_STREAMING_SKIP_COMPACT, true);
    conf.set(FlinkOptions.READ_COMMITS_LIMIT, 2);
    conf.set(FlinkOptions.READ_START_COMMIT, String.valueOf((Long.valueOf(firstInstant.requestedTime()) - 100)));
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(0);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      function.monitorDirAndForwardSplits(sourceContext);
      assertEquals(0, sourceContext.splits.size(), "There should be no inputSplits");

      // Step3: assert current IssuedOffset couldn't be null.
      // Base on "IncrementalInputSplits#inputSplits => .startCompletionTime(issuedOffset != null ? issuedOffset : this.conf.getString(FlinkOptions.READ_START_COMMIT))"
      // If IssuedOffset still was null, hudi would take FlinkOptions.READ_START_COMMIT again, which means streaming read is blocked.
      assertNotNull(function.getIssuedOffset());
      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testConsumeFromSpecifiedCommit() throws Exception {
    // write 2 commits first, use the second commit time as the specified start instant,
    // all the splits should come from the second commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    String specifiedCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.READ_START_COMMIT, specifiedCommit);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);

      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getLatestCommit().equals(specifiedCommit)),
          "All the splits should be with specified instant time");

      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testConsumeFromEarliestCommit() throws Exception {
    // write 2 commits first, then specify the start commit as 'earliest',
    // all the splits should come from the earliest commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    String specifiedCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);

      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().noneMatch(split -> split.getInstantRange().isPresent()),
          "No instants should have range limit");
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getLatestCommit().equals(specifiedCommit)),
          "All the splits should be with specified instant time");

      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testConsumingHollowInstants() throws Exception {
    // write 4 commits
    conf.setString("hoodie.parquet.small.file.limit", "0"); // invalidate the small file strategy
    for (int i = 0; i < 8; i += 2) {
      List<RowData> dataset = TestData.dataSetInsert(i + 1, i + 2);
      TestData.writeData(dataset, conf);
    }

    // we got 4 commits on the timeline: c1, c2, c3, c4
    // re-create the metadata file for c2 and c3 so that they have greater completion time than c4.
    // the completion time sequence become: c1, c4, c2, c3,
    // we will test with the same consumption sequence.
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    List<HoodieInstant> oriInstants = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertThat(oriInstants.size(), is(4));
    List<HoodieCommitMetadata> metadataList = new ArrayList<>();
    // timeline: c1, c2.inflight, c3.inflight, c4
    for (int i = 1; i <= 2; i++) {
      HoodieInstant instant = oriInstants.get(i);
      metadataList.add(TestUtils.deleteInstantFile(metaClient, instant));
    }

    List<HoodieInstant> instants = metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
    assertThat(instants.size(), is(2));

    String c2 = oriInstants.get(1).requestedTime();
    String c3 = oriInstants.get(2).requestedTime();
    String c4 = instants.get(1).requestedTime();

    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      // timeline: c1, c2.inflight, c3.inflight, c4
      // -> c1
      CountDownLatch latch = new CountDownLatch(2);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);

      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1"));
      assertTrue(sourceContext.splits.stream().noneMatch(split -> split.getInstantRange().isPresent()),
          "No instants should have range limit");
      assertTrue(sourceContext.splits.stream().anyMatch(split -> split.getLatestCommit().equals(c4)),
          "At least one input split's latest commit time should be equal to the specified instant time.");

      // reset the source context
      latch = new CountDownLatch(1);
      sourceContext.reset(latch);

      // timeline: c1, c2, c3.inflight, c4
      // c4 -> c2
      TestUtils.saveInstantAsComplete(metaClient, oriInstants.get(1), metadataList.get(0)); // complete c2
      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");
      assertTrue(sourceContext.splits.stream().allMatch(split -> isPointInstantRange(split.getInstantRange().get(), c2)),
          "All the splits should have point instant range");
      assertTrue(sourceContext.splits.stream().anyMatch(split -> split.getLatestCommit().equals(c2)),
          "At least one input split's latest commit time should be equal to the specified instant time.");

      // reset the source context
      latch = new CountDownLatch(1);
      sourceContext.reset(latch);

      // timeline: c1, c2, c3, c4
      // c4 -> c3
      TestUtils.saveInstantAsComplete(metaClient, oriInstants.get(2), metadataList.get(1)); // complete c3
      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");
      assertTrue(sourceContext.splits.stream().allMatch(split -> isPointInstantRange(split.getInstantRange().get(), c3)),
          "All the splits should have point instant range");
      assertTrue(sourceContext.splits.stream().anyMatch(split -> split.getLatestCommit().equals(c3)),
          "At least one input split's latest commit time should be equal to the specified instant time.");

      // Stop the stream task.
      function.close();
    }
  }

  @Test
  public void testCheckpointRestore() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    OperatorSubtaskState state;
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      Thread.sleep(1000L);

      state = harness.snapshot(1, 1);

      // Stop the stream task.
      function.close();

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");

    }

    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    StreamReadMonitoringFunction function2 = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function2)) {
      harness.setup();
      // Recover to process the remaining snapshots.
      harness.initializeState(state);
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function2);

      // Stop the stream task.
      function.close();

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");
    }
  }

  /**
   * When stopping with savepoint, these interface methods are called:
   * <ul>
   * <li>cancel()</li>
   * <li>snapshotState()</li>
   * <li>close()</li>
   * </ul>
   * This test ensured that the state is saved properly when these 3 methods are called in the order listed above.
   */
  @Test
  public void testStopWithSavepointAndRestore() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    OperatorSubtaskState state;
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function);

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      Thread.sleep(1000L);

      // Simulate a stop-with-savepoint
      function.cancel();

      state = harness.snapshot(1, 1);

      // Stop the stream task.
      function.close();

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().noneMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");

    }

    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    StreamReadMonitoringFunction function2 = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function2)) {
      harness.setup();
      // Recover to process the remaining snapshots.
      harness.initializeState(state);
      harness.open();

      CountDownLatch latch = new CountDownLatch(4);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function2);

      // Stop the stream task.
      function.close();

      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      assertThat("Should produce the expected splits",
          sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");
    }
  }

  @Test
  public void testCheckpointRestoreWithLimit() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    conf.set(FlinkOptions.READ_SPLITS_LIMIT, 2);
    StreamReadMonitoringFunction function = TestUtils.getMonitorFunc(conf);
    OperatorSubtaskState state;
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();
      CountDownLatch latch = new CountDownLatch(2);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function);
      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      state = harness.snapshot(1, 1);
      // Stop the stream task.
      function.close();
      assertThat("Should produce the expected splits", sourceContext.getPartitionPaths(), is("par1,par2"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All instants should have range limit");
    }
    conf.set(FlinkOptions.READ_SPLITS_LIMIT, Integer.MAX_VALUE);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    StreamReadMonitoringFunction function2 = TestUtils.getMonitorFunc(conf);
    try (AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> harness = createHarness(function2)) {
      harness.setup();
      // Recover to process the remaining snapshots.
      harness.initializeState(state);
      harness.open();
      CountDownLatch latch = new CountDownLatch(6);
      CollectingSourceContext sourceContext = new CollectingSourceContext(latch);
      runAsync(sourceContext, function2);
      assertTrue(latch.await(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS), "Should finish splits generation");
      // Stop the stream task.
      function2.close();
      assertThat("Should produce the expected splits", sourceContext.getPartitionPaths(), is("par1,par2,par3,par4"));
      assertTrue(sourceContext.splits.stream().allMatch(split -> split.getInstantRange().isPresent()),
          "All the instants should have range limit");
    }
  }

  private static boolean isPointInstantRange(InstantRange instantRange, String timestamp) {
    return instantRange != null
        && Objects.equals(timestamp, instantRange.getStartInstant().get())
        && Objects.equals(timestamp, instantRange.getEndInstant().get());
  }

  private AbstractStreamOperatorTestHarness<MergeOnReadInputSplit> createHarness(
      StreamReadMonitoringFunction function) throws Exception {
    StreamSource<MergeOnReadInputSplit, StreamReadMonitoringFunction> streamSource = new StreamSource<>(function);
    return new AbstractStreamOperatorTestHarness<>(streamSource, 1, 1, 0);
  }

  private void runAsync(
      CollectingSourceContext sourceContext,
      StreamReadMonitoringFunction function) {
    Thread task = new Thread(() -> {
      try {
        function.run(sourceContext);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    task.start();
  }

  /**
   * Source context that collects the outputs in to a list.
   */
  private static class CollectingSourceContext implements SourceFunctionAdapter.SourceContext<MergeOnReadInputSplit> {
    private final List<MergeOnReadInputSplit> splits = new ArrayList<>();
    private final Object checkpointLock = new Object();
    private volatile CountDownLatch latch;

    CollectingSourceContext(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void collect(MergeOnReadInputSplit element) {
      splits.add(element);
      latch.countDown();
    }

    @Override
    public void collectWithTimestamp(MergeOnReadInputSplit element, long timestamp) {
      collect(element);
    }

    @Override
    public void emitWatermark(Watermark mark) {

    }

    @Override
    public void markAsTemporarilyIdle() {

    }

    @Override
    public Object getCheckpointLock() {
      return checkpointLock;
    }

    @Override
    public void close() {

    }

    public void reset(CountDownLatch latch) {
      this.latch = latch;
      this.splits.clear();
    }

    public String getPartitionPaths() {
      return this.splits.stream()
          .map(TestUtils::getSplitPartitionPath)
          .distinct()
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.joining(","));
    }
  }
}
