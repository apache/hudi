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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link StreamReadMonitoringFunction}.
 */
public class TestStreamReadMonitoringFunction {
  private static final long WAIT_TIME_MILLIS = 5 * 1000L;

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.setInteger(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 2); // check every 2 seconds

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
  public void testConsumeFromSpecifiedCommit() throws Exception {
    // write 2 commits first, use the second commit time as the specified start instant,
    // all the splits should come from the second commit.
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    String specifiedCommit = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.READ_START_COMMIT, specifiedCommit);
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
    conf.setString(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
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
  private static class CollectingSourceContext implements SourceFunction.SourceContext<MergeOnReadInputSplit> {
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
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.joining(","));
    }
  }
}
