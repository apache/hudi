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

package org.apache.hudi.sink;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.MockCoordinatorExecutor;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for StreamingSinkOperatorCoordinator.
 */
public class TestStreamWriteOperatorCoordinator {
  private StreamWriteOperatorCoordinator coordinator;

  @TempDir
  File tempFile;
  NonThrownExecutor executor;

  @BeforeEach
  public void before() throws Exception {
    constructCoordinator(2);

    coordinator.handleEventFromOperator(0, WriteMetadataEvent.emptyBootstrap(0));
    coordinator.handleEventFromOperator(1, WriteMetadataEvent.emptyBootstrap(1));
  }

  @AfterEach
  public void after() throws Exception {
    coordinator.close();
  }

  private void constructCoordinator(int taskNum) throws Exception {
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), taskNum);
    coordinator = new StreamWriteOperatorCoordinator(
        TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), context);
    coordinator.start();
    executor = new MockCoordinatorExecutor(context);
    coordinator.setExecutor(executor);
  }

  @Test
  void testInstantState() {
    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    OperatorEvent event0 = createOperatorEvent(0, instant, "par1", true, 0.1);
    OperatorEvent event1 = createOperatorEvent(1, instant, "par2", false, 0.2);
    coordinator.handleEventFromOperator(0, event0);
    coordinator.handleEventFromOperator(1, event1);

    coordinator.notifyCheckpointComplete(1);
    String inflight = TestUtils.getLastPendingInstant(tempFile.getAbsolutePath());
    String lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertThat("Instant should be complete", lastCompleted, is(instant));
    assertNotEquals("", inflight, "Should start a new instant");
    assertNotEquals(instant, inflight, "Should start a new instant");
  }

  @Test
  public void testTableInitialized() throws IOException {
    final org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(new Configuration());
    String basePath = tempFile.getAbsolutePath();
    try (FileSystem fs = FSUtils.getFs(basePath, hadoopConf)) {
      assertTrue(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)));
    }
  }

  @Test
  public void testCheckpointAndRestore() throws Exception {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    coordinator.resetToCheckpoint(1, future.get());
  }

  @Test
  public void testReceiveInvalidEvent() {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    OperatorEvent event = WriteMetadataEvent.builder()
        .taskID(0)
        .instantTime("abc")
        .writeStatus(Collections.emptyList())
        .build();

    assertError(() -> coordinator.handleEventFromOperator(0, event),
        "Receive an unexpected event for instant abc from task 0");
  }

  @Test
  public void testCheckpointCompleteWithPartialEvents() {
    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String instant = coordinator.getInstant();
    OperatorEvent event = WriteMetadataEvent.builder()
        .taskID(0)
        .instantTime(instant)
        .writeStatus(Collections.emptyList())
        .build();
    coordinator.handleEventFromOperator(0, event);

    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1),
        "Returns early for empty write results");
    String lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertNull(lastCompleted, "Returns early for empty write results");
    assertNull(coordinator.getEventBuffer()[0]);

    OperatorEvent event1 = createOperatorEvent(1, instant, "par2", false, 0.2);
    coordinator.handleEventFromOperator(1, event1);
    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(2),
        "Commits the instant with partial events anyway");
    lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertThat("Commits the instant with partial events anyway", lastCompleted, is(instant));
  }

  @Test
  public void testHiveSyncInvoked() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.HIVE_SYNC_ENABLED, true);
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    coordinator = new StreamWriteOperatorCoordinator(conf, context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));

    final WriteMetadataEvent event0 = WriteMetadataEvent.emptyBootstrap(0);

    coordinator.handleEventFromOperator(0, event0);

    String instant = mockWriteWithMetadata();
    assertNotEquals("", instant);

    // never throw for hive synchronization now
    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1));
  }

  @Test
  void testSyncMetadataTable() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    conf.setInteger(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS, 5);
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    coordinator = new StreamWriteOperatorCoordinator(conf, context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));

    final WriteMetadataEvent event0 = WriteMetadataEvent.emptyBootstrap(0);

    coordinator.handleEventFromOperator(0, event0);

    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = StreamerUtil.createMetaClient(metadataTableBasePath, HadoopConfigurations.getHadoopConf(conf));
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(1L));
    assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // test metadata table compaction
    // write another 4 commits
    for (int i = 1; i < 5; i++) {
      instant = mockWriteWithMetadata();
      metadataTableMetaClient.reloadActiveTimeline();
      completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
      assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(i + 1L));
      assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(instant));
    }
    // the 5th commit triggers the compaction
    mockWriteWithMetadata();
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(7L));
    assertThat(completedTimeline.nthFromLastInstant(1).get().getTimestamp(), is(instant + "001"));
    assertThat(completedTimeline.nthFromLastInstant(1).get().getAction(), is(HoodieTimeline.COMMIT_ACTION));
    // write another 2 commits
    for (int i = 7; i < 8; i++) {
      instant = mockWriteWithMetadata();
      metadataTableMetaClient.reloadActiveTimeline();
      completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
      assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(i + 1L));
      assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(instant));
    }

    // write another commit to trigger clean
    instant = mockWriteWithMetadata();
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(10L));
    assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(instant + "002"));
    assertThat(completedTimeline.lastInstant().get().getAction(), is(HoodieTimeline.CLEAN_ACTION));

    // write another commit
    mockWriteWithMetadata();
    // write another commit
    instant = mockWriteWithMetadata();
    // write another commit to trigger compaction
    mockWriteWithMetadata();
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(14L));
    assertThat(completedTimeline.nthFromLastInstant(1).get().getTimestamp(), is(instant + "001"));
    assertThat(completedTimeline.nthFromLastInstant(1).get().getAction(), is(HoodieTimeline.COMMIT_ACTION));
  }

  @Test
  void testSyncMetadataTableWithReusedInstant() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    coordinator = new StreamWriteOperatorCoordinator(conf, context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));

    final WriteMetadataEvent event0 = WriteMetadataEvent.emptyBootstrap(0);

    coordinator.handleEventFromOperator(0, event0);

    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = StreamerUtil.createMetaClient(metadataTableBasePath, HadoopConfigurations.getHadoopConf(conf));
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(1L));
    assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // writes a normal commit
    mockWriteWithMetadata();
    instant = coordinator.getInstant();
    // creates an inflight commit on the metadata timeline
    metadataTableMetaClient.getActiveTimeline()
        .createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieActiveTimeline.DELTA_COMMIT_ACTION, instant));
    metadataTableMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instant);
    metadataTableMetaClient.reloadActiveTimeline();

    // write another commit with existing instant on the metadata timeline
    instant = mockWriteWithMetadata();
    metadataTableMetaClient.reloadActiveTimeline();

    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.getInstants().count(), is(3L));
    assertThat(completedTimeline.lastInstant().get().getTimestamp(), is(instant));
  }

  @Test
  public void testEndInputIsTheLastEvent() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    Logger logger = Mockito.mock(Logger.class); // avoid too many logs by executor
    NonThrownExecutor executor = NonThrownExecutor.builder(logger).waitForTasksFinish(true).build();

    try (StreamWriteOperatorCoordinator coordinator = new StreamWriteOperatorCoordinator(conf, context)) {
      coordinator.start();
      coordinator.setExecutor(executor);
      coordinator.handleEventFromOperator(0, WriteMetadataEvent.emptyBootstrap(0));
      TimeUnit.SECONDS.sleep(5); // wait for handled bootstrap event

      int eventCount = 20_000; // big enough to fill executor's queue
      for (int i = 0; i < eventCount; i++) {
        coordinator.handleEventFromOperator(0, createOperatorEvent(0, coordinator.getInstant(), "par1", true, 0.1));
      }

      WriteMetadataEvent endInput = WriteMetadataEvent.builder()
          .taskID(0)
          .instantTime(coordinator.getInstant())
          .writeStatus(Collections.emptyList())
          .endInput(true)
          .build();
      coordinator.handleEventFromOperator(0, endInput);

      // wait for submitted events completed
      executor.close();

      // there should be no events after endInput
      assertNull(coordinator.getEventBuffer()[0]);
    }
  }

  @Test
  public void testInstantFromBootstrapEvents() throws Exception {
    // Test fresh start
    constructCoordinator(2);
    coordinator.handleEventFromOperator(0, WriteMetadataEvent.emptyBootstrap(0));
    coordinator.handleEventFromOperator(1, WriteMetadataEvent.emptyBootstrap(1));
    executor.execute(() -> assertFalse(coordinator.bootstrapInstantFromEventBuffer().isPresent()), "");

    // Test valid commit with same write.task.num = 2
    constructCoordinator(2);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(1).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    executor.execute(() -> assertEquals(coordinator.bootstrapInstantFromEventBuffer().get(), "instant1"), "");

    // Test invalid commit with same write.task.num = 2
    constructCoordinator(2);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, WriteMetadataEvent.emptyBootstrap(1));
    executor.execute(() -> assertFalse(coordinator.bootstrapInstantFromEventBuffer().isPresent()), "");

    // Test valid commit with increasing write.task.num, 2 -> 4
    constructCoordinator(4);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(1).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(2, WriteMetadataEvent.emptyBootstrap(2));
    coordinator.handleEventFromOperator(3, WriteMetadataEvent.emptyBootstrap(3));
    executor.execute(() -> assertEquals(coordinator.bootstrapInstantFromEventBuffer().get(), "instant1"), "");

    // Test invalid commit with increasing write.task.num, 2 -> 4
    constructCoordinator(4);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(1).parallelism(2).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, WriteMetadataEvent.emptyBootstrap(1));
    coordinator.handleEventFromOperator(2, WriteMetadataEvent.emptyBootstrap(2));
    coordinator.handleEventFromOperator(3, WriteMetadataEvent.emptyBootstrap(3));
    executor.execute(() -> assertFalse(coordinator.bootstrapInstantFromEventBuffer().isPresent()), "");

    // Test valid commit with decreasing write.task.num, 4 -> 2
    constructCoordinator(2);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(1).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(1).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    executor.execute(() -> assertEquals(coordinator.bootstrapInstantFromEventBuffer().get(), "instant1"), "");

    // Test invalid commit with decreasing write.task.num, 4 -> 2
    constructCoordinator(2);
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(0, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(0).eventNumOfTask(2).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    coordinator.handleEventFromOperator(1, new WriteMetadataEvent.Builder().bootstrap(true).instantTime("instant1")
        .taskID(1).eventNumOfTask(1).parallelism(4).writeStatus(Collections.emptyList()).lastBatch(false).endInput(false).build());
    executor.execute(() -> assertFalse(coordinator.bootstrapInstantFromEventBuffer().isPresent()), "");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private String mockWriteWithMetadata() {
    final String instant = coordinator.getInstant();
    OperatorEvent event = createOperatorEvent(0, instant, "par1", true, 0.1);

    coordinator.handleEventFromOperator(0, event);
    coordinator.notifyCheckpointComplete(0);
    return instant;
  }

  private static WriteMetadataEvent createOperatorEvent(
      int taskId,
      String instant,
      String partitionPath,
      boolean trackSuccessRecords,
      double failureFraction) {
    final WriteStatus writeStatus = new WriteStatus(trackSuccessRecords, failureFraction);
    writeStatus.setPartitionPath(partitionPath);

    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId("fileId123");
    writeStat.setPath("path123");
    writeStat.setFileSizeInBytes(123);
    writeStat.setTotalWriteBytes(123);
    writeStat.setNumWrites(1);

    writeStatus.setStat(writeStat);

    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus))
        .lastBatch(true)
        .build();
  }

  private void reset() throws Exception {
    FileUtils.cleanDirectory(tempFile);
  }

  private void assertError(Runnable runnable, String message) {
    runnable.run();
    // wait a little while for the task to finish
    assertThat(coordinator.getContext(), instanceOf(MockOperatorCoordinatorContext.class));
    MockOperatorCoordinatorContext context = (MockOperatorCoordinatorContext) coordinator.getContext();
    assertTrue(context.isJobFailed(), message);
  }
}
