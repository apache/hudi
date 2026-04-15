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
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.CoordinationResponseSerDe;
import org.apache.hudi.sink.utils.EventBuffers;
import org.apache.hudi.sink.utils.MockCoordinatorExecutor;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for StreamingSinkOperatorCoordinator.
 */
public class TestStreamWriteOperatorCoordinator {
  private StreamWriteOperatorCoordinator coordinator;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    coordinator = createCoordinator(TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), 2);
  }

  @AfterEach
  public void after() throws Exception {
    coordinator.close();
  }

  @Test
  void testInstantState() {
    requestInstantTime(-1);
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
    try (FileSystem fs = HadoopFSUtils.getFs(basePath, hadoopConf)) {
      assertTrue(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCheckpointAndRestore(boolean isStreamingIndexWriteEnabled) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    if (isStreamingIndexWriteEnabled) {
      conf.set(FlinkOptions.INDEX_TYPE, GLOBAL_RECORD_LEVEL_INDEX.name());
      conf.set(FlinkOptions.INDEX_WRITE_TASKS, 2);
    }
    coordinator = createCoordinator(conf, 2);

    requestInstantTime(-1);
    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    OperatorEvent event0 = createOperatorEvent(0, instant, "par1", true, 0.1);
    OperatorEvent event1 = createOperatorEvent(1, instant, "par2", true, 0.2);
    coordinator.handleEventFromOperator(0, event0);
    coordinator.handleEventFromOperator(1, event1);

    if (isStreamingIndexWriteEnabled) {
      OperatorEvent indexEvent0 = createOperatorEvent(0, -1, instant, "record_index", false, true, 0.1, true);
      OperatorEvent indexEvent1 = createOperatorEvent(1, -1, instant, "record_index", false, true, 0.2, true);
      coordinator.handleEventFromOperator(0, indexEvent0);
      coordinator.handleEventFromOperator(1, indexEvent1);
    }

    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    coordinator.notifyCheckpointComplete(1);
    coordinator.resetToCheckpoint(1, future.get());

    EventBuffers.EventBuffer eventBuffer = coordinator.getEventBuffer();
    assertEquals(2, eventBuffer.getDataWriteEventBuffer().length);
    assertEquals(isStreamingIndexWriteEnabled ? 2 : 0, eventBuffer.getIndexWriteEventBuffer().length);
  }

  @Test
  public void testRestoreFromLegacyState() throws Exception {
    requestInstantTime(-1);
    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    OperatorEvent event0 = createOperatorEvent(0, instant, "par1", true, 0.1);
    OperatorEvent event1 = createOperatorEvent(1, instant, "par2", true, 0.2);
    coordinator.handleEventFromOperator(0, event0);
    coordinator.handleEventFromOperator(1, event1);

    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    coordinator.notifyCheckpointComplete(1);

    Map<Long, Pair<String, EventBuffers.EventBuffer>> eventBuffers = SerializationUtils.deserialize(future.get());
    // convert to legacy event buffers
    Map<Long, Pair<String, WriteMetadataEvent[]>> legacyEventBuffers = new HashMap<>();
    eventBuffers.forEach((ckpId, eventBuffer) -> {
      legacyEventBuffers.put(ckpId, Pair.of(eventBuffer.getLeft(), eventBuffer.getRight().getDataWriteEventBuffer()));
    });
    // simulate recovering from legacy state
    coordinator.resetToCheckpoint(1, SerializationUtils.serialize(legacyEventBuffers));
    EventBuffers.EventBuffer eventBuffer = coordinator.getEventBuffer();
    assertEquals(2, eventBuffer.getDataWriteEventBuffer().length);
    assertEquals(0, eventBuffer.getIndexWriteEventBuffer().length);
  }

  @Test
  public void testReceiveInvalidEvent() {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    OperatorEvent event = WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(1L)
        .instantTime("abc")
        .writeStatus(Collections.emptyList())
        .build();

    assertError(() -> coordinator.handleEventFromOperator(0, event),
        "Receive an unexpected event for instant abc from task 0");
  }

  @Test
  public void testEventReset() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    coordinator = createCoordinator(conf, 2);
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String instant = requestInstantTime(0);
    OperatorEvent event1 = WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(0)
        .instantTime(instant)
        .writeStatus(Collections.emptyList())
        .build();
    coordinator.handleEventFromOperator(0, event1);
    coordinator.subtaskFailed(0, null);
    assertNotNull(coordinator.getEventBuffer().getDataWriteEventBuffer()[0], "Events should not be cleared by subTask failure");

    OperatorEvent event2 = createOperatorEvent(0, 0, instant, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event2);
    coordinator.subtaskFailed(0, null);
    assertNotNull(coordinator.getEventBuffer().getDataWriteEventBuffer()[0], "Events should not be cleared by subTask failure");

    OperatorEvent event3 = createOperatorEvent(0, 0, instant, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event3);
    assertThat("Multiple events of same instant should be merged",
        coordinator.getEventBuffer().getDataWriteEventBuffer()[0].getWriteStatuses().size(), is(1));

    long nextCkpId = 1;
    coordinator.handleCoordinationRequest(Correspondent.InstantTimeRequest.getInstance(nextCkpId));
    OperatorEvent event4 = createOperatorEvent(0, nextCkpId, "002", "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event4);
    assertThat("First instant is not committed yet, new event should not override the old event",
        coordinator.getEventBuffer(0).getDataWriteEventBuffer()[0].getWriteStatuses().size(), is(1));
    assertThat("Second instant should have one newly added write status",
        coordinator.getEventBuffer(1).getDataWriteEventBuffer()[0].getWriteStatuses().size(), is(1));
  }

  @Test
  public void testCheckpointCompleteWithPartialEvents() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), "false");

    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 2);
    coordinator = createCoordinator(conf, 2);

    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String instant = requestInstantTime(coordinator, 0);
    OperatorEvent event = WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(0)
        .instantTime(instant)
        .writeStatus(Collections.emptyList())
        .build();
    coordinator.handleEventFromOperator(0, event);

    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1),
        "Returns early for empty write results");
    String lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertNull(lastCompleted, "Returns early for empty write results");
    assertNull(coordinator.getEventBuffer());
  }

  @Test
  public void testRecommitWithPartialUncommittedEvents() {
    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertNull(lastCompleted, "Returns early for empty write results");
    String instant = requestInstantTime(-1);
    WriteMetadataEvent event1 = createOperatorEvent(0, instant, "par1", false, 0.2);
    event1.setBootstrap(true);
    WriteMetadataEvent event2 = createOperatorEvent(1, instant, "par1", false, 0.2);
    coordinator.handleEventFromOperator(1, event2);
    coordinator.handleEventFromOperator(0, event1);
    lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertNull(lastCompleted, "Should not trigger the partial bootstrap commit");
    coordinator.notifyCheckpointComplete(1);
    lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
    assertThat("Recommits the instant with partial uncommitted events", lastCompleted, is(instant));
  }

  @Test
  public void testStopHeartbeatForUncommittedEventWithLazyCleanPolicy() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.LAZY.name());
    coordinator = createCoordinator(conf, 1);

    assertTrue(coordinator.getWriteClient().getConfig().getFailedWritesCleanPolicy().isLazy());

    // start one instant and not commit it
    requestInstantTime(0);
    String instant = coordinator.getInstant();
    HoodieHeartbeatClient heartbeatClient = coordinator.getWriteClient().getHeartbeatClient();
    assertNotNull(heartbeatClient.getHeartbeat(instant), "Heartbeat is missing");

    String basePath = tempFile.getAbsolutePath();
    HoodieStorage storage =
        coordinator.getWriteClient().getHoodieTable().getStorage();

    assertTrue(HoodieHeartbeatClient.heartbeatExists(storage, basePath, instant),
        "Heartbeat is existed");

    // send bootstrap event to stop the heartbeat for this instant
    WriteMetadataEvent event1 = createBootstrapEvent(0, 0, instant, "par1");
    coordinator.handleEventFromOperator(0, event1);

    assertFalse(HoodieHeartbeatClient.heartbeatExists(storage, basePath, instant),
        "Heartbeat is stopped and cleared");
  }

  @Test
  public void testRecommitWithLazyFailedWritesCleanPolicy() {
    coordinator.getWriteClient().getConfig().setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY, HoodieFailedWritesCleaningPolicy.LAZY.name());
    assertTrue(coordinator.getWriteClient().getConfig().getFailedWritesCleanPolicy().isLazy());
    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String instant = requestInstantTime(0);
    WriteMetadataEvent event1 = createOperatorEvent(0, 0, instant, "par1", true, false, 0.2);
    WriteMetadataEvent event2 = createOperatorEvent(1, 0, instant, "par2", true, false, 0.3);
    coordinator.handleEventFromOperator(0, event1);
    coordinator.handleEventFromOperator(1, event2);
    assertThat("Recommits the instant with lazy failed writes clean policy", TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath()), is(instant));
  }

  @Test
  public void testHiveSyncInvoked() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.HIVE_SYNC_ENABLED, true);
    coordinator = createCoordinator(conf, 1);

    String instant = mockWriteWithMetadata(0);
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
    int metadataCompactionDeltaCommits = 5;
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS, metadataCompactionDeltaCommits);
    coordinator = createCoordinator(conf, 1);

    String instant = coordinator.getInstant();
    assertEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), metadataTableBasePath);
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    HoodieTableMetaClient dataTableMetaClient =
        HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), new Path(metadataTableBasePath).getParent().getParent().toString());
    int metadataPartitions = dataTableMetaClient.getTableConfig().getMetadataPartitions().size();
    assertThat("Instants needed to sync to metadata table do not match", completedTimeline.countInstants(), is(metadataPartitions));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), startsWith(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // test metadata table compaction
    // write few more commits until compaction
    int numCommits;
    int ckp = 0;
    for (numCommits = metadataPartitions; numCommits < metadataCompactionDeltaCommits; numCommits++) {
      instant = mockWriteWithMetadata(ckp++);
      metadataTableMetaClient.reloadActiveTimeline();
      completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
      assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(numCommits + 1));
      assertThat(completedTimeline.lastInstant().get().requestedTime(), is(instant));
    }
    // the 5th commit triggers the compaction
    mockWriteWithMetadata(ckp++);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(numCommits + 2));
    assertThat(completedTimeline.nthFromLastInstant(0).get().getAction(), is(HoodieTimeline.COMMIT_ACTION));
    // write another 2 commits
    for (int i = 7; i < 8; i++) {
      instant = mockWriteWithMetadata(ckp++);
      metadataTableMetaClient.reloadActiveTimeline();
      completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
      assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(i + 1));
      assertThat(completedTimeline.lastInstant().get().requestedTime(), is(instant));
    }

    // write another commit to trigger clean
    instant = mockWriteWithMetadata(ckp++);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(9));

    // write three more commits
    mockWriteWithMetadata(ckp++);
    mockWriteWithMetadata(ckp++);
    mockWriteWithMetadata(ckp++);
    // write another commit to trigger compaction
    mockWriteWithMetadata(ckp++);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(14));
    HoodieInstant compactionInstant = completedTimeline.nthFromLastInstant(1).get();
    assertThat(compactionInstant.getAction(), is(HoodieTimeline.COMMIT_ACTION));

    // remove the last compaction completed file and write another commit
    TestUtils.deleteInstantFile(metadataTableMetaClient, compactionInstant);
    assertTrue(metadataTableMetaClient.reloadActiveTimeline().filterPendingCompactionTimeline().containsInstant(compactionInstant.requestedTime()));
    mockWriteWithMetadata(ckp++);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("The pending compaction should be recommitted",
        completedTimeline.nthFromLastInstant(3).get(), is(compactionInstant));
  }

  @Test
  void testSyncMetadataTableWithLogCompaction() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS, 20);
    conf.setString("hoodie.metadata.log.compaction.enable", "true");
    coordinator = createCoordinator(conf, 1);

    String instant = coordinator.getInstant();
    assertEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), metadataTableBasePath);
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    HoodieTableMetaClient dataTableMetaClient =
        HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), new Path(metadataTableBasePath).getParent().getParent().toString());
    int metadataPartitions = dataTableMetaClient.getTableConfig().getMetadataPartitions().size();
    assertThat("Instants needed to sync to metadata table do not match", completedTimeline.countInstants(), is(metadataPartitions));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), startsWith(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    // test metadata table log compaction
    // already 1 commit is used to initialized FILES partition in MDT
    // write another 4 commits
    int numCommits;
    int ckp = 0;
    for (numCommits = metadataPartitions; numCommits < metadataPartitions + 4; numCommits++) {
      instant = mockWriteWithMetadata(ckp++);
      metadataTableMetaClient.reloadActiveTimeline();
      completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
      assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(numCommits + 1));
      assertThat(completedTimeline.lastInstant().get().requestedTime(), is(instant));
    }
    // the 5th commit triggers the log compaction
    mockWriteWithMetadata(ckp++);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(numCommits + 2));
    assertThat("The log compaction instant time should be new generated",
        completedTimeline.nthFromLastInstant(1).get().requestedTime(), not(instant));
    // log compaction is another delta commit
    assertThat(completedTimeline.nthFromLastInstant(1).get().getAction(), is(HoodieTimeline.DELTA_COMMIT_ACTION));
  }

  @Test
  void testSyncMetadataTableWithRollback() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    coordinator = createCoordinator(conf, 1);

    String instant = coordinator.getInstant();
    assertEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), metadataTableBasePath);
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    HoodieTableMetaClient dataTableMetaClient =
        HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), new Path(metadataTableBasePath).getParent().getParent().toString());
    int metadataPartitions = dataTableMetaClient.getTableConfig().getMetadataPartitions().size();
    assertThat("Instants needed to sync to metadata table do not match", completedTimeline.countInstants(), is(metadataPartitions));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), startsWith(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    int ckp = 0;
    // writes a normal commit
    mockWriteWithMetadata(ckp++);
    instant = requestInstantTime(ckp++);
    // creates an inflight commit on the metadata timeline
    metadataTableMetaClient.getActiveTimeline()
        .createNewInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieActiveTimeline.DELTA_COMMIT_ACTION, instant));
    metadataTableMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instant);
    metadataTableMetaClient.reloadActiveTimeline();
    // reset the coordinator to mimic the job failover.
    coordinator = createCoordinator(conf, 1);

    // write another commit with new instant on the metadata timeline
    instant = mockWriteWithMetadata(ckp);
    metadataTableMetaClient.reloadActiveTimeline();

    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(metadataPartitions + 3));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), is(instant));
    assertThat("The pending instant should be rolled back first",
        completedTimeline.nthFromLastInstant(1).get().getAction(), is(HoodieTimeline.ROLLBACK_ACTION));
  }

  @Test
  public void testEndInputIsTheLastEvent() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    Logger logger = Mockito.mock(Logger.class); // avoid too many logs by executor
    NonThrownExecutor executor = NonThrownExecutor.builder(logger).waitForTasksFinish(true).build();

    try (StreamWriteOperatorCoordinator coordinator = createCoordinator(conf, 1)) {
      coordinator.start();
      coordinator.setExecutor(executor);
      TimeUnit.SECONDS.sleep(5); // wait for handled bootstrap event

      int eventCount = 20_000; // big enough to fill executor's queue
      for (int i = 0; i < eventCount; i++) {
        coordinator.handleEventFromOperator(0, createOperatorEvent(0, 0, coordinator.getInstant(), "par1", false, true, 0.1));
      }

      WriteMetadataEvent endInput = WriteMetadataEvent.builder()
          .taskID(0)
          .checkpointId(0)
          .instantTime(coordinator.getInstant())
          .writeStatus(Collections.emptyList())
          .endInput(true)
          .build();
      coordinator.handleEventFromOperator(0, endInput);

      // wait for submitted events completed
      executor.close();

      // there should be no events after endInput
      assertNull(coordinator.getEventBuffer());
    }
  }

  @Test
  void testLockForMetadataTable() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);

    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.setString("hoodie.write.lock.client.num_retries", "1");

    coordinator = createCoordinator(conf, 1);

    String instant = coordinator.getInstant();
    assertEquals("", instant);

    final String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(tempFile.getAbsolutePath());
    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), metadataTableBasePath);
    HoodieTimeline completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    HoodieTableMetaClient dataTableMetaClient =
        HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf)), new Path(metadataTableBasePath).getParent().getParent().toString());
    int metadataPartitions = dataTableMetaClient.getTableConfig().getMetadataPartitions().size();
    assertThat("Instants needed to sync to metadata table do not match", completedTimeline.countInstants(), is(metadataPartitions));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), startsWith(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP));

    instant = mockWriteWithMetadata(0);
    metadataTableMetaClient.reloadActiveTimeline();
    completedTimeline = metadataTableMetaClient.getActiveTimeline().filterCompletedInstants();
    assertThat("One instant need to sync to metadata table", completedTimeline.countInstants(), is(metadataPartitions + 1));
    assertThat(completedTimeline.lastInstant().get().requestedTime(), is(instant));
  }

  @Test
  public void testCommitOnEmptyBatch() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), "true");
    try (StreamWriteOperatorCoordinator coordinator = createCoordinator(conf, 2)) {
      // Coordinator start the instant
      String instant = requestInstantTime(coordinator, -1);

      OperatorEvent event1 = WriteMetadataEvent.builder()
          .taskID(0)
          .instantTime(instant)
          .writeStatus(Collections.emptyList())
          .lastBatch(true)
          .build();
      OperatorEvent event2 = WriteMetadataEvent.builder()
          .taskID(1)
          .instantTime(instant)
          .writeStatus(Collections.emptyList())
          .lastBatch(true)
          .build();
      coordinator.handleEventFromOperator(0, event1);
      coordinator.handleEventFromOperator(1, event2);

      assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1),
          "Commit the instant");
      String lastCompleted = TestUtils.getLastCompleteInstant(tempFile.getAbsolutePath());
      assertThat("Commits the instant with empty batch anyway", lastCompleted, is(instant));
      assertNull(coordinator.getEventBuffer());
    }
  }

  @Test
  void testHandleInFlightInstantsRequest() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    coordinator = createCoordinator(conf, 2);

    // Request an instant time to create an initial instant
    String instant1 = requestInstantTime(1);
    assertThat(instant1, not(is("")));

    // Add some events to the buffer to simulate ongoing processing
    OperatorEvent event1 = createOperatorEvent(0, 1, instant1, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event1);

    // Request another instant time to create a second instant
    String instant2 = requestInstantTime(2);
    assertThat(instant2, not(is("")));

    // Add more events for the second instant
    OperatorEvent event2 = createOperatorEvent(1, 2, instant2, "par2", false, false, 0.2);
    coordinator.handleEventFromOperator(1, event2);

    // Call handleCoordinationRequest with InflightInstantsRequest
    CompletableFuture<CoordinationResponse> responseFuture =
        coordinator.handleCoordinationRequest(Correspondent.InflightInstantsRequest.getInstance());

    // Unwrap and verify the response
    Correspondent.InflightInstantsResponse response =
        CoordinationResponseSerDe.unwrap(responseFuture.get());

    // Check that the response contains the expected checkpoint IDs and instant times
    Map<Long, String> inflightInstants = response.getInflightInstants();
    assertEquals(2, inflightInstants.size());
    assertTrue(inflightInstants.containsKey(1L));
    assertTrue(inflightInstants.containsKey(2L));
    assertEquals(instant1, inflightInstants.get(1L));
    assertEquals(instant2, inflightInstants.get(2L));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private String requestInstantTime(long checkpointId) {
    return requestInstantTime(this.coordinator, checkpointId);
  }

  private String requestInstantTime(StreamWriteOperatorCoordinator coordinator, long checkpointId) {
    try {
      Correspondent.InstantTimeResponse response = CoordinationResponseSerDe.unwrap(coordinator.handleCoordinationRequest(Correspondent.InstantTimeRequest.getInstance(checkpointId)).get());
      return response.getInstant();
    } catch (Exception e) {
      throw new HoodieException("Error requesting the instant time from the coordinator", e);
    }
  }

  private static StreamWriteOperatorCoordinator createCoordinator(Configuration conf, int subTasks) throws Exception {
    MockOperatorCoordinatorContext coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), subTasks);
    StreamWriteOperatorCoordinator coordinator = new StreamWriteOperatorCoordinator(conf, coordinatorContext);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    coordinator.setInstantRequestExecutor(new MockCoordinatorExecutor(coordinatorContext));
    return coordinator;
  }

  private String mockWriteWithMetadata(long checkpointId) {
    String instant = requestInstantTime(checkpointId);
    OperatorEvent event = createOperatorEvent(0, checkpointId, instant, "par1", false, true, 0.1);

    coordinator.handleEventFromOperator(0, event);
    // uses the next checkpoint id because the checkpoint id used in events come from the last round.
    coordinator.notifyCheckpointComplete(checkpointId + 1);
    return instant;
  }

  private static WriteMetadataEvent createBootstrapEvent(int taskId, long checkpointId, String instant, String partitionPath) {
    return createOperatorEvent(taskId, checkpointId, instant, partitionPath, true, false, 0.1);
  }

  private static WriteMetadataEvent createOperatorEvent(
      int taskId,
      String instant,
      String partitionPath,
      boolean trackSuccessRecords,
      double failureFraction) {
    return createOperatorEvent(taskId, -1, instant, partitionPath, false, trackSuccessRecords, failureFraction);
  }

  private static WriteMetadataEvent createOperatorEvent(
      int taskId,
      long checkpointId,
      String instant,
      String partitionPath,
      boolean isBootstrap,
      boolean trackSuccessRecords,
      double failureFraction) {
    return createOperatorEvent(taskId, checkpointId, instant, partitionPath, isBootstrap, trackSuccessRecords, failureFraction, false);
  }

  private static WriteMetadataEvent createOperatorEvent(
      int taskId,
      long checkpointId,
      String instant,
      String partitionPath,
      boolean isBootstrap,
      boolean trackSuccessRecords,
      double failureFraction,
      boolean isMetadataTable) {
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
        .checkpointId(checkpointId)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus))
        .bootstrap(isBootstrap)
        .lastBatch(true)
        .metadataTable(isMetadataTable)
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

  // -------------------------------------------------------------------------
  //  Kafka Offset Tests
  // -------------------------------------------------------------------------

  @Test
  public void testStringFyMethod() throws Exception {
    String topic = "test-topic";
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 100L);
    offsetMap.put(1, 200L);
    offsetMap.put(2, 300L);

    String result = StreamerUtil.stringFy(topic, "test-cluster", offsetMap);

    assertEquals("kafka_metadata%3Atest-topic%3A0:100;kafka_metadata%3Atest-topic%3A1:200;"
        + "kafka_metadata%3Atest-topic%3A2:300;kafka_metadata%3Akafka_cluster%3Atest-topic%3A:test-cluster", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointException() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenThrow(new IOException("Network error"));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);

    // Fail open: returns cluster metadata even on exception
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointNullKafkaOffsetsInfo() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            String.valueOf(System.currentTimeMillis()),
            null,
            "20231201120000",
            "14458"
        );

    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.of(offsetInfo));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);

    // Fail open: returns cluster metadata when kafkaOffsetsInfo is null
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointMultipleTopics() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets1 =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(
            Collections.singletonMap(0, 100L));
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo topic1 =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo("topic1", "test-cluster", offsets1);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets2 =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(
            Collections.singletonMap(0, 200L));
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo topic2 =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo("topic2", "test-cluster", offsets2);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            String.valueOf(System.currentTimeMillis()),
            Arrays.asList(topic1, topic2),
            "20231201120000",
            "14458"
        );

    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.of(offsetInfo));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    // Multiple topics throws IllegalStateException, caught by fail-open
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointWithoutKafkaTopicName() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca1");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "test-kafka-cluster");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");
    conf.set(FlinkOptions.TOPIC_ID, "test-job_production_dca1_test-owner_kafka_hoodie_10_test-feed");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);

    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 1000L);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(offsetMap);

    String expectedTopicId = "test-job_production_dca1_test-owner_kafka_hoodie_10_test-feed";
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetsInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo(expectedTopicId, "test-cluster", offsets);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            String.valueOf(System.currentTimeMillis()),
            Collections.singletonList(kafkaOffsetsInfo),
            "20231201120000",
            "14458"
        );

    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.of(offsetInfo));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);

    assertNotNull(result);
    assertEquals("kafka_metadata%3Atest-topic%3A0:1000;kafka_metadata%3Akafka_cluster%3Atest-topic%3A:test-kafka-cluster", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointMissingConfig() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    // Missing DC - only set some configs
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    // Missing DC: returns cluster metadata (fail open)
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, null);
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testStringFyWithEmptyOffsets() {
    String result = StreamerUtil.stringFy("test-topic", "test-cluster", new HashMap<>());
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:test-cluster", result);
  }

  @Test
  public void testStringFyWithNullOffsetMap() {
    String result = StreamerUtil.stringFy("test-topic", "test-cluster", null);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:test-cluster", result);
  }

  @Test
  public void testStringFyWithSinglePartition() {
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 12345L);

    String result = StreamerUtil.stringFy("single-partition-topic", "test-cluster", offsetMap);
    assertEquals("kafka_metadata%3Asingle-partition-topic%3A0:12345;"
        + "kafka_metadata%3Akafka_cluster%3Asingle-partition-topic%3A:test-cluster", result);
  }

  @Test
  public void testStringFyWithUnorderedPartitions() {
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(2, 300L);
    offsetMap.put(0, 100L);
    offsetMap.put(3, 400L);
    offsetMap.put(1, 200L);

    String result = StreamerUtil.stringFy("unordered-topic", "test-cluster", offsetMap);
    assertEquals("kafka_metadata%3Aunordered-topic%3A0:100;kafka_metadata%3Aunordered-topic%3A1:200;"
        + "kafka_metadata%3Aunordered-topic%3A2:300;kafka_metadata%3Aunordered-topic%3A3:400;"
        + "kafka_metadata%3Akafka_cluster%3Aunordered-topic%3A:test-cluster", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointEmptyOffsets() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.empty());

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);

    // Fail open: returns cluster metadata when no offsets found
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointNegativeCheckpointId() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    // Negative checkpoint ID: returns cluster metadata (fail open)
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, -1L, null);
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);

    result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, -100L, null);
    assertNotNull(result);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test", result);
  }

  @Test
  public void testStringFyWithNullOrEmptyTopic() {
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 100L);
    offsetMap.put(1, 200L);

    assertEquals("", StreamerUtil.stringFy(null, "test-cluster", offsetMap));
    assertEquals("", StreamerUtil.stringFy("", "test-cluster", offsetMap));
  }

  @Test
  public void testStringFyWithNullCluster() {
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 100L);
    offsetMap.put(1, 200L);

    String result = StreamerUtil.stringFy("test-topic", null, offsetMap);
    assertEquals("kafka_metadata%3Atest-topic%3A0:100;kafka_metadata%3Atest-topic%3A1:200", result);

    result = StreamerUtil.stringFy("test-topic", "", offsetMap);
    assertEquals("kafka_metadata%3Atest-topic%3A0:100;kafka_metadata%3Atest-topic%3A1:200", result);
  }

  @Test
  public void testCollectKafkaOffsetCheckpointWithInvalidPartitionData() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "test-job");
    conf.set(FlinkOptions.HADOOP_USER, "test-user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-test");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "test-topic");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");

    // Negative partition ID
    Map<Integer, Long> offsetMapNegativePartition = new HashMap<>();
    offsetMapNegativePartition.put(-1, 1000L);
    offsetMapNegativePartition.put(0, 2000L);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsetsNegPartition =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(offsetMapNegativePartition);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetsInfoNegPartition =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo("test-topic", "test-cluster", offsetsNegPartition);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfoNegPartition =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            String.valueOf(System.currentTimeMillis()),
            Collections.singletonList(kafkaOffsetsInfoNegPartition),
            "20231201120000",
            "14458"
        );

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.eq(123L),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.of(offsetInfoNegPartition));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, mockClient);
    // Negative partition is still included in offsets (stringFy doesn't filter)
    assertNotNull(result);
    assertTrue(result.contains("kafka_metadata%3Akafka_cluster%3Atest-topic%3A:kafka-test"));
  }

  @Test
  public void testCollectKafkaOffsetCheckpointFormat() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dca");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow_1");
    conf.set(FlinkOptions.HADOOP_USER, "hoover");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "kafka-ingestion-dca");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "kafka-ingestion-dca");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena-test");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "test-service");
    conf.set(FlinkOptions.SERVICE_TIER, "DEFAULT");
    conf.set(FlinkOptions.SERVICE_NAME, "ingestion-rt");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME,
        "production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow");
    conf.set(FlinkOptions.TOPIC_ID,
        "production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow_1_"
        + "production_dca1_hadoop_platform_self_serve_5_kafka_hoodie_streaming_"
        + "hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow");

    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);

    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 1000L);
    offsetMap.put(1, 2000L);
    offsetMap.put(2, 3000L);

    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(offsetMap);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetsInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo(
            "production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow_1_"
            + "production_dca1_hadoop_platform_self_serve_5_kafka_hoodie_streaming_"
            + "hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow",
            "test-cluster",
            offsets);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            String.valueOf(System.currentTimeMillis()),
            Collections.singletonList(kafkaOffsetsInfo),
            "20231201120000",
            "14458"
        );

    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Option.of(offsetInfo));

    FlinkCheckpointClient mockClient = new FlinkCheckpointClient(mockGateway);
    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 14458L, mockClient);

    assertNotNull(result);
    assertTrue(result.contains("kafka_metadata%3A"));
    assertTrue(result.contains("production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow"));
    assertTrue(result.contains("kafka_metadata%3Akafka_cluster%3A"));
    assertTrue(result.contains("kafka-ingestion-dca"));
  }
}
