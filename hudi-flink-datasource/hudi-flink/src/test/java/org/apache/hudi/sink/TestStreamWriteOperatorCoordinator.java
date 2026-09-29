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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.CoordinationResponseSerDe;
import org.apache.hudi.sink.utils.MockCoordinatorExecutor;
import org.apache.hudi.sink.utils.MockCorrespondent;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
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
    coordinator = startCoordinator(TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), 2);
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

  @ParameterizedTest
  @ValueSource(strings = {"upsert", "insert", "bulk_insert", "insert_overwrite", "insert_overwrite_table", "delete"})
  void testValidationOncePerInstant(String operation) throws Exception {
    coordinator.close();
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, operation);
    coordinator = startCoordinator(conf, 2);
    HoodieFlinkWriteClient writeClient = Mockito.spy(coordinator.getWriteClient());
    Field writeClientField = StreamWriteOperatorCoordinator.class.getDeclaredField("writeClient");
    writeClientField.setAccessible(true);
    writeClientField.set(coordinator, writeClient);

    String firstInstant = requestInstantTime(1);
    assertEquals(firstInstant, requestInstantTime(1));
    Mockito.verify(writeClient, Mockito.times(1)).preTxn(Mockito.eq(WriteOperationType.fromValue(operation)), Mockito.any());
    Mockito.verify(writeClient, Mockito.times(1)).validateAgainstTableProperties(Mockito.any(), Mockito.any());

    // Complete the first checkpoint so the next request can create a new instant.
    coordinator.handleEventFromOperator(0, createOperatorEvent(0, 1, firstInstant, "par1", false, true, 0.1));
    coordinator.handleEventFromOperator(1, createOperatorEvent(1, 1, firstInstant, "par2", false, true, 0.2));
    coordinator.notifyCheckpointComplete(2);
    assertNull(coordinator.getEventBuffer(1));
    String nextInstant = requestInstantTime(2);
    assertNotEquals(firstInstant, nextInstant);
    Mockito.verify(writeClient, Mockito.times(2)).preTxn(Mockito.eq(WriteOperationType.fromValue(operation)), Mockito.any());
    Mockito.verify(writeClient, Mockito.times(2)).validateAgainstTableProperties(Mockito.any(), Mockito.any());
    assertFalse(((MockOperatorCoordinatorContext) coordinator.getContext()).isJobFailed());
  }

  @ParameterizedTest
  @ValueSource(strings = {"upsert", "insert", "bulk_insert", "insert_overwrite", "insert_overwrite_table", "delete"})
  void testColumnDropFailsBeforeInstantIsPublished(String operation) throws Exception {
    coordinator.close();
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, operation);
    coordinator = startCoordinator(conf, 2);
    HoodieWriteConfig writeConfig = coordinator.getWriteClient().getConfig();
    assertFalse(writeConfig.shouldValidateAvroSchema());
    assertFalse(writeConfig.shouldAllowAutoEvolutionColumnDrop());

    // Complete an instant using the original schema, then drop a column for the next instant.
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, writeConfig.getSchema());
    HoodieTestTable.of(StreamerUtil.createMetaClient(conf)).addCommit("001", Option.of(metadata));
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH,
        getClass().getClassLoader().getResource("test_read_schema_dropped_age.avsc").toString());
    writeConfig.setSchema(StreamerUtil.getSourceSchema(conf).toString());

    assertInstantCreationFails(conf, MissingSchemaFieldException.class, "age");
  }

  @Test
  void testTablePropertyChangesAreValidatedBeforeInstantIsPublished() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    Properties updatedProperties = new Properties();
    updatedProperties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), updatedProperties);

    assertInstantCreationFails(conf, HoodieException.class, HoodieTableConfig.POPULATE_META_FIELDS.key());
  }

  private void assertInstantCreationFails(Configuration conf, Class<? extends Throwable> causeType, String message) throws Exception {
    Correspondent.InstantTimeResponse response = CoordinationResponseSerDe.unwrap(
        coordinator.handleCoordinationRequest(Correspondent.InstantTimeRequest.getInstance(1))
            .get(1, TimeUnit.SECONDS));
    assertNull(response.getInstant());
    assertNull(coordinator.getEventBuffer(1));
    MockOperatorCoordinatorContext context = (MockOperatorCoordinatorContext) coordinator.getContext();
    assertTrue(context.isJobFailed());
    Throwable cause = context.getJobFailureReason();
    while (cause != null && !(causeType.isInstance(cause) && cause.getMessage() != null && cause.getMessage().contains(message))) {
      cause = cause.getCause();
    }
    assertNotNull(cause, "Expected " + causeType.getSimpleName() + " containing: " + message);
    HoodieTimeline pending = StreamerUtil.createMetaClient(conf).reloadActiveTimeline().filterPendingExcludingCompaction();
    assertTrue(pending.empty(), "Validation must fail before a new instant is created");
  }

  @Test
  public void testTableInitialized() throws IOException {
    final org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(new Configuration());
    String basePath = tempFile.getAbsolutePath();
    try (FileSystem fs = HadoopFSUtils.getFs(basePath, hadoopConf)) {
      assertTrue(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)));
    }
  }

  /**
   * Verifies both coordinator restore paths. In case 1, a newly constructed coordinator receives
   * checkpoint data before {@link StreamWriteOperatorCoordinator#start()}, so it must deserialize
   * the event buffers for restoration during start. In case 2, global failover resets an already
   * started coordinator, so it recommits its live event buffers directly.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCheckpointAndRestore(boolean restartCoordinator) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    requestInstantTime(-1);
    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    // Writer checkpoint 1 finishes after the coordinator has taken its snapshot.
    coordinator.checkpointCoordinator(1, new CompletableFuture<>());
    OperatorEvent event0 = createOperatorEvent(0, instant, "par1", true, 0.1);
    OperatorEvent event1 = createOperatorEvent(1, instant, "par2", true, 0.2);
    coordinator.handleEventFromOperator(0, event0);
    coordinator.handleEventFromOperator(1, event1);

    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(2, future);

    if (restartCoordinator) {
      coordinator.close();
      coordinator = createCoordinator(conf, 4);
      coordinator.resetToCheckpoint(2, future.get());
      WriteMetadataEvent[] eventBuffer = coordinator.getEventBuffer(-1);
      assertEquals(2, eventBuffer.length);
      // Exercise recommit during start, not only checkpoint deserialization.
      coordinator.start();
    } else {
      // Global failover recommits the live buffers of the already started coordinator.
      coordinator.resetToCheckpoint(2, future.get());
    }

    assertNull(coordinator.getEventBuffer(-1));
    assertNull(((MockOperatorCoordinatorContext) coordinator.getContext()).getJobFailureReason());
    assertTrue(StreamerUtil.createMetaClient(conf).reloadActiveTimeline()
        .filterCompletedInstants().containsInstant(instant));
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
  void testDeferredRecommitAfterScaleUp() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    String restoredInstant = restoreFirstCheckpointAfterScaleUp(conf);
    String nextInstant = requestInstantTime(1);
    assertNotEquals(restoredInstant, nextInstant);
    assertEquals(nextInstant, coordinator.getInstant());
    coordinator.checkpointCoordinator(2, new CompletableFuture<>());
    sendCheckpointEvents(1, nextInstant, 4);

    coordinator.notifyCheckpointComplete(2);

    HoodieTimeline completed = StreamerUtil.createMetaClient(conf)
        .reloadActiveTimeline().filterCompletedInstants();
    assertTrue(completed.containsInstant(restoredInstant));
    assertTrue(completed.containsInstant(nextInstant));
    assertEquals(2, completed.readCommitMetadata(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, restoredInstant))
        .getPartitionToWriteStats().size(), "Both original writers must be included in the restored commit");
    assertEquals(4, completed.readCommitMetadata(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, nextInstant))
        .getPartitionToWriteStats().size(), "All scaled-up writers must be included in the next commit");
    assertNull(coordinator.getEventBuffer(-1));
    assertNull(coordinator.getEventBuffer(1));
    assertNull(((MockOperatorCoordinatorContext) coordinator.getContext()).getJobFailureReason());
  }

  @Disabled("https://github.com/apache/hudi/issues/19922: deferred bootstrap metadata is omitted from the next checkpoint")
  @Test
  void testDeferredRecommitSurvivesAnotherRestart() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    String restoredInstant = restoreFirstCheckpointAfterScaleUp(conf);
    String nextInstant = requestInstantTime(1);
    CompletableFuture<byte[]> secondCheckpoint = new CompletableFuture<>();
    coordinator.checkpointCoordinator(2, secondCheckpoint);
    sendCheckpointEvents(1, nextInstant, 4);
    coordinator.close();

    // Checkpoint 2 succeeded, but the job stopped before notifyCheckpointComplete could commit it.
    // The writers now restore only checkpoint 2's batch; the coordinator must preserve the older batch.
    coordinator = createCoordinator(conf, 4);
    coordinator.resetToCheckpoint(2, secondCheckpoint.get());
    coordinator.start();
    setSynchronousExecutors(coordinator);
    for (int task = 0; task < 4; task++) {
      coordinator.handleEventFromOperator(task, createBootstrapEvent(task, 1, nextInstant, "new" + task));
    }
    coordinator.notifyCheckpointComplete(3);

    HoodieTimeline completed = StreamerUtil.createMetaClient(conf).reloadActiveTimeline().filterCompletedInstants();
    assertTrue(completed.containsInstant(nextInstant));
    assertTrue(completed.containsInstant(restoredInstant), "The first batch must survive a second restart before its deferred commit");
  }

  private String restoreFirstCheckpointAfterScaleUp(Configuration conf) throws Exception {
    resetToMergeOnRead(conf);
    String restoredInstant = requestInstantTime(-1);
    CompletableFuture<byte[]> firstCheckpoint = new CompletableFuture<>();
    // The first coordinator snapshot precedes the writer snapshots, so only writer state has this batch.
    coordinator.checkpointCoordinator(1, firstCheckpoint);
    coordinator.close();

    coordinator = createCoordinator(conf, 4);
    coordinator.resetToCheckpoint(1, firstCheckpoint.get());
    coordinator.start();
    setSynchronousExecutors(coordinator);
    coordinator.handleEventFromOperator(0, createBootstrapEvent(0, -1, restoredInstant, "par1"));
    coordinator.handleEventFromOperator(1, createBootstrapEvent(1, -1, restoredInstant, "par2"));
    WriteMetadataEvent[] buffer = coordinator.getEventBuffer(-1);
    assertFalse(java.util.Arrays.stream(buffer).allMatch(event -> event != null && event.isBootstrap()));
    assertEquals("", coordinator.getInstant());
    assertFalse(StreamerUtil.createMetaClient(conf).reloadActiveTimeline().filterCompletedInstants().containsInstant(restoredInstant));
    return restoredInstant;
  }

  private void sendCheckpointEvents(long checkpointId, String instant, int parallelism) {
    for (int task = 0; task < parallelism; task++) {
      coordinator.handleEventFromOperator(task,
          createOperatorEvent(task, checkpointId, instant, "new" + task, false, true, 0.1));
    }
    assertNull(((MockOperatorCoordinatorContext) coordinator.getContext()).getJobFailureReason());
  }

  @Test
  public void testEventReset() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    resetToMergeOnRead(conf);
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
    assertNotNull(coordinator.getEventBuffer()[0], "Events should not be cleared by subTask failure");

    OperatorEvent event2 = createOperatorEvent(0, 0, instant, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event2);
    coordinator.subtaskFailed(0, null);
    assertNotNull(coordinator.getEventBuffer()[0], "Events should not be cleared by subTask failure");

    OperatorEvent event3 = createOperatorEvent(0, 0, instant, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event3);
    assertThat("Multiple events of same instant should be merged",
        coordinator.getEventBuffer()[0].getWriteStatuses().size(), is(1));

    long nextCkpId = 1;
    String instant2 = requestInstantTime(nextCkpId);
    OperatorEvent event4 = createOperatorEvent(0, nextCkpId, instant2, "par1", false, false, 0.1);
    coordinator.handleEventFromOperator(0, event4);
    assertThat("First instant is not committed yet, new event should not override the old event",
        coordinator.getEventBuffer(0)[0].getWriteStatuses().size(), is(1));
    assertThat("Second instant should have one newly added write status",
        coordinator.getEventBuffer(1)[0].getWriteStatuses().size(), is(1));
  }

  @Test
  public void testCheckpointCompleteWithPartialEvents() throws Exception {
    // reset
    reset();
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), "false");

    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 2);
    coordinator = startCoordinator(conf, 2);

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
    coordinator = startCoordinator(conf, 1);

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
    coordinator = startCoordinator(conf, 1);

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
    coordinator = startCoordinator(conf, 1);

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
    coordinator = startCoordinator(conf, 1);

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
    coordinator = startCoordinator(conf, 1);

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
    coordinator = startCoordinator(conf, 1);

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

    try (StreamWriteOperatorCoordinator coordinator = startCoordinator(conf, 1)) {
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

    coordinator = startCoordinator(conf, 1);

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
    try (StreamWriteOperatorCoordinator coordinator = startCoordinator(conf, 2)) {
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
  void testInstantRequestPollsWhileCreationBlockedThenSucceeds() throws Exception {
    MockOperatorCoordinatorContext ctx = (MockOperatorCoordinatorContext) coordinator.getContext();
    CountDownLatch lockHeld = new CountDownLatch(1);
    NonThrownExecutor gatedWorker = Mockito.spy(new GatedInstantRequestExecutor(
        Mockito.mock(Logger.class),
        (errMsg, t) -> ctx.failJob(new HoodieException(errMsg, t)),
        lockHeld));
    coordinator.setInstantRequestExecutor(gatedWorker);

    try {
      // The first request starts creation; subsequent requests must not queue more work while it is blocked.
      for (long checkpointId : new long[] {1L, 1L, 2L}) {
        Correspondent.InstantTimeResponse pending = CoordinationResponseSerDe.unwrap(
            coordinator.handleCoordinationRequest(Correspondent.InstantTimeRequest.getInstance(checkpointId))
                .get(1, TimeUnit.SECONDS));
        assertNull(pending.getInstant());
      }
      assertTrue(gatedWorker.hasRunningTasks());
      Mockito.verify(gatedWorker, Mockito.times(1)).execute(Mockito.any(), Mockito.eq("request instant time"));
    } finally {
      lockHeld.countDown();
    }

    String instant = requestInstantTime(1L);
    assertNotNull(instant);
    assertEquals(instant, requestInstantTime(1L), "Repeated requests must reuse the instant");
    assertFalse(ctx.isJobFailed());
    HoodieTimeline inflights = StreamerUtil.createMetaClient(TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()))
        .reloadActiveTimeline().filterInflights();
    assertEquals(1, inflights.countInstants());
    assertTrue(inflights.containsInstant(instant));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private String requestInstantTime(long checkpointId) {
    return requestInstantTime(this.coordinator, checkpointId);
  }

  private String requestInstantTime(StreamWriteOperatorCoordinator coordinator, long checkpointId) {
    return new MockCorrespondent(coordinator)
        .requestInstantTime(checkpointId, TimeUnit.SECONDS.toMillis(10));
  }

  private void resetToMergeOnRead(Configuration conf) throws Exception {
    coordinator.close();
    reset();
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    coordinator = startCoordinator(conf, 2);
  }

  private static StreamWriteOperatorCoordinator startCoordinator(Configuration conf, int subTasks) throws Exception {
    StreamWriteOperatorCoordinator coordinator = createCoordinator(conf, subTasks);
    coordinator.start();
    setSynchronousExecutors(coordinator);
    return coordinator;
  }

  private static void setSynchronousExecutors(StreamWriteOperatorCoordinator coordinator) throws Exception {
    MockOperatorCoordinatorContext coordinatorContext = (MockOperatorCoordinatorContext) coordinator.getContext();
    coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    coordinator.setInstantRequestExecutor(new MockCoordinatorExecutor(coordinatorContext));
  }

  private static StreamWriteOperatorCoordinator createCoordinator(Configuration conf, int subTasks) {
    MockOperatorCoordinatorContext coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), subTasks);
    return new StreamWriteOperatorCoordinator(conf, coordinatorContext);
  }

  /**
   * A real single-thread instant-request worker whose submitted creation task blocks on a latch before
   * running, to deterministically delay instant creation (simulating a held table lock).
   */
  private static final class GatedInstantRequestExecutor extends NonThrownExecutor {
    private final CountDownLatch gate;

    private GatedInstantRequestExecutor(Logger logger, ExceptionHook exceptionHook, CountDownLatch gate) {
      super(logger, null, exceptionHook, true);
      this.gate = gate;
    }

    @Override
    public void execute(ThrowingRunnable<Throwable> action, String actionName, Object... actionParams) {
      super.execute(() -> {
        gate.await();
        action.run();
      }, actionName, actionParams);
    }
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
    final WriteStatus writeStatus = new WriteStatus(trackSuccessRecords, failureFraction);
    writeStatus.setPartitionPath(partitionPath);

    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId("fileId123");
    writeStat.setPath(partitionPath + "/" + FSUtils.makeBaseFileName(instant, "1-0-1", "fileId123", ".parquet"));
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
