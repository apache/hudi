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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.sink.utils.MockCoordinatorExecutor;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

  @BeforeEach
  public void before() throws Exception {
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 2);
    coordinator = new StreamWriteOperatorCoordinator(
        TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));
  }

  @AfterEach
  public void after() throws Exception {
    coordinator.close();
  }

  @Test
  void testInstantState() {
    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    WriteStatus writeStatus = new WriteStatus(true, 0.1D);
    writeStatus.setPartitionPath("par1");
    writeStatus.setStat(new HoodieWriteStat());
    OperatorEvent event0 = BatchWriteSuccessEvent.builder()
        .taskID(0)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus))
        .isLastBatch(true)
        .build();

    WriteStatus writeStatus1 = new WriteStatus(false, 0.2D);
    writeStatus1.setPartitionPath("par2");
    writeStatus1.setStat(new HoodieWriteStat());
    OperatorEvent event1 = BatchWriteSuccessEvent.builder()
        .taskID(1)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus1))
        .isLastBatch(true)
        .build();
    coordinator.handleEventFromOperator(0, event0);
    coordinator.handleEventFromOperator(1, event1);

    coordinator.notifyCheckpointComplete(1);
    String inflight = coordinator.getWriteClient().getLastPendingInstant(HoodieTableType.COPY_ON_WRITE);
    String lastCompleted = coordinator.getWriteClient().getLastCompletedInstant(HoodieTableType.COPY_ON_WRITE);
    assertThat("Instant should be complete", lastCompleted, is(instant));
    assertNotEquals("", inflight, "Should start a new instant");
    assertNotEquals(instant, inflight, "Should start a new instant");
  }

  @Test
  public void testTableInitialized() throws IOException {
    final org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
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
    OperatorEvent event = BatchWriteSuccessEvent.builder()
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
    OperatorEvent event = BatchWriteSuccessEvent.builder()
        .taskID(0)
        .instantTime(instant)
        .writeStatus(Collections.emptyList())
        .build();
    coordinator.handleEventFromOperator(0, event);

    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1),
        "Returns early for empty write results");
    String lastCompleted = coordinator.getWriteClient().getLastCompletedInstant(HoodieTableType.COPY_ON_WRITE);
    assertNull(lastCompleted, "Returns early for empty write results");
    assertNull(coordinator.getEventBuffer()[0]);

    WriteStatus writeStatus1 = new WriteStatus(false, 0.2D);
    writeStatus1.setPartitionPath("par2");
    writeStatus1.setStat(new HoodieWriteStat());
    OperatorEvent event1 = BatchWriteSuccessEvent.builder()
        .taskID(1)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus1))
        .isLastBatch(true)
        .build();
    coordinator.handleEventFromOperator(1, event1);
    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(2),
        "Commits the instant with partial events anyway");
    lastCompleted = coordinator.getWriteClient().getLastCompletedInstant(HoodieTableType.COPY_ON_WRITE);
    assertThat("Commits the instant with partial events anyway", lastCompleted, is(instant));
  }

  @Test
  public void testHiveSyncInvoked() throws Exception {
    // override the default configuration
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.HIVE_SYNC_ENABLED, true);
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    coordinator = new StreamWriteOperatorCoordinator(conf, context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));

    String instant = coordinator.getInstant();
    assertNotEquals("", instant);

    WriteStatus writeStatus = new WriteStatus(true, 0.1D);
    writeStatus.setPartitionPath("par1");
    writeStatus.setStat(new HoodieWriteStat());
    OperatorEvent event0 = BatchWriteSuccessEvent.builder()
        .taskID(0)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus))
        .isLastBatch(true)
        .build();

    coordinator.handleEventFromOperator(0, event0);

    // never throw for hive synchronization now
    assertDoesNotThrow(() -> coordinator.notifyCheckpointComplete(1));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void assertError(Runnable runnable, String message) {
    runnable.run();
    // wait a little while for the task to finish
    assertThat(coordinator.getContext(), instanceOf(MockOperatorCoordinatorContext.class));
    MockOperatorCoordinatorContext context = (MockOperatorCoordinatorContext) coordinator.getContext();
    assertTrue(context.isJobFailed(), message);
  }
}
