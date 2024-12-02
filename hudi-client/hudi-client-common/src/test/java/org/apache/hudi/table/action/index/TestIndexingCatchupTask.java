/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.index;

import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestIndexingCatchupTask {

  @Mock
  private HoodieTableMetadataWriter metadataWriter;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieTableMetaClient metadataMetaClient;
  @Mock
  private TransactionManager transactionManager;
  @Mock
  private HoodieEngineContext engineContext;
  @Mock
  private HoodieTable table;
  @Mock
  private HoodieHeartbeatClient heartbeatClient;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Mock out the behavior of the method to mimic a regular successful run
   */
  @Test
  public void testTaskSuccessful() throws IOException {
    List<HoodieInstant> instants = Collections.singletonList(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "001"));
    Set<String> metadataCompletedInstants = new HashSet<>();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/some/path")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .build();
    // Simulate lazy clean policy and heartbeat expired
    when(table.getConfig()).thenReturn(writeConfig);
    when(heartbeatClient.isHeartbeatExpired("002")).thenReturn(false);
    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        instants,
        metadataCompletedInstants,
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext,
        table,
        heartbeatClient);

    task.run();
    assertEquals("001", task.currentCaughtupInstant);
  }

  /**
   * Instant never gets completed, and we interrupt the task to see if it throws the expected HoodieIndexException.
   */
  @Test
  public void testTaskInterrupted() throws IOException {
    HoodieInstant neverCompletedInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "001");
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieActiveTimeline filteredTimeline = mock(HoodieActiveTimeline.class);
    HoodieActiveTimeline furtherFilteredTimeline = mock(HoodieActiveTimeline.class);

    when(metaClient.reloadActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(filteredTimeline);
    when(filteredTimeline.filter(any())).thenReturn(furtherFilteredTimeline);
    AtomicInteger callCount = new AtomicInteger(0);
    when(furtherFilteredTimeline.firstInstant()).thenAnswer(invocation -> {
      if (callCount.incrementAndGet() > 3) {
        throw new InterruptedException("Simulated interruption");
      }
      return Option.empty();
    });

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/some/path")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .build();
    // Simulate heartbeat exists and not expired
    when(table.getConfig()).thenReturn(writeConfig);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/some/path"));
    when(storage.exists(any())).thenReturn(true);
    when(heartbeatClient.isHeartbeatExpired("001")).thenReturn(false);

    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        Collections.singletonList(neverCompletedInstant),
        new HashSet<>(),
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext,
        table,
        heartbeatClient);

    // simulate catchup task timeout
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread(() -> {
      try {
        task.awaitInstantCaughtUp(neverCompletedInstant);
      } catch (HoodieIndexException e) {
        latch.countDown();
      }
    });
    // validate that the task throws the expected exception
    thread.start();
    try {
      latch.await();
    } catch (InterruptedException e) {
      fail("Should have thrown HoodieIndexException and not interrupted exception. This means latch count down was not called.");
    }
  }

  /**
   * Test case to cover heartbeat expiry. Validate that awaitInstantCaughtUp
   * returns null when heartbeat has expired for the given instant.
   */
  @Test
  public void testHeartbeatExpired() throws IOException {
    HoodieInstant expiredInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "002");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/some/path")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .build();
    // Simulate heartbeat exists and expired
    when(table.getConfig()).thenReturn(writeConfig);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/some/path"));
    when(storage.exists(any())).thenReturn(true);
    when(heartbeatClient.isHeartbeatExpired("002")).thenReturn(true);

    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        Collections.singletonList(expiredInstant),
        new HashSet<>(),
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext,
        table,
        heartbeatClient
    );

    assertTrue(task.awaitInstantCaughtUp(expiredInstant), "Expected null as the instant's heartbeat has expired.");
  }

  /**
   * Test case to cover the scenario where the heartbeat does not exist for the given instant.
   * Validate that awaitInstantCaughtUp returns true when heartbeat does not exist for the given instant.
   */
  @Test
  public void testNoHeartbeat() throws IOException {
    HoodieInstant pendingInstantWithNoHeartbeat = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "002");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/some/path")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .build();
    // Simulate heartbeat exists and expired
    when(table.getConfig()).thenReturn(writeConfig);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/some/path"));
    when(storage.exists(any())).thenReturn(false);

    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        Collections.singletonList(pendingInstantWithNoHeartbeat),
        new HashSet<>(),
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext,
        table,
        heartbeatClient
    );

    assertTrue(task.awaitInstantCaughtUp(pendingInstantWithNoHeartbeat), "Expected null as the instant's heartbeat has expired.");
  }

  static class DummyIndexingCatchupTask extends AbstractIndexingCatchupTask {
    public DummyIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                                    List<HoodieInstant> instantsToIndex,
                                    Set<String> metadataCompletedInstants,
                                    HoodieTableMetaClient metaClient,
                                    HoodieTableMetaClient metadataMetaClient,
                                    TransactionManager transactionManager,
                                    String currentCaughtupInstant,
                                    HoodieEngineContext engineContext,
                                    HoodieTable table,
                                    HoodieHeartbeatClient heartbeatClient) {
      super(metadataWriter, instantsToIndex, metadataCompletedInstants, metaClient, metadataMetaClient, transactionManager, currentCaughtupInstant, engineContext, table, heartbeatClient);
    }

    @Override
    public void run() {
      // no-op, just a test dummy implementation
    }

    @Override
    public void updateIndexForWriteAction(HoodieInstant instant) {
      // no-op, just a test dummy implementation
    }
  }
}
