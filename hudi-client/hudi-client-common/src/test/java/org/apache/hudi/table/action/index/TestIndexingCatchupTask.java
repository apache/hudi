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

import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  @BeforeEach
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Mock out the behavior of the method to mimic a regular successful run
   */
  @Test
  public void testTaskSuccessful() {
    List<HoodieInstant> instants = Collections.singletonList(new HoodieInstant(HoodieInstant.State.REQUESTED, "commit", "001"));
    Set<String> metadataCompletedInstants = new HashSet<>();
    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        instants,
        metadataCompletedInstants,
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext);

    task.run();
    assertEquals("001", task.currentCaughtupInstant);
  }

  /**
   * Instant never gets completed, and we interrupt the task to see if it throws the expected HoodieIndexException.
   */
  @Test
  public void testTaskInterrupted() {
    HoodieInstant neverCompletedInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, "commit", "001");
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

    AbstractIndexingCatchupTask task = new DummyIndexingCatchupTask(
        metadataWriter,
        Collections.singletonList(neverCompletedInstant),
        new HashSet<>(),
        metaClient,
        metadataMetaClient,
        transactionManager,
        "001",
        engineContext);

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

  static class DummyIndexingCatchupTask extends AbstractIndexingCatchupTask {
    public DummyIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                                    List<HoodieInstant> instantsToIndex,
                                    Set<String> metadataCompletedInstants,
                                    HoodieTableMetaClient metaClient,
                                    HoodieTableMetaClient metadataMetaClient,
                                    TransactionManager transactionManager,
                                    String currentCaughtupInstant,
                                    HoodieEngineContext engineContext) {
      super(metadataWriter, instantsToIndex, metadataCompletedInstants, metaClient, metadataMetaClient, transactionManager, currentCaughtupInstant, engineContext);
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
